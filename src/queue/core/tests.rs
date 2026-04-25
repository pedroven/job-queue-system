use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, UNIX_EPOCH};

use super::Queue;
use crate::consumer::{self, Consumer};
use crate::error::QueueError;
use crate::models::{self, Job, JobPriority, Worker, WorkerStatus, testing::make_test_job};
use crate::persistence::{InMemoryJobRepository, JobRepository, SqliteJobRepository};
use crate::queue::levels::JobQueues;
use crate::queue::worker;
use crate::task::TaskRegistry;

fn default_registry() -> TaskRegistry {
    let mut registry = TaskRegistry::new();
    registry.register("default", |_| Ok(()));
    registry
}

fn create_queue(num_workers: usize) -> Queue {
    Queue::new(
        num_workers,
        Arc::new(InMemoryJobRepository::new()),
        default_registry(),
    )
    .unwrap()
}

fn create_queue_with_repo(repo: Arc<dyn JobRepository>) -> Arc<Queue> {
    Arc::new(Queue::new(0, repo, default_registry()).unwrap())
}

struct FailNTimesConsumer {
    remaining_failures: Mutex<u32>,
}

impl FailNTimesConsumer {
    fn new(failures: u32) -> Self {
        Self {
            remaining_failures: Mutex::new(failures),
        }
    }
}

impl Consumer for FailNTimesConsumer {
    fn consume(&self, _job: &Job) -> Result<(), QueueError> {
        let mut remaining = self.remaining_failures.lock().unwrap();
        if *remaining > 0 {
            *remaining -= 1;
            Err(QueueError::JobFailed("simulated failure".to_string()))
        } else {
            Ok(())
        }
    }
}

#[test]
fn test_queue_new_creates_correct_number_of_workers() {
    let queue = create_queue(4);
    assert_eq!(queue.workers.len(), 4);
}

#[test]
fn test_queue_new_workers_start_idle() {
    let queue = create_queue(2);
    for worker in &queue.workers {
        let w = worker.lock().unwrap();
        assert!(matches!(w.status, WorkerStatus::Idle));
        assert!(w.current_job_id.is_none());
    }
}

#[test]
fn test_queue_new_zero_workers() {
    let queue = create_queue(0);
    assert_eq!(queue.workers.len(), 0);
}

#[test]
fn test_enqueue_adds_job_to_queue() {
    let queue = create_queue(0);
    let job = make_test_job("job-1", "test payload");
    queue.enqueue(job).unwrap();

    let (lock, _) = &*queue.jobs;
    let jobs = lock.lock().unwrap();
    assert_eq!(jobs.len(), 1);
    let normal = jobs.queue_for(JobPriority::Normal).unwrap();
    assert_eq!(normal[0].id, "job-1");
}

#[test]
fn test_enqueue_preserves_fifo_order() {
    let queue = create_queue(0);
    queue.enqueue(make_test_job("job-1", "first")).unwrap();
    queue.enqueue(make_test_job("job-2", "second")).unwrap();
    queue.enqueue(make_test_job("job-3", "third")).unwrap();

    let (lock, _) = &*queue.jobs;
    let jobs = lock.lock().unwrap();
    assert_eq!(jobs.len(), 3);
    let normal = jobs.queue_for(JobPriority::Normal).unwrap();
    assert_eq!(normal[0].id, "job-1");
    assert_eq!(normal[1].id, "job-2");
    assert_eq!(normal[2].id, "job-3");
}

#[test]
fn test_wait_for_job_returns_front_job() {
    let jobs = Arc::new((Mutex::new(JobQueues::default()), Condvar::new()));
    {
        let (lock, _) = &*jobs;
        let mut q = lock.lock().unwrap();
        q.push_back(make_test_job("job-1", "payload"));
    }
    let result = Queue::wait_for_job(&jobs);
    assert!(result.is_some());
    assert_eq!(result.unwrap().id, "job-1");
}

#[test]
fn test_enqueue_priority_drains_before_normal() {
    let queue = create_queue(0);
    queue.enqueue(make_test_job("n-1", "normal")).unwrap();
    queue
        .enqueue_priority(make_test_job("p-1", "priority"))
        .unwrap();
    queue.enqueue(make_test_job("n-2", "normal")).unwrap();

    let first = Queue::wait_for_job(&queue.jobs).unwrap();
    assert_eq!(first.id, "p-1");
    let second = Queue::wait_for_job(&queue.jobs).unwrap();
    assert_eq!(second.id, "n-1");
    let third = Queue::wait_for_job(&queue.jobs).unwrap();
    assert_eq!(third.id, "n-2");
}

#[test]
fn test_enqueue_priority_persists_with_high_priority() {
    let repo = Arc::new(InMemoryJobRepository::new());
    let queue = Queue::new(0, repo.clone(), default_registry()).unwrap();
    queue
        .enqueue_priority(make_test_job("p-1", "payload"))
        .unwrap();

    let persisted = repo.find_by_id("p-1").unwrap();
    assert_eq!(persisted.priority, models::JobPriority::High);
}

#[test]
fn test_restart_reloads_priority_first_then_fifo() {
    // SqliteJobRepository enforces `ORDER BY priority DESC, created_at ASC`
    // on recovery, and Queue::new routes each restored job into the matching
    // level. The combination must yield: all High jobs first (in insertion
    // order), then all Normal jobs (in insertion order).
    let repo = Arc::new(SqliteJobRepository::new(":memory:").unwrap());

    let mut n1 = make_test_job("n-1", "normal-first");
    n1.created_at = UNIX_EPOCH + Duration::from_secs(1);
    repo.save(&n1).unwrap();

    let mut p1 = make_test_job("p-1", "priority-first");
    p1.priority = models::JobPriority::High;
    p1.created_at = UNIX_EPOCH + Duration::from_secs(2);
    repo.save(&p1).unwrap();

    let mut n2 = make_test_job("n-2", "normal-second");
    n2.created_at = UNIX_EPOCH + Duration::from_secs(3);
    repo.save(&n2).unwrap();

    let mut p2 = make_test_job("p-2", "priority-second");
    p2.priority = models::JobPriority::High;
    p2.created_at = UNIX_EPOCH + Duration::from_secs(4);
    repo.save(&p2).unwrap();

    let queue = Queue::new(0, repo.clone(), default_registry()).unwrap();

    let drained: Vec<String> = (0..4)
        .map(|_| Queue::wait_for_job(&queue.jobs).unwrap().id)
        .collect();
    assert_eq!(drained, vec!["p-1", "p-2", "n-1", "n-2"]);
}

#[test]
fn test_process_job_updates_worker_status() {
    let worker_mutex = Mutex::new(Worker {
        id: "worker-0".to_string(),
        status: WorkerStatus::Idle,
        current_job_id: None,
    });
    let consumer = consumer::JobConsumer;
    let job = make_test_job("job-1", "payload");

    worker::process_job(&worker_mutex, &consumer, job, &Arc::new(create_queue(0)));

    let w = worker_mutex.lock().unwrap();
    assert!(matches!(w.status, WorkerStatus::Idle));
    assert!(w.current_job_id.is_none());
}

#[test]
fn test_workers_consume_enqueued_jobs() {
    let queue = Arc::new(create_queue(2));
    queue.start_workers();

    queue.enqueue(make_test_job("job-1", "hello")).unwrap();
    queue.enqueue(make_test_job("job-2", "world")).unwrap();

    thread::sleep(Duration::from_millis(100));

    let (lock, _) = &*queue.jobs;
    let jobs = lock.lock().unwrap();
    assert_eq!(jobs.len(), 0);
}

#[test]
fn test_multiple_jobs_processed_concurrently() {
    let queue = Arc::new(create_queue(4));
    queue.start_workers();

    for i in 0..10 {
        queue
            .enqueue(make_test_job(
                &format!("job-{}", i),
                &format!("payload-{}", i),
            ))
            .unwrap();
    }

    thread::sleep(Duration::from_millis(200));

    let (lock, _) = &*queue.jobs;
    let jobs = lock.lock().unwrap();
    assert_eq!(jobs.len(), 0);
}

#[test]
fn test_job_succeeds_on_first_attempt() {
    let repo = Arc::new(InMemoryJobRepository::new());
    let queue = create_queue_with_repo(repo.clone());
    let job = make_test_job("job-1", "payload");
    repo.save(&job).unwrap();

    let consumer = consumer::JobConsumer;
    worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

    let pending = repo.find_all_pending().unwrap();
    assert_eq!(pending.len(), 0);

    let dl = repo.find_all_dead_letter().unwrap();
    assert_eq!(dl.len(), 0);
}

#[test]
fn test_job_succeeds_after_retries() {
    let repo = Arc::new(InMemoryJobRepository::new());
    let queue = create_queue_with_repo(repo.clone());
    let job = make_test_job("job-1", "payload");
    repo.save(&job).unwrap();

    let consumer = FailNTimesConsumer::new(2);
    worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

    let pending = repo.find_all_pending().unwrap();
    assert_eq!(pending.len(), 0);

    let dl = repo.find_all_dead_letter().unwrap();
    assert_eq!(dl.len(), 0);
}

#[test]
fn test_job_exhausts_retries_moves_to_dlq() {
    let repo = Arc::new(InMemoryJobRepository::new());
    let queue = create_queue_with_repo(repo.clone());
    let job = make_test_job("job-1", "payload");
    repo.save(&job).unwrap();

    let consumer = FailNTimesConsumer::new(5);
    worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

    let dl = repo.find_all_dead_letter().unwrap();
    assert_eq!(dl.len(), 1);
    assert_eq!(dl[0].original_job_id, "job-1");
    assert_eq!(dl[0].error, "job failed: simulated failure");

    let persisted = repo.find_by_id("job-1").unwrap();
    assert_eq!(persisted.retry_count, 3);
    assert_eq!(persisted.status, models::JobStatus::Failed);
}

#[test]
fn test_retry_count_is_persisted() {
    let repo = Arc::new(InMemoryJobRepository::new());
    let queue = create_queue_with_repo(repo.clone());
    let job = make_test_job("job-1", "payload");
    repo.save(&job).unwrap();

    let consumer = FailNTimesConsumer::new(2);
    worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

    let persisted = repo.find_by_id("job-1").unwrap();
    assert_eq!(persisted.retry_count, 2);
    assert_eq!(persisted.status, models::JobStatus::Completed);

    let dl = repo.find_all_dead_letter().unwrap();
    assert_eq!(dl.len(), 0);
}

#[test]
fn test_cancel_pending_marks_status_and_removes_from_queue() {
    let repo = Arc::new(InMemoryJobRepository::new());
    let queue = Queue::new(0, repo.clone(), default_registry()).unwrap();
    queue.enqueue(make_test_job("job-1", "payload")).unwrap();

    queue.cancel("job-1").unwrap();

    let (lock, _) = &*queue.jobs;
    assert_eq!(lock.lock().unwrap().len(), 0);
    assert_eq!(
        repo.find_by_id("job-1").unwrap().status,
        models::JobStatus::Cancelled
    );
}

#[test]
fn test_cancel_running_marks_status() {
    let repo = Arc::new(InMemoryJobRepository::new());
    let queue = Queue::new(0, repo.clone(), default_registry()).unwrap();
    let mut job = make_test_job("job-1", "payload");
    job.status = models::JobStatus::Running;
    repo.save(&job).unwrap();

    queue.cancel("job-1").unwrap();

    assert_eq!(
        repo.find_by_id("job-1").unwrap().status,
        models::JobStatus::Cancelled
    );
}

#[test]
fn test_cancel_terminal_returns_cannot_cancel() {
    let repo = Arc::new(InMemoryJobRepository::new());
    let queue = Queue::new(0, repo.clone(), default_registry()).unwrap();
    repo.save(&make_test_job("job-1", "payload")).unwrap();
    repo.update_status("job-1", models::JobStatus::Completed)
        .unwrap();

    let err = queue.cancel("job-1").unwrap_err();
    assert!(matches!(err, QueueError::CannotCancel { .. }));
}

#[test]
fn test_cancel_unknown_job_returns_not_found() {
    let queue = create_queue(0);
    let err = queue.cancel("nope").unwrap_err();
    assert!(matches!(err, QueueError::NotFound(_)));
}

#[test]
fn test_handle_job_tries_bails_after_cancel_between_attempts() {
    // A consumer that always fails. After the first attempt, the test cancels
    // the job; the loop must skip remaining attempts (no DLQ entry, retry
    // count stays at 1).
    struct CancelOnFirstFailure {
        repo: Arc<dyn JobRepository>,
        cancelled: Mutex<bool>,
    }
    impl Consumer for CancelOnFirstFailure {
        fn consume(&self, job: &Job) -> Result<(), QueueError> {
            let mut done = self.cancelled.lock().unwrap();
            if !*done {
                *done = true;
                self.repo
                    .update_status(&job.id, models::JobStatus::Cancelled)
                    .unwrap();
            }
            Err(QueueError::JobFailed("simulated".to_string()))
        }
    }

    let repo: Arc<dyn JobRepository> = Arc::new(InMemoryJobRepository::new());
    let queue = create_queue_with_repo(repo.clone());
    let mut job = make_test_job("job-1", "payload");
    job.max_attempts = 10;
    repo.save(&job).unwrap();

    let consumer = CancelOnFirstFailure {
        repo: repo.clone(),
        cancelled: Mutex::new(false),
    };
    worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

    let persisted = repo.find_by_id("job-1").unwrap();
    assert_eq!(persisted.status, models::JobStatus::Cancelled);
    assert_eq!(persisted.retry_count, 1, "should not retry past cancel");
    assert_eq!(repo.find_all_dead_letter().unwrap().len(), 0);
}

#[test]
fn test_metrics_reporter_handle_stop_terminates_thread() {
    let queue = Arc::new(create_queue(0));
    // Long interval — if stop() didn't work, this test would hang well past
    // its time budget waiting for the loop to wake on its own.
    let handle = queue.start_metrics_reporter(Duration::from_secs(60));
    let start = std::time::Instant::now();
    handle.stop();
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "stop() must wake the reporter via cvar, not wait out the interval",
    );
}

#[test]
fn test_process_job_skips_when_already_cancelled() {
    // Pre-marks the job Cancelled before process_job runs — the easy case
    // (no race). The narrow window where cancel() arrives between worker.rs's
    // find_by_id and update_status(Running) is not covered here; the
    // repository's sticky-Cancelled guard is what protects that race.
    let repo = Arc::new(InMemoryJobRepository::new());
    let queue = create_queue_with_repo(repo.clone());
    let job = make_test_job("job-1", "payload");
    repo.save(&job).unwrap();
    repo.update_status("job-1", models::JobStatus::Cancelled)
        .unwrap();

    let worker_mutex = Mutex::new(Worker {
        id: "worker-0".to_string(),
        status: WorkerStatus::Idle,
        current_job_id: None,
    });
    let consumer = consumer::JobConsumer;
    worker::process_job(&worker_mutex, &consumer, job, &queue);

    assert_eq!(
        repo.find_by_id("job-1").unwrap().status,
        models::JobStatus::Cancelled
    );
}
