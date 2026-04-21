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
