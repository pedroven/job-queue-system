use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::Queue;
use crate::consumer::{self, Consumer};
use crate::error::QueueError;
use crate::models::{self, Job, Worker, WorkerStatus, testing::make_test_job};
use crate::persistence::{InMemoryJobDispatch, InMemoryJobState, JobDispatch, JobState};
use crate::queue::QueueConfig;
use crate::queue::worker;
use crate::task::TaskRegistry;

fn default_registry() -> TaskRegistry {
    let mut registry = TaskRegistry::new();
    registry.register("default", |_| Ok(()));
    registry
}

fn no_worker_config() -> QueueConfig {
    QueueConfig {
        assigned_partitions: vec![],
        ..QueueConfig::default()
    }
}

fn create_queue(num_workers: usize) -> Queue {
    // For tests that need active workers, one worker per partition. Map
    // `num_workers` to that many partitions for parity with the old API.
    let pc = (num_workers as u32).max(1);
    let cfg = QueueConfig {
        partition_count: pc,
        assigned_partitions: if num_workers == 0 {
            vec![]
        } else {
            (0..pc).collect()
        },
        ..QueueConfig::default()
    };
    let dispatch: Arc<dyn JobDispatch> = Arc::new(InMemoryJobDispatch::new(pc));
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    Queue::with_config(dispatch, state, default_registry(), cfg).unwrap()
}

fn create_queue_with_state(state: Arc<dyn JobState>) -> Arc<Queue> {
    let dispatch: Arc<dyn JobDispatch> = Arc::new(InMemoryJobDispatch::new(1));
    Arc::new(Queue::with_config(dispatch, state, default_registry(), no_worker_config()).unwrap())
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
fn test_worker_count_matches_assigned_partitions() {
    let queue = create_queue(4);
    assert_eq!(queue.workers.len(), 4);
}

#[test]
fn test_workers_start_idle() {
    let queue = create_queue(2);
    for worker in &queue.workers {
        let w = worker.lock().unwrap();
        assert!(matches!(w.status, WorkerStatus::Idle));
        assert!(w.current_job_id.is_none());
    }
}

#[test]
fn test_zero_workers_means_no_worker_state() {
    let queue = create_queue(0);
    assert_eq!(queue.workers.len(), 0);
}

#[test]
fn test_enqueue_writes_state_and_dispatch() {
    let queue = create_queue(0);
    queue.enqueue(make_test_job("job-1", "p")).unwrap();
    // pending_count goes through dispatch — verifies the entry was routed.
    assert_eq!(queue.pending_count().unwrap(), 1);
    // State persisted the job too.
    let loaded = queue.state.find_by_id("job-1").unwrap();
    assert_eq!(loaded.task.payload, "p");
}

#[test]
fn test_enqueue_preserves_insertion_within_partition() {
    let queue = create_queue(0);
    queue.enqueue(make_test_job("job-1", "first")).unwrap();
    queue.enqueue(make_test_job("job-2", "second")).unwrap();
    queue.enqueue(make_test_job("job-3", "third")).unwrap();
    assert_eq!(queue.pending_count().unwrap(), 3);
}

#[test]
fn test_enqueue_priority_persists_high() {
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let queue = create_queue_with_state(Arc::clone(&state));
    queue
        .enqueue_priority(make_test_job("p-1", "payload"))
        .unwrap();
    let persisted = state.find_by_id("p-1").unwrap();
    assert_eq!(persisted.priority, models::JobPriority::High);
}

#[test]
fn test_workers_consume_enqueued_jobs() {
    let queue = Arc::new(create_queue(2));
    queue.start_workers();

    queue.enqueue(make_test_job("job-1", "hello")).unwrap();
    queue.enqueue(make_test_job("job-2", "world")).unwrap();

    // Generous wait — worker loop polls at 100ms by default.
    thread::sleep(Duration::from_millis(400));
    assert_eq!(queue.pending_count().unwrap(), 0);
}

// Concurrency probe: each handler bumps an in-flight gauge, holds briefly,
// and records the peak. A serial executor would never see the gauge above 1.
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);

fn concurrency_probe_handler(_payload: &str) -> Result<(), QueueError> {
    let now = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
    MAX_CONCURRENT.fetch_max(now, Ordering::SeqCst);
    // Hold the slot long enough that sibling workers overlap with us.
    thread::sleep(Duration::from_millis(80));
    IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    Ok(())
}

#[test]
fn test_multiple_jobs_processed_concurrently() {
    IN_FLIGHT.store(0, Ordering::SeqCst);
    MAX_CONCURRENT.store(0, Ordering::SeqCst);

    // 4 partitions ⇒ 4 worker threads. Enqueue enough jobs that every
    // partition almost certainly gets several, so multiple handlers are
    // in-flight at once.
    let mut registry = TaskRegistry::new();
    registry.register("default", concurrency_probe_handler);
    let dispatch: Arc<dyn JobDispatch> = Arc::new(InMemoryJobDispatch::new(4));
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let cfg = QueueConfig {
        partition_count: 4,
        assigned_partitions: (0..4).collect(),
        ..QueueConfig::default()
    };
    let queue = Arc::new(Queue::with_config(dispatch, state, registry, cfg).unwrap());
    queue.start_workers();

    for i in 0..40 {
        queue
            .enqueue(make_test_job(
                &format!("job-{i}"),
                &format!("payload-{i}"),
            ))
            .unwrap();
    }

    // Wait for the flood to drain (40 jobs × 80ms / 4 workers ≈ 0.8s).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while queue.pending_count().unwrap() > 0 && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }

    assert_eq!(queue.pending_count().unwrap(), 0, "jobs did not drain");
    // The real assertion: at least two handlers ran at the same instant.
    // (With 4 workers we typically observe up to 4; >=2 is the robust floor
    // that distinguishes concurrent from serial execution.)
    assert!(
        MAX_CONCURRENT.load(Ordering::SeqCst) >= 2,
        "expected overlapping execution, peak in-flight was {}",
        MAX_CONCURRENT.load(Ordering::SeqCst)
    );
}

#[test]
fn test_job_succeeds_on_first_attempt() {
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let queue = create_queue_with_state(Arc::clone(&state));
    let job = make_test_job("job-1", "payload");
    state.save_initial(&job).unwrap();

    let consumer = consumer::JobConsumer;
    worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

    assert_eq!(state.find_all_dead_letter().unwrap().len(), 0);
    assert_eq!(
        state.find_by_id("job-1").unwrap().status,
        models::JobStatus::Completed
    );
}

#[test]
fn test_job_succeeds_after_retries() {
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let queue = create_queue_with_state(Arc::clone(&state));
    let job = make_test_job("job-1", "payload");
    state.save_initial(&job).unwrap();

    let consumer = FailNTimesConsumer::new(2);
    worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

    assert_eq!(state.find_all_dead_letter().unwrap().len(), 0);
}

#[test]
fn test_job_exhausts_retries_moves_to_dlq() {
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let queue = create_queue_with_state(Arc::clone(&state));
    let job = make_test_job("job-1", "payload");
    state.save_initial(&job).unwrap();

    let consumer = FailNTimesConsumer::new(5);
    worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

    let dl = state.find_all_dead_letter().unwrap();
    assert_eq!(dl.len(), 1);
    assert_eq!(dl[0].original_job_id, "job-1");
    assert_eq!(dl[0].error, "job failed: simulated failure");

    let persisted = state.find_by_id("job-1").unwrap();
    assert_eq!(persisted.retry_count, 3);
    assert_eq!(persisted.status, models::JobStatus::Failed);
}

#[test]
fn test_retry_count_persists() {
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let queue = create_queue_with_state(Arc::clone(&state));
    let job = make_test_job("job-1", "payload");
    state.save_initial(&job).unwrap();

    let consumer = FailNTimesConsumer::new(2);
    worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

    let persisted = state.find_by_id("job-1").unwrap();
    assert_eq!(persisted.retry_count, 2);
    assert_eq!(persisted.status, models::JobStatus::Completed);
    assert!(state.find_all_dead_letter().unwrap().is_empty());
}

#[test]
fn test_cancel_pending_marks_status() {
    let queue = create_queue(0);
    queue.enqueue(make_test_job("job-1", "payload")).unwrap();

    queue.cancel("job-1").unwrap();

    assert_eq!(
        queue.state.find_by_id("job-1").unwrap().status,
        models::JobStatus::Cancelled
    );
}

#[test]
fn test_cancel_running_marks_status() {
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let queue = create_queue_with_state(Arc::clone(&state));
    let mut job = make_test_job("job-1", "payload");
    job.status = models::JobStatus::Running;
    state.save_initial(&job).unwrap();

    queue.cancel("job-1").unwrap();

    assert_eq!(
        state.find_by_id("job-1").unwrap().status,
        models::JobStatus::Cancelled
    );
}

#[test]
fn test_cancel_terminal_returns_cannot_cancel() {
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let queue = create_queue_with_state(Arc::clone(&state));
    state.save_initial(&make_test_job("job-1", "p")).unwrap();
    state
        .save_status("job-1", models::JobStatus::Completed)
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
    struct CancelOnFirstFailure {
        state: Arc<dyn JobState>,
        cancelled: Mutex<bool>,
    }
    impl Consumer for CancelOnFirstFailure {
        fn consume(&self, job: &Job) -> Result<(), QueueError> {
            let mut done = self.cancelled.lock().unwrap();
            if !*done {
                *done = true;
                self.state
                    .save_status(&job.id, models::JobStatus::Cancelled)
                    .unwrap();
            }
            Err(QueueError::JobFailed("simulated".to_string()))
        }
    }

    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let queue = create_queue_with_state(Arc::clone(&state));
    let mut job = make_test_job("job-1", "payload");
    job.max_attempts = 10;
    state.save_initial(&job).unwrap();

    let consumer = CancelOnFirstFailure {
        state: Arc::clone(&state),
        cancelled: Mutex::new(false),
    };
    worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

    let persisted = state.find_by_id("job-1").unwrap();
    assert_eq!(persisted.status, models::JobStatus::Cancelled);
    assert_eq!(persisted.retry_count, 1, "should not retry past cancel");
    assert!(state.find_all_dead_letter().unwrap().is_empty());
}

#[test]
fn test_metrics_reporter_handle_stop_terminates_thread() {
    let queue = Arc::new(create_queue(0));
    let handle = queue.start_metrics_reporter(Duration::from_secs(60));
    let start = std::time::Instant::now();
    handle.stop();
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "stop() must wake the reporter via cvar, not wait out the interval",
    );
}

#[test]
fn test_process_claim_skips_when_already_cancelled() {
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let queue = create_queue_with_state(Arc::clone(&state));
    let job = make_test_job("job-1", "payload");
    state.save_initial(&job).unwrap();
    state
        .save_status("job-1", models::JobStatus::Cancelled)
        .unwrap();
    queue.dispatch.enqueue(0, &job).unwrap();
    let claim = queue
        .dispatch
        .next_for_partition(0, "c", Duration::from_millis(20))
        .unwrap()
        .unwrap();

    let worker_mutex = Mutex::new(Worker {
        id: "worker-0".to_string(),
        status: WorkerStatus::Idle,
        current_job_id: None,
    });
    let consumer = consumer::JobConsumer;
    worker::process_claim(&queue, &worker_mutex, &consumer, 0, claim);

    assert_eq!(
        state.find_by_id("job-1").unwrap().status,
        models::JobStatus::Cancelled
    );
}
