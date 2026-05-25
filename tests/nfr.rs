//! Non-functional-requirement tests for the extension challenges.
//!
//! These exercise system-level invariants — what *must hold* under
//! concurrent load — rather than per-method behavior, which is covered
//! by unit tests in each module.
//!
//! - Extension #1 (cancellation): cancelled jobs never run side effects;
//!   running-job cancellation finalizes as `Cancelled` in the repo.
//! - Extension #4 (back pressure): under a producer flood, the pending
//!   depth stays bounded by the hard threshold (within the documented
//!   N-1 slack from concurrent producers).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use job_queue_system::error::QueueError;
use job_queue_system::models::{Job, JobStatus};
use job_queue_system::persistence::{InMemoryJobRepository, JobRepository};
use job_queue_system::producer::{JobProducer, Producer};
use job_queue_system::queue::{Queue, QueueConfig};
use job_queue_system::task::TaskRegistry;

// Handlers are `fn(&str)`, not closures, so per-test signal travels
// through a `static`. Cargo runs integration tests in parallel within
// the same process, so each test gets its *own* counter + handler +
// task name to avoid cross-test contamination.
static EXEC_COUNT_FAST: AtomicUsize = AtomicUsize::new(0);
static EXEC_COUNT_SLOW: AtomicUsize = AtomicUsize::new(0);

fn fast_counting_handler(_payload: &str) -> Result<(), QueueError> {
    EXEC_COUNT_FAST.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn slow_counting_handler(_payload: &str) -> Result<(), QueueError> {
    thread::sleep(Duration::from_millis(150));
    EXEC_COUNT_SLOW.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn make_job(task_name: &str, id: &str) -> Job {
    Job::with_task_name(id.into(), task_name.into(), "p".into())
}

#[test]
fn nfr_cancellation_pending_jobs_never_execute() {
    EXEC_COUNT_FAST.store(0, Ordering::SeqCst);

    let repo: Arc<dyn JobRepository> = Arc::new(InMemoryJobRepository::new());
    let mut registry = TaskRegistry::new();
    registry.register("nfr_fast", fast_counting_handler);
    let queue = Arc::new(Queue::new(2, Arc::clone(&repo), registry).unwrap());

    // Workers are constructed but threads aren't spawned until
    // `start_workers()` — enqueue and cancel first so the cancelled
    // jobs leave the in-memory level before any worker can pop them.
    for i in 0..10 {
        queue
            .enqueue(make_job("nfr_fast", &format!("pend-{i}")))
            .unwrap();
    }
    for i in 0..5 {
        queue.cancel(&format!("pend-{i}")).unwrap();
    }

    queue.start_workers();
    thread::sleep(Duration::from_millis(200));

    // Invariant: only the 5 non-cancelled jobs ran.
    assert_eq!(EXEC_COUNT_FAST.load(Ordering::SeqCst), 5);

    // Repo state matches: 5 cancelled + 5 completed, total 10.
    let mut cancelled = 0;
    let mut completed = 0;
    for i in 0..10 {
        let status = repo.find_by_id(&format!("pend-{i}")).unwrap().status;
        match status {
            JobStatus::Cancelled => cancelled += 1,
            JobStatus::Completed => completed += 1,
            other => panic!("unexpected status for pend-{i}: {other:?}"),
        }
    }
    assert_eq!(cancelled, 5);
    assert_eq!(completed, 5);
}

#[test]
fn nfr_cancellation_running_job_finalizes_as_cancelled() {
    EXEC_COUNT_SLOW.store(0, Ordering::SeqCst);

    let repo: Arc<dyn JobRepository> = Arc::new(InMemoryJobRepository::new());
    let mut registry = TaskRegistry::new();
    registry.register("nfr_slow", slow_counting_handler);
    let queue = Arc::new(Queue::new(1, Arc::clone(&repo), registry).unwrap());
    queue.start_workers();

    queue.enqueue(make_job("nfr_slow", "job-running")).unwrap();
    // Let the worker pick up the job and start the 150ms sleep.
    thread::sleep(Duration::from_millis(40));
    queue.cancel("job-running").unwrap();

    // Wait past the slow handler's completion. Sticky-Cancelled must
    // reject the worker's post-handler `Completed` write so the repo
    // still reports Cancelled.
    thread::sleep(Duration::from_millis(250));

    let final_status = repo.find_by_id("job-running").unwrap().status;
    assert_eq!(
        final_status,
        JobStatus::Cancelled,
        "running-job cancel must remain sticky-Cancelled even after the handler returns"
    );
}

#[test]
fn nfr_backpressure_bounds_depth_under_concurrent_flood() {
    // No workers: depth only grows. Concurrent producers race on
    // `pending_count` + `enqueue`, so the hard threshold may be
    // exceeded by up to N-1 (documented in QueueConfig). We assert the
    // bound holds within that slack.
    let producers = 4_usize;
    let per_producer = 50_usize;
    let hard = 10_u64;

    let repo: Arc<dyn JobRepository> = Arc::new(InMemoryJobRepository::new());
    let config = QueueConfig {
        backpressure_soft_threshold: hard,
        backpressure_hard_threshold: hard,
        backpressure_delay: Duration::from_millis(0),
        ..QueueConfig::default()
    };
    let queue = Arc::new(
        Queue::with_config(0, Arc::clone(&repo), TaskRegistry::new(), config).unwrap(),
    );

    let mut handles = Vec::new();
    let rejections = Arc::new(AtomicUsize::new(0));
    let accepts = Arc::new(AtomicUsize::new(0));

    for p in 0..producers {
        let queue = Arc::clone(&queue);
        let rejections = Arc::clone(&rejections);
        let accepts = Arc::clone(&accepts);
        handles.push(thread::spawn(move || {
            let producer = JobProducer::new(queue);
            for i in 0..per_producer {
                let job = make_job("nfr_bp", &format!("p{p}-j{i}"));
                match producer.produce(job) {
                    Ok(()) => {
                        accepts.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(QueueError::QueueFull { .. }) => {
                        rejections.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(other) => panic!("unexpected producer error: {other:?}"),
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let depth = queue.len() as u64;
    let max_allowed = hard + (producers as u64 - 1);
    assert!(
        depth <= max_allowed,
        "depth {depth} exceeded hard ({hard}) + slack ({}) = {max_allowed}",
        producers - 1,
    );
    assert_eq!(
        accepts.load(Ordering::SeqCst) + rejections.load(Ordering::SeqCst),
        producers * per_producer,
        "every produce attempt must resolve to either accept or QueueFull",
    );
    assert!(
        rejections.load(Ordering::SeqCst) > 0,
        "flood far above the hard threshold must trigger at least one rejection",
    );
}
