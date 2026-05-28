//! Non-functional-requirement tests for the extension challenges.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use job_queue_system::error::QueueError;
use job_queue_system::models::{Job, JobStatus};
use job_queue_system::persistence::{InMemoryJobDispatch, InMemoryJobState, JobDispatch, JobState};
use job_queue_system::producer::{JobProducer, Producer};
use job_queue_system::queue::{Queue, QueueConfig};
use job_queue_system::task::TaskRegistry;

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

fn config(num_workers: usize) -> QueueConfig {
    let pc = num_workers.max(1) as u32;
    QueueConfig {
        partition_count: pc,
        assigned_partitions: if num_workers == 0 {
            vec![]
        } else {
            (0..pc).collect()
        },
        ..QueueConfig::default()
    }
}

fn build_queue(num_workers: usize, registry: TaskRegistry) -> (Arc<Queue>, Arc<dyn JobState>) {
    let cfg = config(num_workers);
    let dispatch: Arc<dyn JobDispatch> = Arc::new(InMemoryJobDispatch::new(cfg.partition_count));
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    (
        Arc::new(Queue::with_config(dispatch, Arc::clone(&state), registry, cfg).unwrap()),
        state,
    )
}

#[test]
fn nfr_cancellation_pending_jobs_never_execute() {
    EXEC_COUNT_FAST.store(0, Ordering::SeqCst);

    let mut registry = TaskRegistry::new();
    registry.register("nfr_fast", fast_counting_handler);
    let (queue, state) = build_queue(2, registry);

    // Enqueue + cancel before workers start, so cancelled jobs never reach
    // a consumer. The worker loop's cancel-check belt-and-braces is exercised
    // by the running-job test below.
    for i in 0..10 {
        queue
            .enqueue(make_job("nfr_fast", &format!("pend-{i}")))
            .unwrap();
    }
    for i in 0..5 {
        queue.cancel(&format!("pend-{i}")).unwrap();
    }

    queue.start_workers();
    thread::sleep(Duration::from_millis(500));

    assert_eq!(EXEC_COUNT_FAST.load(Ordering::SeqCst), 5);

    let mut cancelled = 0;
    let mut completed = 0;
    for i in 0..10 {
        let status = state.find_by_id(&format!("pend-{i}")).unwrap().status;
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

    let mut registry = TaskRegistry::new();
    registry.register("nfr_slow", slow_counting_handler);
    let (queue, state) = build_queue(1, registry);
    queue.start_workers();

    queue.enqueue(make_job("nfr_slow", "job-running")).unwrap();
    thread::sleep(Duration::from_millis(150));
    queue.cancel("job-running").unwrap();

    thread::sleep(Duration::from_millis(400));

    let final_status = state.find_by_id("job-running").unwrap().status;
    assert_eq!(
        final_status,
        JobStatus::Cancelled,
        "running-job cancel must remain sticky-Cancelled even after the handler returns"
    );
}

#[test]
fn nfr_backpressure_bounds_depth_under_concurrent_flood() {
    let producers = 4_usize;
    let per_producer = 50_usize;
    let hard = 10_u64;

    let cfg = QueueConfig {
        backpressure_soft_threshold: hard,
        backpressure_hard_threshold: hard,
        backpressure_delay: Duration::from_millis(0),
        partition_count: 1,
        assigned_partitions: vec![],
        ..QueueConfig::default()
    };
    let dispatch: Arc<dyn JobDispatch> = Arc::new(InMemoryJobDispatch::new(1));
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let queue = Arc::new(Queue::with_config(dispatch, state, TaskRegistry::new(), cfg).unwrap());

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

    let depth = queue.pending_count().unwrap();
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
