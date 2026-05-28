//! NFR: workers scale horizontally without per-job coordination.
//!
//! Two `Queue` instances pinned to disjoint partition sets share one Redis.
//! We flood the queue and prove:
//!   1. every job ran exactly once (no duplicate delivery across instances);
//!   2. (ignored/perf) 2 instances clear the flood meaningfully faster than 1.
//!
//! Needs Redis at `JOB_QUEUE_REDIS_URL` (default `redis://127.0.0.1:6379`).
//! Skips with a printed SKIP line when Redis is unreachable, so `cargo test`
//! stays green without it. Start one with:
//!   docker run -d --name jqs-redis -p 6379:6379 redis:7

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use job_queue_system::error::QueueError;
use job_queue_system::models::Job;
use job_queue_system::persistence::{
    JobDispatch, JobState, RedisDispatchConfig, RedisJobDispatch, RedisJobState,
};
use job_queue_system::queue::{Queue, QueueConfig};
use job_queue_system::task::TaskRegistry;

fn redis_url() -> String {
    std::env::var("JOB_QUEUE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

fn redis_up() -> bool {
    redis::Client::open(redis_url())
        .and_then(|c| c.get_connection().map(|_| ()))
        .is_ok()
}

/// Per-payload execution tally. Handlers are `fn(&str)` so the only way to
/// record work is through a static. This file's single non-ignored test owns
/// the map; the ignored perf test doesn't assert on it.
static EXECUTIONS: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();

fn executions() -> &'static Mutex<HashMap<String, usize>> {
    EXECUTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn recording_handler(payload: &str) -> Result<(), QueueError> {
    let mut map = executions().lock().unwrap();
    *map.entry(payload.to_string()).or_insert(0) += 1;
    Ok(())
}

fn registry() -> TaskRegistry {
    let mut r = TaskRegistry::new();
    r.register("scale_job", recording_handler);
    r
}

fn unique_prefix(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("jqsnfr:{label}:{}:{nanos}", std::process::id())
}

/// Build a Queue bound to `assigned` partitions of `partition_count`, sharing
/// `prefix` on Redis. Reaper left off so the exactly-once assertion isn't
/// muddied by redelivery.
fn instance(prefix: &str, partition_count: u32, assigned: Vec<u32>) -> Arc<Queue> {
    let url = redis_url();
    let cfg = RedisDispatchConfig {
        url: url.clone(),
        partition_count,
        key_prefix: prefix.to_string(),
    };
    let dispatch: Arc<dyn JobDispatch> = Arc::new(RedisJobDispatch::new(cfg).expect("dispatch"));
    let state: Arc<dyn JobState> =
        Arc::new(RedisJobState::with_prefix(&url, prefix).expect("state"));
    let qcfg = QueueConfig {
        partition_count,
        assigned_partitions: assigned,
        worker_block_duration: Duration::from_millis(50),
        ..QueueConfig::default()
    };
    Arc::new(Queue::with_config(dispatch, state, registry(), qcfg).expect("queue"))
}

fn enqueue_n(queue: &Queue, n: usize) {
    for i in 0..n {
        let job = Job::with_task_name(
            format!("scale-{i}"),
            "scale_job".into(),
            format!("payload-{i}"),
        );
        queue.enqueue(job).unwrap();
    }
}

/// Poll until pending hits zero or the deadline passes. Returns true if
/// drained.
fn wait_drained(queue: &Queue, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if queue.pending_count().unwrap_or(1) == 0 {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    queue.pending_count().unwrap_or(1) == 0
}

#[test]
fn nfr_two_instances_deliver_each_job_exactly_once() {
    if !redis_up() {
        eprintln!(
            "SKIP nfr_two_instances_deliver_each_job_exactly_once: no Redis at {}",
            redis_url()
        );
        return;
    }
    executions().lock().unwrap().clear();

    const PARTITIONS: u32 = 4;
    const JOBS: usize = 400;
    let prefix = unique_prefix("exactly-once");

    // Two instances, disjoint partition ownership: A owns {0,1}, B owns {2,3}.
    // Either can enqueue (both know partition_count=4).
    let instance_a = instance(&prefix, PARTITIONS, vec![0, 1]);
    let instance_b = instance(&prefix, PARTITIONS, vec![2, 3]);

    enqueue_n(&instance_a, JOBS);

    instance_a.start_workers();
    instance_b.start_workers();

    // Both instances see the same shared streams, so either one's
    // pending_count reflects the whole system.
    let drained = wait_drained(&instance_a, Duration::from_secs(20));

    let map = executions().lock().unwrap();
    let distinct = map.len();
    let duplicates: Vec<_> = map.iter().filter(|&(_, &c)| c != 1).collect();

    assert!(
        drained,
        "queue did not drain within timeout; ran {distinct} jobs"
    );
    assert_eq!(
        distinct, JOBS,
        "expected {JOBS} distinct jobs to run, saw {distinct}"
    );
    assert!(
        duplicates.is_empty(),
        "these jobs ran more than once across instances: {duplicates:?}"
    );

    // Cleanup shared Redis keys.
    let _ = RedisJobState::with_prefix(&redis_url(), &prefix).and_then(|s| s.flush_prefix());
    let cfg = RedisDispatchConfig {
        url: redis_url(),
        partition_count: PARTITIONS,
        key_prefix: prefix.clone(),
    };
    if let Ok(d) = RedisJobDispatch::new(cfg) {
        let _ = d.flush_prefix();
    }
}

#[test]
#[ignore = "throughput NFR; run with `cargo test --release -- --ignored`"]
fn nfr_two_instances_scale_throughput() {
    if !redis_up() {
        eprintln!("SKIP nfr_two_instances_scale_throughput: no Redis");
        return;
    }

    const PARTITIONS: u32 = 8;
    const JOBS: usize = 4000;

    // Single instance owns all 8 partitions.
    let prefix1 = unique_prefix("scale-1x");
    let single = instance(&prefix1, PARTITIONS, (0..PARTITIONS).collect());
    enqueue_n(&single, JOBS);
    let t0 = Instant::now();
    single.start_workers();
    assert!(wait_drained(&single, Duration::from_secs(60)));
    let single_elapsed = t0.elapsed();

    // Two instances split the 8 partitions.
    let prefix2 = unique_prefix("scale-2x");
    let a = instance(&prefix2, PARTITIONS, (0..4).collect());
    let b = instance(&prefix2, PARTITIONS, (4..8).collect());
    enqueue_n(&a, JOBS);
    let t1 = Instant::now();
    a.start_workers();
    b.start_workers();
    assert!(wait_drained(&a, Duration::from_secs(60)));
    let double_elapsed = t1.elapsed();

    let speedup = single_elapsed.as_secs_f64() / double_elapsed.as_secs_f64();
    println!(
        "nfr_horizontal_scaling: 1x={:.3}s 2x={:.3}s speedup={speedup:.2}x",
        single_elapsed.as_secs_f64(),
        double_elapsed.as_secs_f64(),
    );
    assert!(
        speedup >= 1.7,
        "2 instances should clear the flood >=1.7x faster, got {speedup:.2}x"
    );
}
