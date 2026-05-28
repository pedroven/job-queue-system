//! Throughput NFR: ≥1,000 job submissions per second through `JobProducer`
//! against `SqliteJobDispatch` + `SqliteJobState`.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use job_queue_system::models::Job;
use job_queue_system::persistence::{JobDispatch, JobState, SqliteJobDispatch, SqliteJobState};
use job_queue_system::producer::{JobProducer, Producer};
use job_queue_system::queue::{Queue, QueueConfig};
use job_queue_system::task::TaskRegistry;

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new(label: &str) -> Self {
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("jqs-{label}-{pid}-{nanos}.db"));
        TempDb { path }
    }

    fn as_str(&self) -> &str {
        self.path.to_str().expect("temp path is valid UTF-8")
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let mut wal = self.path.clone();
        wal.set_extension("db-wal");
        let _ = std::fs::remove_file(&wal);
        let mut shm = self.path.clone();
        shm.set_extension("db-shm");
        let _ = std::fs::remove_file(&shm);
    }
}

#[test]
#[ignore = "throughput NFR; run with `cargo test --release -- --ignored`"]
fn nfr_submissions_per_second_at_least_1000() {
    const PRODUCERS: usize = 8;
    const PER_PRODUCER: usize = 1500;
    const TOTAL: usize = PRODUCERS * PER_PRODUCER;

    let db = TempDb::new("throughput");
    let dispatch: Arc<dyn JobDispatch> =
        Arc::new(SqliteJobDispatch::new(db.as_str(), 1).expect("open sqlite dispatch"));
    let state: Arc<dyn JobState> =
        Arc::new(SqliteJobState::new(db.as_str()).expect("open sqlite state"));

    let ceiling = (TOTAL as u64) * 2;
    let config = QueueConfig {
        backpressure_soft_threshold: ceiling,
        backpressure_hard_threshold: ceiling,
        backpressure_delay: Duration::from_millis(0),
        assigned_partitions: vec![],
        ..QueueConfig::default()
    };
    let queue = Arc::new(
        Queue::with_config(dispatch, Arc::clone(&state), TaskRegistry::new(), config)
            .expect("queue init"),
    );

    let start = Instant::now();
    let mut handles = Vec::with_capacity(PRODUCERS);
    for p in 0..PRODUCERS {
        let queue = Arc::clone(&queue);
        handles.push(thread::spawn(move || {
            let producer = JobProducer::new(queue);
            for i in 0..PER_PRODUCER {
                producer
                    .produce(Job::with_task_name(
                        format!("p{p}-j{i}"),
                        "nfr_bench".into(),
                        "p".into(),
                    ))
                    .expect("produce");
            }
        }));
    }
    for h in handles {
        h.join().expect("producer join");
    }
    let elapsed = start.elapsed();

    let rate = TOTAL as f64 / elapsed.as_secs_f64();
    println!(
        "nfr_throughput: producers={PRODUCERS} per_producer={PER_PRODUCER} \
         total={TOTAL} elapsed={:.3}s rate={rate:.0} jobs/s",
        elapsed.as_secs_f64(),
    );

    assert_eq!(
        queue.pending_count().expect("pending_count"),
        TOTAL as u64,
        "not all submissions persisted",
    );

    assert!(
        rate >= 1000.0,
        "submission rate {rate:.0} jobs/s is below the NFR floor of 1000 jobs/s",
    );
}
