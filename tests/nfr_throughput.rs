//! Throughput NFR: ≥1,000 job submissions per second through `JobProducer`
//! against `SqliteJobRepository` (the production backend).
//!
//! Marked `#[ignore]` because:
//! - The measurement is meaningful only in `--release`.
//! - It depends on disk fsync rate, so it's environment-sensitive.
//!
//! Run with:
//!   cargo test --release --test nfr_throughput -- --ignored --nocapture
//!
//! If this fails by a small margin, the first thing to try is adding
//! `PRAGMA synchronous=NORMAL` alongside the existing WAL pragma in
//! `src/persistence/sqlite.rs` — WAL+NORMAL is durable enough for a job
//! queue (no torn writes, only a tiny window of "committed but not yet
//! persisted" on power loss) and typically multiplies fsync throughput.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use job_queue_system::models::Job;
use job_queue_system::persistence::{JobRepository, SqliteJobRepository};
use job_queue_system::producer::{JobProducer, Producer};
use job_queue_system::queue::{Queue, QueueConfig};
use job_queue_system::task::TaskRegistry;

/// Owns a SQLite file path and removes it (plus its WAL/SHM siblings) on
/// drop, so a failed assertion doesn't leak files into the temp dir.
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
    // 8 producers × 1500 jobs = 12,000 submissions. At the 1k/s floor that
    // finishes in ~12s; well under the default test timeout.
    const PRODUCERS: usize = 8;
    const PER_PRODUCER: usize = 1500;
    const TOTAL: usize = PRODUCERS * PER_PRODUCER;

    let db = TempDb::new("throughput");
    let repo: Arc<dyn JobRepository> =
        Arc::new(SqliteJobRepository::new(db.as_str()).expect("open sqlite"));

    // Thresholds well above TOTAL so back-pressure never kicks in — this
    // bench isolates raw submission cost, not throttle behavior.
    let ceiling = (TOTAL as u64) * 2;
    let config = QueueConfig {
        backpressure_soft_threshold: ceiling,
        backpressure_hard_threshold: ceiling,
        backpressure_delay: Duration::from_millis(0),
        ..QueueConfig::default()
    };
    let queue = Arc::new(
        Queue::with_config(0, Arc::clone(&repo), TaskRegistry::new(), config)
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

    // Sanity: every submission must have landed in the repo.
    assert_eq!(
        repo.pending_count().expect("pending_count"),
        TOTAL as u64,
        "not all submissions persisted",
    );

    assert!(
        rate >= 1000.0,
        "submission rate {rate:.0} jobs/s is below the NFR floor of 1000 jobs/s",
    );
}
