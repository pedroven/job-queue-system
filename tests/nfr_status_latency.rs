//! Status-query latency NFR: `JobState::find_by_id` p99 < 100ms.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use job_queue_system::models::Job;
use job_queue_system::persistence::{JobState, SqliteJobState};

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
#[ignore = "latency NFR; run with `cargo test --release -- --ignored`"]
fn nfr_status_query_p99_under_100ms() {
    const SEED: usize = 100_000;
    const SAMPLES: usize = 5_000;

    let db = TempDb::new("latency");
    let state = SqliteJobState::new(db.as_str()).expect("open sqlite");
    let state: Arc<dyn JobState> = Arc::new(state);

    for i in 0..SEED {
        state
            .save_initial(&Job::with_task_name(
                format!("job-{i}"),
                "nfr_bench".into(),
                "p".into(),
            ))
            .expect("seed");
    }

    let mut latencies = Vec::with_capacity(SAMPLES);
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..SAMPLES {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let pick = ((s >> 32) as usize) % SEED;
        let id = format!("job-{pick}");
        let start = Instant::now();
        state.find_by_id(&id).expect("find_by_id");
        latencies.push(start.elapsed());
    }

    latencies.sort();
    let p50 = latencies[SAMPLES / 2];
    let p99 = latencies[(SAMPLES * 99) / 100];
    let max = latencies[SAMPLES - 1];
    println!(
        "nfr_status_latency: seed={SEED} samples={SAMPLES} \
         p50={p50:?} p99={p99:?} max={max:?}"
    );

    assert!(
        p99 < Duration::from_millis(100),
        "find_by_id p99 {p99:?} exceeds NFR ceiling 100ms (p50={p50:?}, max={max:?})",
    );
}
