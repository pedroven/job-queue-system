use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::models::WorkerStatus;

/// Atomic counters incremented by workers as jobs finalize. Held inside the
/// `Queue` so any thread can update them without taking a lock.
pub(crate) struct MetricsCounters {
    completed: AtomicU64,
    failed_attempts: AtomicU64,
    dead_lettered: AtomicU64,
    started_at: Instant,
    /// Latest EWMA-smoothed throughput (finalized jobs/sec). Updated only by
    /// `tick_throughput`; readers see whatever the last tick wrote.
    throughput_ewma: Mutex<EwmaState>,
}

struct EwmaState {
    last_tick: Instant,
    last_finalized: u64,
    rate: f64,
}

/// Time constant for the throughput EWMA. With dt ≈ tick interval, this gives
/// roughly a 1-minute responsiveness window (older samples decay to ~37% in
/// `TAU` seconds).
const THROUGHPUT_TAU: Duration = Duration::from_secs(60);

impl Default for MetricsCounters {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            completed: AtomicU64::new(0),
            failed_attempts: AtomicU64::new(0),
            dead_lettered: AtomicU64::new(0),
            started_at: now,
            throughput_ewma: Mutex::new(EwmaState {
                last_tick: now,
                last_finalized: 0,
                rate: 0.0,
            }),
        }
    }
}

impl MetricsCounters {
    pub(crate) fn record_completed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts each *failed attempt*, not failed jobs — a job that succeeds on
    /// retry 3 contributes 2 here. The DLQ counter (`record_dead_lettered`)
    /// is what corresponds to "jobs that ultimately failed".
    pub(crate) fn record_failed_attempt(&self) {
        self.failed_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dead_lettered(&self) {
        self.dead_lettered.fetch_add(1, Ordering::Relaxed);
    }

    /// Sample the current finalized count and fold it into the EWMA. Intended
    /// to be called from the reporter loop on a fixed cadence so the EWMA's
    /// effective time constant is stable. Ad-hoc snapshot readers should not
    /// call this — jittery `dt` between calls produces noisy estimates.
    pub(crate) fn tick_throughput(&self) {
        let mut state = self
            .throughput_ewma
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let dt = now.duration_since(state.last_tick).as_secs_f64();
        if dt <= 0.0 {
            return;
        }
        let finalized =
            self.completed.load(Ordering::Relaxed) + self.dead_lettered.load(Ordering::Relaxed);
        let delta = finalized.saturating_sub(state.last_finalized) as f64;
        let instantaneous = delta / dt;
        let alpha = 1.0 - (-dt / THROUGHPUT_TAU.as_secs_f64()).exp();
        state.rate = alpha * instantaneous + (1.0 - alpha) * state.rate;
        state.last_tick = now;
        state.last_finalized = finalized;
    }

    fn current_throughput(&self) -> f64 {
        self.throughput_ewma
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .rate
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricsSnapshot {
    pub queue_depth: u64,
    pub dead_letter_depth: u64,
    pub worker_count: u64,
    pub idle_workers: u64,
    pub busy_workers: u64,
    pub completed_total: u64,
    /// Number of failed *attempts*, not failed jobs. A job that succeeds on
    /// retry 3 contributes 2 here. See `dead_lettered_total` for jobs that
    /// ultimately failed.
    pub failed_attempts_total: u64,
    pub dead_lettered_total: u64,
    pub uptime: Duration,
    /// EWMA of finalized jobs per second (completed + dead-lettered), updated
    /// by the reporter tick. `0.0` until the first tick after startup.
    pub throughput_per_sec: f64,
    /// Fraction of finalized jobs that ended in the DLQ. `0.0` when no jobs
    /// have finalized yet.
    pub failure_rate: f64,
}

impl MetricsSnapshot {
    pub fn render(&self) -> String {
        format!(
            "queue: depth={} dlq={} workers={} (idle={} busy={}) \
             completed={} failed_attempts={} dead_lettered={} \
             throughput={:.2}/s failure_rate={:.2}% uptime={}s",
            self.queue_depth,
            self.dead_letter_depth,
            self.worker_count,
            self.idle_workers,
            self.busy_workers,
            self.completed_total,
            self.failed_attempts_total,
            self.dead_lettered_total,
            self.throughput_per_sec,
            self.failure_rate * 100.0,
            self.uptime.as_secs(),
        )
    }
}

pub(crate) fn build_snapshot(
    counters: &MetricsCounters,
    queue_depth: u64,
    dead_letter_depth: u64,
    worker_statuses: impl IntoIterator<Item = WorkerStatus>,
) -> MetricsSnapshot {
    let mut worker_count = 0u64;
    let mut busy = 0u64;
    let mut idle = 0u64;
    for status in worker_statuses {
        worker_count += 1;
        match status {
            WorkerStatus::Busy => busy += 1,
            WorkerStatus::Idle => idle += 1,
            WorkerStatus::ShuttingDown => {}
        }
    }

    let completed = counters.completed.load(Ordering::Relaxed);
    let failed_attempts = counters.failed_attempts.load(Ordering::Relaxed);
    let dead_lettered = counters.dead_lettered.load(Ordering::Relaxed);
    let uptime = counters.started_at.elapsed();
    let finalized = completed + dead_lettered;
    let failure_rate = if finalized > 0 {
        dead_lettered as f64 / finalized as f64
    } else {
        0.0
    };

    MetricsSnapshot {
        queue_depth,
        dead_letter_depth,
        worker_count,
        idle_workers: idle,
        busy_workers: busy,
        completed_total: completed,
        failed_attempts_total: failed_attempts,
        dead_lettered_total: dead_lettered,
        uptime,
        throughput_per_sec: counters.current_throughput(),
        failure_rate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_with_no_activity_is_zeroed() {
        let counters = MetricsCounters::default();
        let snap = build_snapshot(&counters, 0, 0, std::iter::empty());
        assert_eq!(snap.queue_depth, 0);
        assert_eq!(snap.completed_total, 0);
        assert_eq!(snap.dead_lettered_total, 0);
        assert_eq!(snap.failure_rate, 0.0);
        assert_eq!(snap.worker_count, 0);
        assert_eq!(snap.throughput_per_sec, 0.0);
    }

    #[test]
    fn snapshot_aggregates_worker_statuses() {
        let counters = MetricsCounters::default();
        let statuses = [
            WorkerStatus::Idle,
            WorkerStatus::Busy,
            WorkerStatus::Busy,
            WorkerStatus::Idle,
        ];
        let snap = build_snapshot(&counters, 7, 2, statuses);
        assert_eq!(snap.queue_depth, 7);
        assert_eq!(snap.dead_letter_depth, 2);
        assert_eq!(snap.worker_count, 4);
        assert_eq!(snap.idle_workers, 2);
        assert_eq!(snap.busy_workers, 2);
    }

    #[test]
    fn failure_rate_uses_finalized_total() {
        let counters = MetricsCounters::default();
        for _ in 0..3 {
            counters.record_completed();
        }
        counters.record_dead_lettered();
        let snap = build_snapshot(&counters, 0, 1, std::iter::empty());
        assert_eq!(snap.completed_total, 3);
        assert_eq!(snap.dead_lettered_total, 1);
        // 1 dead-lettered out of 4 finalized = 25%.
        assert_eq!(snap.failure_rate, 0.25);
    }

    #[test]
    fn render_includes_key_fields() {
        let counters = MetricsCounters::default();
        counters.record_completed();
        let snap = build_snapshot(&counters, 5, 0, [WorkerStatus::Idle]);
        let s = snap.render();
        assert!(s.contains("depth=5"));
        assert!(s.contains("workers=1"));
        assert!(s.contains("completed=1"));
    }

    #[test]
    fn throughput_ewma_rises_with_activity_then_decays() {
        let counters = MetricsCounters::default();
        // Backdate last_tick so dt is non-trivial without sleeping.
        {
            let mut state = counters.throughput_ewma.lock().unwrap();
            state.last_tick = Instant::now() - Duration::from_secs(10);
        }
        for _ in 0..50 {
            counters.record_completed();
        }
        counters.tick_throughput();
        let after_burst = counters.current_throughput();
        assert!(after_burst > 0.0, "EWMA should rise after a burst");

        // No more activity; another tick after some elapsed time should pull
        // the EWMA toward zero.
        {
            let mut state = counters.throughput_ewma.lock().unwrap();
            state.last_tick = Instant::now() - Duration::from_secs(60);
        }
        counters.tick_throughput();
        assert!(counters.current_throughput() < after_burst);
    }
}
