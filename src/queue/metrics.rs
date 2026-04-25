use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::models::WorkerStatus;

/// Atomic counters incremented by workers as jobs finalize. Held inside the
/// `Queue` so any thread can update them without taking a lock.
pub(crate) struct MetricsCounters {
    pub completed: AtomicU64,
    pub failed_attempts: AtomicU64,
    pub dead_lettered: AtomicU64,
    pub started_at: Instant,
}

impl Default for MetricsCounters {
    fn default() -> Self {
        Self {
            completed: AtomicU64::new(0),
            failed_attempts: AtomicU64::new(0),
            dead_lettered: AtomicU64::new(0),
            started_at: Instant::now(),
        }
    }
}

impl MetricsCounters {
    pub(crate) fn record_completed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_failed_attempt(&self) {
        self.failed_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dead_lettered(&self) {
        self.dead_lettered.fetch_add(1, Ordering::Relaxed);
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
    pub failed_attempts_total: u64,
    pub dead_lettered_total: u64,
    pub uptime: Duration,
    /// Finalized jobs per second since startup (completed + dead-lettered).
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

    let secs = uptime.as_secs_f64();
    let finalized = completed + dead_lettered;
    let throughput_per_sec = if secs > 0.0 {
        finalized as f64 / secs
    } else {
        0.0
    };
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
        throughput_per_sec,
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
}
