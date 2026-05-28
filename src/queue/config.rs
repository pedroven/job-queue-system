use std::time::Duration;

use crate::error::QueueError;

/// Tunable parameters for a `Queue`. Defaults assume single-process,
/// single-partition use; override `partition_count` + `assigned_partitions`
/// for horizontal scale-out.
///
/// Back-pressure note: the hard threshold is advisory, not an invariant.
/// `pending_count()` and `enqueue()` are not serialized, so under concurrent
/// producers the actual depth may briefly exceed `backpressure_hard_threshold`
/// by up to `N-1` (N = live producers). Set the threshold below the real
/// capacity if strict bounding matters.
#[derive(Clone, Debug)]
pub struct QueueConfig {
    /// Total partition count in the system. Producers hash `job.id` modulo
    /// this number to pick a destination partition.
    pub partition_count: u32,
    /// Partitions this queue instance owns. One worker thread is spawned
    /// per entry. Empty = no workers (useful when the queue is a producer
    /// only).
    pub assigned_partitions: Vec<u32>,
    /// Retry backoff base for failed jobs. Sleep before attempt `n` is
    /// `retry_backoff_base * 2^(n+1)` where `n` is the zero-indexed attempt
    /// that just failed (so first retry sleeps 2× base, second 4×, and so on).
    pub retry_backoff_base: Duration,
    /// Pending-job count at which producers start to throttle.
    pub backpressure_soft_threshold: u64,
    /// Pending-job count at which producers are rejected with `QueueFull`.
    pub backpressure_hard_threshold: u64,
    /// Sleep applied on each produce call once depth is between soft and hard.
    pub backpressure_delay: Duration,
    /// How long a single `next_for_partition` call blocks waiting for work.
    /// Tradeoff: longer = fewer wakeups but slower shutdown response.
    pub worker_block_duration: Duration,
    /// Jobs whose claim is older than this are eligible for reaping back to
    /// Pending. Should comfortably exceed the longest expected handler run.
    pub claim_lease: Duration,
    /// How often the reaper thread runs.
    pub reaper_interval: Duration,
}

impl QueueConfig {
    pub(crate) fn validate(&self) -> Result<(), QueueError> {
        if self.partition_count == 0 {
            return Err(QueueError::InvalidConfig(
                "partition_count must be > 0".into(),
            ));
        }
        for p in &self.assigned_partitions {
            if *p >= self.partition_count {
                return Err(QueueError::InvalidConfig(format!(
                    "assigned partition {p} >= partition_count {}",
                    self.partition_count
                )));
            }
        }
        if self.backpressure_hard_threshold == 0 {
            return Err(QueueError::InvalidConfig(
                "backpressure_hard_threshold must be > 0".into(),
            ));
        }
        if self.backpressure_soft_threshold > self.backpressure_hard_threshold {
            return Err(QueueError::InvalidConfig(format!(
                "backpressure_soft_threshold ({}) must be <= backpressure_hard_threshold ({})",
                self.backpressure_soft_threshold, self.backpressure_hard_threshold
            )));
        }
        Ok(())
    }
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            partition_count: 1,
            assigned_partitions: vec![0],
            retry_backoff_base: Duration::from_secs(1),
            backpressure_soft_threshold: 80,
            backpressure_hard_threshold: 100,
            backpressure_delay: Duration::from_millis(50),
            worker_block_duration: Duration::from_millis(100),
            claim_lease: Duration::from_secs(60),
            reaper_interval: Duration::from_secs(30),
        }
    }
}
