use std::time::Duration;

use crate::error::QueueError;

/// Tunable parameters for a `Queue`. All fields have sensible defaults via
/// `QueueConfig::default()`; override selectively when constructing a queue
/// via `Queue::with_config`.
///
/// Back-pressure note: the hard threshold is advisory, not an invariant.
/// `pending_count()` and `enqueue()` are not serialized, so under concurrent
/// producers the actual depth may briefly exceed `backpressure_hard_threshold`
/// by up to `N-1` (N = live producers). Set the threshold below the real
/// capacity if strict bounding matters.
#[derive(Clone, Debug)]
pub struct QueueConfig {
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
}

impl QueueConfig {
    pub(crate) fn validate(&self) -> Result<(), QueueError> {
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
            retry_backoff_base: Duration::from_secs(1),
            backpressure_soft_threshold: 80,
            backpressure_hard_threshold: 100,
            backpressure_delay: Duration::from_millis(50),
        }
    }
}
