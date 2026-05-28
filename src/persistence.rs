use std::time::Duration;

use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobStatus};

mod memory_dispatch;
mod memory_state;
mod redis_dispatch;
mod redis_state;
mod sqlite_dispatch;
mod sqlite_schema;
mod sqlite_state;

pub use memory_dispatch::InMemoryJobDispatch;
pub use memory_state::InMemoryJobState;
pub use redis_dispatch::{RedisDispatchConfig, RedisJobDispatch};
pub use redis_state::RedisJobState;
pub use sqlite_dispatch::SqliteJobDispatch;
pub use sqlite_state::SqliteJobState;

/// A job pulled from the dispatch backend, paired with the backend-specific
/// handle (`entry_id`) the consumer must echo back via `ack` or
/// `reclaim_stale` so the dispatch layer can target the right entry without
/// trusting the job body.
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub job: Job,
    pub entry_id: String,
}

/// Hot-path: route, claim, ack. Implementations partition the work so two
/// workers on the same partition never see the same job, and offer a reaper
/// hook for crash recovery.
pub trait JobDispatch: Send + Sync {
    /// Append the job to its partition's queue. The impl reads
    /// `job.priority` to decide which sub-stream/sub-table the entry lands
    /// in (highs drained first per partition).
    fn enqueue(&self, partition: u32, job: &Job) -> Result<(), QueueError>;

    /// Block up to `block` waiting for the next job in `partition`. Returns
    /// `Ok(None)` on timeout. `consumer_id` identifies this worker within
    /// the partition's consumer group — used by Redis to track pending
    /// entries per consumer, ignored by single-process backends.
    fn next_for_partition(
        &self,
        partition: u32,
        consumer_id: &str,
        block: Duration,
    ) -> Result<Option<ClaimedJob>, QueueError>;

    /// Mark the claim as finished. After ack the entry will not be redelivered.
    fn ack(&self, partition: u32, claim: &ClaimedJob) -> Result<(), QueueError>;

    /// Re-claim entries on `partition` whose owning consumer hasn't ack'd
    /// within `idle_after`. Returns the now-owned entries so the reaper can
    /// flip their state back to Pending and re-enqueue if needed.
    fn reclaim_stale(
        &self,
        partition: u32,
        idle_after: Duration,
    ) -> Result<Vec<ClaimedJob>, QueueError>;

    /// Approximate count of unconsumed entries across every partition this
    /// backend knows about. Drives producer back-pressure — exact accuracy
    /// is not required.
    fn pending_count(&self) -> Result<u64, QueueError>;
}

/// Slow-path: job records, retry counts, DLQ. State writes don't sit on the
/// dispatch hot path so they're free to use a heavier backend (hash store,
/// SQL row) without bottlenecking enqueue.
pub trait JobState: Send + Sync {
    /// First write for a freshly-produced job. Persists every field of `job`
    /// (status defaults to `Pending`). Distinct from `save_status` because
    /// the row/hash didn't exist before; `AlreadyExists` if the id collides.
    fn save_initial(&self, job: &Job) -> Result<(), QueueError>;

    /// Status transitions issued by the worker (Running, Completed, Failed,
    /// Cancelled). Implementations enforce the sticky-Cancelled invariant.
    fn save_status(&self, id: &str, status: JobStatus) -> Result<(), QueueError>;

    fn save_retry_count(&self, id: &str, retry_count: u32) -> Result<(), QueueError>;
    fn find_by_id(&self, id: &str) -> Result<Job, QueueError>;
    fn save_dead_letter(&self, dl: &DeadLetterJob) -> Result<(), QueueError>;
    fn find_all_dead_letter(&self) -> Result<Vec<DeadLetterJob>, QueueError>;
}
