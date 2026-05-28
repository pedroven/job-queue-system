use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::QueueError;
use crate::models::Job;
use crate::persistence::{ClaimedJob, JobDispatch};

/// In-memory dispatch backend used by tests and the single-process default.
/// Each partition has its own `Mutex<PartitionState>` + `Condvar`, so two
/// workers on different partitions never contend.
pub struct InMemoryJobDispatch {
    partition_count: u32,
    partitions: Vec<(Mutex<PartitionState>, Condvar)>,
}

#[derive(Default)]
struct PartitionState {
    /// Pending entries bucketed by `JobPriority as u32`. `BTreeMap`'s reverse
    /// iteration drains higher discriminants first — matches `JobQueues`.
    pending: BTreeMap<u32, VecDeque<(String, Job)>>,
    /// Entries handed out via `next_for_partition` but not yet ack'd.
    inflight: HashMap<String, InflightEntry>,
    next_seq: u64,
}

struct InflightEntry {
    job: Job,
    claimed_at: Instant,
}

impl InMemoryJobDispatch {
    pub fn new(partition_count: u32) -> Self {
        assert!(partition_count > 0, "partition_count must be > 0");
        let mut partitions = Vec::with_capacity(partition_count as usize);
        for _ in 0..partition_count {
            partitions.push((Mutex::new(PartitionState::default()), Condvar::new()));
        }
        Self {
            partition_count,
            partitions,
        }
    }

    fn part(&self, partition: u32) -> Result<&(Mutex<PartitionState>, Condvar), QueueError> {
        self.partitions
            .get(partition as usize)
            .ok_or(QueueError::InvalidPartition {
                partition,
                count: self.partition_count,
            })
    }
}

impl PartitionState {
    fn pop_next(&mut self) -> Option<(String, Job)> {
        for queue in self.pending.values_mut().rev() {
            if let Some(entry) = queue.pop_front() {
                return Some(entry);
            }
        }
        None
    }

    fn pending_len(&self) -> usize {
        self.pending.values().map(VecDeque::len).sum()
    }

    fn next_entry_id(&mut self) -> String {
        self.next_seq += 1;
        format!("mem-{}", self.next_seq)
    }
}

impl JobDispatch for InMemoryJobDispatch {
    fn enqueue(&self, partition: u32, job: &Job) -> Result<(), QueueError> {
        let (lock, cvar) = self.part(partition)?;
        let mut state = lock.lock()?;
        let entry_id = state.next_entry_id();
        let prio = job.priority as u32;
        state
            .pending
            .entry(prio)
            .or_default()
            .push_back((entry_id, job.clone()));
        cvar.notify_one();
        Ok(())
    }

    fn next_for_partition(
        &self,
        partition: u32,
        _consumer_id: &str,
        block: Duration,
    ) -> Result<Option<ClaimedJob>, QueueError> {
        let (lock, cvar) = self.part(partition)?;
        let mut state = lock.lock()?;
        if state.pending_len() == 0 {
            // No work yet — block up to `block` waiting for an enqueue or a
            // reaper-driven re-push.
            let (s, _to) = cvar
                .wait_timeout(state, block)
                .unwrap_or_else(|e| e.into_inner());
            state = s;
        }
        if let Some((entry_id, job)) = state.pop_next() {
            state.inflight.insert(
                entry_id.clone(),
                InflightEntry {
                    job: job.clone(),
                    claimed_at: Instant::now(),
                },
            );
            return Ok(Some(ClaimedJob { job, entry_id }));
        }
        Ok(None)
    }

    fn ack(&self, partition: u32, claim: &ClaimedJob) -> Result<(), QueueError> {
        let (lock, _) = self.part(partition)?;
        let mut state = lock.lock()?;
        state.inflight.remove(&claim.entry_id);
        Ok(())
    }

    fn reclaim_stale(
        &self,
        partition: u32,
        idle_after: Duration,
    ) -> Result<Vec<ClaimedJob>, QueueError> {
        let (lock, cvar) = self.part(partition)?;
        let mut state = lock.lock()?;
        let now = Instant::now();
        let stale_ids: Vec<String> = state
            .inflight
            .iter()
            .filter(|(_, e)| now.duration_since(e.claimed_at) >= idle_after)
            .map(|(k, _)| k.clone())
            .collect();
        let mut reclaimed = Vec::with_capacity(stale_ids.len());
        for entry_id in stale_ids {
            if let Some(entry) = state.inflight.remove(&entry_id) {
                let job = entry.job.clone();
                let prio = job.priority as u32;
                // Push to the front so reclaimed work jumps the line — it's
                // already been waiting since the original enqueue.
                state
                    .pending
                    .entry(prio)
                    .or_default()
                    .push_front((entry_id.clone(), job.clone()));
                reclaimed.push(ClaimedJob { job, entry_id });
            }
        }
        if !reclaimed.is_empty() {
            cvar.notify_all();
        }
        Ok(reclaimed)
    }

    fn pending_count(&self) -> Result<u64, QueueError> {
        let mut total: u64 = 0;
        for (lock, _) in &self.partitions {
            let state = lock.lock()?;
            total += state.pending_len() as u64;
        }
        Ok(total)
    }
}

impl InMemoryJobDispatch {
    /// Test-only: peek how many jobs are pending in a partition. Used by
    /// unit tests that need to assert routing without consuming.
    #[cfg(test)]
    pub(crate) fn pending_in_partition(&self, partition: u32) -> usize {
        let (lock, _) = self.part(partition).unwrap();
        lock.lock().unwrap().pending_len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{JobPriority, testing::make_test_job};

    #[test]
    fn test_enqueue_and_next_returns_job() {
        let d = InMemoryJobDispatch::new(1);
        d.enqueue(0, &make_test_job("j1", "p")).unwrap();
        let claim = d
            .next_for_partition(0, "c1", Duration::from_millis(10))
            .unwrap()
            .unwrap();
        assert_eq!(claim.job.id, "j1");
    }

    #[test]
    fn test_next_returns_none_on_timeout() {
        let d = InMemoryJobDispatch::new(1);
        let claim = d
            .next_for_partition(0, "c1", Duration::from_millis(20))
            .unwrap();
        assert!(claim.is_none());
    }

    #[test]
    fn test_high_priority_drains_first() {
        let d = InMemoryJobDispatch::new(1);
        d.enqueue(0, &make_test_job("n1", "p")).unwrap();
        let mut high = make_test_job("h1", "p");
        high.priority = JobPriority::High;
        d.enqueue(0, &high).unwrap();
        let first = d
            .next_for_partition(0, "c", Duration::from_millis(10))
            .unwrap()
            .unwrap();
        assert_eq!(first.job.id, "h1");
    }

    #[test]
    fn test_ack_removes_inflight() {
        let d = InMemoryJobDispatch::new(1);
        d.enqueue(0, &make_test_job("j", "p")).unwrap();
        let claim = d
            .next_for_partition(0, "c", Duration::from_millis(10))
            .unwrap()
            .unwrap();
        d.ack(0, &claim).unwrap();
        let stale = d.reclaim_stale(0, Duration::ZERO).unwrap();
        assert_eq!(stale.len(), 0);
    }

    #[test]
    fn test_reclaim_stale_repushes_unacked() {
        let d = InMemoryJobDispatch::new(1);
        d.enqueue(0, &make_test_job("j", "p")).unwrap();
        let _claim = d
            .next_for_partition(0, "c", Duration::from_millis(10))
            .unwrap()
            .unwrap();
        // No ack. After a zero-idle reclaim, the job is back in pending.
        let reclaimed = d.reclaim_stale(0, Duration::ZERO).unwrap();
        assert_eq!(reclaimed.len(), 1);
        let again = d
            .next_for_partition(0, "c", Duration::from_millis(10))
            .unwrap()
            .unwrap();
        assert_eq!(again.job.id, "j");
    }

    #[test]
    fn test_partitions_are_isolated() {
        let d = InMemoryJobDispatch::new(2);
        d.enqueue(0, &make_test_job("a", "p")).unwrap();
        d.enqueue(1, &make_test_job("b", "p")).unwrap();
        assert_eq!(d.pending_in_partition(0), 1);
        assert_eq!(d.pending_in_partition(1), 1);

        let from0 = d
            .next_for_partition(0, "c", Duration::from_millis(10))
            .unwrap()
            .unwrap();
        let from1 = d
            .next_for_partition(1, "c", Duration::from_millis(10))
            .unwrap()
            .unwrap();
        assert_eq!(from0.job.id, "a");
        assert_eq!(from1.job.id, "b");
    }

    #[test]
    fn test_pending_count_sums_across_partitions() {
        let d = InMemoryJobDispatch::new(3);
        d.enqueue(0, &make_test_job("a", "p")).unwrap();
        d.enqueue(1, &make_test_job("b", "p")).unwrap();
        d.enqueue(1, &make_test_job("c", "p")).unwrap();
        assert_eq!(d.pending_count().unwrap(), 3);
    }

    #[test]
    fn test_invalid_partition_errors() {
        let d = InMemoryJobDispatch::new(1);
        let err = d.enqueue(5, &make_test_job("j", "p")).unwrap_err();
        assert!(matches!(err, QueueError::InvalidPartition { .. }));
    }
}
