use std::collections::{BTreeMap, VecDeque};

use crate::models::Job;
#[cfg(test)]
use crate::models::JobPriority;

/// In-memory staging for pending jobs, keyed by `JobPriority` discriminant
/// (`u32` via `#[repr(u32)]`). Higher discriminant = higher priority.
/// Adding a new `JobPriority` variant requires no changes here: `push_back`
/// creates the level on demand and `pop_next` iterates highest-first.
#[derive(Default)]
pub struct JobQueues {
    levels: BTreeMap<u32, VecDeque<Job>>,
}

impl JobQueues {
    pub(crate) fn is_empty(&self) -> bool {
        self.levels.values().all(VecDeque::is_empty)
    }

    pub(crate) fn len(&self) -> usize {
        self.levels.values().map(VecDeque::len).sum()
    }

    pub(crate) fn push_back(&mut self, job: Job) {
        let level = job.priority as u32;
        self.levels.entry(level).or_default().push_back(job);
    }

    pub(crate) fn remove(&mut self, job_id: &str) -> Option<Job> {
        for queue in self.levels.values_mut() {
            if let Some(idx) = queue.iter().position(|j| j.id == job_id) {
                return queue.remove(idx);
            }
        }
        None
    }

    pub(crate) fn pop_next(&mut self) -> Option<Job> {
        // BTreeMap iterates in ascending key order; reverse for highest-first.
        for queue in self.levels.values_mut().rev() {
            if let Some(job) = queue.pop_front() {
                return Some(job);
            }
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn queue_for(&self, priority: JobPriority) -> Option<&VecDeque<Job>> {
        self.levels.get(&(priority as u32))
    }
}
