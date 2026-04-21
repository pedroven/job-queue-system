use std::sync::Mutex;
use std::time::SystemTime;

use crate::error::QueueError;

use super::model::{ScheduledJob, ScheduledJobRepository};

pub struct InMemoryScheduledJobRepository {
    jobs: Mutex<Vec<ScheduledJob>>,
}

impl InMemoryScheduledJobRepository {
    pub fn new() -> Self {
        Self {
            jobs: Mutex::new(Vec::new()),
        }
    }
}

impl Default for InMemoryScheduledJobRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl ScheduledJobRepository for InMemoryScheduledJobRepository {
    fn save(&self, job: &ScheduledJob) -> Result<(), QueueError> {
        let mut jobs = self.jobs.lock()?;
        if let Some(existing) = jobs.iter_mut().find(|j| j.id == job.id) {
            *existing = job.clone();
        } else {
            jobs.push(job.clone());
        }
        Ok(())
    }

    fn save_if_absent(&self, job: &ScheduledJob) -> Result<bool, QueueError> {
        let mut jobs = self.jobs.lock()?;
        if jobs.iter().any(|j| j.id == job.id) {
            return Ok(false);
        }
        jobs.push(job.clone());
        Ok(true)
    }

    fn find_all_enabled(&self) -> Result<Vec<ScheduledJob>, QueueError> {
        Ok(self
            .jobs
            .lock()?
            .iter()
            .filter(|j| j.enabled)
            .cloned()
            .collect())
    }

    fn find_due(&self, now: SystemTime) -> Result<Vec<ScheduledJob>, QueueError> {
        Ok(self
            .jobs
            .lock()?
            .iter()
            .filter(|j| j.enabled && j.next_run_at <= now)
            .cloned()
            .collect())
    }

    fn update_after_fire(
        &self,
        id: &str,
        fired_at: SystemTime,
        next_run_at: SystemTime,
    ) -> Result<(), QueueError> {
        let mut jobs = self.jobs.lock()?;
        let job = jobs
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or_else(|| QueueError::NotFound(id.to_string()))?;
        job.last_run_at = Some(fired_at);
        job.next_run_at = next_run_at;
        Ok(())
    }
}
