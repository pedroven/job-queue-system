use std::sync::Mutex;

use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobStatus};
use crate::persistence::JobState;

/// In-memory job-state store. Pairs with `InMemoryJobDispatch` for tests and
/// single-process dev. Holds the full `Job` so tests can read it back via
/// `find_by_id` and observe status transitions.
#[derive(Default)]
pub struct InMemoryJobState {
    jobs: Mutex<Vec<Job>>,
    dead_letter_jobs: Mutex<Vec<DeadLetterJob>>,
}

impl InMemoryJobState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test-only: snapshot of jobs that are still `Pending`, optionally
    /// filtered by task name. Public for integration tests that need to
    /// inspect the queue's state after `perform_async` enqueues (which
    /// generates random ids and hides them from the caller).
    pub fn __find_all_pending_with_task(&self, task_name: &str) -> Vec<Job> {
        self.jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|j| j.status == JobStatus::Pending && j.task.name == task_name)
            .cloned()
            .collect()
    }
}

impl JobState for InMemoryJobState {
    fn save_initial(&self, job: &Job) -> Result<(), QueueError> {
        let mut jobs = self.jobs.lock()?;
        if jobs.iter().any(|j| j.id == job.id) {
            return Err(QueueError::AlreadyExists(job.id.clone()));
        }
        jobs.push(job.clone());
        Ok(())
    }

    fn save_status(&self, id: &str, status: JobStatus) -> Result<(), QueueError> {
        let mut jobs = self.jobs.lock()?;
        let job = jobs
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or_else(|| QueueError::NotFound(id.to_string()))?;
        // Sticky-Cancelled: once cancelled, no later transition may overwrite
        // it. Closes the cancel/worker race.
        if job.status != JobStatus::Cancelled {
            job.status = status;
        }
        Ok(())
    }

    fn save_retry_count(&self, id: &str, retry_count: u32) -> Result<(), QueueError> {
        let mut jobs = self.jobs.lock()?;
        let job = jobs
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or_else(|| QueueError::NotFound(id.to_string()))?;
        job.retry_count = retry_count;
        Ok(())
    }

    fn find_by_id(&self, id: &str) -> Result<Job, QueueError> {
        let jobs = self.jobs.lock()?;
        jobs.iter()
            .find(|j| j.id == id)
            .cloned()
            .ok_or_else(|| QueueError::NotFound(id.to_string()))
    }

    fn save_dead_letter(&self, dl: &DeadLetterJob) -> Result<(), QueueError> {
        let mut dl_jobs = self.dead_letter_jobs.lock()?;
        if dl_jobs.iter().any(|j| j.id == dl.id) {
            return Err(QueueError::AlreadyExists(dl.id.clone()));
        }
        dl_jobs.push(dl.clone());
        Ok(())
    }

    fn find_all_dead_letter(&self) -> Result<Vec<DeadLetterJob>, QueueError> {
        Ok(self.dead_letter_jobs.lock()?.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::testing::make_test_job;

    #[test]
    fn test_save_initial_rejects_duplicate() {
        let s = InMemoryJobState::new();
        s.save_initial(&make_test_job("j", "p")).unwrap();
        let err = s.save_initial(&make_test_job("j", "p")).unwrap_err();
        assert!(matches!(err, QueueError::AlreadyExists(_)));
    }

    #[test]
    fn test_save_status_is_sticky_cancelled() {
        let s = InMemoryJobState::new();
        s.save_initial(&make_test_job("j", "p")).unwrap();
        s.save_status("j", JobStatus::Cancelled).unwrap();
        s.save_status("j", JobStatus::Completed).unwrap();
        assert_eq!(s.find_by_id("j").unwrap().status, JobStatus::Cancelled);
    }

    #[test]
    fn test_save_retry_count_persists() {
        let s = InMemoryJobState::new();
        s.save_initial(&make_test_job("j", "p")).unwrap();
        s.save_retry_count("j", 4).unwrap();
        assert_eq!(s.find_by_id("j").unwrap().retry_count, 4);
    }

    #[test]
    fn test_find_by_id_missing_errors() {
        let s = InMemoryJobState::new();
        assert!(matches!(
            s.find_by_id("no").unwrap_err(),
            QueueError::NotFound(_)
        ));
    }
}
