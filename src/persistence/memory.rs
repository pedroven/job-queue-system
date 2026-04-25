use std::sync::Mutex;

use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobStatus};
use crate::persistence::JobRepository;

#[derive(Default)]
pub struct InMemoryJobRepository {
    jobs: Mutex<Vec<Job>>,
    dead_letter_jobs: Mutex<Vec<DeadLetterJob>>,
}

impl InMemoryJobRepository {
    pub fn new() -> Self {
        Self::default()
    }
}

impl JobRepository for InMemoryJobRepository {
    fn save(&self, job: &Job) -> Result<(), QueueError> {
        let mut jobs = self.jobs.lock()?;
        if jobs.iter().any(|j| j.id == job.id) {
            return Err(QueueError::AlreadyExists(job.id.clone()));
        }
        jobs.push(job.clone());
        Ok(())
    }

    fn find_by_id(&self, job_id: &str) -> Result<Job, QueueError> {
        let jobs = self.jobs.lock()?;
        jobs.iter()
            .find(|j| j.id == job_id)
            .cloned()
            .ok_or_else(|| QueueError::NotFound(job_id.to_string()))
    }

    fn save_dead_letter(&self, job: &DeadLetterJob) -> Result<(), QueueError> {
        let mut dl_jobs = self.dead_letter_jobs.lock()?;
        if dl_jobs.iter().any(|j| j.id == job.id) {
            return Err(QueueError::AlreadyExists(job.id.clone()));
        }
        dl_jobs.push(job.clone());
        Ok(())
    }

    fn find_all_pending(&self) -> Result<Vec<Job>, QueueError> {
        let jobs = self.jobs.lock()?;
        let pending = jobs
            .iter()
            .filter(|j| j.status == JobStatus::Pending)
            .cloned()
            .collect();
        Ok(pending)
    }

    fn pending_count(&self) -> Result<u64, QueueError> {
        let jobs = self.jobs.lock()?;
        Ok(jobs
            .iter()
            .filter(|j| j.status == JobStatus::Pending)
            .count() as u64)
    }

    fn find_all_dead_letter(&self) -> Result<Vec<DeadLetterJob>, QueueError> {
        let dl_jobs = self.dead_letter_jobs.lock()?;
        Ok(dl_jobs.clone())
    }

    fn update_status(&self, job_id: &str, status: JobStatus) -> Result<(), QueueError> {
        let mut jobs = self.jobs.lock()?;
        let job = jobs
            .iter_mut()
            .find(|j| j.id == job_id)
            .ok_or_else(|| QueueError::NotFound(job_id.to_string()))?;
        // Cancelled is sticky — see SqliteJobRepository::update_status.
        if job.status != JobStatus::Cancelled {
            job.status = status;
        }
        Ok(())
    }

    fn update_retry_count(&self, job_id: &str, retry_count: u32) -> Result<(), QueueError> {
        let mut jobs = self.jobs.lock()?;
        let job = jobs
            .iter_mut()
            .find(|j| j.id == job_id)
            .ok_or_else(|| QueueError::NotFound(job_id.to_string()))?;
        job.retry_count = retry_count;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{TaskRecord, testing::make_test_job};

    fn make_dead_letter_job(id: &str, original_job_id: &str) -> DeadLetterJob {
        DeadLetterJob {
            id: id.to_string(),
            original_job_id: original_job_id.to_string(),
            task: TaskRecord {
                id: format!("task-{original_job_id}"),
                name: "default".to_string(),
                payload: "payload".to_string(),
            },
            error: "something went wrong".to_string(),
            failed_at: std::time::SystemTime::now(),
        }
    }

    #[test]
    fn test_in_memory_save_and_find_pending() {
        let repo = InMemoryJobRepository::new();
        repo.save(&make_test_job("job-1", "payload-1")).unwrap();
        repo.save(&make_test_job("job-2", "payload-2")).unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].id, "job-1");
        assert_eq!(pending[1].id, "job-2");
    }

    #[test]
    fn test_in_memory_update_status_excludes_from_pending() {
        let repo = InMemoryJobRepository::new();
        repo.save(&make_test_job("job-1", "payload")).unwrap();
        repo.update_status("job-1", JobStatus::Completed).unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_in_memory_update_status_not_found() {
        let repo = InMemoryJobRepository::new();
        let result = repo.update_status("nonexistent", JobStatus::Running);
        assert!(result.is_err());
    }

    #[test]
    fn test_in_memory_save_and_find_dead_letter() {
        let repo = InMemoryJobRepository::new();
        repo.save_dead_letter(&make_dead_letter_job("dl-1", "job-1"))
            .unwrap();
        repo.save_dead_letter(&make_dead_letter_job("dl-2", "job-2"))
            .unwrap();

        let dl = repo.find_all_dead_letter().unwrap();
        assert_eq!(dl.len(), 2);
        assert_eq!(dl[0].original_job_id, "job-1");
        assert_eq!(dl[1].original_job_id, "job-2");
    }

    #[test]
    fn test_in_memory_find_by_id() {
        let repo = InMemoryJobRepository::new();
        repo.save(&make_test_job("job-1", "payload-1")).unwrap();

        let job = repo.find_by_id("job-1").unwrap();
        assert_eq!(job.id, "job-1");
        assert_eq!(job.task.payload, "payload-1");
    }

    #[test]
    fn test_in_memory_find_by_id_not_found() {
        let repo = InMemoryJobRepository::new();
        let result = repo.find_by_id("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_in_memory_update_retry_count() {
        let repo = InMemoryJobRepository::new();
        repo.save(&make_test_job("job-1", "payload")).unwrap();

        repo.update_retry_count("job-1", 3).unwrap();
        let job = repo.find_by_id("job-1").unwrap();
        assert_eq!(job.retry_count, 3);
    }

    #[test]
    fn test_in_memory_pending_count_excludes_non_pending() {
        let repo = InMemoryJobRepository::new();
        repo.save(&make_test_job("job-1", "p1")).unwrap();
        repo.save(&make_test_job("job-2", "p2")).unwrap();
        repo.save(&make_test_job("job-3", "p3")).unwrap();
        repo.update_status("job-2", JobStatus::Running).unwrap();
        repo.update_status("job-3", JobStatus::Completed).unwrap();

        assert_eq!(repo.pending_count().unwrap(), 1);
    }

    #[test]
    fn test_in_memory_cancelled_status_is_sticky() {
        let repo = InMemoryJobRepository::new();
        repo.save(&make_test_job("job-1", "payload")).unwrap();
        repo.update_status("job-1", JobStatus::Cancelled).unwrap();

        repo.update_status("job-1", JobStatus::Running).unwrap();
        repo.update_status("job-1", JobStatus::Completed).unwrap();
        repo.update_status("job-1", JobStatus::Failed).unwrap();

        assert_eq!(
            repo.find_by_id("job-1").unwrap().status,
            JobStatus::Cancelled
        );
    }

    #[test]
    fn test_in_memory_update_retry_count_not_found() {
        let repo = InMemoryJobRepository::new();
        let result = repo.update_retry_count("nonexistent", 1);
        assert!(result.is_err());
    }
}
