use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobStatus};

mod memory;
mod sqlite;

pub use memory::InMemoryJobRepository;
pub use sqlite::SqliteJobRepository;

pub trait JobRepository: Send + Sync {
    fn save(&self, job: &Job) -> Result<(), QueueError>;
    fn save_dead_letter(&self, job: &DeadLetterJob) -> Result<(), QueueError>;
    fn find_by_id(&self, job_id: &str) -> Result<Job, QueueError>;
    fn find_all_pending(&self) -> Result<Vec<Job>, QueueError>;
    fn find_all_dead_letter(&self) -> Result<Vec<DeadLetterJob>, QueueError>;
    fn update_status(&self, job_id: &str, status: JobStatus) -> Result<(), QueueError>;
    fn update_retry_count(&self, job_id: &str, retry_count: u32) -> Result<(), QueueError>;
}
