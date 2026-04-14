use std::fmt;
use std::time::SystemTime;

use crate::error::QueueError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
        }
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for JobStatus {
    type Err = QueueError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(JobStatus::Pending),
            "running" => Ok(JobStatus::Running),
            "completed" => Ok(JobStatus::Completed),
            "failed" => Ok(JobStatus::Failed),
            _ => Err(QueueError::InvalidStatus(s.to_string())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub status: JobStatus,
    pub retry_count: u32,
    pub task: Task,
    pub max_attempts: u32,
    pub created_at: SystemTime,
}

impl Job {
    pub fn with_task_name(id: String, task_name: String, payload: String) -> Self {
        Job {
            task: Task {
                id: format!("task-{id}"),
                name: task_name,
                payload,
            },
            id,
            status: JobStatus::Pending,
            retry_count: 0,
            max_attempts: 3,
            created_at: SystemTime::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeadLetterJob {
    pub id: String,
    pub original_job_id: String,
    pub task: Task,
    pub error: String,
    pub failed_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: String,
    pub name: String,
    pub payload: String,
}

#[derive(Debug)]
pub enum WorkerStatus {
    Idle,
    Busy,
    ShuttingDown,
}

#[derive(Debug)]
pub struct Worker {
    pub id: String,
    pub status: WorkerStatus,
    pub current_job_id: Option<String>,
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    pub fn make_test_job(id: &str, payload: &str) -> Job {
        Job::with_task_name(id.to_string(), "default".to_string(), payload.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_with_task_name_sets_name() {
        let job = Job::with_task_name(
            "job-1".to_string(),
            "send_email".to_string(),
            "payload".to_string(),
        );
        assert_eq!(job.task.name, "send_email");
        assert_eq!(job.task.payload, "payload");
        assert_eq!(job.id, "job-1");
    }
}
