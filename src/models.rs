use std::fmt;
use std::time::SystemTime;

use crate::error::QueueError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
            JobStatus::Cancelled => "cancelled",
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
            "cancelled" => Ok(JobStatus::Cancelled),
            _ => Err(QueueError::InvalidStatus(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum JobPriority {
    High = 2,
    Normal = 1,
}

impl TryFrom<u32> for JobPriority {
    type Error = QueueError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(JobPriority::High),
            1 => Ok(JobPriority::Normal),
            _ => Err(QueueError::InvalidPriority(value)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub status: JobStatus,
    pub retry_count: u32,
    pub task: TaskRecord,
    pub max_attempts: u32,
    pub priority: JobPriority,
    pub created_at: SystemTime,
}

impl Job {
    pub fn with_task_name(id: String, task_name: String, payload: String) -> Self {
        Job {
            task: TaskRecord {
                id: format!("task-{id}"),
                name: task_name,
                payload,
            },
            id,
            status: JobStatus::Pending,
            retry_count: 0,
            max_attempts: 3,
            priority: JobPriority::Normal,
            created_at: SystemTime::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeadLetterJob {
    pub id: String,
    pub original_job_id: String,
    pub task: TaskRecord,
    pub error: String,
    pub failed_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct TaskRecord {
    pub id: String,
    pub name: String,
    pub payload: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    // Locks the on-disk encoding: `priority as u32` is persisted directly and
    // `TryFrom<u32>` is how we decode. Reordering variants or changing a
    // discriminant would silently break existing databases — this test catches
    // that at compile-of-tests time.
    #[test]
    fn test_job_priority_wire_format_is_stable() {
        assert_eq!(JobPriority::High as u32, 2);
        assert_eq!(JobPriority::Normal as u32, 1);
        assert_eq!(JobPriority::try_from(2).unwrap(), JobPriority::High);
        assert_eq!(JobPriority::try_from(1).unwrap(), JobPriority::Normal);
        assert!(JobPriority::try_from(0).is_err());
        assert!(JobPriority::try_from(3).is_err());
    }
}
