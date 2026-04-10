use std::time::SystemTime;

pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

pub struct Job {
    pub id: String,
    pub status: JobStatus,
    pub retry_count: u32,
    pub task: Task,
    pub max_retries: u32,
    pub created_at: SystemTime,
}

pub struct DeadLetterJob {
    pub id: String,
    pub original_job_id: String,
    pub task: Task,
    pub error: String,
    pub failed_at: SystemTime,
}

pub struct Task {
    pub id: String,
    pub payload: String,
}

pub enum WorkerStatus {
    Idle,
    Busy,
    ShuttingDown,
}

pub struct Worker {
    pub id: String,
    pub status: WorkerStatus,
    pub current_job_id: Option<String>,
}
