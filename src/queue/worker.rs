use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use crate::consumer::Consumer;
use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobStatus, Worker, WorkerStatus};
use crate::queue::Queue;

pub(crate) fn process_job(
    worker: &Mutex<Worker>,
    consumer: &dyn Consumer,
    job: Job,
    queue: &Arc<Queue>,
) {
    let job_id = job.id.clone();
    {
        let mut w = worker.lock().unwrap_or_else(|e| e.into_inner());
        w.status = WorkerStatus::Busy;
        w.current_job_id = Some(job_id.clone());
    }

    if let Err(e) = queue
        .job_repository
        .update_status(&job_id, JobStatus::Running)
    {
        eprintln!("Failed to update job {job_id} to Running: {e}");
    }

    if let Err(e) = handle_job_tries(queue, consumer, job, queue.backoff_base) {
        eprintln!("Failed to handle job tries: {e}");
    }

    {
        let mut w = worker.lock().unwrap_or_else(|e| e.into_inner());
        w.status = WorkerStatus::Idle;
        w.current_job_id = None;
    }
}

pub(crate) fn handle_job_tries(
    queue: &Arc<Queue>,
    consumer: &dyn Consumer,
    mut job: Job,
    backoff_base: Duration,
) -> Result<(), QueueError> {
    let mut last_error = None;

    for attempt in 0..job.max_attempts {
        match consumer.consume(&job) {
            Ok(()) => {
                queue
                    .job_repository
                    .update_status(&job.id, JobStatus::Completed)?;
                return Ok(());
            }
            Err(e) => {
                job.retry_count += 1;
                last_error = Some(e.to_string());
                queue
                    .job_repository
                    .update_status(&job.id, JobStatus::Failed)?;
                queue
                    .job_repository
                    .update_retry_count(&job.id, job.retry_count)?;
                thread::sleep(backoff_base * (1 << (attempt + 1)));
            }
        }
    }

    if let Some(error) = last_error {
        move_to_dead_letter(queue, job, error)?;
    }
    Ok(())
}

fn move_to_dead_letter(queue: &Arc<Queue>, job: Job, error: String) -> Result<(), QueueError> {
    let dead_letter_job = DeadLetterJob {
        id: format!("dl-{}", job.id),
        original_job_id: job.id,
        task: job.task.clone(),
        error,
        failed_at: SystemTime::now(),
    };
    queue.job_repository.save_dead_letter(&dead_letter_job)?;
    let mut dl_jobs = queue.dead_letter_jobs.lock()?;
    dl_jobs.push_back(dead_letter_job);
    Ok(())
}
