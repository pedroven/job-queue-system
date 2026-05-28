use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use crate::consumer::Consumer;
use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobStatus, Worker, WorkerStatus};
use crate::persistence::ClaimedJob;
use crate::queue::Queue;

/// Per-partition main loop. Spawned once per assigned partition by
/// `Queue::start_workers`; runs until the process exits.
pub(crate) fn run_partition_loop(
    queue: &Arc<Queue>,
    worker: &Mutex<Worker>,
    consumer: &dyn Consumer,
    partition: u32,
) {
    loop {
        let claim = match queue.dispatch.next_for_partition(
            partition,
            &queue.consumer_id,
            queue.config.worker_block_duration,
        ) {
            Ok(Some(c)) => c,
            // Timeout — loop again. No sleep needed; the dispatch impl
            // already blocked for `worker_block_duration`.
            Ok(None) => continue,
            Err(e) => {
                eprintln!("worker p{partition} next_for_partition error: {e}");
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        process_claim(queue, worker, consumer, partition, claim);
    }
}

pub(crate) fn process_claim(
    queue: &Arc<Queue>,
    worker: &Mutex<Worker>,
    consumer: &dyn Consumer,
    partition: u32,
    claim: ClaimedJob,
) {
    let job_id = claim.job.id.clone();
    {
        let mut w = worker.lock().unwrap_or_else(|e| e.into_inner());
        w.status = WorkerStatus::Busy;
        w.current_job_id = Some(job_id.clone());
    }

    let cancelled_before_start = matches!(
        queue.state.find_by_id(&job_id),
        Ok(j) if j.status == JobStatus::Cancelled
    );

    if !cancelled_before_start {
        if let Err(e) = queue.state.save_status(&job_id, JobStatus::Running) {
            eprintln!("worker: failed to mark {job_id} Running: {e}");
        }
        if let Err(e) = handle_job_tries(
            queue,
            consumer,
            claim.job.clone(),
            queue.config.retry_backoff_base,
        ) {
            eprintln!("worker: handle_job_tries error: {e}");
        }
    }

    // Always ack — even cancelled jobs need their dispatch entry cleared
    // so the partition stream doesn't accumulate ghosts.
    if let Err(e) = queue.dispatch.ack(partition, &claim) {
        eprintln!("worker: ack failed for {job_id}: {e}");
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
        if matches!(
            queue.state.find_by_id(&job.id),
            Ok(j) if j.status == JobStatus::Cancelled
        ) {
            return Ok(());
        }
        match consumer.consume(&job) {
            Ok(()) => {
                queue.state.save_status(&job.id, JobStatus::Completed)?;
                queue.metrics.record_completed();
                return Ok(());
            }
            Err(e) => {
                job.retry_count += 1;
                last_error = Some(e.to_string());
                queue.state.save_status(&job.id, JobStatus::Failed)?;
                queue.state.save_retry_count(&job.id, job.retry_count)?;
                queue.metrics.record_failed_attempt();
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
    queue.state.save_dead_letter(&dead_letter_job)?;
    queue.metrics.record_dead_lettered();
    Ok(())
}
