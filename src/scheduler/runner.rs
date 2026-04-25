use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::QueueError;
use crate::models::Job;
use crate::producer::Producer;

use super::model::{ScheduledJob, ScheduledJobRepository, next_fire_after};

pub struct Scheduler {
    repo: Arc<dyn ScheduledJobRepository>,
    producer: Arc<dyn Producer>,
}

impl Scheduler {
    pub fn new(repo: Arc<dyn ScheduledJobRepository>, producer: Arc<dyn Producer>) -> Self {
        Self { repo, producer }
    }

    /// Spawns the scheduler thread. The thread wakes every `tick_interval` and
    /// calls `tick`, or earlier if `SchedulerHandle::stop` is invoked.
    pub fn start(self, tick_interval: Duration) -> SchedulerHandle {
        let shutdown = Arc::new((Mutex::new(false), Condvar::new()));
        let shutdown_thread = Arc::clone(&shutdown);

        let handle = thread::Builder::new()
            .name("scheduler".into())
            .spawn(move || {
                let (lock, cvar) = &*shutdown_thread;
                loop {
                    // Run a tick; log-and-continue on error so one bad row
                    // doesn't kill the scheduler thread.
                    if let Err(e) = self.tick(SystemTime::now()) {
                        eprintln!("scheduler tick error: {e}");
                    }

                    let stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
                    if *stopped {
                        break;
                    }
                    let (stopped, _) = cvar
                        .wait_timeout(stopped, tick_interval)
                        .unwrap_or_else(|e| e.into_inner());
                    if *stopped {
                        break;
                    }
                }
            })
            .expect("failed to spawn scheduler thread");

        SchedulerHandle {
            shutdown,
            thread: Some(handle),
        }
    }

    /// Evaluates all enabled scheduled jobs and enqueues any whose
    /// `next_run_at <= now`. Returns the number of jobs enqueued.
    ///
    /// Semantics — **at-least-once delivery, exactly-once enqueue per slot:**
    /// - Slot-keyed ids: each fire uses `job.id = "sched:{scheduled.id}:{slot_epoch}"`
    ///   where the slot is the pre-advance `next_run_at`. If `update_after_fire`
    ///   fails after `produce` succeeds, the next tick re-fires the same slot
    ///   and the queue's `AlreadyExists` check drops the duplicate at the
    ///   enqueue boundary — the handler runs once per slot.
    /// - No catch-up: `next_run_at` is computed from `now`, not from the
    ///   skipped slot. Missed slots from pause/restart are dropped to avoid
    ///   a restart-time thundering herd. Thread a catch-up policy through
    ///   config if per-slot guarantees become required.
    /// - Per-row isolation: a failing row logs and is skipped; the rest of
    ///   the due batch still fires this tick.
    pub fn tick(&self, now: SystemTime) -> Result<usize, QueueError> {
        let due = self.repo.find_due(now)?;

        let mut fired = 0;
        for scheduled in due {
            match self.fire_one(&scheduled, now) {
                Ok(()) => fired += 1,
                Err(e) => eprintln!("scheduler: failed to fire {}: {e}", scheduled.id),
            }
        }
        Ok(fired)
    }

    fn fire_one(&self, scheduled: &ScheduledJob, now: SystemTime) -> Result<(), QueueError> {
        let job_id = slot_job_id(&scheduled.id, scheduled.next_run_at);
        let job = Job::with_task_name(
            job_id,
            scheduled.task_name.clone(),
            scheduled.payload.clone(),
        );
        match self.producer.produce(job) {
            Ok(()) => {}
            // A prior tick enqueued this slot but failed to advance — the
            // row is due again and we see our own leftover. Fall through to
            // advance `next_run_at`; the handler has the queued job already.
            Err(QueueError::AlreadyExists(_)) => {}
            Err(e) => return Err(e),
        }
        let next = next_fire_after(&scheduled.cron_expression, now)?;
        self.repo.update_after_fire(&scheduled.id, now, next)?;
        Ok(())
    }
}

/// Deterministic id for a scheduled fire. Keyed by `(scheduled.id, slot_epoch)`
/// so re-fires of an unadvanced slot collide on the job table's primary key.
/// Sub-second resolution: the slot epoch is whole seconds, matching the
/// persistence layer's granularity.
fn slot_job_id(scheduled_id: &str, slot: SystemTime) -> String {
    let slot_epoch = slot
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("sched:{scheduled_id}:{slot_epoch}")
}

pub struct SchedulerHandle {
    shutdown: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl SchedulerHandle {
    /// Signals the scheduler thread to stop and blocks until it exits.
    pub fn stop(mut self) {
        self.shutdown_and_join();
    }

    fn signal_stop(&self) {
        let (lock, cvar) = &*self.shutdown;
        let mut stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
        *stopped = true;
        cvar.notify_all();
    }

    fn shutdown_and_join(&mut self) {
        self.signal_stop();
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for SchedulerHandle {
    fn drop(&mut self) {
        self.shutdown_and_join();
    }
}

#[cfg(test)]
mod tests;
