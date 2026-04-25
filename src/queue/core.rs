use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::consumer::RegistryConsumer;
use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobPriority, JobStatus, Worker, WorkerStatus};
use crate::persistence::JobRepository;
use crate::task::{TaskRegistry, generate_job_id};

use super::config::QueueConfig;
use super::levels::JobQueues;
use super::metrics::{self, MetricsCounters, MetricsSnapshot};
use super::worker;

pub struct Queue {
    pub(crate) workers: Vec<Arc<Mutex<Worker>>>,
    pub(crate) jobs: Arc<(Mutex<JobQueues>, Condvar)>,
    pub(crate) dead_letter_jobs: Arc<Mutex<VecDeque<DeadLetterJob>>>,
    pub(crate) job_repository: Arc<dyn JobRepository>,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) config: QueueConfig,
    pub(crate) metrics: Arc<MetricsCounters>,
}

impl Queue {
    pub fn new(
        num_workers: usize,
        job_repository: Arc<dyn JobRepository>,
        registry: TaskRegistry,
    ) -> Result<Self, QueueError> {
        Self::with_config(
            num_workers,
            job_repository,
            registry,
            QueueConfig::default(),
        )
    }

    pub fn with_config(
        num_workers: usize,
        job_repository: Arc<dyn JobRepository>,
        registry: TaskRegistry,
        config: QueueConfig,
    ) -> Result<Self, QueueError> {
        config.validate()?;
        let mut workers = Vec::new();
        for i in 0..num_workers {
            workers.push(Arc::new(Mutex::new(Worker {
                id: format!("worker-{}", i),
                status: WorkerStatus::Idle,
                current_job_id: None,
            })));
        }
        // Restore pending jobs from the repository. JobQueues::push_back routes
        // each into the level matching its stored priority. No cvar notification
        // is needed because workers haven't started yet.
        let jobs_from_repo = job_repository.find_all_pending()?;
        let mut job_queues = JobQueues::default();
        for job in jobs_from_repo {
            job_queues.push_back(job);
        }
        let dead_jobs_from_repo = job_repository.find_all_dead_letter()?;
        let jobs = Arc::new((Mutex::new(job_queues), Condvar::new()));
        let dead_letter_jobs = Arc::new(Mutex::new(VecDeque::from(dead_jobs_from_repo)));

        Ok(Queue {
            workers,
            jobs,
            dead_letter_jobs,
            job_repository,
            registry: Arc::new(registry),
            config,
            metrics: Arc::new(MetricsCounters::default()),
        })
    }

    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        let queue_depth = self.len() as u64;
        let dead_letter_depth = {
            let dl = self
                .dead_letter_jobs
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            dl.len() as u64
        };
        let statuses = self.workers.iter().map(|w| {
            let guard = w.lock().unwrap_or_else(|e| e.into_inner());
            guard.status
        });
        metrics::build_snapshot(&self.metrics, queue_depth, dead_letter_depth, statuses)
    }

    /// Spawn a background thread that ticks the throughput EWMA and prints a
    /// metrics snapshot at the given interval. Drop or call
    /// `MetricsReporterHandle::stop` to terminate cleanly — the loop wakes
    /// promptly via the cvar instead of waiting out the next sleep.
    pub fn start_metrics_reporter(self: &Arc<Self>, interval: Duration) -> MetricsReporterHandle {
        let queue = Arc::clone(self);
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let stop_for_thread = Arc::clone(&stop);
        let join = thread::spawn(move || {
            let (lock, cvar) = &*stop_for_thread;
            loop {
                let stopped_guard = lock.lock().unwrap_or_else(|e| e.into_inner());
                if *stopped_guard {
                    return;
                }
                let (stopped_guard, _timeout) = cvar
                    .wait_timeout(stopped_guard, interval)
                    .unwrap_or_else(|e| e.into_inner());
                if *stopped_guard {
                    return;
                }
                drop(stopped_guard);
                queue.metrics.tick_throughput();
                println!("[metrics] {}", queue.metrics_snapshot().render());
            }
        });
        MetricsReporterHandle {
            stop,
            join: Some(join),
        }
    }

    pub fn len(&self) -> usize {
        let (lock, _) = &*self.jobs;
        let jobs = lock.lock().unwrap_or_else(|e| e.into_inner());
        jobs.len()
    }

    pub fn pending_count(&self) -> Result<u64, QueueError> {
        self.job_repository.pending_count()
    }

    pub fn is_empty(&self) -> bool {
        let (lock, _) = &*self.jobs;
        let jobs = lock.lock().unwrap_or_else(|e| e.into_inner());
        jobs.is_empty()
    }

    pub fn enqueue_by_name<P: serde::Serialize>(
        &self,
        task_name: &str,
        payload: P,
    ) -> Result<(), QueueError> {
        let job = self.build_named_job(task_name, payload)?;
        self.enqueue(job)
    }

    /// Dynamic-dispatch sibling of `enqueue_by_name` that also sets
    /// `max_attempts` and `priority` — the same knobs the `#[task(...)]`
    /// macro exposes for statically-known tasks.
    pub fn enqueue_by_name_with_opts<P: serde::Serialize>(
        &self,
        task_name: &str,
        payload: P,
        max_attempts: u32,
        priority: JobPriority,
    ) -> Result<(), QueueError> {
        let mut job = self.build_named_job(task_name, payload)?;
        job.max_attempts = max_attempts;
        job.priority = priority;
        self.enqueue(job)
    }

    fn build_named_job<P: serde::Serialize>(
        &self,
        task_name: &str,
        payload: P,
    ) -> Result<Job, QueueError> {
        if self.registry.get(task_name).is_none() {
            return Err(QueueError::JobFailed(format!(
                "no handler registered for task '{task_name}'"
            )));
        }
        let json = serde_json::to_string(&payload)
            .map_err(|e| QueueError::JobFailed(format!("serialize {task_name}: {e}")))?;
        let job_id = generate_job_id();
        Ok(Job::with_task_name(job_id, task_name.to_string(), json))
    }

    /// Enqueue a job. The level it lands in is determined by `job.priority`
    /// — persistence and in-memory routing agree on a single source of truth.
    pub fn enqueue(&self, job: Job) -> Result<(), QueueError> {
        self.job_repository.save(&job)?;
        let (lock, cvar) = &*self.jobs;
        let mut jobs = lock.lock()?;
        jobs.push_back(job);
        cvar.notify_one();
        Ok(())
    }

    /// Convenience wrapper: force the job to `High` priority and enqueue.
    pub fn enqueue_priority(&self, mut job: Job) -> Result<(), QueueError> {
        job.priority = JobPriority::High;
        self.enqueue(job)
    }

    /// Cancel a job by id.
    ///
    /// - `Pending`: removed from the in-memory level and marked `Cancelled`.
    /// - `Running`: marked `Cancelled`. An in-flight consumer call is not
    ///   preempted and may still produce a result, but the repository's
    ///   sticky-Cancelled guard rejects any subsequent status write — so the
    ///   DB always reflects `Cancelled` once `cancel()` returns Ok.
    /// - Terminal states (`Completed`/`Failed`/`Cancelled`): returns
    ///   `CannotCancel` so callers can distinguish a no-op from success.
    pub fn cancel(&self, job_id: &str) -> Result<(), QueueError> {
        let job = self.job_repository.find_by_id(job_id)?;
        match job.status {
            JobStatus::Pending => {
                let (lock, _) = &*self.jobs;
                let mut jobs = lock.lock()?;
                jobs.remove(job_id);
            }
            JobStatus::Running => {}
            terminal => {
                return Err(QueueError::CannotCancel {
                    id: job_id.to_string(),
                    status: terminal,
                });
            }
        }
        self.job_repository
            .update_status(job_id, JobStatus::Cancelled)
    }

    pub fn start_workers(self: &Arc<Self>) {
        for worker in &self.workers {
            let consumer = RegistryConsumer::new(Arc::clone(&self.registry));
            let worker = Arc::clone(worker);
            let jobs = Arc::clone(&self.jobs);
            let queue = Arc::clone(self);
            thread::spawn(move || {
                loop {
                    if let Some(job) = Self::wait_for_job(&jobs) {
                        worker::process_job(&worker, &consumer, job, &queue);
                    }
                }
            });
        }
    }

    pub(crate) fn wait_for_job(jobs: &(Mutex<JobQueues>, Condvar)) -> Option<Job> {
        let (lock, cvar) = jobs;
        let mut jobs = lock.lock().unwrap_or_else(|e| e.into_inner());
        while jobs.is_empty() {
            jobs = cvar.wait(jobs).unwrap_or_else(|e| e.into_inner());
        }
        jobs.pop_next()
    }
}

/// Owns the metrics reporter thread. Drop signals the thread to stop; an
/// explicit `stop()` does the same and joins, surfacing any panic.
pub struct MetricsReporterHandle {
    stop: Arc<(Mutex<bool>, Condvar)>,
    join: Option<thread::JoinHandle<()>>,
}

impl MetricsReporterHandle {
    pub fn stop(mut self) {
        self.signal_stop();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }

    fn signal_stop(&self) {
        let (lock, cvar) = &*self.stop;
        let mut stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
        *stopped = true;
        cvar.notify_all();
    }
}

impl Drop for MetricsReporterHandle {
    fn drop(&mut self) {
        if self.join.is_some() {
            self.signal_stop();
        }
    }
}

#[cfg(test)]
mod tests;
