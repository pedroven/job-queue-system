use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::consumer::RegistryConsumer;
use crate::error::QueueError;
use crate::models::{Job, JobPriority, JobStatus, Worker, WorkerStatus};
use crate::persistence::{JobDispatch, JobState};
use crate::task::{TaskRegistry, generate_job_id};

use super::config::QueueConfig;
use super::metrics::{self, MetricsCounters, MetricsSnapshot};
use super::partition;
use super::reaper::{Reaper, ReaperThread};
use super::worker;

/// The queue. Owns dispatch (hot path) + state (slow path) + per-partition
/// worker threads. There is no in-memory mirror — `dispatch` is the source
/// of truth for routing.
pub struct Queue {
    pub(crate) workers: Vec<Arc<Mutex<Worker>>>,
    pub(crate) dispatch: Arc<dyn JobDispatch>,
    pub(crate) state: Arc<dyn JobState>,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) config: QueueConfig,
    pub(crate) metrics: Arc<MetricsCounters>,
    /// Per-queue consumer identifier — included in Redis `XREADGROUP` so
    /// the consumer group can attribute PEL entries to this process.
    pub(crate) consumer_id: String,
}

impl Queue {
    pub fn new(
        dispatch: Arc<dyn JobDispatch>,
        state: Arc<dyn JobState>,
        registry: TaskRegistry,
    ) -> Result<Self, QueueError> {
        Self::with_config(dispatch, state, registry, QueueConfig::default())
    }

    pub fn with_config(
        dispatch: Arc<dyn JobDispatch>,
        state: Arc<dyn JobState>,
        registry: TaskRegistry,
        config: QueueConfig,
    ) -> Result<Self, QueueError> {
        config.validate()?;
        let consumer_id = format!(
            "queue-{}-{}",
            std::process::id(),
            generate_job_id().chars().take(8).collect::<String>()
        );
        let mut workers = Vec::with_capacity(config.assigned_partitions.len());
        for (i, p) in config.assigned_partitions.iter().enumerate() {
            workers.push(Arc::new(Mutex::new(Worker {
                id: format!("worker-p{p}-{i}"),
                status: WorkerStatus::Idle,
                current_job_id: None,
            })));
        }
        Ok(Queue {
            workers,
            dispatch,
            state,
            registry: Arc::new(registry),
            config,
            metrics: Arc::new(MetricsCounters::default()),
            consumer_id,
        })
    }

    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        let queue_depth = self.pending_count().unwrap_or(0);
        let dead_letter_depth = self
            .state
            .find_all_dead_letter()
            .map(|v| v.len() as u64)
            .unwrap_or(0);
        let statuses = self.workers.iter().map(|w| {
            let guard = w.lock().unwrap_or_else(|e| e.into_inner());
            guard.status
        });
        metrics::build_snapshot(&self.metrics, queue_depth, dead_letter_depth, statuses)
    }

    /// Spawn a background thread that ticks the throughput EWMA and prints
    /// a metrics snapshot at the given interval. Drop or call
    /// `MetricsReporterHandle::stop` to terminate cleanly.
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

    pub fn start_reaper(self: &Arc<Self>) -> ReaperHandle {
        let reaper = Reaper::new(
            Arc::clone(&self.dispatch),
            Arc::clone(&self.state),
            self.config.assigned_partitions.clone(),
            self.config.claim_lease,
        );
        ReaperHandle {
            inner: ReaperThread::spawn(reaper, self.config.reaper_interval),
        }
    }

    pub fn pending_count(&self) -> Result<u64, QueueError> {
        self.dispatch.pending_count()
    }

    pub fn is_empty(&self) -> Result<bool, QueueError> {
        Ok(self.pending_count()? == 0)
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
    /// `max_attempts` and `priority`.
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
        Ok(Job::with_task_name(
            generate_job_id(),
            task_name.to_string(),
            json,
        ))
    }

    /// Persist initial state, then route the job to its partition's
    /// dispatch backend. Ordering matters: if dispatch.enqueue races a
    /// worker before state is written, the worker can find_by_id and see
    /// nothing — so state.save_initial happens first.
    pub fn enqueue(&self, job: Job) -> Result<(), QueueError> {
        self.state.save_initial(&job)?;
        let partition = partition::partition_for(&job.id, self.config.partition_count);
        self.dispatch.enqueue(partition, &job)
    }

    /// Convenience wrapper: force the job to `High` priority and enqueue.
    pub fn enqueue_priority(&self, mut job: Job) -> Result<(), QueueError> {
        job.priority = JobPriority::High;
        self.enqueue(job)
    }

    /// Cancel a job by id.
    ///
    /// - `Pending`/`Running`: sets status to `Cancelled` in state. The
    ///   sticky-Cancelled guard in `JobState::save_status` ensures any
    ///   later status write from the worker is dropped. We don't try to
    ///   evict the stream entry — workers re-check status before running
    ///   the handler and ack the entry as a no-op.
    /// - Terminal states (`Completed`/`Failed`/`Cancelled`): returns
    ///   `CannotCancel` so callers can distinguish a no-op from success.
    pub fn cancel(&self, job_id: &str) -> Result<(), QueueError> {
        let job = self.state.find_by_id(job_id)?;
        match job.status {
            JobStatus::Pending | JobStatus::Running => {
                self.state.save_status(job_id, JobStatus::Cancelled)
            }
            terminal => Err(QueueError::CannotCancel {
                id: job_id.to_string(),
                status: terminal,
            }),
        }
    }

    pub fn start_workers(self: &Arc<Self>) {
        for (worker, &partition) in self
            .workers
            .iter()
            .zip(self.config.assigned_partitions.iter())
        {
            let consumer = RegistryConsumer::new(Arc::clone(&self.registry));
            let worker = Arc::clone(worker);
            let queue = Arc::clone(self);
            thread::spawn(move || {
                worker::run_partition_loop(&queue, &worker, &consumer, partition);
            });
        }
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

pub struct ReaperHandle {
    inner: ReaperThread,
}

impl ReaperHandle {
    pub fn stop(self) {
        self.inner.stop();
    }
}

#[cfg(test)]
mod tests;
