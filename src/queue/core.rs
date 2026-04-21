use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use crate::consumer::RegistryConsumer;
use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobPriority, Worker, WorkerStatus};
use crate::persistence::JobRepository;
use crate::task::{TaskRegistry, generate_job_id};

use super::config::QueueConfig;
use super::levels::JobQueues;
use super::worker;

pub struct Queue {
    pub(crate) workers: Vec<Arc<Mutex<Worker>>>,
    pub(crate) jobs: Arc<(Mutex<JobQueues>, Condvar)>,
    pub(crate) dead_letter_jobs: Arc<Mutex<VecDeque<DeadLetterJob>>>,
    pub(crate) job_repository: Arc<dyn JobRepository>,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) config: QueueConfig,
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
        })
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

#[cfg(test)]
mod tests;
