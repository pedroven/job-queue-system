use crate::consumer::RegistryConsumer;
use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobPriority, Worker, WorkerStatus};
use crate::persistence::JobRepository;
use crate::task::{TaskRegistry, generate_job_id};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

mod worker;

/// In-memory staging for pending jobs, keyed by `JobPriority` discriminant
/// (`u32` via `#[repr(u32)]`). Higher discriminant = higher priority.
/// Adding a new `JobPriority` variant requires no changes here: `push_back`
/// creates the level on demand and `pop_next` iterates highest-first.
#[derive(Default)]
pub struct JobQueues {
    levels: BTreeMap<u32, VecDeque<Job>>,
}

impl JobQueues {
    pub(crate) fn is_empty(&self) -> bool {
        self.levels.values().all(VecDeque::is_empty)
    }

    pub(crate) fn len(&self) -> usize {
        self.levels.values().map(VecDeque::len).sum()
    }

    pub(crate) fn push_back(&mut self, job: Job) {
        let level = job.priority as u32;
        self.levels.entry(level).or_default().push_back(job);
    }

    fn pop_next(&mut self) -> Option<Job> {
        // BTreeMap iterates in ascending key order; reverse for highest-first.
        for queue in self.levels.values_mut().rev() {
            if let Some(job) = queue.pop_front() {
                return Some(job);
            }
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn queue_for(&self, priority: JobPriority) -> Option<&VecDeque<Job>> {
        self.levels.get(&(priority as u32))
    }
}

/// Tunable parameters for a `Queue`. All fields have sensible defaults via
/// `QueueConfig::default()`; override selectively when constructing a queue
/// via `Queue::with_config`.
///
/// Back-pressure note: the hard threshold is advisory, not an invariant.
/// `pending_count()` and `enqueue()` are not serialized, so under concurrent
/// producers the actual depth may briefly exceed `backpressure_hard_threshold`
/// by up to `N-1` (N = live producers). Set the threshold below the real
/// capacity if strict bounding matters.
#[derive(Clone, Debug)]
pub struct QueueConfig {
    /// Retry backoff base for failed jobs (`backoff_base * 2^attempt`).
    pub retry_backoff_base: Duration,
    /// Pending-job count at which producers start to throttle.
    pub backpressure_soft_threshold: u64,
    /// Pending-job count at which producers are rejected with `QueueFull`.
    pub backpressure_hard_threshold: u64,
    /// Sleep applied on each produce call once depth is between soft and hard.
    pub backpressure_delay: Duration,
}

impl QueueConfig {
    fn validate(&self) -> Result<(), QueueError> {
        if self.backpressure_hard_threshold == 0 {
            return Err(QueueError::InvalidConfig(
                "backpressure_hard_threshold must be > 0".into(),
            ));
        }
        if self.backpressure_soft_threshold > self.backpressure_hard_threshold {
            return Err(QueueError::InvalidConfig(format!(
                "backpressure_soft_threshold ({}) must be <= backpressure_hard_threshold ({})",
                self.backpressure_soft_threshold, self.backpressure_hard_threshold
            )));
        }
        Ok(())
    }
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            retry_backoff_base: Duration::from_secs(1),
            backpressure_soft_threshold: 80,
            backpressure_hard_threshold: 100,
            backpressure_delay: Duration::from_millis(50),
        }
    }
}

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
mod tests {
    use super::*;
    use crate::consumer::{self, Consumer};
    use crate::models::{self, testing::make_test_job};
    use crate::persistence::{InMemoryJobRepository, JobRepository, SqliteJobRepository};
    use std::sync::Mutex as StdMutex;
    use std::time::{Duration, UNIX_EPOCH};

    fn default_registry() -> TaskRegistry {
        let mut registry = TaskRegistry::new();
        registry.register("default", |_| Ok(()));
        registry
    }

    fn create_queue(num_workers: usize) -> Queue {
        Queue::new(
            num_workers,
            Arc::new(InMemoryJobRepository::new()),
            default_registry(),
        )
        .unwrap()
    }

    fn create_queue_with_repo(repo: Arc<dyn JobRepository>) -> Arc<Queue> {
        Arc::new(Queue::new(0, repo, default_registry()).unwrap())
    }

    struct FailNTimesConsumer {
        remaining_failures: StdMutex<u32>,
    }

    impl FailNTimesConsumer {
        fn new(failures: u32) -> Self {
            Self {
                remaining_failures: StdMutex::new(failures),
            }
        }
    }

    impl Consumer for FailNTimesConsumer {
        fn consume(&self, _job: &Job) -> Result<(), QueueError> {
            let mut remaining = self.remaining_failures.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                Err(QueueError::JobFailed("simulated failure".to_string()))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn test_queue_new_creates_correct_number_of_workers() {
        let queue = create_queue(4);
        assert_eq!(queue.workers.len(), 4);
    }

    #[test]
    fn test_queue_new_workers_start_idle() {
        let queue = create_queue(2);
        for worker in &queue.workers {
            let w = worker.lock().unwrap();
            assert!(matches!(w.status, WorkerStatus::Idle));
            assert!(w.current_job_id.is_none());
        }
    }

    #[test]
    fn test_queue_new_zero_workers() {
        let queue = create_queue(0);
        assert_eq!(queue.workers.len(), 0);
    }

    #[test]
    fn test_enqueue_adds_job_to_queue() {
        let queue = create_queue(0);
        let job = make_test_job("job-1", "test payload");
        queue.enqueue(job).unwrap();

        let (lock, _) = &*queue.jobs;
        let jobs = lock.lock().unwrap();
        assert_eq!(jobs.len(), 1);
        let normal = jobs.queue_for(JobPriority::Normal).unwrap();
        assert_eq!(normal[0].id, "job-1");
    }

    #[test]
    fn test_enqueue_preserves_fifo_order() {
        let queue = create_queue(0);
        queue.enqueue(make_test_job("job-1", "first")).unwrap();
        queue.enqueue(make_test_job("job-2", "second")).unwrap();
        queue.enqueue(make_test_job("job-3", "third")).unwrap();

        let (lock, _) = &*queue.jobs;
        let jobs = lock.lock().unwrap();
        assert_eq!(jobs.len(), 3);
        let normal = jobs.queue_for(JobPriority::Normal).unwrap();
        assert_eq!(normal[0].id, "job-1");
        assert_eq!(normal[1].id, "job-2");
        assert_eq!(normal[2].id, "job-3");
    }

    #[test]
    fn test_wait_for_job_returns_front_job() {
        let jobs = Arc::new((Mutex::new(JobQueues::default()), Condvar::new()));
        {
            let (lock, _) = &*jobs;
            let mut q = lock.lock().unwrap();
            q.push_back(make_test_job("job-1", "payload"));
        }
        let result = Queue::wait_for_job(&jobs);
        assert!(result.is_some());
        assert_eq!(result.unwrap().id, "job-1");
    }

    #[test]
    fn test_enqueue_priority_drains_before_normal() {
        let queue = create_queue(0);
        queue.enqueue(make_test_job("n-1", "normal")).unwrap();
        queue
            .enqueue_priority(make_test_job("p-1", "priority"))
            .unwrap();
        queue.enqueue(make_test_job("n-2", "normal")).unwrap();

        let first = Queue::wait_for_job(&queue.jobs).unwrap();
        assert_eq!(first.id, "p-1");
        let second = Queue::wait_for_job(&queue.jobs).unwrap();
        assert_eq!(second.id, "n-1");
        let third = Queue::wait_for_job(&queue.jobs).unwrap();
        assert_eq!(third.id, "n-2");
    }

    #[test]
    fn test_enqueue_priority_persists_with_high_priority() {
        let repo = Arc::new(InMemoryJobRepository::new());
        let queue = Queue::new(0, repo.clone(), default_registry()).unwrap();
        queue
            .enqueue_priority(make_test_job("p-1", "payload"))
            .unwrap();

        let persisted = repo.find_by_id("p-1").unwrap();
        assert_eq!(persisted.priority, models::JobPriority::High);
    }

    #[test]
    fn test_restart_reloads_priority_first_then_fifo() {
        // SqliteJobRepository enforces `ORDER BY priority DESC, created_at ASC`
        // on recovery, and Queue::new routes each restored job into the matching
        // level. The combination must yield: all High jobs first (in insertion
        // order), then all Normal jobs (in insertion order).
        let repo = Arc::new(SqliteJobRepository::new(":memory:").unwrap());

        let mut n1 = make_test_job("n-1", "normal-first");
        n1.created_at = UNIX_EPOCH + Duration::from_secs(1);
        repo.save(&n1).unwrap();

        let mut p1 = make_test_job("p-1", "priority-first");
        p1.priority = models::JobPriority::High;
        p1.created_at = UNIX_EPOCH + Duration::from_secs(2);
        repo.save(&p1).unwrap();

        let mut n2 = make_test_job("n-2", "normal-second");
        n2.created_at = UNIX_EPOCH + Duration::from_secs(3);
        repo.save(&n2).unwrap();

        let mut p2 = make_test_job("p-2", "priority-second");
        p2.priority = models::JobPriority::High;
        p2.created_at = UNIX_EPOCH + Duration::from_secs(4);
        repo.save(&p2).unwrap();

        let queue = Queue::new(0, repo.clone(), default_registry()).unwrap();

        let drained: Vec<String> = (0..4)
            .map(|_| Queue::wait_for_job(&queue.jobs).unwrap().id)
            .collect();
        assert_eq!(drained, vec!["p-1", "p-2", "n-1", "n-2"]);
    }

    #[test]
    fn test_process_job_updates_worker_status() {
        let worker = Mutex::new(Worker {
            id: "worker-0".to_string(),
            status: WorkerStatus::Idle,
            current_job_id: None,
        });
        let consumer = consumer::JobConsumer;
        let job = make_test_job("job-1", "payload");

        worker::process_job(&worker, &consumer, job, &Arc::new(create_queue(0)));

        let w = worker.lock().unwrap();
        assert!(matches!(w.status, WorkerStatus::Idle));
        assert!(w.current_job_id.is_none());
    }

    #[test]
    fn test_workers_consume_enqueued_jobs() {
        let queue = Arc::new(create_queue(2));
        queue.start_workers();

        queue.enqueue(make_test_job("job-1", "hello")).unwrap();
        queue.enqueue(make_test_job("job-2", "world")).unwrap();

        thread::sleep(Duration::from_millis(100));

        let (lock, _) = &*queue.jobs;
        let jobs = lock.lock().unwrap();
        assert_eq!(jobs.len(), 0);
    }

    #[test]
    fn test_multiple_jobs_processed_concurrently() {
        let queue = Arc::new(create_queue(4));
        queue.start_workers();

        for i in 0..10 {
            queue
                .enqueue(make_test_job(
                    &format!("job-{}", i),
                    &format!("payload-{}", i),
                ))
                .unwrap();
        }

        thread::sleep(Duration::from_millis(200));

        let (lock, _) = &*queue.jobs;
        let jobs = lock.lock().unwrap();
        assert_eq!(jobs.len(), 0);
    }

    #[test]
    fn test_job_succeeds_on_first_attempt() {
        let repo = Arc::new(InMemoryJobRepository::new());
        let queue = create_queue_with_repo(repo.clone());
        let job = make_test_job("job-1", "payload");
        repo.save(&job).unwrap();

        let consumer = consumer::JobConsumer;
        worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 0);

        let dl = repo.find_all_dead_letter().unwrap();
        assert_eq!(dl.len(), 0);
    }

    #[test]
    fn test_job_succeeds_after_retries() {
        let repo = Arc::new(InMemoryJobRepository::new());
        let queue = create_queue_with_repo(repo.clone());
        let job = make_test_job("job-1", "payload");
        repo.save(&job).unwrap();

        let consumer = FailNTimesConsumer::new(2);
        worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 0);

        let dl = repo.find_all_dead_letter().unwrap();
        assert_eq!(dl.len(), 0);
    }

    #[test]
    fn test_job_exhausts_retries_moves_to_dlq() {
        let repo = Arc::new(InMemoryJobRepository::new());
        let queue = create_queue_with_repo(repo.clone());
        let job = make_test_job("job-1", "payload");
        repo.save(&job).unwrap();

        let consumer = FailNTimesConsumer::new(5);
        worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

        let dl = repo.find_all_dead_letter().unwrap();
        assert_eq!(dl.len(), 1);
        assert_eq!(dl[0].original_job_id, "job-1");
        assert_eq!(dl[0].error, "job failed: simulated failure");

        let persisted = repo.find_by_id("job-1").unwrap();
        assert_eq!(persisted.retry_count, 3);
        assert_eq!(persisted.status, models::JobStatus::Failed);
    }

    #[test]
    fn test_retry_count_is_persisted() {
        let repo = Arc::new(InMemoryJobRepository::new());
        let queue = create_queue_with_repo(repo.clone());
        let job = make_test_job("job-1", "payload");
        repo.save(&job).unwrap();

        let consumer = FailNTimesConsumer::new(2);
        worker::handle_job_tries(&queue, &consumer, job, Duration::ZERO).unwrap();

        let persisted = repo.find_by_id("job-1").unwrap();
        assert_eq!(persisted.retry_count, 2);
        assert_eq!(persisted.status, models::JobStatus::Completed);

        let dl = repo.find_all_dead_letter().unwrap();
        assert_eq!(dl.len(), 0);
    }
}
