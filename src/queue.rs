use crate::consumer::RegistryConsumer;
use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, Worker, WorkerStatus};
use crate::persistence::JobRepository;
use crate::task::{TaskRegistry, generate_job_id};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

mod worker;

pub struct Queue {
    pub(crate) workers: Vec<Arc<Mutex<Worker>>>,
    pub(crate) jobs: Arc<(Mutex<VecDeque<Job>>, Condvar)>,
    pub(crate) dead_letter_jobs: Arc<Mutex<VecDeque<DeadLetterJob>>>,
    pub(crate) job_repository: Arc<dyn JobRepository>,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) backoff_base: std::time::Duration,
}

impl Queue {
    pub fn new(
        num_workers: usize,
        job_repository: Arc<dyn JobRepository>,
        registry: TaskRegistry,
    ) -> Result<Self, QueueError> {
        let mut workers = Vec::new();
        for i in 0..num_workers {
            workers.push(Arc::new(Mutex::new(Worker {
                id: format!("worker-{}", i),
                status: WorkerStatus::Idle,
                current_job_id: None,
            })));
        }
        // Restore pending jobs from the repository. No cvar notification is needed
        // because workers haven't started yet — when they first call wait_for_job,
        // the while-loop check finds the deque non-empty and proceeds immediately.
        let jobs_from_repo = job_repository.find_all_pending()?;
        let dead_jobs_from_repo = job_repository.find_all_dead_letter()?;
        let jobs = Arc::new((Mutex::new(VecDeque::from(jobs_from_repo)), Condvar::new()));
        let dead_letter_jobs = Arc::new(Mutex::new(VecDeque::from(dead_jobs_from_repo)));

        Ok(Queue {
            workers,
            jobs,
            dead_letter_jobs,
            job_repository,
            registry: Arc::new(registry),
            backoff_base: std::time::Duration::from_secs(1),
        })
    }

    pub fn len(&self) -> usize {
        let (lock, _) = &*self.jobs;
        let jobs = lock.lock().unwrap_or_else(|e| e.into_inner());
        jobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn enqueue_by_name<P: serde::Serialize>(
        &self,
        task_name: &str,
        payload: P,
    ) -> Result<(), QueueError> {
        if self.registry.get(task_name).is_none() {
            return Err(QueueError::JobFailed(format!(
                "no handler registered for task '{task_name}'"
            )));
        }
        let json = serde_json::to_string(&payload)
            .map_err(|e| QueueError::JobFailed(format!("serialize {task_name}: {e}")))?;
        let job_id = generate_job_id();
        let job = Job::with_task_name(job_id, task_name.to_string(), json);
        self.enqueue(job)
    }

    pub fn enqueue(&self, job: Job) -> Result<(), QueueError> {
        let (lock, cvar) = &*self.jobs;
        self.job_repository.save(&job)?;
        let mut jobs = lock.lock()?;
        jobs.push_back(job);
        cvar.notify_one();
        Ok(())
    }

    pub fn start_workers(self: &Arc<Self>) {
        for worker in &self.workers {
            let consumer = RegistryConsumer::new(Arc::clone(&self.registry));
            let worker = Arc::clone(worker);
            let jobs = Arc::clone(&self.jobs);
            let queue = Arc::clone(self);
            thread::spawn(move || {
                loop {
                    let job = Self::wait_for_job(&jobs);
                    if let Some(job) = job {
                        worker::process_job(&worker, &consumer, job, &queue);
                    }
                }
            });
        }
    }

    pub(crate) fn wait_for_job(jobs: &(Mutex<VecDeque<Job>>, Condvar)) -> Option<Job> {
        let (lock, cvar) = jobs;
        let mut jobs = lock.lock().unwrap_or_else(|e| e.into_inner());
        while jobs.is_empty() {
            jobs = cvar.wait(jobs).unwrap_or_else(|e| e.into_inner());
        }
        jobs.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::{self, Consumer};
    use crate::models::{self, testing::make_test_job};
    use crate::persistence::{InMemoryJobRepository, JobRepository};
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

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
        assert_eq!(jobs[0].id, "job-1");
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
        assert_eq!(jobs[0].id, "job-1");
        assert_eq!(jobs[1].id, "job-2");
        assert_eq!(jobs[2].id, "job-3");
    }

    #[test]
    fn test_wait_for_job_returns_front_job() {
        let jobs = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
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
