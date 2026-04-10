use crate::consumer::{self, Consumer};
use crate::error::QueueError;
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::{sync::Arc, thread};

pub mod models;

pub struct Queue {
    workers: Vec<Arc<Mutex<models::Worker>>>,
    jobs: Arc<(Mutex<VecDeque<models::Job>>, Condvar)>,
    dead_letter_jobs: Arc<Mutex<VecDeque<models::DeadLetterJob>>>,
    job_repository: Arc<dyn crate::persistence::JobRepository>,
}

impl Queue {
    pub fn new(
        num_workers: usize,
        job_repository: Arc<dyn crate::persistence::JobRepository>,
    ) -> Result<Self, QueueError> {
        let mut workers = Vec::new();
        for i in 0..num_workers {
            workers.push(Arc::new(Mutex::new(models::Worker {
                id: format!("worker-{}", i),
                status: models::WorkerStatus::Idle,
                current_job_id: None,
            })));
        }
        // Restore pending jobs from the repository. No cvar notification is needed
        // because workers haven't started yet — when they first call wait_for_job,
        // the while-loop check finds the deque non-empty and proceeds immediately.
        let jobs_from_repo = job_repository.find_all_pending()?;
        let jobs = Arc::new((Mutex::new(VecDeque::from(jobs_from_repo)), Condvar::new()));
        Ok(Queue {
            workers,
            jobs,
            dead_letter_jobs: Arc::new(Mutex::new(VecDeque::new())),
            job_repository,
        })
    }

    pub fn len(&self) -> usize {
        let (lock, _) = &*self.jobs;
        let jobs = lock.lock().unwrap_or_else(|e| e.into_inner());
        jobs.len()
    }

    pub fn enqueue(&self, job: models::Job) -> Result<(), QueueError> {
        let (lock, cvar) = &*self.jobs;
        self.job_repository.save(&job)?;
        let mut jobs = lock.lock()?;
        jobs.push_back(job);
        cvar.notify_one();
        Ok(())
    }

    pub fn start_workers(self: &Arc<Self>) {
        for worker in &self.workers {
            let consumer = consumer::JobConsumer;
            let worker = Arc::clone(worker);
            let jobs = Arc::clone(&self.jobs);
            let queue = Arc::clone(self);
            thread::spawn(move || {
                loop {
                    let job = Self::wait_for_job(&jobs);
                    if let Some(job) = job {
                        Self::process_job(&worker, &consumer, job, &queue);
                    }
                }
            });
        }
    }

    fn wait_for_job(jobs: &(Mutex<VecDeque<models::Job>>, Condvar)) -> Option<models::Job> {
        let (lock, cvar) = jobs;
        let mut jobs = lock.lock().unwrap_or_else(|e| e.into_inner());
        while jobs.is_empty() {
            jobs = cvar.wait(jobs).unwrap_or_else(|e| e.into_inner());
        }
        jobs.pop_front()
    }

    fn process_job(
        worker: &Mutex<models::Worker>,
        consumer: &consumer::JobConsumer,
        job: models::Job,
        queue: &Arc<Queue>,
    ) {
        let job_id = job.id.clone();
        {
            let mut w = worker.lock().unwrap_or_else(|e| e.into_inner());
            w.status = models::WorkerStatus::Busy;
            w.current_job_id = Some(job_id.clone());
        }

        if let Err(e) = queue
            .job_repository
            .update_status(&job_id, models::JobStatus::Running)
        {
            eprintln!("Failed to update job {job_id} to Running: {e}");
        }

        let result = consumer.consume(job);

        let final_status = if result.is_ok() {
            models::JobStatus::Completed
        } else {
            models::JobStatus::Failed
        };
        if let Err(e) = queue.job_repository.update_status(&job_id, final_status) {
            eprintln!("Failed to update job {job_id} status: {e}");
        }

        {
            let mut w = worker.lock().unwrap_or_else(|e| e.into_inner());
            w.status = models::WorkerStatus::Idle;
            w.current_job_id = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::queue::models::testing::make_test_job;

    fn create_queue(num_workers: usize) -> Queue {
        Queue::new(
            num_workers,
            Arc::new(crate::persistence::InMemoryJobRepository::new()),
        )
        .unwrap()
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
            assert!(matches!(w.status, models::WorkerStatus::Idle));
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
        let worker = Mutex::new(models::Worker {
            id: "worker-0".to_string(),
            status: models::WorkerStatus::Idle,
            current_job_id: None,
        });
        let consumer = consumer::JobConsumer;
        let job = make_test_job("job-1", "payload");

        Queue::process_job(&worker, &consumer, job, &Arc::new(create_queue(0)));

        let w = worker.lock().unwrap();
        assert!(matches!(w.status, models::WorkerStatus::Idle));
        assert!(w.current_job_id.is_none());
    }

    #[test]
    fn test_workers_consume_enqueued_jobs() {
        let queue = Arc::new(create_queue(2));
        queue.start_workers();

        queue.enqueue(make_test_job("job-1", "hello")).unwrap();
        queue.enqueue(make_test_job("job-2", "world")).unwrap();

        // Give workers time to process
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
}
