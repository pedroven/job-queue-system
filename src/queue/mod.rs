use crate::consumer::{self, Consumer};
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::{sync::Arc, thread};

pub mod models;

pub struct Queue {
    workers: Vec<Arc<Mutex<models::Worker>>>,
    jobs: Arc<(Mutex<VecDeque<models::Job>>, Condvar)>,
    dead_letter_jobs: Arc<Mutex<VecDeque<models::DeadLetterJob>>>,
}

impl Queue {
    pub fn new(num_workers: usize) -> Self {
        let mut workers = Vec::new();
        for i in 0..num_workers {
            workers.push(Arc::new(Mutex::new(models::Worker {
                id: format!("worker-{}", i),
                status: models::WorkerStatus::Idle,
                current_job_id: None,
            })));
        }
        Queue {
            workers,
            jobs: Arc::new((Mutex::new(VecDeque::new()), Condvar::new())),
            dead_letter_jobs: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn len(&self) -> usize {
        let (lock, _) = &*self.jobs;
        let jobs = lock.lock().unwrap();
        jobs.len()
    }

    pub fn enqueue(&self, job: models::Job) {
        let (lock, cvar) = &*self.jobs;
        let mut jobs = lock.lock().unwrap();
        jobs.push_back(job);
        cvar.notify_one();
    }

    pub fn start_workers(&self) {
        for worker in &self.workers {
            let consumer = consumer::JobConsumer;
            let worker = Arc::clone(worker);
            let jobs = Arc::clone(&self.jobs);
            thread::spawn(move || {
                loop {
                    let job = Self::wait_for_job(&jobs);
                    if let Some(job) = job {
                        Self::process_job(&worker, &consumer, job);
                    }
                }
            });
        }
    }

    fn wait_for_job(jobs: &(Mutex<VecDeque<models::Job>>, Condvar)) -> Option<models::Job> {
        let (lock, cvar) = jobs;
        let mut jobs = lock.lock().unwrap();
        while jobs.is_empty() {
            jobs = cvar.wait(jobs).unwrap();
        }
        jobs.pop_front()
    }

    fn process_job(
        worker: &Mutex<models::Worker>,
        consumer: &consumer::JobConsumer,
        job: models::Job,
    ) {
        {
            let mut w = worker.lock().unwrap();
            w.status = models::WorkerStatus::Busy;
            w.current_job_id = Some(job.id.clone());
        }
        consumer.consume(job).unwrap();
        {
            let mut w = worker.lock().unwrap();
            w.status = models::WorkerStatus::Idle;
            w.current_job_id = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_job(id: &str, payload: &str) -> models::Job {
        models::Job {
            id: id.to_string(),
            status: models::JobStatus::Pending,
            retry_count: 0,
            task: models::Task {
                id: format!("task-{}", id),
                payload: payload.to_string(),
            },
            max_retries: 3,
            created_at: std::time::SystemTime::now(),
        }
    }

    #[test]
    fn test_queue_new_creates_correct_number_of_workers() {
        let queue = Queue::new(4);
        assert_eq!(queue.workers.len(), 4);
    }

    #[test]
    fn test_queue_new_workers_start_idle() {
        let queue = Queue::new(2);
        for worker in &queue.workers {
            let w = worker.lock().unwrap();
            assert!(matches!(w.status, models::WorkerStatus::Idle));
            assert!(w.current_job_id.is_none());
        }
    }

    #[test]
    fn test_queue_new_zero_workers() {
        let queue = Queue::new(0);
        assert_eq!(queue.workers.len(), 0);
    }

    #[test]
    fn test_enqueue_adds_job_to_queue() {
        let queue = Queue::new(0);
        let job = make_job("job-1", "test payload");
        queue.enqueue(job);

        let (lock, _) = &*queue.jobs;
        let jobs = lock.lock().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, "job-1");
    }

    #[test]
    fn test_enqueue_preserves_fifo_order() {
        let queue = Queue::new(0);
        queue.enqueue(make_job("job-1", "first"));
        queue.enqueue(make_job("job-2", "second"));
        queue.enqueue(make_job("job-3", "third"));

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
            q.push_back(make_job("job-1", "payload"));
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
        let job = make_job("job-1", "payload");

        Queue::process_job(&worker, &consumer, job);

        let w = worker.lock().unwrap();
        assert!(matches!(w.status, models::WorkerStatus::Idle));
        assert!(w.current_job_id.is_none());
    }

    #[test]
    fn test_workers_consume_enqueued_jobs() {
        let queue = Arc::new(Queue::new(2));
        queue.start_workers();

        queue.enqueue(make_job("job-1", "hello"));
        queue.enqueue(make_job("job-2", "world"));

        // Give workers time to process
        thread::sleep(Duration::from_millis(100));

        let (lock, _) = &*queue.jobs;
        let jobs = lock.lock().unwrap();
        assert_eq!(jobs.len(), 0);
    }

    #[test]
    fn test_multiple_jobs_processed_concurrently() {
        let queue = Arc::new(Queue::new(4));
        queue.start_workers();

        for i in 0..10 {
            queue.enqueue(make_job(&format!("job-{}", i), &format!("payload-{}", i)));
        }

        thread::sleep(Duration::from_millis(200));

        let (lock, _) = &*queue.jobs;
        let jobs = lock.lock().unwrap();
        assert_eq!(jobs.len(), 0);
    }
}
