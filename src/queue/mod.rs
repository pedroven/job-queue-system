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
