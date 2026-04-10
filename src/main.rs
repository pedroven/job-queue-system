mod consumer;
mod error;
mod persistence;
mod producer;
mod queue;

use producer::Producer;
use queue::models::Job;
use std::sync::Arc;

fn main() {
    let db_path = std::env::var("JOB_QUEUE_DB").unwrap_or_else(|_| "jobs.db".to_string());
    let job_repository: Arc<dyn persistence::JobRepository> =
        Arc::new(persistence::SqliteJobRepository::new(&db_path).expect("Failed to open database"));

    let num_workers = 4;
    let queue = Arc::new(
        queue::Queue::new(num_workers, Arc::clone(&job_repository))
            .expect("Failed to initialize queue"),
    );
    queue.start_workers();

    let producer = producer::JobProducer::new(Arc::clone(&queue));
    let mut job_counter: u64 = 0;

    loop {
        println!("Add the payload:");
        let mut payload = String::new();
        std::io::stdin()
            .read_line(&mut payload)
            .expect("Failed to read line");

        job_counter += 1;
        let job = Job::new(format!("job-{job_counter}"), payload.trim().to_string());
        producer.produce(job).unwrap();
    }
}
