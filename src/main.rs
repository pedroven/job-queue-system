mod consumer;
mod producer;
mod queue;

use producer::Producer;
use std::sync::Arc;

fn main() {
    let num_workers = 4;
    let queue = Arc::new(queue::Queue::new(num_workers));
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
        let job = queue::models::Job {
            id: format!("job-{}", job_counter),
            task: queue::models::Task {
                id: format!("task-{}", job_counter),
                payload: payload.trim().to_string(),
            },
            status: queue::models::JobStatus::Pending,
            retry_count: 0,
            max_retries: 3,
            created_at: std::time::SystemTime::now(),
        };
        producer.produce(job).unwrap();
    }
}
