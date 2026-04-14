use std::sync::Arc;
use std::thread;
use std::time::Duration;

use job_queue_system::error::QueueError;
use job_queue_system::persistence::{InMemoryJobRepository, JobRepository};
use job_queue_system::queue::Queue;
use job_queue_system::task;
use job_queue_system::task_registry;

#[task]
fn greet(name: String) -> Result<(), QueueError> {
    println!("hello, {name}!");
    Ok(())
}

fn main() {
    let repo: Arc<dyn JobRepository> = Arc::new(InMemoryJobRepository::new());
    let registry = task_registry![greet];

    let queue = Arc::new(Queue::new(2, repo, registry).unwrap());
    queue.start_workers();
    task::set_global_queue(Arc::clone(&queue)).unwrap();

    greet.perform_async("world".to_string()).unwrap();
    greet.perform_async("rustacean".to_string()).unwrap();

    thread::sleep(Duration::from_millis(100));
}
