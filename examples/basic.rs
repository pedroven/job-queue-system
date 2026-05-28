use std::sync::Arc;
use std::thread;
use std::time::Duration;

use job_queue_system::error::QueueError;
use job_queue_system::persistence::{InMemoryJobDispatch, InMemoryJobState, JobDispatch, JobState};
use job_queue_system::queue::Queue;
use job_queue_system::task;
use job_queue_system::task_registry;

#[task]
fn greet(name: String) -> Result<(), QueueError> {
    println!("hello, {name}!");
    Ok(())
}

fn main() {
    let dispatch: Arc<dyn JobDispatch> = Arc::new(InMemoryJobDispatch::new(1));
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let registry = task_registry![greet];

    let queue = Arc::new(Queue::new(dispatch, state, registry).unwrap());
    queue.start_workers();
    task::set_global_queue(Arc::clone(&queue)).unwrap();

    greet.perform_async("world".to_string()).unwrap();
    greet.perform_async("rustacean".to_string()).unwrap();

    thread::sleep(Duration::from_millis(400));
}
