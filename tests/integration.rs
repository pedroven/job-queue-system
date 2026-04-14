use std::sync::Arc;
use std::thread;
use std::time::Duration;

use job_queue_system::models::Job;
use job_queue_system::persistence::{InMemoryJobRepository, JobRepository};
use job_queue_system::queue::Queue;
use job_queue_system::task::TaskRegistry;

#[test]
fn enqueue_and_consume_through_public_api() {
    let repo: Arc<dyn JobRepository> = Arc::new(InMemoryJobRepository::new());

    let mut registry = TaskRegistry::new();
    registry.register("noop", |_| Ok(()));

    let queue = Arc::new(Queue::new(2, Arc::clone(&repo), registry).unwrap());
    queue.start_workers();

    for i in 0..5 {
        let job = Job::with_task_name(
            format!("job-{i}"),
            "noop".to_string(),
            format!("payload-{i}"),
        );
        queue.enqueue(job).unwrap();
    }

    thread::sleep(Duration::from_millis(150));
    assert_eq!(queue.len(), 0);
}

#[test]
fn pending_jobs_are_restored_on_restart() {
    let repo: Arc<dyn JobRepository> = Arc::new(InMemoryJobRepository::new());

    let job = Job::with_task_name("job-1".into(), "noop".into(), "payload".into());
    repo.save(&job).unwrap();

    let mut registry = TaskRegistry::new();
    registry.register("noop", |_| Ok(()));

    let queue = Queue::new(0, Arc::clone(&repo), registry).unwrap();
    assert_eq!(queue.len(), 1);
}
