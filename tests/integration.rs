use std::sync::Arc;
use std::thread;
use std::time::Duration;

use job_queue_system::models::Job;
use job_queue_system::persistence::{InMemoryJobDispatch, InMemoryJobState, JobDispatch, JobState};
use job_queue_system::queue::Queue;
use job_queue_system::task::TaskRegistry;

#[test]
fn enqueue_and_consume_through_public_api() {
    let dispatch: Arc<dyn JobDispatch> = Arc::new(InMemoryJobDispatch::new(1));
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());

    let mut registry = TaskRegistry::new();
    registry.register("noop", |_| Ok(()));

    let queue = Arc::new(Queue::new(dispatch, state, registry).unwrap());
    queue.start_workers();

    for i in 0..5 {
        let job = Job::with_task_name(
            format!("job-{i}"),
            "noop".to_string(),
            format!("payload-{i}"),
        );
        queue.enqueue(job).unwrap();
    }

    thread::sleep(Duration::from_millis(400));
    assert_eq!(queue.pending_count().unwrap(), 0);
}

#[test]
fn restart_recovery_via_state_is_a_state_concern_not_dispatch() {
    // Old test ("pending_jobs_are_restored_on_restart") tied to the in-memory
    // cvar source-of-truth. Under the new design the dispatch backend itself
    // is the source of truth — no in-memory mirror to restore. So we replace
    // it with the equivalent invariant: a freshly-built Queue talking to an
    // existing dispatch sees the entries that were already there.
    let dispatch = Arc::new(InMemoryJobDispatch::new(1));
    let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
    let job = Job::with_task_name("job-1".into(), "noop".into(), "payload".into());
    state.save_initial(&job).unwrap();
    dispatch.enqueue(0, &job).unwrap();

    let mut registry = TaskRegistry::new();
    registry.register("noop", |_| Ok(()));

    let queue = Queue::new(dispatch as Arc<dyn JobDispatch>, state, registry).unwrap();
    assert_eq!(queue.pending_count().unwrap(), 1);
}
