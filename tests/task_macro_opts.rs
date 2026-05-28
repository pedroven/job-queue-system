use std::sync::{Arc, OnceLock};

use job_queue_system::error::QueueError;
use job_queue_system::models::{Job, JobPriority};
use job_queue_system::persistence::{InMemoryJobDispatch, InMemoryJobState, JobDispatch, JobState};
use job_queue_system::queue::{Queue, QueueConfig};
use job_queue_system::task;
use job_queue_system::task::TaskRegistry;

#[task]
fn macro_default_task(msg: String) -> Result<(), QueueError> {
    let _ = msg;
    Ok(())
}

#[task(max_attempts = 7, priority = JobPriority::High)]
fn macro_high_priority_task(msg: String) -> Result<(), QueueError> {
    let _ = msg;
    Ok(())
}

#[task(max_attempts = 1)]
fn macro_low_retry_task(msg: String) -> Result<(), QueueError> {
    let _ = msg;
    Ok(())
}

static TEST_STATE: OnceLock<Arc<InMemoryJobState>> = OnceLock::new();

fn shared_state() -> &'static Arc<InMemoryJobState> {
    TEST_STATE.get_or_init(|| {
        let state = Arc::new(InMemoryJobState::new());
        let dispatch: Arc<dyn JobDispatch> = Arc::new(InMemoryJobDispatch::new(1));
        let mut registry = TaskRegistry::new();
        registry.register(macro_default_task.name, macro_default_task.handler);
        registry.register(
            macro_high_priority_task.name,
            macro_high_priority_task.handler,
        );
        registry.register(macro_low_retry_task.name, macro_low_retry_task.handler);
        let cfg = QueueConfig {
            assigned_partitions: vec![],
            ..Default::default()
        };
        let state_dyn: Arc<dyn JobState> = state.clone();
        let queue = Arc::new(Queue::with_config(dispatch, state_dyn, registry, cfg).unwrap());
        task::set_global_queue(queue).unwrap();
        state
    })
}

fn find_job(state: &InMemoryJobState, task_name: &str) -> Job {
    // perform_async enqueues; no workers ⇒ jobs sit in state with Pending
    // status. We can't iterate all jobs (JobState only exposes find_by_id),
    // so we look up by the per-task slot by scanning known ids.
    // Simpler: each test enqueues a unique task name, so we just iterate
    // possible jobs by reading the dispatch backend isn't accessible here.
    // The clean replacement: capture the job id at enqueue time. Since
    // perform_async generates a random id internally, we instead peek at
    // state via a small helper.
    state
        .__find_all_pending_with_task(task_name)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no pending job for task {task_name}"))
}

#[test]
fn default_task_uses_normal_priority_and_three_attempts() {
    let state = shared_state();
    macro_default_task
        .perform_async("hello".to_string())
        .unwrap();

    let job = find_job(state, "macro_default_task");
    assert_eq!(job.priority, JobPriority::Normal);
    assert_eq!(job.max_attempts, 3);
}

#[test]
fn high_priority_args_are_baked_into_perform_async() {
    let state = shared_state();
    macro_high_priority_task
        .perform_async("urgent".to_string())
        .unwrap();

    let job = find_job(state, "macro_high_priority_task");
    assert_eq!(job.priority, JobPriority::High);
    assert_eq!(job.max_attempts, 7);
}

#[test]
fn max_attempts_only_keeps_default_priority() {
    let state = shared_state();
    macro_low_retry_task
        .perform_async("cheap".to_string())
        .unwrap();

    let job = find_job(state, "macro_low_retry_task");
    assert_eq!(job.priority, JobPriority::Normal);
    assert_eq!(job.max_attempts, 1);
}
