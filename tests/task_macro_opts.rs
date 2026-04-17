use std::sync::{Arc, OnceLock};

use job_queue_system::error::QueueError;
use job_queue_system::models::{Job, JobPriority};
use job_queue_system::persistence::{InMemoryJobRepository, JobRepository};
use job_queue_system::queue::Queue;
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

// Cargo compiles each file under `tests/` to its own test binary, so this
// OnceLock + shared repo is scoped to this file's process. Safe here; don't
// lift the pattern into an integration harness that runs multiple files
// together without reconsidering isolation.
static TEST_REPO: OnceLock<Arc<InMemoryJobRepository>> = OnceLock::new();

fn shared_repo() -> &'static Arc<InMemoryJobRepository> {
    TEST_REPO.get_or_init(|| {
        let repo = Arc::new(InMemoryJobRepository::new());
        let mut registry = TaskRegistry::new();
        registry.register(macro_default_task.name, macro_default_task.handler);
        registry.register(
            macro_high_priority_task.name,
            macro_high_priority_task.handler,
        );
        registry.register(macro_low_retry_task.name, macro_low_retry_task.handler);
        // 0 workers — jobs stay put so we can inspect the persisted state
        // without racing consumers.
        let repo_dyn: Arc<dyn JobRepository> = repo.clone();
        let queue = Arc::new(Queue::new(0, repo_dyn, registry).unwrap());
        task::set_global_queue(queue).unwrap();
        repo
    })
}

fn find_job(repo: &InMemoryJobRepository, task_name: &str) -> Job {
    repo.find_all_pending()
        .unwrap()
        .into_iter()
        .find(|j| j.task.name == task_name)
        .unwrap_or_else(|| panic!("no pending job for task {task_name}"))
}

#[test]
fn default_task_uses_normal_priority_and_three_attempts() {
    let repo = shared_repo();
    macro_default_task
        .perform_async("hello".to_string())
        .unwrap();

    let job = find_job(repo, "macro_default_task");
    assert_eq!(job.priority, JobPriority::Normal);
    assert_eq!(job.max_attempts, 3);
}

#[test]
fn high_priority_args_are_baked_into_perform_async() {
    let repo = shared_repo();
    macro_high_priority_task
        .perform_async("urgent".to_string())
        .unwrap();

    let job = find_job(repo, "macro_high_priority_task");
    assert_eq!(job.priority, JobPriority::High);
    assert_eq!(job.max_attempts, 7);
}

#[test]
fn max_attempts_only_keeps_default_priority() {
    let repo = shared_repo();
    macro_low_retry_task
        .perform_async("cheap".to_string())
        .unwrap();

    let job = find_job(repo, "macro_low_retry_task");
    assert_eq!(job.priority, JobPriority::Normal);
    assert_eq!(job.max_attempts, 1);
}
