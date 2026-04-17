use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::error::QueueError;
use crate::models::{Job, JobPriority};

pub type TaskHandler = fn(&str) -> Result<(), QueueError>;

/// A task is identified by its `name` + `handler`, and is parameterized by
/// the tuple of argument types its payload carries (`Args`). The macro emits
/// a `pub static` of this type per `#[task]` function.
/// Shape the registry and macro-generated task structs conform to.
pub struct TaskDefinition {
    pub name: &'static str,
    pub handler: TaskHandler,
}

impl TaskDefinition {
    pub const fn new(name: &'static str, handler: TaskHandler) -> Self {
        Self { name, handler }
    }
}

/// Shared enqueue logic used by every macro-generated `perform_async`.
/// `Args` is the tuple of the task's arguments.
pub fn enqueue<Args: Serialize>(task_name: &'static str, args: Args) -> Result<(), QueueError> {
    enqueue_with_opts(task_name, args, 3, JobPriority::Normal)
}

/// Enqueue with caller-provided `max_attempts` and `priority`. Used by the
/// `#[task(max_attempts = …, priority = …)]` macro to bake per-task defaults
/// into the generated `perform_async`. `Queue::enqueue` routes to the correct
/// level based on `job.priority`.
pub fn enqueue_with_opts<Args: Serialize>(
    task_name: &'static str,
    args: Args,
    max_attempts: u32,
    priority: JobPriority,
) -> Result<(), QueueError> {
    let payload = serialize_payload(task_name, &args)?;
    let mut job = Job::with_task_name(generate_job_id(), task_name.to_string(), payload);
    job.max_attempts = max_attempts;
    job.priority = priority;
    global_queue().enqueue(job)
}

pub fn deserialize_payload<P: DeserializeOwned>(
    task_name: &str,
    payload: &str,
) -> Result<P, QueueError> {
    serde_json::from_str(payload)
        .map_err(|e| QueueError::JobFailed(format!("deserialize {task_name}: {e}")))
}

pub fn serialize_payload<P: serde::Serialize>(
    task_name: &str,
    payload: &P,
) -> Result<String, QueueError> {
    serde_json::to_string(payload)
        .map_err(|e| QueueError::JobFailed(format!("serialize {task_name}: {e}")))
}

pub fn generate_job_id() -> String {
    Uuid::new_v4().to_string()
}

static GLOBAL_QUEUE: OnceLock<Arc<crate::queue::Queue>> = OnceLock::new();

/// Install the queue that `#[task]`-generated `.perform_async(...)` calls will use.
/// Call once at startup, after building the queue.
pub fn set_global_queue(queue: Arc<crate::queue::Queue>) -> Result<(), QueueError> {
    GLOBAL_QUEUE
        .set(queue)
        .map_err(|_| QueueError::JobFailed("global queue already installed".to_string()))
}

pub fn global_queue() -> &'static Arc<crate::queue::Queue> {
    GLOBAL_QUEUE
        .get()
        .expect("global queue not installed — call task::set_global_queue() at startup")
}

#[derive(Default)]
pub struct TaskRegistry {
    handlers: HashMap<String, TaskHandler>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, name: &str, handler: TaskHandler) {
        self.handlers.insert(name.to_string(), handler);
    }

    pub fn get(&self, name: &str) -> Option<&TaskHandler> {
        self.handlers.get(name)
    }
}

#[macro_export]
macro_rules! task_registry {
    ($($task:expr),+ $(,)?) => {{
        let mut registry = $crate::task::TaskRegistry::new();
        $(
            registry.register($task.name, $task.handler);
        )+
        registry
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noop_handler(_payload: &str) -> Result<(), QueueError> {
        Ok(())
    }

    #[test]
    fn test_task_definition_new() {
        let def = TaskDefinition::new("send_email", noop_handler);
        assert_eq!(def.name, "send_email");
    }

    #[test]
    fn test_registry_register_and_get() {
        let mut registry = TaskRegistry::new();
        registry.register("send_email", noop_handler);

        assert!(registry.get("send_email").is_some());
        assert!(registry.get("unknown").is_none());
    }

    #[test]
    fn test_task_registry_macro() {
        static TASK_A: TaskDefinition = TaskDefinition::new("task_a", noop_handler);
        static TASK_B: TaskDefinition = TaskDefinition::new("task_b", noop_handler);

        let registry = task_registry![TASK_A, TASK_B];
        assert!(registry.get("task_a").is_some());
        assert!(registry.get("task_b").is_some());
        assert!(registry.get("task_c").is_none());
    }

    #[test]
    fn test_generate_job_id_is_unique() {
        let id1 = generate_job_id();
        let id2 = generate_job_id();
        assert_ne!(id1, id2);
    }
}
