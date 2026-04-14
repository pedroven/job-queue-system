use std::sync::Arc;

use crate::error::QueueError;
use crate::models::Job;
use crate::task::TaskRegistry;

pub trait Consumer {
    fn consume(&self, job: &Job) -> Result<(), QueueError>;
}

pub struct JobConsumer;

impl Consumer for JobConsumer {
    fn consume(&self, job: &Job) -> Result<(), QueueError> {
        println!("Consuming job: {:?}", job.id);
        println!("Consuming task: {:?}", job.task.id);
        println!("Payload: {:?}", job.task.payload);
        Ok(())
    }
}

pub struct RegistryConsumer {
    registry: Arc<TaskRegistry>,
}

impl RegistryConsumer {
    pub fn new(registry: Arc<TaskRegistry>) -> Self {
        Self { registry }
    }
}

impl Consumer for RegistryConsumer {
    fn consume(&self, job: &Job) -> Result<(), QueueError> {
        let handler = self.registry.get(&job.task.name).ok_or_else(|| {
            QueueError::JobFailed(format!(
                "no handler registered for task '{}'",
                job.task.name
            ))
        })?;
        handler(&job.task.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::testing::make_test_job;

    #[test]
    fn test_consume_returns_ok() {
        let consumer = JobConsumer;
        let job = make_test_job("job-1", "test payload");
        let result = consumer.consume(&job);
        assert!(result.is_ok());
    }

    #[test]
    fn test_consume_handles_empty_payload() {
        let consumer = JobConsumer;
        let job = make_test_job("job-2", "");
        let result = consumer.consume(&job);
        assert!(result.is_ok());
    }

    #[test]
    fn test_registry_consumer_dispatches_handler() {
        use std::sync::atomic::{AtomicBool, Ordering};

        static CALLED: AtomicBool = AtomicBool::new(false);

        fn test_handler(_payload: &str) -> Result<(), QueueError> {
            CALLED.store(true, Ordering::Relaxed);
            Ok(())
        }

        let mut registry = TaskRegistry::new();
        registry.register("default", test_handler);
        let consumer = RegistryConsumer::new(Arc::new(registry));

        let job = make_test_job("job-1", "payload");
        consumer.consume(&job).unwrap();
        assert!(CALLED.load(Ordering::Relaxed));
    }

    #[test]
    fn test_registry_consumer_errors_on_unknown_task() {
        let registry = TaskRegistry::new();
        let consumer = RegistryConsumer::new(Arc::new(registry));

        let job = Job::with_task_name(
            "job-1".to_string(),
            "nonexistent".to_string(),
            "payload".to_string(),
        );
        let result = consumer.consume(&job);
        assert!(result.is_err());
    }

    #[test]
    fn test_registry_consumer_routes_to_correct_handler() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static A_COUNT: AtomicUsize = AtomicUsize::new(0);
        static B_COUNT: AtomicUsize = AtomicUsize::new(0);

        fn handler_a(_payload: &str) -> Result<(), QueueError> {
            A_COUNT.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn handler_b(_payload: &str) -> Result<(), QueueError> {
            B_COUNT.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        let mut registry = TaskRegistry::new();
        registry.register("task_a", handler_a);
        registry.register("task_b", handler_b);
        let consumer = RegistryConsumer::new(Arc::new(registry));

        let job_a = Job::with_task_name(
            "j1".to_string(),
            "task_a".to_string(),
            "a-payload".to_string(),
        );
        let job_b = Job::with_task_name(
            "j2".to_string(),
            "task_b".to_string(),
            "b-payload".to_string(),
        );

        let a_before = A_COUNT.load(Ordering::Relaxed);
        let b_before = B_COUNT.load(Ordering::Relaxed);

        consumer.consume(&job_a).unwrap();
        assert_eq!(A_COUNT.load(Ordering::Relaxed), a_before + 1);
        assert_eq!(B_COUNT.load(Ordering::Relaxed), b_before);

        consumer.consume(&job_b).unwrap();
        assert_eq!(B_COUNT.load(Ordering::Relaxed), b_before + 1);
    }
}
