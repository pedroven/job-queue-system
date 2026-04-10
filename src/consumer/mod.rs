use crate::error::QueueError;
use crate::queue::models;

pub trait Consumer {
    fn consume(&self, job: &models::Job) -> Result<(), QueueError>;
}

pub struct JobConsumer;

impl Consumer for JobConsumer {
    fn consume(&self, job: &models::Job) -> Result<(), QueueError> {
        println!("Consuming job: {:?}", job.id);
        println!("Consuming task: {:?}", job.task.id);
        println!("Payload: {:?}", job.task.payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::models::testing::make_test_job;

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
}
