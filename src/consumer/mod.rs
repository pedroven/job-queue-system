use crate::queue::models;

pub trait Consumer {
    fn consume(&self, job: models::Job) -> Result<(), String>;
}

pub struct JobConsumer;

impl Consumer for JobConsumer {
    fn consume(&self, job: models::Job) -> Result<(), String> {
        // Logic to consume a task from the queue and process it
        println!("Consuming job: {:?}", job.id);
        println!("Consuming task: {:?}", job.task.id);
        println!("Payload: {:?}", job.task.payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_job(id: &str, payload: &str) -> models::Job {
        models::Job {
            id: id.to_string(),
            status: models::JobStatus::Pending,
            retry_count: 0,
            task: models::Task {
                id: format!("task-{}", id),
                payload: payload.to_string(),
            },
            max_retries: 3,
            created_at: std::time::SystemTime::now(),
        }
    }

    #[test]
    fn test_consume_returns_ok() {
        let consumer = JobConsumer;
        let job = make_job("job-1", "test payload");
        let result = consumer.consume(job);
        assert!(result.is_ok());
    }

    #[test]
    fn test_consume_handles_empty_payload() {
        let consumer = JobConsumer;
        let job = make_job("job-2", "");
        let result = consumer.consume(job);
        assert!(result.is_ok());
    }
}
