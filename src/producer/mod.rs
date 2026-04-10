use crate::queue::{self, models::Job};
use std::sync::Arc;

pub trait Producer {
    fn produce(&self, job: Job) -> Result<(), String>;
}

pub struct JobProducer {
    queue: Arc<queue::Queue>,
}

impl JobProducer {
    pub fn new(queue: Arc<queue::Queue>) -> Self {
        JobProducer { queue }
    }
}

impl Producer for JobProducer {
    fn produce(&self, job: Job) -> Result<(), String> {
        self.queue.enqueue(job);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::models;

    fn make_job(id: &str, payload: &str) -> Job {
        Job {
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
    fn test_produce_returns_ok() {
        let queue = Arc::new(queue::Queue::new(0));
        let producer = JobProducer::new(Arc::clone(&queue));
        let result = producer.produce(make_job("job-1", "payload"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_produce_enqueues_job() {
        let queue = Arc::new(queue::Queue::new(0));
        let producer = JobProducer::new(Arc::clone(&queue));
        producer.produce(make_job("job-1", "first")).unwrap();
        producer.produce(make_job("job-2", "second")).unwrap();

        assert_eq!(queue.len(), 2);
    }
}
