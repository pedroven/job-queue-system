use crate::error::QueueError;
use crate::queue::{self, models::Job};
use std::sync::Arc;

pub trait Producer {
    fn produce(&self, job: Job) -> Result<(), QueueError>;
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
    fn produce(&self, job: Job) -> Result<(), QueueError> {
        self.queue.enqueue(job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::models::testing::make_test_job;

    fn create_queue(num_workers: usize) -> queue::Queue {
        queue::Queue::new(
            num_workers,
            Arc::new(crate::persistence::InMemoryJobRepository::new()),
        )
        .unwrap()
    }

    #[test]
    fn test_produce_returns_ok() {
        let queue = Arc::new(create_queue(0));
        let producer = JobProducer::new(Arc::clone(&queue));
        let result = producer.produce(make_test_job("job-1", "payload"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_produce_enqueues_job() {
        let queue = Arc::new(create_queue(0));
        let producer = JobProducer::new(Arc::clone(&queue));
        producer.produce(make_test_job("job-1", "first")).unwrap();
        producer.produce(make_test_job("job-2", "second")).unwrap();

        assert_eq!(queue.len(), 2);
    }
}
