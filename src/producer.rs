use std::sync::Arc;

use crate::error::QueueError;
use crate::models::Job;
use crate::queue::Queue;

pub trait Producer {
    fn produce(&self, job: Job) -> Result<(), QueueError>;
}

pub struct JobProducer {
    queue: Arc<Queue>,
}

impl JobProducer {
    pub fn new(queue: Arc<Queue>) -> Self {
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
    use crate::models::testing::make_test_job;
    use crate::persistence::InMemoryJobRepository;
    use crate::task::TaskRegistry;

    fn create_queue(num_workers: usize) -> Queue {
        Queue::new(
            num_workers,
            Arc::new(InMemoryJobRepository::new()),
            TaskRegistry::new(),
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
