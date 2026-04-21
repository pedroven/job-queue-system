use std::sync::Arc;

use crate::error::QueueError;
use crate::models::Job;
use crate::queue::Queue;

pub trait Producer: Send + Sync {
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
    /// Hybrid back pressure: pending depth drives throttling.
    /// - depth >= hard_threshold  → reject with `QueueError::QueueFull`
    /// - depth >= soft_threshold  → sleep `backpressure_delay`, then enqueue
    /// - otherwise                → enqueue immediately
    fn produce(&self, job: Job) -> Result<(), QueueError> {
        let config = &self.queue.config;
        let depth = self.queue.pending_count()?;

        if depth >= config.backpressure_hard_threshold {
            return Err(QueueError::QueueFull {
                depth,
                hard_threshold: config.backpressure_hard_threshold,
            });
        }
        if depth >= config.backpressure_soft_threshold {
            std::thread::sleep(config.backpressure_delay);
        }
        self.queue.enqueue(job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::JobStatus;
    use crate::models::testing::make_test_job;
    use crate::persistence::{InMemoryJobRepository, JobRepository};
    use crate::queue::QueueConfig;
    use crate::task::TaskRegistry;
    use std::time::{Duration, Instant};

    fn create_queue(num_workers: usize) -> Arc<Queue> {
        create_queue_with(
            num_workers,
            Arc::new(InMemoryJobRepository::new()),
            QueueConfig::default(),
        )
    }

    fn create_queue_with_config(config: QueueConfig) -> Arc<Queue> {
        create_queue_with(0, Arc::new(InMemoryJobRepository::new()), config)
    }

    fn create_queue_with(
        num_workers: usize,
        repo: Arc<dyn JobRepository>,
        config: QueueConfig,
    ) -> Arc<Queue> {
        Arc::new(Queue::with_config(num_workers, repo, TaskRegistry::new(), config).unwrap())
    }

    #[test]
    fn test_produce_returns_ok() {
        let queue = create_queue(0);
        let producer = JobProducer::new(Arc::clone(&queue));
        let result = producer.produce(make_test_job("job-1", "payload"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_produce_enqueues_job() {
        let queue = create_queue(0);
        let producer = JobProducer::new(Arc::clone(&queue));
        producer.produce(make_test_job("job-1", "first")).unwrap();
        producer.produce(make_test_job("job-2", "second")).unwrap();

        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn test_produce_below_soft_threshold_enqueues_without_error() {
        // Depth 0 is below the soft threshold, so produce must succeed and
        // enqueue the job. We don't assert on timing — that would be flaky
        // under CI load. The complementary "sleep happens at soft" check is
        // covered in `test_produce_at_soft_threshold_sleeps`.
        let config = QueueConfig {
            backpressure_soft_threshold: 10,
            backpressure_hard_threshold: 20,
            backpressure_delay: Duration::from_millis(200),
            ..QueueConfig::default()
        };
        let queue = create_queue_with_config(config);
        let producer = JobProducer::new(Arc::clone(&queue));

        producer.produce(make_test_job("job-1", "payload")).unwrap();
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_produce_at_soft_threshold_sleeps() {
        let config = QueueConfig {
            backpressure_soft_threshold: 2,
            backpressure_hard_threshold: 10,
            backpressure_delay: Duration::from_millis(80),
            ..QueueConfig::default()
        };
        let queue = create_queue_with_config(config);
        let producer = JobProducer::new(Arc::clone(&queue));

        producer.produce(make_test_job("job-1", "p")).unwrap();
        producer.produce(make_test_job("job-2", "p")).unwrap();

        let start = Instant::now();
        producer.produce(make_test_job("job-3", "p")).unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(80),
            "expected throttle sleep, took {elapsed:?}"
        );
        assert_eq!(queue.len(), 3);
    }

    #[test]
    fn test_produce_at_hard_threshold_rejects() {
        let config = QueueConfig {
            backpressure_soft_threshold: 1,
            backpressure_hard_threshold: 2,
            backpressure_delay: Duration::from_millis(0),
            ..QueueConfig::default()
        };
        let queue = create_queue_with_config(config);
        let producer = JobProducer::new(Arc::clone(&queue));

        producer.produce(make_test_job("job-1", "p")).unwrap();
        producer.produce(make_test_job("job-2", "p")).unwrap();

        let err = producer.produce(make_test_job("job-3", "p")).unwrap_err();
        match err {
            QueueError::QueueFull {
                depth,
                hard_threshold,
            } => {
                assert_eq!(depth, 2);
                assert_eq!(hard_threshold, 2);
            }
            other => panic!("expected QueueFull, got {other:?}"),
        }
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn test_with_config_rejects_invalid_thresholds() {
        fn assert_invalid(config: QueueConfig) {
            let result = Queue::with_config(
                0,
                Arc::new(InMemoryJobRepository::new()),
                TaskRegistry::new(),
                config,
            );
            match result {
                Err(QueueError::InvalidConfig(_)) => {}
                Err(other) => panic!("expected InvalidConfig, got {other:?}"),
                Ok(_) => panic!("expected InvalidConfig, got Ok"),
            }
        }

        assert_invalid(QueueConfig {
            backpressure_soft_threshold: 10,
            backpressure_hard_threshold: 5,
            ..QueueConfig::default()
        });
        assert_invalid(QueueConfig {
            backpressure_soft_threshold: 0,
            backpressure_hard_threshold: 0,
            ..QueueConfig::default()
        });
    }

    #[test]
    fn test_produce_recovers_when_depth_drops() {
        let repo = Arc::new(InMemoryJobRepository::new());
        let config = QueueConfig {
            backpressure_soft_threshold: 1,
            backpressure_hard_threshold: 2,
            backpressure_delay: Duration::from_millis(0),
            ..QueueConfig::default()
        };
        let queue = create_queue_with(0, Arc::clone(&repo) as Arc<dyn JobRepository>, config);
        let producer = JobProducer::new(Arc::clone(&queue));

        producer.produce(make_test_job("job-1", "p")).unwrap();
        producer.produce(make_test_job("job-2", "p")).unwrap();
        assert!(producer.produce(make_test_job("job-3", "p")).is_err());

        // Simulate workers draining: flip persisted statuses away from Pending.
        repo.update_status("job-1", JobStatus::Completed).unwrap();
        repo.update_status("job-2", JobStatus::Completed).unwrap();

        assert!(producer.produce(make_test_job("job-3", "p")).is_ok());
    }
}
