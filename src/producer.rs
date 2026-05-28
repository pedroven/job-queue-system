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
    use crate::persistence::{InMemoryJobDispatch, InMemoryJobState, JobDispatch, JobState};
    use crate::queue::QueueConfig;
    use crate::task::TaskRegistry;
    use std::time::{Duration, Instant};

    fn no_worker_config() -> QueueConfig {
        QueueConfig {
            assigned_partitions: vec![],
            ..QueueConfig::default()
        }
    }

    fn create_queue_with_config(config: QueueConfig) -> Arc<Queue> {
        let dispatch: Arc<dyn JobDispatch> =
            Arc::new(InMemoryJobDispatch::new(config.partition_count));
        let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
        Arc::new(Queue::with_config(dispatch, state, TaskRegistry::new(), config).unwrap())
    }

    fn create_queue_with_state(state: Arc<dyn JobState>, config: QueueConfig) -> Arc<Queue> {
        let dispatch: Arc<dyn JobDispatch> =
            Arc::new(InMemoryJobDispatch::new(config.partition_count));
        Arc::new(Queue::with_config(dispatch, state, TaskRegistry::new(), config).unwrap())
    }

    #[test]
    fn test_produce_returns_ok() {
        let queue = create_queue_with_config(no_worker_config());
        let producer = JobProducer::new(Arc::clone(&queue));
        let result = producer.produce(make_test_job("job-1", "payload"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_produce_enqueues_job() {
        let queue = create_queue_with_config(no_worker_config());
        let producer = JobProducer::new(Arc::clone(&queue));
        producer.produce(make_test_job("job-1", "first")).unwrap();
        producer.produce(make_test_job("job-2", "second")).unwrap();

        assert_eq!(queue.pending_count().unwrap(), 2);
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
            ..no_worker_config()
        };
        let queue = create_queue_with_config(config);
        let producer = JobProducer::new(Arc::clone(&queue));

        producer.produce(make_test_job("job-1", "payload")).unwrap();
        assert_eq!(queue.pending_count().unwrap(), 1);
    }

    #[test]
    fn test_produce_at_soft_threshold_sleeps() {
        let config = QueueConfig {
            backpressure_soft_threshold: 2,
            backpressure_hard_threshold: 10,
            backpressure_delay: Duration::from_millis(80),
            ..no_worker_config()
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
        assert_eq!(queue.pending_count().unwrap(), 3);
    }

    #[test]
    fn test_produce_at_hard_threshold_rejects() {
        let config = QueueConfig {
            backpressure_soft_threshold: 1,
            backpressure_hard_threshold: 2,
            backpressure_delay: Duration::from_millis(0),
            ..no_worker_config()
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
        assert_eq!(queue.pending_count().unwrap(), 2);
    }

    #[test]
    fn test_with_config_rejects_invalid_thresholds() {
        fn assert_invalid(config: QueueConfig) {
            let dispatch: Arc<dyn JobDispatch> = Arc::new(InMemoryJobDispatch::new(1));
            let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
            let result = Queue::with_config(dispatch, state, TaskRegistry::new(), config);
            match result {
                Err(QueueError::InvalidConfig(_)) => {}
                Err(other) => panic!("expected InvalidConfig, got {other:?}"),
                Ok(_) => panic!("expected InvalidConfig, got Ok"),
            }
        }

        assert_invalid(QueueConfig {
            backpressure_soft_threshold: 10,
            backpressure_hard_threshold: 5,
            ..no_worker_config()
        });
        assert_invalid(QueueConfig {
            backpressure_soft_threshold: 0,
            backpressure_hard_threshold: 0,
            ..no_worker_config()
        });
    }

    #[test]
    fn test_produce_recovers_when_depth_drops() {
        // Drain-via-dispatch: after produce+claim, pending_count drops and a
        // previously-rejected produce can succeed.
        let state: Arc<dyn JobState> = Arc::new(InMemoryJobState::new());
        let config = QueueConfig {
            backpressure_soft_threshold: 1,
            backpressure_hard_threshold: 2,
            backpressure_delay: Duration::from_millis(0),
            ..no_worker_config()
        };
        let queue = create_queue_with_state(Arc::clone(&state), config);
        let producer = JobProducer::new(Arc::clone(&queue));

        producer.produce(make_test_job("job-1", "p")).unwrap();
        producer.produce(make_test_job("job-2", "p")).unwrap();
        assert!(producer.produce(make_test_job("job-3", "p")).is_err());

        // Drain by claiming + acking via the dispatch directly. State stays
        // around but the pending stream shrinks → back-pressure clears.
        let c1 = queue
            .dispatch
            .next_for_partition(0, "c", Duration::from_millis(20))
            .unwrap()
            .unwrap();
        state.save_status(&c1.job.id, JobStatus::Completed).unwrap();
        queue.dispatch.ack(0, &c1).unwrap();
        let c2 = queue
            .dispatch
            .next_for_partition(0, "c", Duration::from_millis(20))
            .unwrap()
            .unwrap();
        state.save_status(&c2.job.id, JobStatus::Completed).unwrap();
        queue.dispatch.ack(0, &c2).unwrap();

        assert!(producer.produce(make_test_job("job-3", "p")).is_ok());
    }
}
