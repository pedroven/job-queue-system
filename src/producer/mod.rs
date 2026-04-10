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
