use crate::queue::models::Task;

pub trait Producer {
    fn produce(&self, task: Task) -> Result<(), String>;
}

pub struct JobProducer;

impl Producer for JobProducer {
    fn produce(&self, task: Task) -> Result<(), String> {
        // Logic to add the task to the queue
        Ok(())
    }
}