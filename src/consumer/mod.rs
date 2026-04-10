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
