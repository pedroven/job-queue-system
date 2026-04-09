pub trait Consumer {
    fn consume(&self) -> Result<(), String>;
}

pub struct JobConsumer;

impl Consumer for JobConsumer {
    fn consume(&self) -> Result<(), String> {
        // Logic to consume a task from the queue and process it
        Ok(())
    }
}