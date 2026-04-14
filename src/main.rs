use std::sync::Arc;

use job_queue_system::error::QueueError;
use job_queue_system::task_registry;
use job_queue_system::{persistence, queue, task};

#[task]
fn send_email(to: String) -> Result<(), QueueError> {
    println!("Sending email to: {to}");
    Ok(())
}

#[task]
fn process_image(path: String) -> Result<(), QueueError> {
    println!("Processing image: {path}");
    Ok(())
}

#[task]
fn sum_two_numbers(a: &i32, b: &i32) -> i32 {
    let result = a + b;
    println!("sum({a}, {b}) = {result}");
    result
}

fn main() {
    let db_path = std::env::var("JOB_QUEUE_DB").unwrap_or_else(|_| "jobs.db".to_string());
    let job_repository: Arc<dyn persistence::JobRepository> =
        Arc::new(persistence::SqliteJobRepository::new(&db_path).expect("Failed to open database"));

    let num_workers = 4;
    let registry = task_registry![send_email, process_image, sum_two_numbers];
    let queue = Arc::new(
        queue::Queue::new(num_workers, Arc::clone(&job_repository), registry)
            .expect("Failed to initialize queue"),
    );
    queue.start_workers();
    task::set_global_queue(Arc::clone(&queue)).expect("global queue already installed");

    loop {
        println!("Enter task (send_email / process_image / sum):");
        let mut task_name = String::new();
        std::io::stdin()
            .read_line(&mut task_name)
            .expect("Failed to read line");

        let result = match task_name.trim() {
            "send_email" => send_email.perform_async(read_line("to")),
            "process_image" => process_image.perform_async(read_line("path")),
            "sum" => {
                let a: i32 = read_line("a").parse().expect("a must be an integer");
                let b: i32 = read_line("b").parse().expect("b must be an integer");
                sum_two_numbers.perform_async(a, b)
            }
            other => {
                eprintln!("Unknown task: {other}");
                continue;
            }
        };
        if let Err(e) = result {
            eprintln!("{e:?}");
        }
    }
}

fn read_line(prompt: &str) -> String {
    println!("{prompt}:");
    let mut buf = String::new();
    std::io::stdin()
        .read_line(&mut buf)
        .expect("Failed to read line");
    buf.trim().to_string()
}
