use std::sync::Arc;
use std::time::{Duration, SystemTime};

use job_queue_system::error::QueueError;
use job_queue_system::models::JobPriority;
use job_queue_system::producer::JobProducer;
use job_queue_system::scheduler::{
    ScheduledJob, ScheduledJobRepository, Scheduler, SqliteScheduledJobRepository,
};
use job_queue_system::task_registry;
use job_queue_system::{persistence, queue, task};

#[task(max_attempts = 5, priority = JobPriority::High)]
fn send_email(to: String) -> Result<(), QueueError> {
    println!("Sending email to: {to}");
    Ok(())
}

#[task]
fn process_image(path: String) -> Result<(), QueueError> {
    println!("Processing image: {path}");
    Ok(())
}

#[task(max_attempts = 1)]
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

    let scheduled_repo: Arc<dyn ScheduledJobRepository> = Arc::new(
        SqliteScheduledJobRepository::new(&db_path).expect("Failed to open scheduled_jobs store"),
    );
    seed_default_schedules(scheduled_repo.as_ref());

    let producer = Arc::new(JobProducer::new(Arc::clone(&queue)));
    let scheduler = Scheduler::new(Arc::clone(&scheduled_repo), producer);
    let _scheduler_handle = scheduler.start(Duration::from_secs(1));

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

/// Registers the demo recurring schedules on first boot only. Uses
/// `save_if_absent` so restarts don't clobber persisted `last_run_at` /
/// `next_run_at` progress on rows the scheduler has already advanced.
///
/// Cron dialect: 7 fields — `sec min hour dom mon dow year` — driven by the
/// `cron` crate. This is NOT standard 5-field Unix cron; pasting `"*/5 * * * *"`
/// will fail with `QueueError::InvalidCron`.
fn seed_default_schedules(repo: &dyn ScheduledJobRepository) {
    let specs = [
        (
            "daily-report",
            "process_image",
            "daily.png",
            "0 0 9 * * * *",
        ),
        (
            "heartbeat",
            "send_email",
            "ops@example.com",
            "0 */5 * * * * *",
        ),
    ];
    for (id, task_name, payload, cron) in specs {
        match ScheduledJob::new(
            id.into(),
            task_name.into(),
            payload.into(),
            cron.into(),
            SystemTime::now(),
        ) {
            Ok(job) => {
                if let Err(e) = repo.save_if_absent(&job) {
                    eprintln!("failed to seed schedule {id}: {e}");
                }
            }
            Err(e) => eprintln!("invalid schedule {id}: {e}"),
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
