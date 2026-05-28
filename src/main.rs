use std::sync::Arc;
use std::time::{Duration, SystemTime};

use job_queue_system::error::QueueError;
use job_queue_system::models::JobPriority;
use job_queue_system::persistence::{
    self, JobDispatch, JobState, RedisDispatchConfig, RedisJobDispatch, RedisJobState,
    SqliteJobDispatch, SqliteJobState,
};
use job_queue_system::producer::JobProducer;
use job_queue_system::scheduler::{
    AlwaysOnLease, RedisSchedulerLease, ScheduledJob, ScheduledJobRepository, Scheduler,
    SchedulerLease, SqliteScheduledJobRepository,
};
use job_queue_system::task_registry;
use job_queue_system::{queue, task};

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
    let registry = task_registry![send_email, process_image, sum_two_numbers];

    // Backend selection: `JOB_QUEUE_REDIS_URL` flips on Redis dispatch and
    // state. Otherwise default to SQLite single-partition.
    let backend = std::env::var("JOB_QUEUE_REDIS_URL").ok();
    let partition_count: u32 = std::env::var("JOB_QUEUE_PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let config = queue::QueueConfig {
        partition_count,
        assigned_partitions: (0..partition_count).collect(),
        ..Default::default()
    };

    let (dispatch, state): (Arc<dyn JobDispatch>, Arc<dyn JobState>) = match &backend {
        Some(url) => {
            let cfg = RedisDispatchConfig::new(url.as_str(), partition_count);
            let d = RedisJobDispatch::new(cfg).expect("connect redis dispatch");
            let s = RedisJobState::new(url).expect("connect redis state");
            (Arc::new(d), Arc::new(s))
        }
        None => {
            let db_path = std::env::var("JOB_QUEUE_DB").unwrap_or_else(|_| "jobs.db".to_string());
            let d =
                SqliteJobDispatch::new(&db_path, partition_count).expect("open sqlite dispatch");
            let s = SqliteJobState::new(&db_path).expect("open sqlite state");
            (Arc::new(d), Arc::new(s))
        }
    };
    let _ = persistence::InMemoryJobState::new; // suppress unused-import warning if applicable

    let queue =
        Arc::new(queue::Queue::with_config(dispatch, state, registry, config).expect("init queue"));
    queue.start_workers();
    let _metrics_handle = queue.start_metrics_reporter(Duration::from_secs(10));
    let _reaper_handle = queue.start_reaper();
    task::set_global_queue(Arc::clone(&queue)).expect("global queue already installed");

    let db_path = std::env::var("JOB_QUEUE_DB").unwrap_or_else(|_| "jobs.db".to_string());
    let scheduled_repo: Arc<dyn ScheduledJobRepository> = Arc::new(
        SqliteScheduledJobRepository::new(&db_path).expect("Failed to open scheduled_jobs store"),
    );
    seed_default_schedules(scheduled_repo.as_ref());

    let producer = Arc::new(JobProducer::new(Arc::clone(&queue)));
    let lease: Arc<dyn SchedulerLease> = match &backend {
        Some(url) => Arc::new(
            RedisSchedulerLease::new(url, "scheduler:lease").expect("connect scheduler lease"),
        ),
        None => Arc::new(AlwaysOnLease),
    };
    let scheduler = Scheduler::new(Arc::clone(&scheduled_repo), producer, lease);
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
