# Job Queue System

A job queue system built in Rust with a producer-consumer architecture and
multithreaded workers. Jobs are persisted to SQLite, so pending work survives
process restarts. Concurrency is built on `std` primitives (`Arc<Mutex<T>>` and
`Condvar`) — there is no async runtime.

> **Looking to scale across processes or hosts?** A
> [`horizontal-scaling`](https://github.com/pedroven/job-queue-system/tree/horizontal-scaling)
> branch extends this design with partitioned work distribution and a pluggable
> Redis backend (Redis Streams dispatch + scheduler leader election), so multiple
> instances can share one queue. See that branch's README for details.

## Features

- Thread-safe job queue using `Mutex` and `Condvar`
- Configurable number of worker threads
- Producer-consumer pattern with trait-based interfaces (`Producer` / `Consumer`)
- SQLite persistence (`SqliteJobRepository`) with an in-memory repository for tests
- `#[task]` attribute macro for declaring jobs and enqueueing them with `perform_async`
- Per-task priorities and retry limits (`max_attempts`)
- Dead letter queue for jobs that exhaust their retries
- Job cancellation
- Metrics reporting — queue depth, DLQ depth, worker counts, completed / failed /
  dead-lettered totals, throughput, and failure rate
- Cron scheduler for recurring jobs, with schedules persisted to SQLite

## Getting Started

### Prerequisites

- Rust (2024 edition)

### Build

```sh
cargo build
```

### Run

```sh
cargo run
```

The binary opens a SQLite database (path from the `JOB_QUEUE_DB` environment
variable, defaulting to `jobs.db`), starts worker threads, a metrics reporter,
and the cron scheduler, then prompts on stdin for a task to enqueue
(`send_email`, `process_image`, or `sum`).

### Test

```sh
cargo test
```

## Usage

Declare a job with the `#[task]` macro, register it, install a global queue, and
enqueue work with `perform_async`:

```rust
use std::sync::Arc;
use job_queue_system::error::QueueError;
use job_queue_system::persistence::{InMemoryJobRepository, JobRepository};
use job_queue_system::queue::Queue;
use job_queue_system::{task, task_registry};

#[task]
fn greet(name: String) -> Result<(), QueueError> {
    println!("hello, {name}!");
    Ok(())
}

fn main() {
    let repo: Arc<dyn JobRepository> = Arc::new(InMemoryJobRepository::new());
    let registry = task_registry![greet];

    let queue = Arc::new(Queue::new(2, repo, registry).unwrap());
    queue.start_workers();
    task::set_global_queue(Arc::clone(&queue)).unwrap();

    greet.perform_async("world".to_string()).unwrap();
}
```

`#[task]` accepts options such as `max_attempts` and `priority`:

```rust
use job_queue_system::models::JobPriority;

#[task(max_attempts = 5, priority = JobPriority::High)]
fn send_email(to: String) -> Result<(), QueueError> {
    println!("Sending email to: {to}");
    Ok(())
}
```

Run the bundled example end to end:

```sh
cargo run --example basic
```

## Development Workflow (TDD)

This project follows Test-Driven Development:

1. **Red** — Write a failing test that describes the expected behavior.
2. **Green** — Write the minimal code to make the test pass.
3. **Refactor** — Clean up the implementation while keeping tests green.

Tests are colocated in each module using `#[cfg(test)]` blocks, with broader
integration and non-functional-requirement suites under `tests/`. Always run
`cargo test` before considering a change complete.

## Architecture

```
src/
├── main.rs            # Entry point — opens SQLite, starts workers, scheduler, metrics; reads stdin
├── lib.rs             # Crate root — module declarations and re-exports
├── models.rs          # Data models (Job, TaskRecord, Worker, DeadLetterJob) and status enums
├── error.rs           # QueueError enum
├── task.rs            # Task registry and handler trait
├── producer.rs        # Producer trait and JobProducer
├── consumer.rs        # Consumer trait and JobConsumer
├── queue.rs + queue/  # Queue struct (core), config, priority levels, worker helpers, metrics
├── persistence.rs + persistence/   # JobRepository trait, SqliteJobRepository, InMemoryJobRepository
└── scheduler.rs + scheduler/        # ScheduledJob model, repositories, cron Scheduler + background tick
job-queue-macros/      # Proc-macro crate providing #[task]
```

### How it works

1. `main` opens a `SqliteJobRepository`, builds a `Queue` with N workers, and starts them.
2. Each worker thread blocks on a `Condvar` until a job is available.
3. A `JobProducer` (or a `#[task]`'s `perform_async`) enqueues jobs, persisting them to SQLite.
4. When a job is enqueued, `notify_one` wakes a waiting worker.
5. The worker marks itself Busy, runs the registered task handler via `JobConsumer`, then returns to Idle.
6. Failed jobs are retried up to `max_attempts`; jobs that exhaust their retries move to the dead letter queue.
7. The `Scheduler` evaluates cron expressions on a background tick and enqueues due jobs through the producer.

### Cron dialect

The scheduler uses the `cron` crate's **7-field** format
(`sec min hour day-of-month month day-of-week year`) — not standard 5-field Unix
cron. For example, "every 5 minutes" is `0 */5 * * * * *`.

## Design

- **Persistence** — jobs are stored in SQLite (`rusqlite`, bundled) so pending work survives restarts.
- **Trait-based interfaces** — `Producer`, `Consumer`, and `JobRepository` traits allow alternative implementations (e.g. the in-memory repository used in tests).
- **Concurrency** — `Arc<Mutex<T>>` for shared state and `Condvar` for signaling between producer and worker threads; no async runtime.

## Dependencies

- `rusqlite` (with `bundled`) — SQLite persistence
- `serde` / `serde_json` — payload and config (de)serialization
- `uuid` — job and task identifiers
- `cron` + `chrono` — cron parsing and time handling for the scheduler
- `job-queue-macros` (local) — the `#[task]` attribute macro
