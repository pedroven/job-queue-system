# Job Queue System

A job queue system built in Rust with a producer-consumer architecture and
multithreaded workers. Work is **partitioned** so each partition has exactly one
active consumer — adding workers (threads, processes, or hosts) scales throughput
without per-job cross-worker coordination. Concurrency is built on `std`
primitives (`Arc<Mutex<T>>` and `Condvar`); there is no async runtime.

Producers route each job deterministically by `hash(job.id) % partition_count`,
so coordination is paid only at partition-assignment time, not on the hot path.

## Features

- Thread-safe workers using `Mutex` and `Condvar` (one worker thread per assigned partition)
- **Horizontal scaling** — deterministic partition routing with single-consumer-per-partition semantics
- Pluggable persistence behind two traits:
  - **`JobDispatch`** (hot path: enqueue, claim, ack, reclaim) — Redis Streams, SQLite, or in-memory
  - **`JobState`** (slow path: status, retries, DLQ, lookups) — Redis hashes, SQLite, or in-memory
- Producer-consumer pattern with trait-based interfaces (`Producer` / `Consumer`)
- `#[task]` attribute macro for declaring jobs and enqueueing them with `perform_async`
- Per-task priorities and retry limits (`max_attempts`)
- Dead letter queue for jobs that exhaust their retries
- Stale-claim recovery via a background **reaper** (`reclaim_stale`)
- Back-pressure on enqueue (via `Queue::pending_count`)
- Job cancellation
- Metrics reporting — queue depth, DLQ depth, worker counts, completed / failed /
  dead-lettered totals, throughput, and failure rate
- Cron scheduler for recurring jobs, with leader election so only one host fires
  schedules at a time

## Getting Started

### Prerequisites

- Rust (2024 edition)
- Redis 7+ (optional — only for the Redis backend and multi-host scaling)

### Build

```sh
cargo build
```

### Run

```sh
cargo run            # SQLite backend (default), single partition
```

Backend and scale are selected by environment variables at startup:

- `JOB_QUEUE_REDIS_URL` — when set (e.g. `redis://127.0.0.1:6379`), uses the Redis
  dispatch + state backends and a Redis `SET NX EX` scheduler lease. Otherwise
  SQLite + an always-on lease.
- `JOB_QUEUE_PARTITIONS` — total partition count (default `1`). This process owns
  all partitions `0..N`.
- `JOB_QUEUE_DB` — SQLite path (default `jobs.db`); also holds the scheduled-jobs
  table regardless of the dispatch backend.

A local Redis for development and tests:

```sh
docker run -d --name jqs-redis -p 6379:6379 redis:7
```

### Test

```sh
cargo test
```

Redis-dependent tests **skip with a printed SKIP line** when Redis is
unreachable, so `cargo test` stays green without it. Start Redis (above) to
exercise them. Performance NFRs are `#[ignore]`d; run them with:

```sh
cargo test --release -- --ignored
```

## Usage

Declare a job with the `#[task]` macro, register it, install a global queue, and
enqueue work with `perform_async`:

```rust
use std::sync::Arc;
use job_queue_system::error::QueueError;
use job_queue_system::queue::Queue;
use job_queue_system::{task, task_registry};

#[task(max_attempts = 5, priority = job_queue_system::models::JobPriority::High)]
fn send_email(to: String) -> Result<(), QueueError> {
    println!("Sending email to: {to}");
    Ok(())
}

// Build a queue over your chosen JobDispatch + JobState backends, then:
//   queue.start_workers();
//   task::set_global_queue(Arc::clone(&queue)).unwrap();
//   send_email.perform_async("ops@example.com".to_string()).unwrap();
```

See `src/main.rs` for backend selection and `examples/basic.rs` for a complete,
runnable setup.

## Development Workflow (TDD)

This project follows Test-Driven Development:

1. **Red** — Write a failing test that describes the expected behavior.
2. **Green** — Write the minimal code to make the test pass.
3. **Refactor** — Clean up the implementation while keeping tests green.

Tests are colocated in each module using `#[cfg(test)]` blocks, with integration
and non-functional-requirement suites under `tests/`. Always run `cargo test`
before considering a change complete.

## Architecture

```
src/
├── main.rs            # Entry point — selects backend from env, starts workers + reaper + metrics + scheduler
├── lib.rs             # Crate root — module declarations and re-exports
├── models.rs          # Data models (Job, TaskRecord, Worker, DeadLetterJob) and status enums
├── error.rs           # QueueError enum (incl. Redis, InvalidPartition)
├── task.rs            # Task registry and handler trait
├── producer.rs        # Producer trait and JobProducer (back-pressure via pending_count)
├── consumer.rs        # Consumer trait and JobConsumer
├── queue.rs + queue/  # Queue (core), config, partition routing, per-partition worker loops, reaper, metrics
├── persistence.rs + persistence/   # JobDispatch + JobState traits, ClaimedJob; memory/sqlite/redis impls
└── scheduler.rs + scheduler/        # ScheduledJob model, repositories, lease (leader election), cron runner
job-queue-macros/      # Proc-macro crate providing #[task]
```

### Persistence backends

Persistence is split into two traits so the hot and slow paths can scale
independently:

- **`JobDispatch`** — enqueue, claim, ack, reclaim. `ClaimedJob { job, entry_id }`
  carries a backend-specific handle so `ack` / `reclaim_stale` target the right
  entry without trusting the job body.
  - `RedisJobDispatch` — Redis Streams + consumer groups (production, multi-host)
  - `SqliteJobDispatch` — atomic `UPDATE … RETURNING` claim (single-host dev)
  - `InMemoryJobDispatch` — tests
- **`JobState`** — status, retry counts, DLQ, lookups.
  - `RedisJobState` — hashes with a TTL on terminal status
  - `SqliteJobState`
  - `InMemoryJobState`

### How it works

1. `main` selects backends from env vars and builds a `Queue` owning
   `Arc<dyn JobDispatch>`, `Arc<dyn JobState>`, and a `partition_count`.
2. The queue starts one worker thread per assigned partition; each loops on
   `claim` for its partition.
3. A `JobProducer` (or a `#[task]`'s `perform_async`) routes each job to a
   partition by `hash(job.id) % partition_count` and enqueues it.
4. The worker runs the registered task handler via `JobConsumer`, then `ack`s the
   claim.
5. Failed jobs are retried up to `max_attempts`; jobs that exhaust their retries
   move to the dead letter queue.
6. A background **reaper** periodically calls `reclaim_stale` to recover claims
   held by crashed or stalled workers.
7. The `Scheduler` checks its lease before each tick; only the lease holder
   evaluates cron expressions and enqueues due jobs.

### Scaling out

Run multiple instances against one Redis, each owning a disjoint slice of
partitions, to scale throughput. Set `JOB_QUEUE_PARTITIONS` to the total count
and give each process its partition range. Because each partition has exactly one
active consumer, jobs are delivered exactly once per partition without
cross-worker coordination on the hot path. `tests/nfr_horizontal_scaling.rs`
exercises two `Queue` instances on disjoint partitions against one Redis and
asserts exactly-once-per-job delivery (plus a release-only ≥1.7× throughput
check at 2× instances).

### Cron dialect

The scheduler uses the `cron` crate's **7-field** format
(`sec min hour day-of-month month day-of-week year`) — not standard 5-field Unix
cron. For example, "every 5 minutes" is `0 */5 * * * * *`.

## Dependencies

- `rusqlite` (with `bundled`) — SQLite dispatch + state backends
- `redis` (with `streams`) — Redis Streams dispatch, hash state, and scheduler lease
- `serde` / `serde_json` — payload and config (de)serialization
- `uuid` — job and task identifiers
- `cron` + `chrono` — cron parsing and time handling for the scheduler
- `job-queue-macros` (local) — the `#[task]` attribute macro
