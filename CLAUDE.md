# Job Queue System

## Overview

A job queue system built in Rust with a producer-consumer architecture using multithreaded workers. Work is **partitioned** so each partition has exactly one active consumer — adding workers (threads, processes, or hosts) scales throughput without per-job cross-worker coordination. Producers route each job deterministically by `hash(job.id) % partition_count`; coordination is paid only at partition-assignment time, not on the hot path.

Persistence is split into two backends behind two traits:
- **`JobDispatch`** — the hot path (enqueue, claim, ack, reclaim). Implementations: `RedisJobDispatch` (Redis Streams + consumer groups, production/multi-host), `SqliteJobDispatch` (atomic `UPDATE … RETURNING` claim, single-host dev), `InMemoryJobDispatch` (tests).
- **`JobState`** — the slow path (status, retry counts, DLQ, lookups). Implementations: `RedisJobState` (hashes with TTL on terminal status), `SqliteJobState`, `InMemoryJobState`.

`ClaimedJob { job, entry_id }` carries the backend-specific handle so `ack`/`reclaim_stale` target the right entry without trusting the job body.

## Build & Run

```sh
cargo build
cargo run            # SQLite backend (default), single partition
```

Backend + scale are selected by env vars at startup (see `src/main.rs`):

- `JOB_QUEUE_REDIS_URL` — when set (e.g. `redis://127.0.0.1:6379`), uses the Redis dispatch + state backends and a Redis `SET NX EX` scheduler lease. Otherwise SQLite + an always-on lease.
- `JOB_QUEUE_PARTITIONS` — total partition count (default `1`). This process owns all partitions `0..N`.
- `JOB_QUEUE_DB` — SQLite path (default `jobs.db`); also holds the scheduled-jobs table regardless of dispatch backend.

A local Redis for development/tests:

```sh
docker run -d --name jqs-redis -p 6379:6379 redis:7
```

## Check

```sh
cargo check
cargo clippy
```

## Architecture

- `src/main.rs` — Entry point. Selects the backend from env vars, builds the queue, starts workers + reaper + metrics reporter + scheduler, and reads payloads from stdin in a loop.
- `src/lib.rs` — Crate root. Declares modules and re-exports (`QueueError`, `#[task]` macro).
- `src/queue.rs` + `src/queue/` — module root declares submodules and re-exports. `queue/core.rs` holds the `Queue` struct (owns `Arc<dyn JobDispatch>`, `Arc<dyn JobState>`, `partition_count`; one worker thread per assigned partition) with tests in `queue/core/tests.rs`; `queue/config.rs` the `QueueConfig` (partition_count, assigned_partitions, back-pressure, claim lease, reaper interval) + validation; `queue/partition.rs` the stable FNV-1a `partition_for` routing; `queue/worker.rs` the per-partition `run_partition_loop` / `process_claim` / `handle_job_tries` helpers; `queue/reaper.rs` the `Reaper` + `ReaperThread` that periodically calls `reclaim_stale`; `queue/metrics.rs` the `MetricsCounters` + `MetricsSnapshot`.
- `src/models.rs` — Data models: `Job`, `TaskRecord`, `Worker`, `DeadLetterJob`, and their status enums.
- `src/producer.rs` — `Producer` trait and `JobProducer` implementation that enqueues jobs (back-pressure via `Queue::pending_count`, which reads the dispatch backend).
- `src/consumer.rs` — `Consumer` trait and `JobConsumer` implementation that processes jobs.
- `src/persistence.rs` + `src/persistence/` — module root declares the `JobDispatch` + `JobState` traits and `ClaimedJob`. Submodules: `memory_dispatch.rs`/`memory_state.rs` (test fakes), `sqlite_dispatch.rs`/`sqlite_state.rs` + shared `sqlite_schema.rs`, `redis_dispatch.rs`/`redis_state.rs`.
- `src/scheduler.rs` + `src/scheduler/` — module root declares submodules and re-exports. `scheduler/model.rs` holds `ScheduledJob` + `ScheduledJobRepository`, `scheduler/memory.rs` the in-memory impl, `scheduler/sqlite.rs` the `SqliteScheduledJobRepository`, `scheduler/lease.rs` the `SchedulerLease` trait + `AlwaysOnLease` (single-host) + `RedisSchedulerLease` (`SET NX EX` leader election), `scheduler/runner.rs` the `Scheduler` + `SchedulerHandle` (checks the lease before each tick) with tests in `scheduler/runner/tests.rs`.
- `src/task.rs` — Task registry and handler trait used by consumers.
- `src/error.rs` — `QueueError` enum used across the crate (includes `Redis`, `InvalidPartition`).
- `job-queue-macros/` — Proc-macro crate providing `#[task]`.

## Test

```sh
cargo test
```

Tests are colocated in each module using `#[cfg(test)]` blocks. Integration tests live in `tests/`:
- `tests/redis_backend.rs` — Redis dispatch/state round-trips.
- `tests/nfr_horizontal_scaling.rs` — two `Queue` instances on disjoint partitions against one Redis; asserts exactly-once-per-job delivery and (release-only, `#[ignore]`) ≥1.7× throughput at 2× instances.

Redis-dependent tests **skip with a printed SKIP line** when Redis is unreachable, so `cargo test` stays green without it. Start Redis (see Build & Run) to actually exercise them. Perf NFRs are `#[ignore]`; run with `cargo test --release -- --ignored`.

## Development Workflow (TDD)

Follow Test-Driven Development when adding new features or fixing bugs:

1. **Red** — Write a failing test that describes the expected behavior.
2. **Green** — Write the minimal code to make the test pass.
3. **Refactor** — Clean up the implementation while keeping tests green.

Always run `cargo test` before considering a change complete.

## Conventions

- Rust 2024 edition.
- Concurrency via `Arc<Mutex<T>>` and `Condvar` (no async runtime).
- Traits (`Producer`, `Consumer`) define the public interfaces for producing and consuming jobs.
- Keep methods short and focused — extract helpers for distinct logical steps.
- **500-line file limit.** No `.rs` file (including tests in the same file) may exceed 500 lines. When approaching the limit, split the file into a module folder following the `persistence.rs` + `persistence/` pattern: the flat file declares submodules and re-exports; each submodule holds one cohesive concept (trait, impl, model). Run `wc -l src/**/*.rs` before finishing a task that added substantial code.

## Dependencies

- `rusqlite` (with `bundled` feature) — SQLite dispatch + state backends.
- `redis` (with `streams` feature) — Redis Streams dispatch, hash state, and scheduler lease.
- `serde` / `serde_json` — payload and config (de)serialization.
- `uuid` — job and task identifiers.
- `cron` + `chrono` — cron expression parsing and time handling for the scheduler module.
- `job-queue-macros` (local) — provides the `#[task]` attribute macro.

## Keeping Docs In Sync

`CLAUDE.md` and `AGENTS.md` must stay aligned with the actual codebase. When you add, rename, move, or remove a module, or add/remove a dependency, update both files in the same change. Before finishing a task that touched the layout or `Cargo.toml`, re-read these files and reconcile them — stale docs are a defect.