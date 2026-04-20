# Job Queue System

## Overview

A job queue system built in Rust with a producer-consumer architecture using multithreaded workers. Jobs are persisted to SQLite so pending work survives restarts.

## Build & Run

```sh
cargo build
cargo run
```

## Check

```sh
cargo check
cargo clippy
```

## Architecture

- `src/main.rs` — Entry point. Creates the queue, starts workers, and reads payloads from stdin in a loop.
- `src/lib.rs` — Crate root. Declares modules and re-exports (`QueueError`, `#[task]` macro).
- `src/queue.rs` — Core `Queue` struct. Manages workers, job enqueueing, and dispatching via `Mutex`/`Condvar`.
- `src/queue/` — Queue submodules (e.g. configuration, back pressure helpers).
- `src/models.rs` — Data models: `Job`, `TaskRecord`, `Worker`, `DeadLetterJob`, and their status enums.
- `src/producer.rs` — `Producer` trait and `JobProducer` implementation that enqueues jobs.
- `src/consumer.rs` — `Consumer` trait and `JobConsumer` implementation that processes jobs.
- `src/persistence.rs` + `src/persistence/` — `JobRepository` trait with `SqliteJobRepository` (production) and `InMemoryJobRepository` (tests).
- `src/scheduler.rs` — `Scheduler` + `ScheduledJobRepository` trait. Evaluates cron-defined `ScheduledJob`s and enqueues due jobs via `Producer`.
- `src/task.rs` — Task registry and handler trait used by consumers.
- `src/error.rs` — `QueueError` enum used across the crate.
- `job-queue-macros/` — Proc-macro crate providing `#[task]`.

## Test

```sh
cargo test
```

Tests are colocated in each module using `#[cfg(test)]` blocks.

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

## Dependencies

- `rusqlite` (with `bundled` feature) — SQLite persistence.
- `serde` / `serde_json` — payload and config (de)serialization.
- `uuid` — job and task identifiers.
- `cron` + `chrono` — cron expression parsing and time handling for the scheduler module.
- `job-queue-macros` (local) — provides the `#[task]` attribute macro.

## Keeping Docs In Sync

`CLAUDE.md` and `AGENTS.md` must stay aligned with the actual codebase. When you add, rename, move, or remove a module, or add/remove a dependency, update both files in the same change. Before finishing a task that touched the layout or `Cargo.toml`, re-read these files and reconcile them — stale docs are a defect.