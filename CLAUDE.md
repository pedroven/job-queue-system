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
- `src/queue/mod.rs` — Core `Queue` struct. Manages workers, job enqueueing, and dispatching via `Mutex`/`Condvar`.
- `src/queue/models.rs` — Data models: `Job`, `Task`, `Worker`, `DeadLetterJob`, and their status enums.
- `src/producer/mod.rs` — `Producer` trait and `JobProducer` implementation that enqueues jobs.
- `src/consumer/mod.rs` — `Consumer` trait and `JobConsumer` implementation that processes jobs.
- `src/persistence/mod.rs` — `JobRepository` trait with `SqliteJobRepository` (production) and `InMemoryJobRepository` (tests).

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
- `rusqlite` (with `bundled` feature) for SQLite persistence — the only external dependency.
- Concurrency via `Arc<Mutex<T>>` and `Condvar` (no async runtime).
- Traits (`Producer`, `Consumer`) define the public interfaces for producing and consuming jobs.
- Keep methods short and focused — extract helpers for distinct logical steps.
