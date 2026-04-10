# Job Queue System

## Overview

An in-memory job queue system built in Rust with a producer-consumer architecture using multithreaded workers.

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

## Conventions

- Rust 2024 edition.
- No external dependencies — uses only `std`.
- Concurrency via `Arc<Mutex<T>>` and `Condvar` (no async runtime).
- Traits (`Producer`, `Consumer`) define the public interfaces for producing and consuming jobs.
- Keep methods short and focused — extract helpers for distinct logical steps.
