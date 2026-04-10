# Job Queue System

An in-memory job queue system built in Rust with a producer-consumer architecture and multithreaded workers.

## Features

- Thread-safe job queue using `Mutex` and `Condvar`
- Configurable number of worker threads
- Producer-consumer pattern with trait-based interfaces
- Worker status tracking (Idle, Busy, ShuttingDown)
- Dead letter queue support for failed jobs
- Job retry configuration

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

The program starts worker threads and prompts for payloads via stdin. Each payload is wrapped into a job and dispatched to an available worker.

## Architecture

```
src/
├── main.rs              # Entry point — creates queue, starts workers, reads input
├── queue/
│   ├── mod.rs           # Queue struct — job enqueueing and worker dispatching
│   └── models.rs        # Data models (Job, Task, Worker, DeadLetterJob)
├── producer/
│   └── mod.rs           # Producer trait and JobProducer implementation
└── consumer/
    └── mod.rs           # Consumer trait and JobConsumer implementation
```

### How it works

1. `main` creates a `Queue` with N workers and starts them.
2. Each worker thread blocks on a `Condvar` until a job is available.
3. A `JobProducer` reads payloads from stdin and enqueues `Job` structs.
4. When a job is enqueued, `notify_one` wakes a waiting worker.
5. The worker marks itself as Busy, consumes the job via `JobConsumer`, then returns to Idle.

## Design

- **No external dependencies** — built entirely on `std`.
- **Trait-based interfaces** — `Producer` and `Consumer` traits allow alternative implementations.
- **Concurrency** — `Arc<Mutex<T>>` for shared state, `Condvar` for signaling between producer and consumer threads.
