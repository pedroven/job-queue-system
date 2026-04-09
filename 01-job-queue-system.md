# Job Queue System

## Description

A background job processing system where producers submit jobs and workers consume and execute them asynchronously. This is one of the most practical projects you can build — nearly every production system relies on some form of job queue for tasks like sending emails, resizing images, generating reports, or running scheduled jobs.

The core challenge is building a reliable system where jobs are never lost, processed exactly once (or at least once with idempotency), and workers can scale independently from producers.

## Concepts Practiced

- **Message Queues** — the backbone of the system; jobs are placed in a queue and consumed by workers
- **Task Queues** — higher-level abstraction over message queues with job tracking, retries, and priorities
- **Asynchronism** — producers don't wait for job completion; they submit and move on
- **Back Pressure** — workers signal producers to slow down when the queue depth grows too large
- **Idempotent Operations** — jobs must be safe to retry; executing the same job twice should not cause side effects
- **Availability Patterns** — workers should fail gracefully without losing jobs
- **Eventual Consistency** — job status updates propagate asynchronously; the system is eventually consistent on job state

## Requirements

### Functional Requirements

- **Job Submission** — clients can submit jobs with a type, payload, and optional priority
- **Job Processing** — workers pick up jobs and execute them based on type handlers
- **Job Status Tracking** — clients can query the status of a submitted job (pending, running, completed, failed)
- **Retries** — failed jobs are retried up to a configurable number of times with exponential backoff
- **Priority Queue** — jobs with higher priority are processed before lower-priority ones
- **Scheduled Jobs** — support submitting jobs to run at a specific time or on a recurring schedule
- **Dead Letter Queue** — jobs that exhaust retries are moved to a DLQ for inspection

### Non-Functional Requirements

- Jobs must not be lost even if a worker crashes mid-execution
- The system must support at least 1,000 job submissions per second
- Workers must scale horizontally without coordination overhead
- Job status queries must respond in under 100ms

### Milestones

1. **Basic queue** — in-memory queue with a single producer and single worker
2. **Persistence** — back the queue with a durable store (Redis, PostgreSQL, or RabbitMQ)
3. **Multiple workers** — add competing consumers; ensure a job is only processed by one worker
4. **Retries and DLQ** — implement exponential backoff and dead letter handling
5. **Priority support** — implement a priority queue where higher-priority jobs jump the line
6. **Back pressure** — workers report queue depth; producers slow submission when depth exceeds a threshold
7. **Scheduled jobs** — add a scheduler that enqueues jobs at the right time
8. **Monitoring dashboard** — expose queue depth, job throughput, failure rate, and worker count

## Extension Challenges

- Implement job cancellation after submission
- Add job chaining — job B starts only after job A completes successfully
- Experiment with at-least-once vs exactly-once delivery guarantees and observe the tradeoffs
- Introduce an artificial slow worker and observe back pressure kicking in
