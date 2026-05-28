//! Integration tests for the Redis dispatch + state backends.
//!
//! These need a Redis reachable at `JOB_QUEUE_REDIS_URL` (default
//! `redis://127.0.0.1:6379`). When Redis is unreachable each test prints a
//! SKIP line and returns Ok — so `cargo test` stays green on machines
//! without Redis. Start one with:
//!   docker run -d --name jqs-redis -p 6379:6379 redis:7

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use job_queue_system::models::{DeadLetterJob, Job, JobPriority, JobStatus, TaskRecord};
use job_queue_system::persistence::{
    JobDispatch, JobState, RedisDispatchConfig, RedisJobDispatch, RedisJobState,
};

fn redis_url() -> String {
    std::env::var("JOB_QUEUE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

/// Returns a unique key prefix for this test so parallel tests / repeated
/// runs never collide on Redis state.
fn unique_prefix(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("jqstest:{label}:{}:{nanos}", std::process::id())
}

/// `Some((dispatch, state))` when Redis is reachable, else `None` (caller
/// prints SKIP and returns).
fn backends(label: &str, partitions: u32) -> Option<(RedisJobDispatch, RedisJobState, String)> {
    let url = redis_url();
    // Cheap reachability probe before constructing the real backends.
    let client = match redis::Client::open(url.as_str()) {
        Ok(c) => c,
        Err(_) => return None,
    };
    if client.get_connection().is_err() {
        return None;
    }
    let prefix = unique_prefix(label);
    let cfg = RedisDispatchConfig {
        url: url.clone(),
        partition_count: partitions,
        key_prefix: prefix.clone(),
    };
    let dispatch = RedisJobDispatch::new(cfg).expect("dispatch");
    let state = RedisJobState::with_prefix(&url, &prefix).expect("state");
    Some((dispatch, state, prefix))
}

fn make_job(id: &str, payload: &str) -> Job {
    Job::with_task_name(id.into(), "default".into(), payload.into())
}

#[test]
fn redis_enqueue_claim_ack_roundtrip() {
    let Some((dispatch, _state, _prefix)) = backends("roundtrip", 1) else {
        eprintln!(
            "SKIP redis_enqueue_claim_ack_roundtrip: no Redis at {}",
            redis_url()
        );
        return;
    };
    dispatch.enqueue(0, &make_job("j1", "hello")).unwrap();
    let claim = dispatch
        .next_for_partition(0, "c1", Duration::from_millis(500))
        .unwrap()
        .expect("should claim a job");
    assert_eq!(claim.job.id, "j1");
    assert_eq!(claim.job.task.payload, "hello");
    dispatch.ack(0, &claim).unwrap();
    // After ack the stream is empty.
    assert_eq!(dispatch.pending_count().unwrap(), 0);
    dispatch.flush_prefix().unwrap();
}

#[test]
fn redis_high_priority_drains_first() {
    let Some((dispatch, _state, _prefix)) = backends("priority", 1) else {
        eprintln!("SKIP redis_high_priority_drains_first: no Redis");
        return;
    };
    dispatch.enqueue(0, &make_job("normal-1", "n")).unwrap();
    let mut high = make_job("high-1", "h");
    high.priority = JobPriority::High;
    dispatch.enqueue(0, &high).unwrap();

    let first = dispatch
        .next_for_partition(0, "c", Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert_eq!(first.job.id, "high-1", "High must drain before Normal");
    dispatch.flush_prefix().unwrap();
}

#[test]
fn redis_reclaim_stale_redelivers() {
    let Some((dispatch, _state, _prefix)) = backends("reclaim", 1) else {
        eprintln!("SKIP redis_reclaim_stale_redelivers: no Redis");
        return;
    };
    dispatch.enqueue(0, &make_job("stuck", "p")).unwrap();
    // Claim but never ack — simulates a crashed worker.
    let _claim = dispatch
        .next_for_partition(0, "dead-consumer", Duration::from_millis(500))
        .unwrap()
        .unwrap();
    // Idle=0 means the PEL entry is immediately eligible for reclaim.
    let reclaimed = dispatch.reclaim_stale(0, Duration::ZERO).unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].job.id, "stuck");
    // A fresh consumer can now pick it up.
    let again = dispatch
        .next_for_partition(0, "live-consumer", Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert_eq!(again.job.id, "stuck");
    dispatch.flush_prefix().unwrap();
}

#[test]
fn redis_state_status_transitions_and_sticky_cancel() {
    let Some((_dispatch, state, _prefix)) = backends("state", 1) else {
        eprintln!("SKIP redis_state_status_transitions_and_sticky_cancel: no Redis");
        return;
    };
    state.save_initial(&make_job("j", "p")).unwrap();
    assert_eq!(state.find_by_id("j").unwrap().status, JobStatus::Pending);

    state.save_status("j", JobStatus::Running).unwrap();
    assert_eq!(state.find_by_id("j").unwrap().status, JobStatus::Running);

    // Sticky-Cancelled: once cancelled, later writes are dropped.
    state.save_status("j", JobStatus::Cancelled).unwrap();
    state.save_status("j", JobStatus::Completed).unwrap();
    assert_eq!(state.find_by_id("j").unwrap().status, JobStatus::Cancelled);

    state.flush_prefix().unwrap();
}

#[test]
fn redis_state_dead_letter_roundtrip() {
    let Some((_dispatch, state, _prefix)) = backends("dlq", 1) else {
        eprintln!("SKIP redis_state_dead_letter_roundtrip: no Redis");
        return;
    };
    let dl = DeadLetterJob {
        id: "dl-1".into(),
        original_job_id: "j1".into(),
        task: TaskRecord {
            id: "task-j1".into(),
            name: "default".into(),
            payload: "p".into(),
        },
        error: "boom".into(),
        failed_at: SystemTime::now(),
    };
    state.save_dead_letter(&dl).unwrap();
    let all = state.find_all_dead_letter().unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].original_job_id, "j1");
    assert_eq!(all[0].error, "boom");
    state.flush_prefix().unwrap();
}

#[test]
fn redis_save_initial_rejects_duplicate() {
    let Some((_dispatch, state, _prefix)) = backends("dup", 1) else {
        eprintln!("SKIP redis_save_initial_rejects_duplicate: no Redis");
        return;
    };
    state.save_initial(&make_job("j", "p")).unwrap();
    let err = state.save_initial(&make_job("j", "p")).unwrap_err();
    assert!(matches!(
        err,
        job_queue_system::error::QueueError::AlreadyExists(_)
    ));
    state.flush_prefix().unwrap();
}
