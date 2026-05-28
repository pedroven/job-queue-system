use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, UNIX_EPOCH};

use redis::{Client, Commands};

use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobPriority, JobStatus, TaskRecord};
use crate::persistence::JobState;

/// Default TTL applied to `job:{id}` hashes once status reaches a terminal
/// state. Keeps Redis memory bounded without forcing callers to clean up.
const TERMINAL_TTL: Duration = Duration::from_secs(60 * 60);

pub struct RedisJobState {
    client: Client,
    key_prefix: String,
    conn: Mutex<redis::Connection>,
}

impl RedisJobState {
    pub fn new(url: &str) -> Result<Self, QueueError> {
        Self::with_prefix(url, "jobs")
    }

    pub fn with_prefix(url: &str, key_prefix: &str) -> Result<Self, QueueError> {
        let client = Client::open(url)?;
        let conn = client.get_connection()?;
        Ok(Self {
            client,
            key_prefix: key_prefix.into(),
            conn: Mutex::new(conn),
        })
    }

    fn job_key(&self, id: &str) -> String {
        format!("{}:job:{id}", self.key_prefix)
    }

    fn dlq_stream(&self) -> String {
        format!("{}:dlq", self.key_prefix)
    }

    fn dlq_hash(&self, id: &str) -> String {
        format!("{}:dlq:{id}", self.key_prefix)
    }

    pub fn flush_prefix(&self) -> Result<(), QueueError> {
        // Best-effort cleanup for tests: scan and del every key under the
        // prefix. Not for production — uses `KEYS`, which is O(n).
        let mut conn = self.client.get_connection()?;
        let pattern = format!("{}:*", self.key_prefix);
        let keys: Vec<String> = conn.keys(pattern)?;
        if !keys.is_empty() {
            let _: i64 = conn.del(keys)?;
        }
        Ok(())
    }
}

fn is_terminal(status: JobStatus) -> bool {
    matches!(
        status,
        JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled
    )
}

impl JobState for RedisJobState {
    fn save_initial(&self, job: &Job) -> Result<(), QueueError> {
        let key = self.job_key(&job.id);
        let mut conn = self.conn.lock()?;
        // HSETNX-style insert: `HSET key field val ...` with a prior EXISTS
        // check would race, so we use `HSETNX` on `id` as the marker. If
        // the marker write succeeds, we're the owner and can fill the rest.
        let created: bool = conn.hset_nx(&key, "id", job.id.as_str())?;
        if !created {
            return Err(QueueError::AlreadyExists(job.id.clone()));
        }
        let created_secs = job
            .created_at
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let _: () = conn.hset_multiple(
            &key,
            &[
                ("status", job.status.as_str().to_string()),
                ("retry_count", job.retry_count.to_string()),
                ("task_id", job.task.id.clone()),
                ("task_name", job.task.name.clone()),
                ("payload", job.task.payload.clone()),
                ("max_attempts", job.max_attempts.to_string()),
                ("priority", (job.priority as u32).to_string()),
                ("created_at", created_secs.to_string()),
            ],
        )?;
        Ok(())
    }

    fn save_status(&self, id: &str, status: JobStatus) -> Result<(), QueueError> {
        let key = self.job_key(id);
        let mut conn = self.conn.lock()?;
        let exists: bool = conn.exists(&key)?;
        if !exists {
            return Err(QueueError::NotFound(id.to_string()));
        }
        // Sticky-Cancelled: if the current status is Cancelled, no later
        // transition may overwrite it. This is a read-then-write that races
        // against concurrent updates — for the cancel/worker race the
        // sequence is acceptable because cancel() happens before the worker
        // dequeues; under genuinely concurrent status writes (e.g. two
        // workers fighting), Redis Lua or `WATCH` would be safer. Matches
        // the SQLite impl's `UPDATE … WHERE status != 'cancelled'` guard,
        // which has the same TOCTOU but is acceptable for the same reason.
        let current: Option<String> = conn.hget(&key, "status")?;
        if current.as_deref() == Some(JobStatus::Cancelled.as_str()) {
            return Ok(());
        }
        let _: () = conn.hset(&key, "status", status.as_str())?;
        if is_terminal(status) {
            let _: () = conn.expire(&key, TERMINAL_TTL.as_secs() as i64)?;
        }
        Ok(())
    }

    fn save_retry_count(&self, id: &str, retry_count: u32) -> Result<(), QueueError> {
        let key = self.job_key(id);
        let mut conn = self.conn.lock()?;
        let exists: bool = conn.exists(&key)?;
        if !exists {
            return Err(QueueError::NotFound(id.to_string()));
        }
        let _: () = conn.hset(&key, "retry_count", retry_count)?;
        Ok(())
    }

    fn find_by_id(&self, id: &str) -> Result<Job, QueueError> {
        let key = self.job_key(id);
        let mut conn = self.conn.lock()?;
        let fields: HashMap<String, String> = conn.hgetall(&key)?;
        if fields.is_empty() {
            return Err(QueueError::NotFound(id.to_string()));
        }
        let get = |k: &str| -> Result<String, QueueError> {
            fields
                .get(k)
                .cloned()
                .ok_or_else(|| QueueError::JobFailed(format!("missing job field {k}")))
        };
        let priority_raw: u32 = get("priority")?
            .parse()
            .map_err(|_| QueueError::InvalidPriority(0))?;
        let priority = JobPriority::try_from(priority_raw)?;
        let created_secs: u64 = get("created_at")?.parse().unwrap_or(0);
        Ok(Job {
            id: get("id")?,
            status: get("status")?.parse()?,
            retry_count: get("retry_count")?.parse().unwrap_or(0),
            task: TaskRecord {
                id: get("task_id")?,
                name: get("task_name")?,
                payload: get("payload")?,
            },
            max_attempts: get("max_attempts")?.parse().unwrap_or(3),
            priority,
            created_at: UNIX_EPOCH + Duration::from_secs(created_secs),
        })
    }

    fn save_dead_letter(&self, dl: &DeadLetterJob) -> Result<(), QueueError> {
        let hash_key = self.dlq_hash(&dl.id);
        let stream_key = self.dlq_stream();
        let mut conn = self.conn.lock()?;
        let failed_secs = dl
            .failed_at
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let _: () = conn.hset_multiple(
            &hash_key,
            &[
                ("id", dl.id.clone()),
                ("original_job_id", dl.original_job_id.clone()),
                ("task_id", dl.task.id.clone()),
                ("task_name", dl.task.name.clone()),
                ("payload", dl.task.payload.clone()),
                ("error", dl.error.clone()),
                ("failed_at", failed_secs.to_string()),
            ],
        )?;
        // Stream entry holds just the dlq id; `find_all_dead_letter` walks
        // the stream and hydrates each via the hash. Two-store split keeps
        // `XRANGE` cheap while letting status APIs read random dlq rows in
        // O(1).
        let _: String = conn.xadd(&stream_key, "*", &[("id", dl.id.as_str())])?;
        Ok(())
    }

    fn find_all_dead_letter(&self) -> Result<Vec<DeadLetterJob>, QueueError> {
        let mut conn = self.conn.lock()?;
        let stream_key = self.dlq_stream();
        // XRANGE - + walks the stream oldest-first; cheap because dlq is
        // expected to stay small relative to the live queue.
        let entries: Vec<(String, HashMap<String, String>)> = redis::cmd("XRANGE")
            .arg(&stream_key)
            .arg("-")
            .arg("+")
            .query(&mut *conn)
            .unwrap_or_default();
        let mut out = Vec::with_capacity(entries.len());
        for (_stream_id, fields) in entries {
            let Some(dlq_id) = fields.get("id") else {
                continue;
            };
            let hash_key = self.dlq_hash(dlq_id);
            let h: HashMap<String, String> = conn.hgetall(&hash_key)?;
            if h.is_empty() {
                continue;
            }
            let get = |k: &str| h.get(k).cloned().unwrap_or_default();
            let failed_secs: u64 = h.get("failed_at").and_then(|s| s.parse().ok()).unwrap_or(0);
            out.push(DeadLetterJob {
                id: get("id"),
                original_job_id: get("original_job_id"),
                task: TaskRecord {
                    id: get("task_id"),
                    name: get("task_name"),
                    payload: get("payload"),
                },
                error: get("error"),
                failed_at: UNIX_EPOCH + Duration::from_secs(failed_secs),
            });
        }
        Ok(out)
    }
}
