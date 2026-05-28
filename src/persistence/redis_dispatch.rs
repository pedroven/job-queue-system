use std::sync::Mutex;
use std::time::{Duration, UNIX_EPOCH};

use redis::streams::{StreamAutoClaimReply, StreamId, StreamReadOptions, StreamReadReply};
use redis::{Client, Commands, FromRedisValue, Value};

use crate::error::QueueError;
use crate::models::{Job, JobPriority, JobStatus, TaskRecord};
use crate::persistence::{ClaimedJob, JobDispatch};

const CONSUMER_GROUP: &str = "workers";

/// Construction-time config for `RedisJobDispatch`. Keys are namespaced under
/// `key_prefix` so multiple queues can share one Redis without colliding
/// (handy for integration tests).
#[derive(Clone, Debug)]
pub struct RedisDispatchConfig {
    pub url: String,
    pub partition_count: u32,
    pub key_prefix: String,
}

impl RedisDispatchConfig {
    pub fn new(url: impl Into<String>, partition_count: u32) -> Self {
        Self {
            url: url.into(),
            partition_count,
            key_prefix: "jobs".into(),
        }
    }
}

pub struct RedisJobDispatch {
    client: Client,
    config: RedisDispatchConfig,
    /// Shared connection for the quick ops (enqueue, ack, reclaim,
    /// pending_count). These never block, so one mutex is fine.
    conn: Mutex<redis::Connection>,
    /// One read connection per partition index. `XREADGROUP BLOCK` ties up
    /// its connection until the timeout, so giving each partition its own
    /// connection lets per-partition workers block in parallel instead of
    /// serializing on a single shared connection.
    read_conns: Vec<Mutex<redis::Connection>>,
}

impl RedisJobDispatch {
    pub fn new(config: RedisDispatchConfig) -> Result<Self, QueueError> {
        assert!(config.partition_count > 0, "partition_count must be > 0");
        let client = Client::open(config.url.as_str())?;
        let mut conn = client.get_connection()?;
        // Eagerly create the consumer group on every (partition × priority)
        // stream so the first `XREADGROUP` doesn't trip on NOGROUP. MKSTREAM
        // creates the stream itself if it doesn't exist yet.
        for p in 0..config.partition_count {
            for prio in PRIORITIES {
                let stream = stream_name(&config.key_prefix, p, *prio);
                ensure_group(&mut conn, &stream, CONSUMER_GROUP)?;
            }
        }
        let mut read_conns = Vec::with_capacity(config.partition_count as usize);
        for _ in 0..config.partition_count {
            read_conns.push(Mutex::new(client.get_connection()?));
        }
        Ok(Self {
            client,
            config,
            conn: Mutex::new(conn),
            read_conns,
        })
    }

    fn check_partition(&self, partition: u32) -> Result<(), QueueError> {
        if partition >= self.config.partition_count {
            return Err(QueueError::InvalidPartition {
                partition,
                count: self.config.partition_count,
            });
        }
        Ok(())
    }

    /// Drop in tests / shutdown to clear every dispatch key under the
    /// current prefix. Not on the trait — admin op.
    pub fn flush_prefix(&self) -> Result<(), QueueError> {
        let mut conn = self.client.get_connection()?;
        for p in 0..self.config.partition_count {
            for prio in PRIORITIES {
                let _: () = conn.del(stream_name(&self.config.key_prefix, p, *prio))?;
            }
        }
        Ok(())
    }
}

const PRIORITIES: &[JobPriority] = &[JobPriority::High, JobPriority::Normal];

fn priority_tag(p: JobPriority) -> &'static str {
    match p {
        JobPriority::High => "high",
        JobPriority::Normal => "normal",
    }
}

fn priority_from_tag(s: &str) -> Result<JobPriority, QueueError> {
    match s {
        "high" => Ok(JobPriority::High),
        "normal" => Ok(JobPriority::Normal),
        other => Err(QueueError::InvalidStatus(format!(
            "unknown priority tag {other}"
        ))),
    }
}

fn stream_name(prefix: &str, partition: u32, prio: JobPriority) -> String {
    format!("{prefix}:p{partition}:{}", priority_tag(prio))
}

/// Encode the redis stream and entry id into a round-trippable handle so
/// `ack`/`reclaim_stale` can target the original stream without re-reading
/// the job body. Format: `"{prio_tag}|{stream_entry_id}"`.
fn encode_entry(prio: JobPriority, stream_entry_id: &str) -> String {
    format!("{}|{}", priority_tag(prio), stream_entry_id)
}

fn decode_entry(handle: &str) -> Result<(JobPriority, &str), QueueError> {
    let (prio, sid) = handle
        .split_once('|')
        .ok_or_else(|| QueueError::JobFailed(format!("malformed entry handle: {handle}")))?;
    Ok((priority_from_tag(prio)?, sid))
}

fn ensure_group(conn: &mut redis::Connection, stream: &str, group: &str) -> Result<(), QueueError> {
    let res: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream)
        .arg(group)
        .arg("$")
        .arg("MKSTREAM")
        .query(conn);
    match res {
        Ok(()) => Ok(()),
        // `BUSYGROUP` means the group already exists — that's the idempotent
        // success path. Any other failure is real.
        Err(e) if e.code() == Some("BUSYGROUP") => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn serialize_job(job: &Job) -> Result<String, QueueError> {
    let payload = serde_json::json!({
        "id": job.id,
        "retry_count": job.retry_count,
        "task_id": job.task.id,
        "task_name": job.task.name,
        "task_payload": job.task.payload,
        "max_attempts": job.max_attempts,
        "priority": job.priority as u32,
        "created_at": job
            .created_at
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    });
    serde_json::to_string(&payload)
        .map_err(|e| QueueError::JobFailed(format!("serialize job {}: {e}", job.id)))
}

fn deserialize_job(blob: &str) -> Result<Job, QueueError> {
    let v: serde_json::Value = serde_json::from_str(blob)
        .map_err(|e| QueueError::JobFailed(format!("deserialize job: {e}")))?;
    let get_str = |k: &str| -> Result<String, QueueError> {
        v.get(k)
            .and_then(|s| s.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| QueueError::JobFailed(format!("missing field {k}")))
    };
    let get_u64 = |k: &str| -> Result<u64, QueueError> {
        v.get(k)
            .and_then(|s| s.as_u64())
            .ok_or_else(|| QueueError::JobFailed(format!("missing field {k}")))
    };
    let id = get_str("id")?;
    let priority = JobPriority::try_from(get_u64("priority")? as u32)?;
    Ok(Job {
        id,
        status: JobStatus::Running,
        retry_count: get_u64("retry_count")? as u32,
        task: TaskRecord {
            id: get_str("task_id")?,
            name: get_str("task_name")?,
            payload: get_str("task_payload")?,
        },
        max_attempts: get_u64("max_attempts")? as u32,
        priority,
        created_at: UNIX_EPOCH + Duration::from_secs(get_u64("created_at")?),
    })
}

impl JobDispatch for RedisJobDispatch {
    fn enqueue(&self, partition: u32, job: &Job) -> Result<(), QueueError> {
        self.check_partition(partition)?;
        let stream = stream_name(&self.config.key_prefix, partition, job.priority);
        let blob = serialize_job(job)?;
        let mut conn = self.conn.lock()?;
        // Auto-id (`*`) so Redis stamps a monotonic stream entry id. We
        // include `job_id` in the payload too so consumers can route to
        // state without re-deriving it from the entry id.
        let _: String = conn.xadd(
            &stream,
            "*",
            &[("job_id", job.id.as_str()), ("payload", blob.as_str())],
        )?;
        Ok(())
    }

    fn next_for_partition(
        &self,
        partition: u32,
        consumer_id: &str,
        block: Duration,
    ) -> Result<Option<ClaimedJob>, QueueError> {
        self.check_partition(partition)?;
        let high = stream_name(&self.config.key_prefix, partition, JobPriority::High);
        let normal = stream_name(&self.config.key_prefix, partition, JobPriority::Normal);
        // Each partition reads on its own connection so blocking reads don't
        // serialize across this process's workers. `check_partition` above
        // guarantees the index is in range.
        let mut conn = self.read_conns[partition as usize].lock()?;

        // High first, non-blocking. Strict priority — if any High is
        // queued, drain it before touching Normal.
        let opts = StreamReadOptions::default()
            .group(CONSUMER_GROUP, consumer_id)
            .count(1);
        let reply: StreamReadReply = conn.xread_options(&[&high], &[">"], &opts)?;
        if let Some(claim) = pick_first(&reply, JobPriority::High)? {
            return Ok(Some(claim));
        }

        // Nothing high → block on both. XREADGROUP with multiple streams is
        // a single call: we wake on whichever stream pops first within the
        // block budget. We re-check High before Normal so a High that
        // arrived during the block still wins.
        //
        // Caveat: `COUNT 1` is *per stream*, so this call can claim one High
        // AND one Normal at once. We return the High and the Normal stays
        // claimed in this consumer's PEL, unprocessed, until `reclaim_stale`
        // re-arms it (~claim_lease later). At-least-once still holds — the
        // reaper recovers it — but that one Normal eats a latency hit in this
        // rare both-arrive-during-the-block case. Accepted over the added
        // state of buffering the leftover claim.
        let block_ms = block.as_millis().min(u32::MAX as u128) as usize;
        let opts = StreamReadOptions::default()
            .group(CONSUMER_GROUP, consumer_id)
            .count(1)
            .block(block_ms);
        let reply: StreamReadReply = conn.xread_options(&[&high, &normal], &[">", ">"], &opts)?;
        if let Some(claim) = pick_first(&reply, JobPriority::High)? {
            return Ok(Some(claim));
        }
        if let Some(claim) = pick_first(&reply, JobPriority::Normal)? {
            return Ok(Some(claim));
        }
        Ok(None)
    }

    fn ack(&self, partition: u32, claim: &ClaimedJob) -> Result<(), QueueError> {
        self.check_partition(partition)?;
        let (prio, sid) = decode_entry(&claim.entry_id)?;
        let stream = stream_name(&self.config.key_prefix, partition, prio);
        let mut conn = self.conn.lock()?;
        // XACK marks the entry consumed (removed from PEL). XDEL frees the
        // memory so XLEN stays a fair pending-count proxy long-term. Both
        // are idempotent.
        let _: i64 = conn.xack(&stream, CONSUMER_GROUP, &[sid])?;
        let _: i64 = conn.xdel(&stream, &[sid])?;
        Ok(())
    }

    fn reclaim_stale(
        &self,
        partition: u32,
        idle_after: Duration,
    ) -> Result<Vec<ClaimedJob>, QueueError> {
        self.check_partition(partition)?;
        let mut reclaimed = Vec::new();
        let mut conn = self.conn.lock()?;
        let min_idle_ms = idle_after.as_millis().min(u64::MAX as u128) as u64;
        for prio in PRIORITIES {
            let stream = stream_name(&self.config.key_prefix, partition, *prio);
            // `XAUTOCLAIM` transfers ownership of idle PEL entries to
            // "reaper". We immediately XACK + re-XADD so workers see fresh
            // entries instead of inheriting stale claims.
            let reply: StreamAutoClaimReply = redis::cmd("XAUTOCLAIM")
                .arg(&stream)
                .arg(CONSUMER_GROUP)
                .arg("reaper")
                .arg(min_idle_ms)
                .arg("0-0")
                .arg("COUNT")
                .arg(100)
                .query(&mut *conn)?;
            for entry in reply.claimed {
                let blob = read_payload(&entry)?;
                let job = deserialize_job(&blob)?;
                // Re-enqueue under the same priority so ordering within the
                // priority bucket is roughly preserved. The original entry
                // id is dropped (XAUTOCLAIM transferred it; we ack+del).
                let _: i64 = conn.xack(&stream, CONSUMER_GROUP, &[entry.id.as_str()])?;
                let _: i64 = conn.xdel(&stream, &[entry.id.as_str()])?;
                let new_sid: String = conn.xadd(
                    &stream,
                    "*",
                    &[("job_id", job.id.as_str()), ("payload", blob.as_str())],
                )?;
                reclaimed.push(ClaimedJob {
                    job,
                    entry_id: encode_entry(*prio, &new_sid),
                });
            }
        }
        Ok(reclaimed)
    }

    fn pending_count(&self) -> Result<u64, QueueError> {
        let mut conn = self.client.get_connection()?;
        let mut total: u64 = 0;
        for p in 0..self.config.partition_count {
            for prio in PRIORITIES {
                let stream = stream_name(&self.config.key_prefix, p, *prio);
                let len: u64 = conn.xlen(&stream).unwrap_or(0);
                total += len;
            }
        }
        Ok(total)
    }
}

/// Extract the first claimed entry for the given priority. Reads across
/// all keys in the reply, so it's robust to the multi-stream form of
/// `XREADGROUP`.
fn pick_first(
    reply: &StreamReadReply,
    prio: JobPriority,
) -> Result<Option<ClaimedJob>, QueueError> {
    let tag = priority_tag(prio);
    for key in &reply.keys {
        if !key.key.ends_with(tag) {
            continue;
        }
        if let Some(entry) = key.ids.first() {
            let blob = read_payload(entry)?;
            let job = deserialize_job(&blob)?;
            return Ok(Some(ClaimedJob {
                job,
                entry_id: encode_entry(prio, &entry.id),
            }));
        }
    }
    Ok(None)
}

fn read_payload(entry: &StreamId) -> Result<String, QueueError> {
    let raw = entry.map.get("payload").ok_or_else(|| {
        QueueError::JobFailed(format!("stream entry {} missing payload field", entry.id))
    })?;
    String::from_redis_value(raw)
        .map_err(QueueError::from)
        .or_else(|e| match raw {
            Value::BulkString(bytes) => String::from_utf8(bytes.clone())
                .map_err(|e| QueueError::JobFailed(format!("payload not utf8: {e}"))),
            _ => Err(e),
        })
}
