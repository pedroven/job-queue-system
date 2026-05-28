use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;

use crate::error::QueueError;
use crate::models::{Job, TaskRecord};
use crate::persistence::{ClaimedJob, JobDispatch};

use super::sqlite_state::{decode_priority, system_time_to_epoch};

/// SQLite-backed dispatch. Real atomic claim via `UPDATE … RETURNING`. No
/// in-memory mirror — the table is the source of truth.
///
/// Polling model: `next_for_partition` loops with adaptive backoff (10ms →
/// 100ms) until either a row is claimed or the caller's `block` budget is
/// spent. Cheap when busy (no sleep between successful claims); cheap when
/// idle (10ms granularity is fine for a single-host setup).
pub struct SqliteJobDispatch {
    conn: Mutex<Connection>,
    partition_count: u32,
}

impl SqliteJobDispatch {
    pub fn new(db_path: &str, partition_count: u32) -> Result<Self, QueueError> {
        assert!(partition_count > 0, "partition_count must be > 0");
        let conn = Connection::open(db_path)?;
        super::sqlite_schema::apply(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            partition_count,
        })
    }

    fn check_partition(&self, partition: u32) -> Result<(), QueueError> {
        if partition >= self.partition_count {
            return Err(QueueError::InvalidPartition {
                partition,
                count: self.partition_count,
            });
        }
        Ok(())
    }

    /// One claim attempt. Returns the claimed job or `None` if nothing's
    /// pending in this partition.
    fn try_claim(&self, partition: u32) -> Result<Option<ClaimedJob>, QueueError> {
        let conn = self.conn.lock()?;
        // `RETURNING` lets us read the row we just updated in one round-trip.
        // The subquery picks the highest-priority oldest pending row in the
        // partition; the outer UPDATE flips it to Running and stamps
        // claimed_at so the reaper can find it later.
        let now = system_time_to_epoch(SystemTime::now());
        let mut stmt = conn.prepare(
            "UPDATE jobs
               SET status = 'running', claimed_at = ?1
             WHERE id = (
                 SELECT id FROM jobs
                  WHERE status = 'pending' AND partition = ?2
                  ORDER BY priority DESC, created_at ASC
                  LIMIT 1
             )
             RETURNING id, retry_count, task_id, task_name, payload, max_attempts, priority, created_at",
        )?;
        let row = stmt
            .query_row((now, partition), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u32>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, u32>(5)?,
                    row.get::<_, u32>(6)?,
                    row.get::<_, u64>(7)?,
                ))
            })
            .ok();
        let Some((
            id,
            retry_count,
            task_id,
            task_name,
            payload,
            max_attempts,
            priority,
            created_at,
        )) = row
        else {
            return Ok(None);
        };
        let decoded_priority = decode_priority(&id, priority);
        let entry_id = id.clone();
        let job = Job {
            id,
            status: crate::models::JobStatus::Running,
            retry_count,
            task: TaskRecord {
                id: task_id,
                name: task_name,
                payload,
            },
            max_attempts,
            priority: decoded_priority,
            created_at: epoch_to_system_time(created_at),
        };
        Ok(Some(ClaimedJob { job, entry_id }))
    }
}

fn epoch_to_system_time(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

impl JobDispatch for SqliteJobDispatch {
    fn enqueue(&self, partition: u32, job: &Job) -> Result<(), QueueError> {
        self.check_partition(partition)?;
        let conn = self.conn.lock()?;
        // Upsert nothing — `INSERT` with the row's existing id collides on
        // the PK. We treat duplicate ids as `AlreadyExists` (same shape as
        // `InMemoryJobState::save_initial`) so the scheduler's at-least-once
        // re-fire path stays uniform across backends.
        let res = conn.execute(
            "INSERT INTO jobs (id, status, retry_count, task_id, task_name, payload, max_attempts, priority, partition, claimed_at, created_at)
             VALUES (?1, 'pending', ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9)",
            (
                &job.id,
                job.retry_count,
                &job.task.id,
                &job.task.name,
                &job.task.payload,
                job.max_attempts,
                job.priority as u32,
                partition,
                system_time_to_epoch(job.created_at),
            ),
        );
        match res {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(QueueError::AlreadyExists(job.id.clone()))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn next_for_partition(
        &self,
        partition: u32,
        _consumer_id: &str,
        block: Duration,
    ) -> Result<Option<ClaimedJob>, QueueError> {
        self.check_partition(partition)?;
        let deadline = Instant::now() + block;
        let mut backoff = Duration::from_millis(10);
        loop {
            if let Some(claim) = self.try_claim(partition)? {
                return Ok(Some(claim));
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            // Cap the per-iteration sleep at the remaining budget so we
            // don't oversleep the block deadline.
            let sleep = backoff.min(deadline - now);
            thread::sleep(sleep);
            backoff = (backoff * 2).min(Duration::from_millis(100));
        }
    }

    fn ack(&self, partition: u32, claim: &ClaimedJob) -> Result<(), QueueError> {
        self.check_partition(partition)?;
        // Clear `claimed_at` so the reaper doesn't surface this row again.
        // Status is set by the worker via JobState::save_status — ack here
        // only owns the dispatch-side bookkeeping.
        let conn = self.conn.lock()?;
        conn.execute(
            "UPDATE jobs SET claimed_at = NULL WHERE id = ?1",
            [&claim.entry_id],
        )?;
        Ok(())
    }

    fn reclaim_stale(
        &self,
        partition: u32,
        idle_after: Duration,
    ) -> Result<Vec<ClaimedJob>, QueueError> {
        self.check_partition(partition)?;
        let cutoff = SystemTime::now()
            .checked_sub(idle_after)
            .unwrap_or(UNIX_EPOCH);
        let cutoff_epoch = system_time_to_epoch(cutoff);
        let conn = self.conn.lock()?;
        // Atomic re-arm: flip Running rows whose claim is older than the
        // lease back to Pending and clear claimed_at. RETURNING surfaces
        // them so the reaper can call save_status on them too (defensive —
        // status is already Pending, but the reaper may want to log).
        let mut stmt = conn.prepare(
            "UPDATE jobs
               SET status = 'pending', claimed_at = NULL
             WHERE status = 'running'
               AND partition = ?1
               AND claimed_at IS NOT NULL
               AND claimed_at <= ?2
             RETURNING id, retry_count, task_id, task_name, payload, max_attempts, priority, created_at",
        )?;
        let rows = stmt
            .query_map((partition, cutoff_epoch), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u32>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, u32>(5)?,
                    row.get::<_, u32>(6)?,
                    row.get::<_, u64>(7)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut reclaimed = Vec::with_capacity(rows.len());
        for (id, retry_count, task_id, task_name, payload, max_attempts, priority, created_at) in
            rows
        {
            let decoded_priority = decode_priority(&id, priority);
            let entry_id = id.clone();
            reclaimed.push(ClaimedJob {
                job: Job {
                    id,
                    status: crate::models::JobStatus::Pending,
                    retry_count,
                    task: TaskRecord {
                        id: task_id,
                        name: task_name,
                        payload,
                    },
                    max_attempts,
                    priority: decoded_priority,
                    created_at: epoch_to_system_time(created_at),
                },
                entry_id,
            });
        }
        Ok(reclaimed)
    }

    fn pending_count(&self) -> Result<u64, QueueError> {
        let conn = self.conn.lock()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM jobs WHERE status = 'pending'",
            [],
            |row| row.get(0),
        )?;
        Ok(u64::try_from(count).unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{JobPriority, testing::make_test_job};
    use crate::persistence::SqliteJobState;

    fn pair(path: &str) -> (SqliteJobDispatch, SqliteJobState) {
        let dispatch = SqliteJobDispatch::new(path, 1).unwrap();
        let state = SqliteJobState::new(path).unwrap();
        (dispatch, state)
    }

    #[test]
    fn test_enqueue_then_claim() {
        let (d, _s) = pair(":memory:");
        d.enqueue(0, &make_test_job("j", "p")).unwrap();
        let claim = d
            .next_for_partition(0, "c", Duration::from_millis(50))
            .unwrap()
            .unwrap();
        assert_eq!(claim.job.id, "j");
        assert_eq!(claim.entry_id, "j");
    }

    #[test]
    fn test_next_returns_none_after_block_timeout() {
        let (d, _s) = pair(":memory:");
        let start = Instant::now();
        let r = d
            .next_for_partition(0, "c", Duration::from_millis(50))
            .unwrap();
        assert!(r.is_none());
        assert!(start.elapsed() >= Duration::from_millis(40));
    }

    #[test]
    fn test_claim_is_atomic_under_concurrent_consumers() {
        // Two threads sharing one dispatch must each see the job exactly once.
        let path = format!(
            "file:atomic-{}?mode=memory&cache=shared",
            std::process::id()
        );
        let d = std::sync::Arc::new(SqliteJobDispatch::new(&path, 1).unwrap());
        d.enqueue(0, &make_test_job("only", "p")).unwrap();

        let d1 = std::sync::Arc::clone(&d);
        let d2 = std::sync::Arc::clone(&d);
        let t1 = thread::spawn(move || {
            d1.next_for_partition(0, "c1", Duration::from_millis(200))
                .unwrap()
        });
        let t2 = thread::spawn(move || {
            d2.next_for_partition(0, "c2", Duration::from_millis(200))
                .unwrap()
        });
        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();
        let winners = [&r1, &r2].iter().filter(|r| r.is_some()).count();
        assert_eq!(winners, 1, "exactly one consumer should win the claim");
    }

    #[test]
    fn test_priority_drains_first_then_fifo() {
        let path = format!("file:prio-{}?mode=memory&cache=shared", std::process::id());
        let d = SqliteJobDispatch::new(&path, 1).unwrap();
        d.enqueue(0, &make_test_job("n1", "p")).unwrap();
        let mut high = make_test_job("h1", "p");
        high.priority = JobPriority::High;
        high.created_at = SystemTime::now() + Duration::from_secs(60);
        d.enqueue(0, &high).unwrap();
        let first = d
            .next_for_partition(0, "c", Duration::from_millis(20))
            .unwrap()
            .unwrap();
        assert_eq!(first.job.id, "h1");
        let second = d
            .next_for_partition(0, "c", Duration::from_millis(20))
            .unwrap()
            .unwrap();
        assert_eq!(second.job.id, "n1");
    }

    #[test]
    fn test_reclaim_stale_returns_running_jobs_past_lease() {
        let path = format!("file:reap-{}?mode=memory&cache=shared", std::process::id());
        let d = SqliteJobDispatch::new(&path, 1).unwrap();
        d.enqueue(0, &make_test_job("stuck", "p")).unwrap();
        let _ = d
            .next_for_partition(0, "c", Duration::from_millis(20))
            .unwrap()
            .unwrap();
        // Idle=ZERO means anything claimed_at <= now qualifies.
        let reclaimed = d.reclaim_stale(0, Duration::ZERO).unwrap();
        assert_eq!(reclaimed.len(), 1);
        // After reclaim, status is Pending again — should be claimable.
        let again = d
            .next_for_partition(0, "c", Duration::from_millis(20))
            .unwrap()
            .unwrap();
        assert_eq!(again.job.id, "stuck");
    }

    #[test]
    fn test_ack_clears_claimed_at() {
        let path = format!("file:ack-{}?mode=memory&cache=shared", std::process::id());
        let d = SqliteJobDispatch::new(&path, 1).unwrap();
        d.enqueue(0, &make_test_job("j", "p")).unwrap();
        let claim = d
            .next_for_partition(0, "c", Duration::from_millis(20))
            .unwrap()
            .unwrap();
        d.ack(0, &claim).unwrap();
        // No re-claim with zero lease, because ack cleared claimed_at.
        let reclaimed = d.reclaim_stale(0, Duration::ZERO).unwrap();
        assert!(reclaimed.is_empty());
    }

    #[test]
    fn test_pending_count_only_pending() {
        // Pure dispatch test — state is irrelevant. claim flips status to
        // running so pending_count drops; ack doesn't change the count again.
        let d = SqliteJobDispatch::new(":memory:", 1).unwrap();
        d.enqueue(0, &make_test_job("a", "p")).unwrap();
        d.enqueue(0, &make_test_job("b", "p")).unwrap();
        assert_eq!(d.pending_count().unwrap(), 2);
        let claim = d
            .next_for_partition(0, "c", Duration::from_millis(20))
            .unwrap()
            .unwrap();
        assert_eq!(d.pending_count().unwrap(), 1);
        d.ack(0, &claim).unwrap();
        assert_eq!(d.pending_count().unwrap(), 1);
    }

    #[test]
    fn test_invalid_partition_errors() {
        let d = SqliteJobDispatch::new(":memory:", 1).unwrap();
        let err = d.enqueue(2, &make_test_job("j", "p")).unwrap_err();
        assert!(matches!(err, QueueError::InvalidPartition { .. }));
    }

    #[test]
    fn test_jobs_filtered_by_partition() {
        let path = format!(
            "file:partfilter-{}?mode=memory&cache=shared",
            std::process::id()
        );
        let d = SqliteJobDispatch::new(&path, 4).unwrap();
        d.enqueue(0, &make_test_job("zero", "p")).unwrap();
        d.enqueue(3, &make_test_job("three", "p")).unwrap();
        // Partition 1 has nothing.
        let r = d
            .next_for_partition(1, "c", Duration::from_millis(30))
            .unwrap();
        assert!(r.is_none());
        // Partition 0 sees only "zero".
        let claim = d
            .next_for_partition(0, "c", Duration::from_millis(20))
            .unwrap()
            .unwrap();
        assert_eq!(claim.job.id, "zero");
    }

    #[test]
    fn test_enqueue_duplicate_id_returns_already_exists() {
        let d = SqliteJobDispatch::new(":memory:", 1).unwrap();
        let job = make_test_job("j", "p");
        d.enqueue(0, &job).unwrap();
        let err = d.enqueue(0, &job).unwrap_err();
        assert!(matches!(err, QueueError::AlreadyExists(_)));
    }
}
