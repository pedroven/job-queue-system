use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, ErrorCode, OptionalExtension};

use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobPriority, JobStatus, TaskRecord};
use crate::persistence::JobState;

/// SQLite-backed job state. Owns its own `Connection`; safe to construct
/// alongside `SqliteJobDispatch` on the same db file because both rely on
/// WAL + busy_timeout to coexist without `SQLITE_BUSY`.
pub struct SqliteJobState {
    conn: Mutex<Connection>,
}

impl SqliteJobState {
    pub fn new(db_path: &str) -> Result<Self, QueueError> {
        let conn = Connection::open(db_path)?;
        super::sqlite_schema::apply(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

pub(super) fn system_time_to_epoch(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

pub(super) fn epoch_to_system_time(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

pub(super) fn decode_priority(job_id: &str, raw: u32) -> JobPriority {
    JobPriority::try_from(raw).unwrap_or_else(|_| {
        eprintln!("unknown priority {raw} on job {job_id}; defaulting to Normal");
        JobPriority::Normal
    })
}

impl JobState for SqliteJobState {
    fn save_initial(&self, job: &Job) -> Result<(), QueueError> {
        let conn = self.conn.lock()?;
        let res = conn.execute(
            "INSERT INTO jobs (id, status, retry_count, task_id, task_name, payload, max_attempts, priority, partition, claimed_at, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10)",
            (
                &job.id,
                job.status.as_str(),
                job.retry_count,
                &job.task.id,
                &job.task.name,
                &job.task.payload,
                job.max_attempts,
                job.priority as u32,
                0_u32,
                system_time_to_epoch(job.created_at),
            ),
        );
        match res {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == ErrorCode::ConstraintViolation =>
            {
                Err(QueueError::AlreadyExists(job.id.clone()))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn save_status(&self, id: &str, status: JobStatus) -> Result<(), QueueError> {
        let conn = self.conn.lock()?;
        // Sticky-Cancelled enforcement at the SQL level — see InMemoryJobState
        // for the same rule. Closes the cancel/worker race.
        let rows = conn.execute(
            "UPDATE jobs SET status = ?1 WHERE id = ?2 AND status != 'cancelled'",
            (status.as_str(), id),
        )?;
        if rows == 0 {
            let exists: bool = conn
                .query_row("SELECT 1 FROM jobs WHERE id = ?1", [id], |_| Ok(true))
                .optional()?
                .unwrap_or(false);
            if !exists {
                return Err(QueueError::NotFound(id.to_string()));
            }
        }
        Ok(())
    }

    fn save_retry_count(&self, id: &str, retry_count: u32) -> Result<(), QueueError> {
        let conn = self.conn.lock()?;
        let rows = conn.execute(
            "UPDATE jobs SET retry_count = ?1 WHERE id = ?2",
            (retry_count, id),
        )?;
        if rows == 0 {
            return Err(QueueError::NotFound(id.to_string()));
        }
        Ok(())
    }

    fn find_by_id(&self, id: &str) -> Result<Job, QueueError> {
        let conn = self.conn.lock()?;
        let mut stmt = conn.prepare(
            "SELECT id, status, retry_count, task_id, task_name, payload, max_attempts, priority, created_at
             FROM jobs WHERE id = ?1",
        )?;
        let row = stmt
            .query_row([id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, u32>(6)?,
                    row.get::<_, u32>(7)?,
                    row.get::<_, u64>(8)?,
                ))
            })
            .map_err(|_| QueueError::NotFound(id.to_string()))?;
        let (
            id,
            status_str,
            retry_count,
            task_id,
            task_name,
            payload,
            max_attempts,
            priority,
            created_at,
        ) = row;
        let decoded_priority = decode_priority(&id, priority);
        Ok(Job {
            id,
            status: status_str.parse()?,
            retry_count,
            task: TaskRecord {
                id: task_id,
                name: task_name,
                payload,
            },
            max_attempts,
            priority: decoded_priority,
            created_at: epoch_to_system_time(created_at),
        })
    }

    fn save_dead_letter(&self, dl: &DeadLetterJob) -> Result<(), QueueError> {
        let conn = self.conn.lock()?;
        conn.execute(
            "INSERT INTO dead_letter_jobs (id, original_job_id, task_id, task_name, payload, error, failed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                &dl.id,
                &dl.original_job_id,
                &dl.task.id,
                &dl.task.name,
                &dl.task.payload,
                &dl.error,
                system_time_to_epoch(dl.failed_at),
            ),
        )?;
        Ok(())
    }

    fn find_all_dead_letter(&self) -> Result<Vec<DeadLetterJob>, QueueError> {
        let conn = self.conn.lock()?;
        let mut stmt = conn.prepare(
            "SELECT id, original_job_id, task_id, task_name, payload, error, failed_at
             FROM dead_letter_jobs ORDER BY failed_at ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, u64>(6)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Vec::with_capacity(rows.len());
        for (id, original_job_id, task_id, task_name, payload, error, failed_at) in rows {
            out.push(DeadLetterJob {
                id,
                original_job_id,
                task: TaskRecord {
                    id: task_id,
                    name: task_name,
                    payload,
                },
                error,
                failed_at: epoch_to_system_time(failed_at),
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::testing::make_test_job;

    fn dl(id: &str, original: &str) -> DeadLetterJob {
        DeadLetterJob {
            id: id.to_string(),
            original_job_id: original.to_string(),
            task: TaskRecord {
                id: format!("task-{original}"),
                name: "default".to_string(),
                payload: "p".to_string(),
            },
            error: "boom".to_string(),
            failed_at: SystemTime::now(),
        }
    }

    #[test]
    fn test_save_initial_then_find() {
        let s = SqliteJobState::new(":memory:").unwrap();
        s.save_initial(&make_test_job("j", "hello")).unwrap();
        let loaded = s.find_by_id("j").unwrap();
        assert_eq!(loaded.id, "j");
        assert_eq!(loaded.task.payload, "hello");
    }

    #[test]
    fn test_duplicate_save_initial_errors() {
        let s = SqliteJobState::new(":memory:").unwrap();
        s.save_initial(&make_test_job("j", "p")).unwrap();
        assert!(matches!(
            s.save_initial(&make_test_job("j", "p")).unwrap_err(),
            QueueError::AlreadyExists(_)
        ));
    }

    #[test]
    fn test_sticky_cancelled_blocks_later_transitions() {
        let s = SqliteJobState::new(":memory:").unwrap();
        s.save_initial(&make_test_job("j", "p")).unwrap();
        s.save_status("j", JobStatus::Cancelled).unwrap();
        s.save_status("j", JobStatus::Running).unwrap();
        s.save_status("j", JobStatus::Completed).unwrap();
        assert_eq!(s.find_by_id("j").unwrap().status, JobStatus::Cancelled);
    }

    #[test]
    fn test_save_status_on_missing_returns_not_found() {
        let s = SqliteJobState::new(":memory:").unwrap();
        let err = s.save_status("nope", JobStatus::Running).unwrap_err();
        assert!(matches!(err, QueueError::NotFound(_)));
    }

    #[test]
    fn test_dead_letter_roundtrip() {
        let s = SqliteJobState::new(":memory:").unwrap();
        s.save_dead_letter(&dl("dl1", "j1")).unwrap();
        s.save_dead_letter(&dl("dl2", "j2")).unwrap();
        let all = s.find_all_dead_letter().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].original_job_id, "j1");
    }
}
