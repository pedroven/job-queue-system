use rusqlite::{Connection, ErrorCode};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::QueueError;
use crate::models::{DeadLetterJob, Job, JobPriority, JobStatus, TaskRecord};
use crate::persistence::JobRepository;

pub struct SqliteJobRepository {
    conn: Mutex<Connection>,
}

impl SqliteJobRepository {
    pub fn new(db_path: &str) -> Result<Self, QueueError> {
        let conn = Connection::open(db_path)?;
        // WAL keeps readers unblocked during writes and lets the scheduler
        // connection coexist with the worker connection on the same file
        // without SQLITE_BUSY under contention. `:memory:` silently stays in
        // memory-journal mode. busy_timeout gives writers a short spin before
        // erroring out in any mode.
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                retry_count INTEGER NOT NULL,
                task_id TEXT NOT NULL,
                task_name TEXT NOT NULL DEFAULT 'default',
                payload TEXT NOT NULL,
                max_attempts INTEGER NOT NULL,
                priority INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_status_priority_created
                ON jobs(status, priority DESC, created_at ASC);",
        )?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS dead_letter_jobs (
                id TEXT PRIMARY KEY,
                original_job_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                task_name TEXT NOT NULL DEFAULT 'default',
                payload TEXT NOT NULL,
                error TEXT NOT NULL,
                failed_at INTEGER NOT NULL
            );",
        )?;

        Ok(SqliteJobRepository {
            conn: Mutex::new(conn),
        })
    }
}

fn system_time_to_epoch(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

fn epoch_to_system_time(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

/// Decode a stored priority discriminant, defaulting to `Normal` with a
/// warning if the value is unknown. Keeps `Queue::new` able to boot even if
/// a row has a priority we don't recognize (e.g. rolled back a release that
/// wrote a new variant).
fn decode_priority(job_id: &str, raw: u32) -> JobPriority {
    JobPriority::try_from(raw).unwrap_or_else(|_| {
        eprintln!("unknown priority {raw} on job {job_id}; defaulting to Normal");
        JobPriority::Normal
    })
}

impl JobRepository for SqliteJobRepository {
    fn save(&self, job: &Job) -> Result<(), QueueError> {
        let conn = self.conn.lock()?;
        let res = conn.execute(
            "INSERT INTO jobs (id, status, retry_count, task_id, task_name, payload, max_attempts, priority, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            (
                &job.id,
                job.status.as_str(),
                job.retry_count,
                &job.task.id,
                &job.task.name,
                &job.task.payload,
                job.max_attempts,
                job.priority as u32,
                system_time_to_epoch(job.created_at),
            ),
        );
        match res {
            Ok(_) => Ok(()),
            // The INSERT's only constraint is the `id` primary key, so any
            // constraint violation means the id was already present. Normalize
            // to `AlreadyExists` to match InMemoryJobRepository and give the
            // scheduler's idempotent re-fire path a stable error to match on.
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == ErrorCode::ConstraintViolation =>
            {
                Err(QueueError::AlreadyExists(job.id.clone()))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn find_by_id(&self, job_id: &str) -> Result<Job, QueueError> {
        let conn = self.conn.lock()?;
        let mut stmt = conn.prepare(
            "SELECT id, status, retry_count, task_id, task_name, payload, max_attempts, priority, created_at
             FROM jobs WHERE id = ?1",
        )?;

        let row = stmt
            .query_row([job_id], |row| {
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
            .map_err(|_| QueueError::NotFound(job_id.to_string()))?;

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

    fn save_dead_letter(&self, job: &DeadLetterJob) -> Result<(), QueueError> {
        let conn = self.conn.lock()?;
        conn.execute(
            "INSERT INTO dead_letter_jobs (id, original_job_id, task_id, task_name, payload, error, failed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                &job.id,
                &job.original_job_id,
                &job.task.id,
                &job.task.name,
                &job.task.payload,
                &job.error,
                system_time_to_epoch(job.failed_at),
            ),
        )?;
        Ok(())
    }

    fn find_all_pending(&self) -> Result<Vec<Job>, QueueError> {
        let conn = self.conn.lock()?;
        let mut stmt = conn.prepare(
            "SELECT id, status, retry_count, task_id, task_name, payload, max_attempts, priority, created_at
             FROM jobs WHERE status = 'pending' ORDER BY priority DESC, created_at ASC",
        )?;

        let rows = stmt
            .query_map([], |row| {
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
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut jobs = Vec::with_capacity(rows.len());
        for (
            id,
            status_str,
            retry_count,
            task_id,
            task_name,
            payload,
            max_attempts,
            priority,
            created_at,
        ) in rows
        {
            let decoded_priority = decode_priority(&id, priority);
            jobs.push(Job {
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
            });
        }

        Ok(jobs)
    }

    fn pending_count(&self) -> Result<u64, QueueError> {
        // Uses idx_jobs_status_priority_created (leftmost column = status).
        let conn = self.conn.lock()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM jobs WHERE status = 'pending'",
            [],
            |row| row.get(0),
        )?;
        Ok(u64::try_from(count).unwrap_or(0))
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

        let mut dl_jobs = Vec::with_capacity(rows.len());
        for (id, original_job_id, task_id, task_name, payload, error, failed_at) in rows {
            dl_jobs.push(DeadLetterJob {
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

        Ok(dl_jobs)
    }

    fn update_status(&self, job_id: &str, status: JobStatus) -> Result<(), QueueError> {
        let conn = self.conn.lock()?;
        let rows = conn.execute(
            "UPDATE jobs SET status = ?1 WHERE id = ?2",
            (status.as_str(), job_id),
        )?;

        if rows == 0 {
            return Err(QueueError::NotFound(job_id.to_string()));
        }
        Ok(())
    }

    fn update_retry_count(&self, job_id: &str, retry_count: u32) -> Result<(), QueueError> {
        let conn = self.conn.lock()?;
        let rows = conn.execute(
            "UPDATE jobs SET retry_count = ?1 WHERE id = ?2",
            (retry_count, job_id),
        )?;

        if rows == 0 {
            return Err(QueueError::NotFound(job_id.to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::testing::make_test_job;

    fn make_dead_letter_job(id: &str, original_job_id: &str) -> DeadLetterJob {
        DeadLetterJob {
            id: id.to_string(),
            original_job_id: original_job_id.to_string(),
            task: TaskRecord {
                id: format!("task-{original_job_id}"),
                name: "default".to_string(),
                payload: "payload".to_string(),
            },
            error: "something went wrong".to_string(),
            failed_at: SystemTime::now(),
        }
    }

    #[test]
    fn test_sqlite_save_duplicate_id_returns_already_exists() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save(&make_test_job("job-1", "payload")).unwrap();

        let err = repo.save(&make_test_job("job-1", "payload")).unwrap_err();
        match err {
            QueueError::AlreadyExists(id) => assert_eq!(id, "job-1"),
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn test_sqlite_save_and_find_pending() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save(&make_test_job("job-1", "payload-1")).unwrap();
        repo.save(&make_test_job("job-2", "payload-2")).unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].id, "job-1");
        assert_eq!(pending[0].task.payload, "payload-1");
        assert_eq!(pending[1].id, "job-2");
        assert_eq!(pending[1].task.payload, "payload-2");
    }

    #[test]
    fn test_sqlite_update_status() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save(&make_test_job("job-1", "payload")).unwrap();

        repo.update_status("job-1", JobStatus::Running).unwrap();
        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_sqlite_update_status_not_found() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        let result = repo.update_status("nonexistent", JobStatus::Running);
        assert!(result.is_err());
    }

    #[test]
    fn test_sqlite_update_status_to_completed() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save(&make_test_job("job-1", "payload")).unwrap();

        repo.update_status("job-1", JobStatus::Running).unwrap();
        repo.update_status("job-1", JobStatus::Completed).unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_sqlite_find_pending_only_returns_pending() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save(&make_test_job("job-1", "p1")).unwrap();
        repo.save(&make_test_job("job-2", "p2")).unwrap();
        repo.save(&make_test_job("job-3", "p3")).unwrap();

        repo.update_status("job-1", JobStatus::Completed).unwrap();
        repo.update_status("job-2", JobStatus::Failed).unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "job-3");
    }

    #[test]
    fn test_sqlite_save_and_find_dead_letter() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save_dead_letter(&make_dead_letter_job("dl-1", "job-1"))
            .unwrap();
        repo.save_dead_letter(&make_dead_letter_job("dl-2", "job-2"))
            .unwrap();

        let dl = repo.find_all_dead_letter().unwrap();
        assert_eq!(dl.len(), 2);
        assert_eq!(dl[0].original_job_id, "job-1");
        assert_eq!(dl[0].error, "something went wrong");
        assert_eq!(dl[1].original_job_id, "job-2");
    }

    #[test]
    fn test_sqlite_find_by_id() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save(&make_test_job("job-1", "payload-1")).unwrap();

        let job = repo.find_by_id("job-1").unwrap();
        assert_eq!(job.id, "job-1");
        assert_eq!(job.task.payload, "payload-1");
    }

    #[test]
    fn test_sqlite_find_by_id_not_found() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        let result = repo.find_by_id("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_sqlite_update_retry_count() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save(&make_test_job("job-1", "payload")).unwrap();

        repo.update_retry_count("job-1", 3).unwrap();
        let job = repo.find_by_id("job-1").unwrap();
        assert_eq!(job.retry_count, 3);
    }

    #[test]
    fn test_sqlite_update_retry_count_not_found() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        let result = repo.update_retry_count("nonexistent", 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_sqlite_pending_count_excludes_non_pending() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save(&make_test_job("job-1", "p1")).unwrap();
        repo.save(&make_test_job("job-2", "p2")).unwrap();
        repo.save(&make_test_job("job-3", "p3")).unwrap();
        repo.update_status("job-2", JobStatus::Running).unwrap();
        repo.update_status("job-3", JobStatus::Completed).unwrap();

        assert_eq!(repo.pending_count().unwrap(), 1);
    }

    #[test]
    fn test_sqlite_preserves_task_name() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        let job = Job::with_task_name(
            "job-1".to_string(),
            "send_email".to_string(),
            "user@example.com".to_string(),
        );
        repo.save(&job).unwrap();

        let loaded = repo.find_by_id("job-1").unwrap();
        assert_eq!(loaded.task.name, "send_email");
        assert_eq!(loaded.task.payload, "user@example.com");
    }

    #[test]
    fn test_sqlite_find_all_pending_preserves_task_name() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        let job = Job::with_task_name(
            "job-1".to_string(),
            "process_image".to_string(),
            "img.png".to_string(),
        );
        repo.save(&job).unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].task.name, "process_image");
    }
}
