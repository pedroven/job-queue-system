use crate::error::QueueError;
use crate::queue::models;
use rusqlite::Connection;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait JobRepository: Send + Sync {
    fn save(&self, job: &models::Job) -> Result<(), QueueError>;
    fn find_all_pending(&self) -> Result<Vec<models::Job>, QueueError>;
    fn update_status(&self, job_id: &str, status: models::JobStatus) -> Result<(), QueueError>;
}

pub struct SqliteJobRepository {
    conn: Mutex<Connection>,
}

impl SqliteJobRepository {
    pub fn new(db_path: &str) -> Result<Self, QueueError> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                retry_count INTEGER NOT NULL,
                task_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                max_retries INTEGER NOT NULL,
                created_at INTEGER NOT NULL
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

impl JobRepository for SqliteJobRepository {
    fn save(&self, job: &models::Job) -> Result<(), QueueError> {
        let conn = self.conn.lock()?;
        conn.execute(
            "INSERT INTO jobs (id, status, retry_count, task_id, payload, max_retries, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                &job.id,
                job.status.as_str(),
                job.retry_count,
                &job.task.id,
                &job.task.payload,
                job.max_retries,
                system_time_to_epoch(job.created_at),
            ),
        )?;
        Ok(())
    }

    fn find_all_pending(&self) -> Result<Vec<models::Job>, QueueError> {
        let conn = self.conn.lock()?;
        let mut stmt = conn.prepare(
            "SELECT id, status, retry_count, task_id, payload, max_retries, created_at
             FROM jobs WHERE status = 'pending' ORDER BY created_at ASC",
        )?;

        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, u32>(5)?,
                    row.get::<_, u64>(6)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut jobs = Vec::with_capacity(rows.len());
        for (id, status_str, retry_count, task_id, payload, max_retries, created_at) in rows {
            jobs.push(models::Job {
                id,
                status: status_str.parse()?,
                retry_count,
                task: models::Task {
                    id: task_id,
                    payload,
                },
                max_retries,
                created_at: epoch_to_system_time(created_at),
            });
        }

        Ok(jobs)
    }

    fn update_status(&self, job_id: &str, status: models::JobStatus) -> Result<(), QueueError> {
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
}

#[derive(Default)]
pub struct InMemoryJobRepository {
    jobs: Mutex<Vec<models::Job>>,
}

impl InMemoryJobRepository {
    pub fn new() -> Self {
        Self::default()
    }
}

impl JobRepository for InMemoryJobRepository {
    fn save(&self, job: &models::Job) -> Result<(), QueueError> {
        let mut jobs = self.jobs.lock()?;
        if jobs.iter().any(|j| j.id == job.id) {
            return Err(QueueError::AlreadyExists(job.id.clone()));
        }
        jobs.push(job.clone());
        Ok(())
    }

    fn find_all_pending(&self) -> Result<Vec<models::Job>, QueueError> {
        let jobs = self.jobs.lock()?;
        let pending = jobs
            .iter()
            .filter(|j| j.status == models::JobStatus::Pending)
            .cloned()
            .collect();
        Ok(pending)
    }

    fn update_status(&self, job_id: &str, status: models::JobStatus) -> Result<(), QueueError> {
        let mut jobs = self.jobs.lock()?;
        let job = jobs
            .iter_mut()
            .find(|j| j.id == job_id)
            .ok_or_else(|| QueueError::NotFound(job_id.to_string()))?;
        job.status = status;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::models::testing::make_test_job;

    #[test]
    fn test_in_memory_save_and_find_pending() {
        let repo = InMemoryJobRepository::new();
        repo.save(&make_test_job("job-1", "payload-1")).unwrap();
        repo.save(&make_test_job("job-2", "payload-2")).unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].id, "job-1");
        assert_eq!(pending[1].id, "job-2");
    }

    #[test]
    fn test_in_memory_update_status_excludes_from_pending() {
        let repo = InMemoryJobRepository::new();
        repo.save(&make_test_job("job-1", "payload")).unwrap();
        repo.update_status("job-1", models::JobStatus::Completed)
            .unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_in_memory_update_status_not_found() {
        let repo = InMemoryJobRepository::new();
        let result = repo.update_status("nonexistent", models::JobStatus::Running);
        assert!(result.is_err());
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
    }

    #[test]
    fn test_sqlite_update_status() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save(&make_test_job("job-1", "payload")).unwrap();

        repo.update_status("job-1", models::JobStatus::Running)
            .unwrap();
        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_sqlite_update_status_not_found() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        let result = repo.update_status("nonexistent", models::JobStatus::Running);
        assert!(result.is_err());
    }

    #[test]
    fn test_sqlite_update_status_to_completed() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save(&make_test_job("job-1", "payload")).unwrap();

        repo.update_status("job-1", models::JobStatus::Running)
            .unwrap();
        repo.update_status("job-1", models::JobStatus::Completed)
            .unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_sqlite_find_pending_only_returns_pending() {
        let repo = SqliteJobRepository::new(":memory:").unwrap();
        repo.save(&make_test_job("job-1", "p1")).unwrap();
        repo.save(&make_test_job("job-2", "p2")).unwrap();
        repo.save(&make_test_job("job-3", "p3")).unwrap();

        repo.update_status("job-1", models::JobStatus::Completed)
            .unwrap();
        repo.update_status("job-2", models::JobStatus::Failed)
            .unwrap();

        let pending = repo.find_all_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "job-3");
    }
}
