use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;

use crate::error::QueueError;

use super::model::{ScheduledJob, ScheduledJobRepository};

pub struct SqliteScheduledJobRepository {
    conn: Mutex<Connection>,
}

impl SqliteScheduledJobRepository {
    pub fn new(db_path: &str) -> Result<Self, QueueError> {
        let conn = Connection::open(db_path)?;
        // Matches SqliteJobRepository: WAL + busy_timeout so the scheduler's
        // connection doesn't contend with worker writes on the same file.
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS scheduled_jobs (
                id TEXT PRIMARY KEY,
                task_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                cron_expression TEXT NOT NULL,
                next_run_at INTEGER NOT NULL,
                last_run_at INTEGER,
                enabled INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_enabled_next_run
                ON scheduled_jobs(enabled, next_run_at);",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

// Second resolution. Fractions are truncated on both sides, so a round-trip
// of `SystemTime::now()` is not lossless. Tests that compare reloaded
// timestamps must round-trip through these helpers before asserting.
fn system_time_to_epoch(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .expect("SystemTime before UNIX_EPOCH")
        .as_secs() as i64
}

fn epoch_to_system_time(secs: i64) -> SystemTime {
    let secs: u64 = secs
        .try_into()
        .expect("scheduled_jobs epoch column is non-negative");
    UNIX_EPOCH + Duration::from_secs(secs)
}

impl ScheduledJobRepository for SqliteScheduledJobRepository {
    fn save(&self, job: &ScheduledJob) -> Result<(), QueueError> {
        let conn = self.conn.lock()?;
        conn.execute(
            "INSERT INTO scheduled_jobs (id, task_name, payload, cron_expression, next_run_at, last_run_at, enabled)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET
                task_name = excluded.task_name,
                payload = excluded.payload,
                cron_expression = excluded.cron_expression,
                next_run_at = excluded.next_run_at,
                last_run_at = excluded.last_run_at,
                enabled = excluded.enabled",
            (
                &job.id,
                &job.task_name,
                &job.payload,
                &job.cron_expression,
                system_time_to_epoch(job.next_run_at),
                job.last_run_at.map(system_time_to_epoch),
                job.enabled as i64,
            ),
        )?;
        Ok(())
    }

    fn save_if_absent(&self, job: &ScheduledJob) -> Result<bool, QueueError> {
        let conn = self.conn.lock()?;
        let rows = conn.execute(
            "INSERT OR IGNORE INTO scheduled_jobs
                (id, task_name, payload, cron_expression, next_run_at, last_run_at, enabled)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                &job.id,
                &job.task_name,
                &job.payload,
                &job.cron_expression,
                system_time_to_epoch(job.next_run_at),
                job.last_run_at.map(system_time_to_epoch),
                job.enabled as i64,
            ),
        )?;
        Ok(rows == 1)
    }

    fn find_all_enabled(&self) -> Result<Vec<ScheduledJob>, QueueError> {
        let conn = self.conn.lock()?;
        let mut stmt = conn.prepare(
            "SELECT id, task_name, payload, cron_expression, next_run_at, last_run_at, enabled
             FROM scheduled_jobs WHERE enabled = 1 ORDER BY next_run_at ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(ScheduledJob {
                    id: row.get(0)?,
                    task_name: row.get(1)?,
                    payload: row.get(2)?,
                    cron_expression: row.get(3)?,
                    next_run_at: epoch_to_system_time(row.get::<_, i64>(4)?),
                    last_run_at: row.get::<_, Option<i64>>(5)?.map(epoch_to_system_time),
                    enabled: row.get::<_, i64>(6)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn find_due(&self, now: SystemTime) -> Result<Vec<ScheduledJob>, QueueError> {
        let conn = self.conn.lock()?;
        let mut stmt = conn.prepare(
            "SELECT id, task_name, payload, cron_expression, next_run_at, last_run_at, enabled
             FROM scheduled_jobs
             WHERE enabled = 1 AND next_run_at <= ?1
             ORDER BY next_run_at ASC",
        )?;
        let rows = stmt
            .query_map([system_time_to_epoch(now)], |row| {
                Ok(ScheduledJob {
                    id: row.get(0)?,
                    task_name: row.get(1)?,
                    payload: row.get(2)?,
                    cron_expression: row.get(3)?,
                    next_run_at: epoch_to_system_time(row.get::<_, i64>(4)?),
                    last_run_at: row.get::<_, Option<i64>>(5)?.map(epoch_to_system_time),
                    enabled: row.get::<_, i64>(6)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn update_after_fire(
        &self,
        id: &str,
        fired_at: SystemTime,
        next_run_at: SystemTime,
    ) -> Result<(), QueueError> {
        let conn = self.conn.lock()?;
        let rows = conn.execute(
            "UPDATE scheduled_jobs SET last_run_at = ?1, next_run_at = ?2 WHERE id = ?3",
            (
                system_time_to_epoch(fired_at),
                system_time_to_epoch(next_run_at),
                id,
            ),
        )?;
        if rows == 0 {
            return Err(QueueError::NotFound(id.to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVERY_SECOND: &str = "* * * * * * *";

    #[test]
    fn test_sqlite_save_and_find_enabled_roundtrip() {
        let repo = SqliteScheduledJobRepository::new(":memory:").unwrap();
        let now = SystemTime::now();
        let job = ScheduledJob::new(
            "s1".into(),
            "send_email".into(),
            "user@example.com".into(),
            EVERY_SECOND.into(),
            now,
        )
        .unwrap();
        repo.save(&job).unwrap();

        let loaded = repo.find_all_enabled().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "s1");
        assert_eq!(loaded[0].task_name, "send_email");
        assert_eq!(loaded[0].payload, "user@example.com");
        assert_eq!(loaded[0].cron_expression, EVERY_SECOND);
        assert!(loaded[0].enabled);
        assert!(loaded[0].last_run_at.is_none());
    }

    #[test]
    fn test_sqlite_disabled_excluded_from_find_enabled() {
        let repo = SqliteScheduledJobRepository::new(":memory:").unwrap();
        let now = SystemTime::now();
        let mut job = ScheduledJob::new(
            "s1".into(),
            "t".into(),
            "p".into(),
            EVERY_SECOND.into(),
            now,
        )
        .unwrap();
        job.enabled = false;
        repo.save(&job).unwrap();

        assert!(repo.find_all_enabled().unwrap().is_empty());
    }

    #[test]
    fn test_sqlite_update_after_fire_persists_new_times() {
        let repo = SqliteScheduledJobRepository::new(":memory:").unwrap();
        let now = SystemTime::now();
        let job = ScheduledJob::new(
            "s1".into(),
            "t".into(),
            "p".into(),
            EVERY_SECOND.into(),
            now,
        )
        .unwrap();
        repo.save(&job).unwrap();

        let fired_at = now + Duration::from_secs(1);
        let next = now + Duration::from_secs(2);
        repo.update_after_fire("s1", fired_at, next).unwrap();

        let loaded = &repo.find_all_enabled().unwrap()[0];
        assert_eq!(
            loaded.last_run_at.unwrap(),
            epoch_to_system_time(system_time_to_epoch(fired_at))
        );
        assert_eq!(
            loaded.next_run_at,
            epoch_to_system_time(system_time_to_epoch(next))
        );
    }

    #[test]
    fn test_sqlite_update_after_fire_not_found() {
        let repo = SqliteScheduledJobRepository::new(":memory:").unwrap();
        let err = repo
            .update_after_fire("missing", SystemTime::now(), SystemTime::now())
            .unwrap_err();
        assert!(matches!(err, QueueError::NotFound(_)));
    }

    #[test]
    fn test_sqlite_save_if_absent_inserts_when_missing() {
        let repo = SqliteScheduledJobRepository::new(":memory:").unwrap();
        let now = SystemTime::now();
        let job = ScheduledJob::new(
            "s1".into(),
            "t".into(),
            "p".into(),
            EVERY_SECOND.into(),
            now,
        )
        .unwrap();

        assert!(repo.save_if_absent(&job).unwrap());
        assert_eq!(repo.find_all_enabled().unwrap().len(), 1);
    }

    #[test]
    fn test_sqlite_save_if_absent_preserves_existing_progress() {
        let repo = SqliteScheduledJobRepository::new(":memory:").unwrap();
        let now = SystemTime::now();
        let job = ScheduledJob::new(
            "s1".into(),
            "t".into(),
            "p".into(),
            EVERY_SECOND.into(),
            now,
        )
        .unwrap();
        repo.save(&job).unwrap();

        let fired_at = now + Duration::from_secs(10);
        let next = now + Duration::from_secs(20);
        repo.update_after_fire("s1", fired_at, next).unwrap();

        // Re-seed: must NOT clobber last_run_at / next_run_at.
        let reseeded = ScheduledJob::new(
            "s1".into(),
            "t".into(),
            "p".into(),
            EVERY_SECOND.into(),
            now + Duration::from_secs(100),
        )
        .unwrap();
        assert!(!repo.save_if_absent(&reseeded).unwrap());

        let loaded = &repo.find_all_enabled().unwrap()[0];
        assert_eq!(
            loaded.last_run_at.unwrap(),
            epoch_to_system_time(system_time_to_epoch(fired_at))
        );
        assert_eq!(
            loaded.next_run_at,
            epoch_to_system_time(system_time_to_epoch(next))
        );
    }

    #[test]
    fn test_sqlite_find_due_filters_by_next_run_at() {
        let repo = SqliteScheduledJobRepository::new(":memory:").unwrap();
        let now = SystemTime::now();

        let due = ScheduledJob::new(
            "due".into(),
            "t".into(),
            "p".into(),
            EVERY_SECOND.into(),
            now - Duration::from_secs(10),
        )
        .unwrap();
        let not_due = ScheduledJob::new(
            "later".into(),
            "t".into(),
            "p".into(),
            "0 0 0 1 1 * *".into(),
            now,
        )
        .unwrap();
        repo.save(&due).unwrap();
        repo.save(&not_due).unwrap();

        let hits = repo.find_due(now).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "due");
    }

    #[test]
    fn test_sqlite_find_due_excludes_disabled() {
        let repo = SqliteScheduledJobRepository::new(":memory:").unwrap();
        let now = SystemTime::now();
        let mut job = ScheduledJob::new(
            "s1".into(),
            "t".into(),
            "p".into(),
            EVERY_SECOND.into(),
            now - Duration::from_secs(10),
        )
        .unwrap();
        job.enabled = false;
        repo.save(&job).unwrap();

        assert!(repo.find_due(now).unwrap().is_empty());
    }

    #[test]
    fn test_sqlite_save_is_upsert() {
        let repo = SqliteScheduledJobRepository::new(":memory:").unwrap();
        let now = SystemTime::now();
        let mut job = ScheduledJob::new(
            "s1".into(),
            "t".into(),
            "p1".into(),
            EVERY_SECOND.into(),
            now,
        )
        .unwrap();
        repo.save(&job).unwrap();

        job.payload = "p2".into();
        repo.save(&job).unwrap();

        let loaded = repo.find_all_enabled().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].payload, "p2");
    }
}
