use rusqlite::Connection;

use crate::error::QueueError;

/// Idempotent schema setup shared by `SqliteJobState` and `SqliteJobDispatch`.
/// Adds `partition` and `claimed_at` columns versus the pre-partitioning
/// schema; both default sensibly so an existing single-host db keeps working.
pub(super) fn apply(conn: &Connection) -> Result<(), QueueError> {
    // WAL keeps readers unblocked during writes and lets the dispatch and
    // state connections coexist on one file without `SQLITE_BUSY`.
    // synchronous=NORMAL trades a tiny post-commit-fsync window for ~10x
    // commit throughput — acceptable for a job queue that's already
    // at-least-once.
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;",
    )?;
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
            partition INTEGER NOT NULL DEFAULT 0,
            claimed_at INTEGER,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_partition_status_priority_created
            ON jobs(partition, status, priority DESC, created_at ASC);
        CREATE INDEX IF NOT EXISTS idx_jobs_status_claimed_at
            ON jobs(status, claimed_at);",
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
    Ok(())
}
