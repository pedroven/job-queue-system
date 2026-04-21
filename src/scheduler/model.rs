use std::str::FromStr;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use cron::Schedule;

use crate::error::QueueError;

#[derive(Debug, Clone)]
pub struct ScheduledJob {
    pub id: String,
    pub task_name: String,
    pub payload: String,
    pub cron_expression: String,
    pub next_run_at: SystemTime,
    pub last_run_at: Option<SystemTime>,
    pub enabled: bool,
}

impl ScheduledJob {
    pub fn new(
        id: String,
        task_name: String,
        payload: String,
        cron_expression: String,
        now: SystemTime,
    ) -> Result<Self, QueueError> {
        let next_run_at = next_fire_after(&cron_expression, now)?;
        Ok(ScheduledJob {
            id,
            task_name,
            payload,
            cron_expression,
            next_run_at,
            last_run_at: None,
            enabled: true,
        })
    }
}

pub trait ScheduledJobRepository: Send + Sync {
    fn save(&self, job: &ScheduledJob) -> Result<(), QueueError>;
    /// Insert `job` only if no row with the same `id` exists. Returns `true`
    /// when a row was inserted, `false` when the id was already present.
    /// Seed/bootstrap paths use this to avoid clobbering persisted progress
    /// (`last_run_at` / `next_run_at`) on every process restart.
    fn save_if_absent(&self, job: &ScheduledJob) -> Result<bool, QueueError>;
    fn find_all_enabled(&self) -> Result<Vec<ScheduledJob>, QueueError>;
    /// Enabled jobs whose `next_run_at <= now`. Pushes the "due" filter into
    /// storage so the tick loop doesn't pull every row into memory.
    fn find_due(&self, now: SystemTime) -> Result<Vec<ScheduledJob>, QueueError>;
    fn update_after_fire(
        &self,
        id: &str,
        fired_at: SystemTime,
        next_run_at: SystemTime,
    ) -> Result<(), QueueError>;
}

pub(super) fn next_fire_after(expr: &str, after: SystemTime) -> Result<SystemTime, QueueError> {
    let schedule =
        Schedule::from_str(expr).map_err(|e| QueueError::InvalidCron(format!("{expr}: {e}")))?;
    let after_dt: DateTime<Utc> = after.into();
    schedule
        .after(&after_dt)
        .next()
        .map(SystemTime::from)
        .ok_or_else(|| QueueError::InvalidCron(format!("no future fire for '{expr}'")))
}

#[cfg(test)]
mod tests {
    use super::*;

    // "every second" cron — 7 fields: sec min hour dom mon dow year
    pub(in crate::scheduler) const EVERY_SECOND: &str = "* * * * * * *";

    #[test]
    fn test_scheduled_job_new_computes_next_run_in_future() {
        let now = SystemTime::now();
        let job = ScheduledJob::new(
            "s1".into(),
            "send_email".into(),
            "{}".into(),
            EVERY_SECOND.into(),
            now,
        )
        .unwrap();
        assert!(job.next_run_at > now);
        assert!(job.enabled);
        assert!(job.last_run_at.is_none());
    }

    #[test]
    fn test_scheduled_job_new_rejects_invalid_cron() {
        let err = ScheduledJob::new(
            "s1".into(),
            "t".into(),
            "{}".into(),
            "not-a-cron".into(),
            SystemTime::now(),
        )
        .unwrap_err();
        assert!(matches!(err, QueueError::InvalidCron(_)));
    }
}
