use std::sync::Mutex;
use std::time::Duration;

use redis::{Client, Commands};

use crate::error::QueueError;
use crate::task::generate_job_id;

/// Cluster-wide guard the scheduler asks before each tick. With multiple
/// scheduler processes pointed at the same store, only the lease holder
/// fires due rows — the rest skip the tick.
pub trait SchedulerLease: Send + Sync {
    /// Try to acquire the lease, or renew it if we already hold it.
    /// Returns `Ok(true)` when this caller holds the lease right now.
    fn try_acquire(&self) -> Result<bool, QueueError>;
}

/// Single-host scheduler: always the leader, no coordination needed.
pub struct AlwaysOnLease;

impl SchedulerLease for AlwaysOnLease {
    fn try_acquire(&self) -> Result<bool, QueueError> {
        Ok(true)
    }
}

/// Redis-backed lease implemented with `SET NX EX`. Each holder writes a
/// random token at acquire-time and only renews if the stored token still
/// matches (so a slow incumbent doesn't trample a new owner).
pub struct RedisSchedulerLease {
    client: Client,
    key: String,
    token: String,
    ttl: Duration,
    state: Mutex<LeaseState>,
}

struct LeaseState {
    holds: bool,
}

impl RedisSchedulerLease {
    pub fn new(url: &str, key: &str) -> Result<Self, QueueError> {
        Self::with_ttl(url, key, Duration::from_secs(15))
    }

    pub fn with_ttl(url: &str, key: &str, ttl: Duration) -> Result<Self, QueueError> {
        let client = Client::open(url)?;
        Ok(Self {
            client,
            key: key.to_string(),
            token: format!("lease-{}", generate_job_id()),
            ttl,
            state: Mutex::new(LeaseState { holds: false }),
        })
    }
}

impl SchedulerLease for RedisSchedulerLease {
    fn try_acquire(&self) -> Result<bool, QueueError> {
        let mut conn = self.client.get_connection()?;
        let mut state = self.state.lock()?;
        let ttl_secs = self.ttl.as_secs().max(1) as i64;
        if state.holds {
            // Renew only if we still own it. Lua keeps the check + write
            // atomic; otherwise a new owner could slip in between GET and
            // PEXPIRE/SET.
            let script = redis::Script::new(
                r#"
                if redis.call('GET', KEYS[1]) == ARGV[1] then
                    redis.call('PEXPIRE', KEYS[1], ARGV[2])
                    return 1
                else
                    return 0
                end
                "#,
            );
            let renewed: i64 = script
                .key(&self.key)
                .arg(&self.token)
                .arg(self.ttl.as_millis() as i64)
                .invoke(&mut conn)?;
            if renewed == 0 {
                state.holds = false;
            }
            return Ok(state.holds);
        }
        // Not currently holding — try to acquire with NX + EX.
        let opts = redis::SetOptions::default()
            .conditional_set(redis::ExistenceCheck::NX)
            .with_expiration(redis::SetExpiry::EX(ttl_secs as u64));
        let acquired: redis::Value = conn.set_options(&self.key, &self.token, opts)?;
        let holds = matches!(acquired, redis::Value::Okay);
        state.holds = holds;
        Ok(holds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_always_on_lease_is_always_held() {
        let lease = AlwaysOnLease;
        assert!(lease.try_acquire().unwrap());
    }
}
