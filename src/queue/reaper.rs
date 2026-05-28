use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::QueueError;
use crate::models::JobStatus;
use crate::persistence::{JobDispatch, JobState};

/// Runs `reclaim_stale` on every assigned partition on a periodic timer.
/// Each reclaimed job is also flipped to `Pending` in the state store so
/// external observers see the recovered status quickly — `reclaim_stale`
/// only handles dispatch-side bookkeeping.
pub(crate) struct Reaper {
    dispatch: Arc<dyn JobDispatch>,
    state: Arc<dyn JobState>,
    partitions: Vec<u32>,
    lease: Duration,
}

impl Reaper {
    pub fn new(
        dispatch: Arc<dyn JobDispatch>,
        state: Arc<dyn JobState>,
        partitions: Vec<u32>,
        lease: Duration,
    ) -> Self {
        Self {
            dispatch,
            state,
            partitions,
            lease,
        }
    }

    pub fn tick(&self) -> Result<usize, QueueError> {
        let mut reclaimed = 0;
        for &p in &self.partitions {
            let claims = self.dispatch.reclaim_stale(p, self.lease)?;
            for claim in &claims {
                // Defensive: state may already be Pending if reclaim_stale
                // also rewrites status (SQLite does), but Redis dispatch
                // doesn't touch state. Either way, this is idempotent.
                if let Err(e) = self.state.save_status(&claim.job.id, JobStatus::Pending) {
                    eprintln!("reaper: failed to reset status for {}: {e}", claim.job.id);
                }
            }
            reclaimed += claims.len();
        }
        Ok(reclaimed)
    }
}

pub(crate) struct ReaperThread {
    shutdown: Arc<(Mutex<bool>, Condvar)>,
    handle: Option<JoinHandle<()>>,
}

impl ReaperThread {
    pub fn spawn(reaper: Reaper, interval: Duration) -> Self {
        let shutdown = Arc::new((Mutex::new(false), Condvar::new()));
        let shutdown_thread = Arc::clone(&shutdown);
        let handle = thread::Builder::new()
            .name("reaper".into())
            .spawn(move || {
                let (lock, cvar) = &*shutdown_thread;
                loop {
                    if let Err(e) = reaper.tick() {
                        eprintln!("reaper tick error: {e}");
                    }
                    let stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
                    if *stopped {
                        break;
                    }
                    let (stopped, _) = cvar
                        .wait_timeout(stopped, interval)
                        .unwrap_or_else(|e| e.into_inner());
                    if *stopped {
                        break;
                    }
                }
            })
            .expect("spawn reaper thread");
        Self {
            shutdown,
            handle: Some(handle),
        }
    }

    pub fn stop(mut self) {
        self.signal_stop();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    fn signal_stop(&self) {
        let (lock, cvar) = &*self.shutdown;
        let mut stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
        *stopped = true;
        cvar.notify_all();
    }
}

impl Drop for ReaperThread {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.signal_stop();
        }
    }
}
