use super::*;
use crate::persistence::{InMemoryJobRepository, JobRepository};
use crate::producer::JobProducer;
use crate::queue::Queue;
use crate::scheduler::memory::InMemoryScheduledJobRepository;
use crate::scheduler::model::ScheduledJob;
use crate::task::TaskRegistry;
use std::sync::Mutex as StdMutex;
use std::time::Instant;

const EVERY_SECOND: &str = "* * * * * * *";

struct RecordingProducer {
    jobs: StdMutex<Vec<Job>>,
}

impl RecordingProducer {
    fn new() -> Self {
        Self {
            jobs: StdMutex::new(Vec::new()),
        }
    }
    fn count(&self) -> usize {
        self.jobs.lock().unwrap().len()
    }
    fn jobs(&self) -> Vec<Job> {
        self.jobs.lock().unwrap().clone()
    }
}

impl Producer for RecordingProducer {
    fn produce(&self, job: Job) -> Result<(), QueueError> {
        self.jobs.lock().unwrap().push(job);
        Ok(())
    }
}

fn scheduler_with(
    repo: Arc<InMemoryScheduledJobRepository>,
    producer: Arc<RecordingProducer>,
) -> Scheduler {
    Scheduler::new(
        repo as Arc<dyn ScheduledJobRepository>,
        producer as Arc<dyn Producer>,
    )
}

#[test]
fn test_tick_enqueues_due_jobs() {
    let repo = Arc::new(InMemoryScheduledJobRepository::new());
    let producer = Arc::new(RecordingProducer::new());
    let now = SystemTime::now();
    let scheduled = ScheduledJob::new(
        "s1".into(),
        "send_email".into(),
        "user@example.com".into(),
        EVERY_SECOND.into(),
        now,
    )
    .unwrap();
    repo.save(&scheduled).unwrap();

    let scheduler = scheduler_with(repo, Arc::clone(&producer));
    let fired = scheduler
        .tick(scheduled.next_run_at + Duration::from_secs(1))
        .unwrap();
    assert_eq!(fired, 1);
    assert_eq!(producer.count(), 1);
    let enqueued = &producer.jobs()[0];
    assert_eq!(enqueued.task.name, "send_email");
    assert_eq!(enqueued.task.payload, "user@example.com");
}

#[test]
fn test_tick_skips_jobs_not_yet_due() {
    let repo = Arc::new(InMemoryScheduledJobRepository::new());
    let producer = Arc::new(RecordingProducer::new());
    let now = SystemTime::now();
    let scheduled = ScheduledJob::new(
        "s1".into(),
        "t".into(),
        "p".into(),
        EVERY_SECOND.into(),
        now,
    )
    .unwrap();
    repo.save(&scheduled).unwrap();

    let scheduler = scheduler_with(repo, Arc::clone(&producer));
    let fired = scheduler.tick(now).unwrap();
    assert_eq!(fired, 0);
    assert_eq!(producer.count(), 0);
}

#[test]
fn test_tick_advances_next_run_at() {
    let repo = Arc::new(InMemoryScheduledJobRepository::new());
    let producer = Arc::new(RecordingProducer::new());
    let now = SystemTime::now();
    let scheduled = ScheduledJob::new(
        "s1".into(),
        "t".into(),
        "p".into(),
        EVERY_SECOND.into(),
        now,
    )
    .unwrap();
    repo.save(&scheduled).unwrap();

    let scheduler = scheduler_with(Arc::clone(&repo), producer);
    let tick_at = scheduled.next_run_at + Duration::from_secs(1);
    scheduler.tick(tick_at).unwrap();

    let reloaded = &repo.find_all_enabled().unwrap()[0];
    assert!(reloaded.next_run_at > tick_at);
    assert_eq!(reloaded.last_run_at, Some(tick_at));

    // A second tick at the same moment must NOT re-fire the same slot.
    let fired = scheduler.tick(tick_at).unwrap();
    assert_eq!(fired, 0);
}

#[test]
fn test_tick_ignores_disabled_jobs() {
    let repo = Arc::new(InMemoryScheduledJobRepository::new());
    let producer = Arc::new(RecordingProducer::new());
    let now = SystemTime::now();
    let mut scheduled = ScheduledJob::new(
        "s1".into(),
        "t".into(),
        "p".into(),
        EVERY_SECOND.into(),
        now,
    )
    .unwrap();
    scheduled.enabled = false;
    repo.save(&scheduled).unwrap();

    let scheduler = scheduler_with(repo, Arc::clone(&producer));
    let fired = scheduler
        .tick(scheduled.next_run_at + Duration::from_secs(1))
        .unwrap();
    assert_eq!(fired, 0);
    assert_eq!(producer.count(), 0);
}

struct FailingProducer {
    fail_for: &'static str,
    recorded: StdMutex<Vec<Job>>,
}

impl Producer for FailingProducer {
    fn produce(&self, job: Job) -> Result<(), QueueError> {
        if job.task.name == self.fail_for {
            return Err(QueueError::JobFailed("simulated produce failure".into()));
        }
        self.recorded.lock().unwrap().push(job);
        Ok(())
    }
}

#[test]
fn test_tick_isolates_failing_row_from_rest_of_batch() {
    // A producer that fails for one specific task name must not block
    // the other due rows from firing or advancing. The failing row must
    // retain its original next_run_at so it's re-considered next tick.
    let repo = Arc::new(InMemoryScheduledJobRepository::new());
    let now = SystemTime::now();

    let bad = ScheduledJob::new(
        "bad".into(),
        "will_fail".into(),
        "p".into(),
        EVERY_SECOND.into(),
        now - Duration::from_secs(2),
    )
    .unwrap();
    let good = ScheduledJob::new(
        "good".into(),
        "will_succeed".into(),
        "p".into(),
        EVERY_SECOND.into(),
        now - Duration::from_secs(2),
    )
    .unwrap();
    let bad_original_next = bad.next_run_at;
    repo.save(&bad).unwrap();
    repo.save(&good).unwrap();

    let producer: Arc<dyn Producer> = Arc::new(FailingProducer {
        fail_for: "will_fail",
        recorded: StdMutex::new(Vec::new()),
    });
    let scheduler = Scheduler::new(
        Arc::clone(&repo) as Arc<dyn ScheduledJobRepository>,
        Arc::clone(&producer),
    );

    let fired = scheduler.tick(now).unwrap();
    assert_eq!(fired, 1, "only the good row should count as fired");

    let rows = repo.find_all_enabled().unwrap();
    let reloaded_bad = rows.iter().find(|j| j.id == "bad").unwrap();
    let reloaded_good = rows.iter().find(|j| j.id == "good").unwrap();
    assert_eq!(
        reloaded_bad.next_run_at, bad_original_next,
        "failing row must not advance"
    );
    assert!(
        reloaded_good.next_run_at > now,
        "succeeding row must advance past now"
    );
    assert_eq!(reloaded_good.last_run_at, Some(now));
}

/// Wraps an inner repo and fails `update_after_fire` the first N times it
/// is called. Used to reproduce the "produce succeeded, advance failed"
/// crash-equivalent that drives the scheduler's at-least-once re-fire.
struct UpdateFailNTimesRepo {
    inner: Arc<InMemoryScheduledJobRepository>,
    remaining_failures: StdMutex<u32>,
}

impl ScheduledJobRepository for UpdateFailNTimesRepo {
    fn save(&self, job: &ScheduledJob) -> Result<(), QueueError> {
        self.inner.save(job)
    }
    fn save_if_absent(&self, job: &ScheduledJob) -> Result<bool, QueueError> {
        self.inner.save_if_absent(job)
    }
    fn find_all_enabled(&self) -> Result<Vec<ScheduledJob>, QueueError> {
        self.inner.find_all_enabled()
    }
    fn find_due(&self, now: SystemTime) -> Result<Vec<ScheduledJob>, QueueError> {
        self.inner.find_due(now)
    }
    fn update_after_fire(
        &self,
        id: &str,
        fired_at: SystemTime,
        next_run_at: SystemTime,
    ) -> Result<(), QueueError> {
        let mut left = self.remaining_failures.lock().unwrap();
        if *left > 0 {
            *left -= 1;
            return Err(QueueError::JobFailed("simulated update failure".into()));
        }
        drop(left);
        self.inner.update_after_fire(id, fired_at, next_run_at)
    }
}

#[test]
fn test_refire_after_update_failure_dedupes_at_enqueue() {
    // Timeline:
    //   tick #1 — produce(job) OK, update_after_fire FAILS.
    //             Queue has 1 pending job, scheduled row's next_run_at
    //             is NOT advanced, so the same slot is still due.
    //   tick #2 — produce(job) sees AlreadyExists (deterministic slot id
    //             collides with the job left by tick #1), falls through,
    //             update_after_fire SUCCEEDS.
    // Final state: exactly one pending job, id = "sched:<id>:<slot_epoch>".
    let inner = Arc::new(InMemoryScheduledJobRepository::new());
    let now = SystemTime::now();
    let scheduled = ScheduledJob::new(
        "heartbeat".into(),
        "send_email".into(),
        "ops@example.com".into(),
        EVERY_SECOND.into(),
        now - Duration::from_secs(5),
    )
    .unwrap();
    let slot = scheduled.next_run_at;
    inner.save(&scheduled).unwrap();

    let sched_repo: Arc<dyn ScheduledJobRepository> = Arc::new(UpdateFailNTimesRepo {
        inner: Arc::clone(&inner),
        remaining_failures: StdMutex::new(1),
    });

    let job_repo: Arc<dyn JobRepository> = Arc::new(InMemoryJobRepository::new());
    let queue = Arc::new(Queue::new(0, Arc::clone(&job_repo), TaskRegistry::new()).unwrap());
    let producer: Arc<dyn Producer> = Arc::new(JobProducer::new(Arc::clone(&queue)));
    let scheduler = Scheduler::new(Arc::clone(&sched_repo), Arc::clone(&producer));

    // First tick: update fails, but produce already succeeded.
    let fired_first = scheduler.tick(now).unwrap();
    assert_eq!(
        fired_first, 0,
        "fire_one surfaces the update error so the row isn't counted as fired"
    );
    assert_eq!(job_repo.find_all_pending().unwrap().len(), 1);

    // Second tick at the same instant: slot still due (update didn't
    // advance next_run_at). Re-produce collides on the deterministic id,
    // scheduler swallows AlreadyExists and advances.
    let fired_second = scheduler.tick(now).unwrap();
    assert_eq!(fired_second, 1, "second tick must advance the slot");

    let pending = job_repo.find_all_pending().unwrap();
    assert_eq!(pending.len(), 1, "queue must not contain a duplicate");
    assert_eq!(pending[0].id, slot_job_id("heartbeat", slot));

    // Scheduled row advanced past the original slot.
    let reloaded = &inner.find_all_enabled().unwrap()[0];
    assert!(reloaded.next_run_at > slot);
    assert_eq!(reloaded.last_run_at, Some(now));
}

#[test]
fn test_start_fires_ticks_in_background_and_stops_cleanly() {
    let repo = Arc::new(InMemoryScheduledJobRepository::new());
    let producer = Arc::new(RecordingProducer::new());
    let scheduled = ScheduledJob::new(
        "s1".into(),
        "t".into(),
        "p".into(),
        EVERY_SECOND.into(),
        SystemTime::now() - Duration::from_secs(2),
    )
    .unwrap();
    repo.save(&scheduled).unwrap();

    let scheduler = scheduler_with(repo, Arc::clone(&producer));
    let handle = scheduler.start(Duration::from_millis(50));

    let deadline = Instant::now() + Duration::from_secs(2);
    while producer.count() == 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }

    let start = Instant::now();
    handle.stop();
    let stop_elapsed = start.elapsed();

    assert!(
        producer.count() >= 1,
        "expected at least one fire from background thread"
    );
    assert!(
        stop_elapsed < Duration::from_millis(500),
        "stop should wake the thread promptly, took {stop_elapsed:?}"
    );
}
