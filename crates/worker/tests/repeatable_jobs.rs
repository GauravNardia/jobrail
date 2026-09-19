use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use jobrail_core::{
    job::JobState,
    repeat::{RepeatSchedule, RepeatableJob},
};

use jobrail_redis::RedisStorage;

use jobrail_worker::{JobHandler, Worker};

use serial_test::serial;

struct CountingHandler {
    executions: Arc<AtomicU32>,
}

impl JobHandler for CountingHandler {
    fn execute(&self, _payload: serde_json::Value) -> Result<(), String> {
        self.executions.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }
}

#[tokio::test]
#[serial]
async fn repeatable_scheduler_creates_due_execution() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let repeatable_job = RepeatableJob::new(
        "send_report",
        serde_json::json!({
            "user_id": 123
        }),
        RepeatSchedule::EveryMillis(60_000),
    );

    let repeatable_key = format!("jobrail:repeat:{}", repeatable_job.id.0);

    let _: () = redis::cmd("DEL")
        .arg(&repeatable_key)
        .arg("jobrail:queue:repeatable")
        .arg("jobrail:queue:repeatable:schedule")
        .arg("jobrail:queue:scheduled")
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");

    storage
        .save_repeatable_job(repeatable_job.clone())
        .await
        .expect("failed to save repeatable job");

    let mut saved = storage
        .get_repeatable_job(repeatable_job.id)
        .await
        .expect("failed to load repeatable job")
        .expect("repeatable job missing");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64;

    saved.next_run_at = Some(now_ms.saturating_sub(1));

    storage
        .save_repeatable_job(saved)
        .await
        .expect("failed to make job due");

    let created = jobrail_worker::run_repeatable_scheduler_once(&mut storage)
        .await
        .expect("scheduler iteration failed");

    assert_eq!(created, 1);

    let scheduled_jobs: Vec<String> = redis::cmd("ZRANGE")
        .arg("jobrail:queue:scheduled")
        .arg(0)
        .arg(-1)
        .query_async(&mut connection)
        .await
        .expect("failed to inspect scheduled queue");

    assert_eq!(scheduled_jobs.len(), 1);
}

#[tokio::test]
#[serial]
async fn repeatable_job_advances_to_next_occurrence() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let repeatable_job = RepeatableJob::new(
        "send_report",
        serde_json::json!({}),
        RepeatSchedule::EveryMillis(1_000),
    );

    let mut job = repeatable_job.clone();

    job.next_run_at = Some(0);

    storage
        .save_repeatable_job(job.clone())
        .await
        .expect("failed to save repeatable job");

    let run_at = job.next_run_at.expect("missing next run");

    let execution = storage
        .create_repeatable_execution(&job, run_at)
        .await
        .expect("failed to create execution")
        .expect("execution was not created");

    assert_eq!(execution.run_at, Some(run_at));

    let updated = storage
        .get_repeatable_job(repeatable_job.id)
        .await
        .expect("failed to load updated job")
        .expect("repeatable job missing");

    assert_eq!(updated.next_run_at, Some(run_at + 1_000));
}

#[tokio::test]
#[serial]

async fn two_scheduler_passes_create_only_one_execution() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let repeatable_job = RepeatableJob::new(
        "send_report",
        serde_json::json!({}),
        RepeatSchedule::EveryMillis(60_000),
    );

    let repeatable_key = format!("jobrail:repeat:{}", repeatable_job.id.0);

    // Clean this test's data before starting.
    let _: () = redis::cmd("DEL")
        .arg(&repeatable_key)
        .arg("jobrail:queue:repeatable")
        .arg("jobrail:queue:repeatable:schedule")
        .arg("jobrail:queue:scheduled")
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");

    storage
        .save_repeatable_job(repeatable_job.clone())
        .await
        .expect("failed to save repeatable job");

    let mut job = storage
        .get_repeatable_job(repeatable_job.id)
        .await
        .expect("failed to load repeatable job")
        .expect("repeatable job missing");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64;

    job.next_run_at = Some(now_ms.saturating_sub(1));

    storage
        .save_repeatable_job(job)
        .await
        .expect("failed to make job due");

    let first = jobrail_worker::run_repeatable_scheduler_once(&mut storage)
        .await
        .expect("first scheduler failed");

    let second = jobrail_worker::run_repeatable_scheduler_once(&mut storage)
        .await
        .expect("second scheduler failed");

    assert_eq!(first, 1);

    assert_eq!(second, 0);

    // Clean up this test's repeatable job.
    let _: () = redis::cmd("DEL")
        .arg(&repeatable_key)
        .arg("jobrail:queue:repeatable")
        .arg("jobrail:queue:repeatable:schedule")
        .arg("jobrail:queue:scheduled")
        .query_async(&mut connection)
        .await
        .expect("failed to clean repeatable data");
}

#[tokio::test]
#[serial]
async fn repeatable_job_runs_end_to_end_multiple_times() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let repeatable_job = RepeatableJob::new(
        "count",
        serde_json::json!({
            "message": "hello"
        }),
        RepeatSchedule::EveryMillis(100),
    );

    let repeatable_key = format!("jobrail:repeat:{}", repeatable_job.id.0);

    // --------------------------------------------------
    // Clean test data
    // --------------------------------------------------

    let _: () = redis::cmd("DEL")
        .arg(&repeatable_key)
        .arg("jobrail:queue:repeatable")
        .arg("jobrail:queue:repeatable:schedule")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");

    // --------------------------------------------------
    // 1. Create the repeatable definition
    // --------------------------------------------------

    storage
        .save_repeatable_job(repeatable_job.clone())
        .await
        .expect("failed to save repeatable job");

    // --------------------------------------------------
    // 2. Make the first occurrence due
    // --------------------------------------------------

    let mut saved = storage
        .get_repeatable_job(repeatable_job.id)
        .await
        .expect("failed to load repeatable job")
        .expect("repeatable job missing");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64;

    saved.next_run_at = Some(now_ms.saturating_sub(1));

    storage
        .save_repeatable_job(saved)
        .await
        .expect("failed to make first occurrence due");

    // --------------------------------------------------
    // 3. Run repeatable scheduler
    // --------------------------------------------------

    jobrail_worker::run_repeatable_scheduler_once(&mut storage)
        .await
        .expect("repeatable scheduler failed");

    // --------------------------------------------------
    // 4. Promote Scheduled -> Waiting
    // --------------------------------------------------

    let promoted = storage
        .promote_scheduled_jobs()
        .await
        .expect("failed to promote scheduled job");

    assert_eq!(
        promoted.len(),
        1,
        "scheduler should create one scheduled execution"
    );

    let first_job_id = promoted[0].clone();

    // --------------------------------------------------
    // 5. Create worker handler
    // --------------------------------------------------

    let executions = Arc::new(AtomicU32::new(0));

    let handler = Arc::new(CountingHandler {
        executions: Arc::clone(&executions),
    });

    // --------------------------------------------------
    // 6. Run first execution
    // --------------------------------------------------

    Worker::run_once(1, &mut storage, Arc::clone(&handler))
        .await
        .expect("worker failed on first execution");

    // --------------------------------------------------
    // 7. Verify first execution completed
    // --------------------------------------------------

    let first_job = storage
        .get_job(first_job_id.clone())
        .await
        .expect("failed to load first job")
        .expect("first job missing");

    assert_eq!(
        first_job.state,
        JobState::Completed,
        "first execution should be completed"
    );

    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "handler should execute exactly once"
    );

    // --------------------------------------------------
    // 8. Wait for the next repeat interval
    // --------------------------------------------------

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // --------------------------------------------------
    // 9. Run scheduler again
    // --------------------------------------------------

    jobrail_worker::run_repeatable_scheduler_once(&mut storage)
        .await
        .expect("repeatable scheduler failed on second occurrence");

    // --------------------------------------------------
    // 10. Promote second execution
    // --------------------------------------------------

    let promoted = storage
        .promote_scheduled_jobs()
        .await
        .expect("failed to promote second scheduled job");

    assert_eq!(
        promoted.len(),
        1,
        "scheduler should create exactly one second execution"
    );

    let second_job_id = promoted[0].clone();

    assert_ne!(
        first_job_id, second_job_id,
        "each repeatable execution must have a different job ID"
    );

    // --------------------------------------------------
    // 11. Run second execution
    // --------------------------------------------------

    Worker::run_once(1, &mut storage, Arc::clone(&handler))
        .await
        .expect("worker failed on second execution");

    // --------------------------------------------------
    // 12. Verify second execution completed
    // --------------------------------------------------

    let second_job = storage
        .get_job(second_job_id.clone())
        .await
        .expect("failed to load second job")
        .expect("second job missing");

    assert_eq!(
        second_job.state,
        JobState::Completed,
        "second execution should be completed"
    );

    assert_eq!(
        executions.load(Ordering::SeqCst),
        2,
        "handler should execute twice"
    );

    // --------------------------------------------------
    // 13. Verify repeatable job has another occurrence
    // --------------------------------------------------

    let final_repeatable_job = storage
        .get_repeatable_job(repeatable_job.id)
        .await
        .expect("failed to load final repeatable job")
        .expect("repeatable job missing");

    assert!(
        final_repeatable_job.next_run_at.is_some(),
        "repeatable job should have another scheduled occurrence"
    );

    // --------------------------------------------------
    // Cleanup
    // --------------------------------------------------

    let _: () = redis::cmd("DEL")
        .arg(&repeatable_key)
        .arg("jobrail:queue:repeatable")
        .arg("jobrail:queue:repeatable:schedule")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:processing")
        .arg(format!("jobrail:job:{}", first_job_id.0))
        .arg(format!("jobrail:job:{}", second_job_id.0))
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");
}

#[tokio::test]
#[serial]
async fn disabled_repeatable_job_does_not_create_execution() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let repeatable_job = RepeatableJob::new(
        "send_report",
        serde_json::json!({}),
        RepeatSchedule::EveryMillis(100),
    );

    let repeatable_key = format!("jobrail:repeat:{}", repeatable_job.id.0);

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    // Clean test data.
    let _: () = redis::cmd("DEL")
        .arg(&repeatable_key)
        .arg("jobrail:queue:repeatable")
        .arg("jobrail:queue:repeatable:schedule")
        .arg("jobrail:queue:scheduled")
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");

    storage
        .save_repeatable_job(repeatable_job.clone())
        .await
        .expect("failed to save repeatable job");

    let disabled = storage
        .disable_repeatable_job(repeatable_job.id)
        .await
        .expect("failed to disable repeatable job");

    assert!(disabled);

    let created = jobrail_worker::run_repeatable_scheduler_once(&mut storage)
        .await
        .expect("scheduler failed");

    assert_eq!(created, 0);

    let scheduled_jobs: Vec<String> = redis::cmd("ZRANGE")
        .arg("jobrail:queue:scheduled")
        .arg(0)
        .arg(-1)
        .query_async(&mut connection)
        .await
        .expect("failed to inspect scheduled queue");

    assert!(scheduled_jobs.is_empty());

    // Cleanup.
    let _: () = redis::cmd("DEL")
        .arg(&repeatable_key)
        .arg("jobrail:queue:repeatable")
        .arg("jobrail:queue:repeatable:schedule")
        .arg("jobrail:queue:scheduled")
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");
}

#[tokio::test]
#[serial]
async fn repeatable_job_can_be_deleted() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let repeatable_job = RepeatableJob::new(
        "send_report",
        serde_json::json!({}),
        RepeatSchedule::EveryMillis(60_000),
    );

    storage
        .save_repeatable_job(repeatable_job.clone())
        .await
        .expect("failed to save repeatable job");

    let deleted = storage
        .delete_repeatable_job(repeatable_job.id)
        .await
        .expect("failed to delete repeatable job");

    assert!(deleted);

    let loaded = storage
        .get_repeatable_job(repeatable_job.id)
        .await
        .expect("failed to check deleted job");

    assert!(loaded.is_none());
}
