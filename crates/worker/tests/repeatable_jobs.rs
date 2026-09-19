use jobrail_core::repeat::{RepeatSchedule, RepeatableJob};
use jobrail_redis::RedisStorage;
use serial_test::serial;

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
