use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use serial_test::serial;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
#[serial]
async fn scheduled_job_is_promoted_when_due() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    // Clean scheduled and waiting queues.
    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:waiting")
        .query_async(&mut connection)
        .await
        .expect("failed to clean queues");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before UNIX epoch")
        .as_millis() as u64;

    let job = Job::new(
        "scheduled_test",
        serde_json::json!({
            "hello": "world"
        }),
        JobOptions {
            run_at: Some(now),
            ..Default::default()
        },
    );

    let job_id = job.id.clone();

    assert_eq!(job.state, JobState::Scheduled);

    storage
        .schedule_job(job)
        .await
        .expect("failed to schedule job");

    let promoted = storage
        .promote_scheduled_jobs()
        .await
        .expect("failed to promote job");

    assert_eq!(promoted, vec![job_id.clone()]);

    let stored_job = storage
        .get_job(job_id.clone())
        .await
        .expect("failed to read job")
        .expect("job not found");

    assert_eq!(stored_job.state, JobState::Waiting);

    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:waiting")
        .arg(format!("jobrail:job:{}", job_id.0))
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");
}

#[tokio::test]
#[serial]
async fn future_scheduled_job_is_not_promoted_early() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:waiting")
        .query_async(&mut connection)
        .await
        .expect("failed to clean queues");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before UNIX epoch")
        .as_millis() as u64;

    let run_at = now + 60_000;

    let job = Job::new(
        "future_test",
        serde_json::json!({}),
        JobOptions {
            run_at: Some(run_at),
            ..Default::default()
        },
    );

    let job_id = job.id.clone();

    storage
        .schedule_job(job)
        .await
        .expect("failed to schedule job");

    let promoted = storage
        .promote_scheduled_jobs()
        .await
        .expect("failed to promote jobs");

    assert!(promoted.is_empty(), "future job was promoted too early");

    let stored_job = storage
        .get_job(job_id.clone())
        .await
        .expect("failed to read job")
        .expect("job not found");

    assert_eq!(stored_job.state, JobState::Scheduled);

    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:waiting")
        .arg(format!("jobrail:job:{}", job_id.0))
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");
}
