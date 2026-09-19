use jobrail_core::repeat::{RepeatSchedule, RepeatableJob};
use jobrail_redis::RedisStorage;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn repeatable_job_can_be_saved_and_loaded() {
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

    let job_id = repeatable_job.id;

    let redis_key = format!("jobrail:repeat:{}", job_id.0);

    let _: () = redis::cmd("DEL")
        .arg(&redis_key)
        .arg("jobrail:queue:repeatable")
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");

    storage
        .save_repeatable_job(repeatable_job.clone())
        .await
        .expect("failed to save repeatable job");

    let loaded = storage
        .get_repeatable_job(job_id)
        .await
        .expect("failed to get repeatable job")
        .expect("repeatable job not found");

    assert_eq!(loaded.id, repeatable_job.id);

    assert_eq!(loaded.name, "send_report");

    assert!(loaded.enabled);

    let jobs = storage
        .list_repeatable_jobs()
        .await
        .expect("failed to list repeatable jobs");

    assert!(jobs.iter().any(|job| job.id == repeatable_job.id));

    let _: () = redis::cmd("DEL")
        .arg(&redis_key)
        .arg("jobrail:queue:repeatable")
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");
}

#[tokio::test]
#[serial]
async fn repeatable_job_can_be_disabled() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let job = RepeatableJob::new(
        "send_report",
        serde_json::json!({}),
        RepeatSchedule::EveryMillis(60_000),
    );

    storage
        .save_repeatable_job(job.clone())
        .await
        .expect("failed to save repeatable job");

    let disabled = storage
        .disable_repeatable_job(job.id)
        .await
        .expect("failed to disable repeatable job");

    assert!(disabled);

    let loaded = storage
        .get_repeatable_job(job.id)
        .await
        .expect("failed to load repeatable job")
        .expect("repeatable job missing");

    assert!(!loaded.enabled);
}
