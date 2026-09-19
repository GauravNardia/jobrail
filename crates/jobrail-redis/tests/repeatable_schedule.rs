use jobrail_core::repeat::{RepeatSchedule, RepeatableJob};
use jobrail_redis::RedisStorage;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn repeatable_job_is_added_to_schedule_index() {
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

    let job_id = repeatable_job.id.0.to_string();

    let _: () = redis::cmd("DEL")
        .arg(format!("jobrail:repeat:{}", job_id))
        .arg("jobrail:queue:repeatable")
        .arg("jobrail:queue:repeatable:schedule")
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");

    storage
        .save_repeatable_job(repeatable_job.clone())
        .await
        .expect("failed to save repeatable job");

    let saved = storage
        .get_repeatable_job(repeatable_job.id)
        .await
        .expect("failed to load repeatable job")
        .expect("repeatable job not found");

    assert!(saved.next_run_at.is_some());

    let next_run_at = saved.next_run_at.unwrap();

    let scheduled_ids = storage
        .get_due_repeatable_job_ids(next_run_at)
        .await
        .expect("failed to get due repeatable jobs");

    assert!(scheduled_ids.contains(&job_id));

    let _: () = redis::cmd("DEL")
        .arg(format!("jobrail:repeat:{}", job_id))
        .arg("jobrail:queue:repeatable")
        .arg("jobrail:queue:repeatable:schedule")
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");
}
