use jobrail_core::{
    job::JobState,
    repeat::{RepeatSchedule, RepeatableJob},
};

use jobrail_redis::RedisStorage;

use serial_test::serial;

#[tokio::test]
#[serial]
async fn same_repeatable_occurrence_is_created_only_once() {
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

    let run_at = 2_000_000;

    let occurrence_key = format!(
        "jobrail:repeat:occurrence:{}:{}",
        repeatable_job.id.0, run_at
    );

    let _: () = redis::cmd("DEL")
        .arg(&occurrence_key)
        .arg("jobrail:queue:scheduled")
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");

    let first = storage
        .schedule_repeatable_execution(&repeatable_job, run_at)
        .await
        .expect("failed to schedule first execution");

    assert!(first.is_some());

    let first_job = first.unwrap();

    assert_eq!(first_job.state, JobState::Scheduled);

    let second = storage
        .schedule_repeatable_execution(&repeatable_job, run_at)
        .await
        .expect("failed to schedule second execution");

    assert!(second.is_none());

    let scheduled_jobs: Vec<String> = redis::cmd("ZRANGE")
        .arg("jobrail:queue:scheduled")
        .arg(0)
        .arg(-1)
        .query_async(&mut connection)
        .await
        .expect("failed to read scheduled queue");

    assert_eq!(scheduled_jobs.len(), 1);

    assert_eq!(scheduled_jobs[0], first_job.id.0.to_string());

    let _: () = redis::cmd("DEL")
        .arg(&occurrence_key)
        .arg(format!("jobrail:job:{}", first_job.id.0))
        .arg("jobrail:queue:scheduled")
        .query_async(&mut connection)
        .await
        .expect("failed to clean test data");
}
