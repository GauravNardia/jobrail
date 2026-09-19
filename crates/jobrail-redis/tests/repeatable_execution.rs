use jobrail_core::repeat::{RepeatSchedule, RepeatableJob};
use jobrail_redis::RedisStorage;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn repeatable_job_creates_scheduled_execution() {
    let mut storage = RedisStorage::new()
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

    let job = storage
        .schedule_repeatable_execution(&repeatable_job, run_at)
        .await
        .expect("failed to schedule repeatable execution");

    assert_eq!(job.name, "send_report");

    assert_eq!(job.payload["user_id"], 123);

    assert_eq!(job.run_at, Some(run_at));

    assert_eq!(job.state, jobrail_core::job::JobState::Scheduled);

    let loaded = storage
        .get_job(job.id.clone())
        .await
        .expect("failed to load scheduled job")
        .expect("scheduled job not found");

    assert_eq!(loaded.id, job.id);

    assert_eq!(loaded.run_at, Some(run_at));
}
