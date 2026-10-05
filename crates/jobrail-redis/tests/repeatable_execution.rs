use jobrail_core::repeat::{RepeatSchedule, RepeatableJob};
use jobrail_redis::RedisStorage;
use serial_test::serial;

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_millis() as u64
}

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
        .expect("failed to schedule repeatable execution")
        .expect("scheduled execution was not created");

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

#[tokio::test]
async fn disabled_repeatable_job_cannot_create_execution() -> redis::RedisResult<()> {
    use jobrail_core::repeat::{RepeatSchedule, RepeatableJob, RepeatableJobId};

    let mut storage = RedisStorage::new().await?;

    let repeatable = RepeatableJob {
        id: RepeatableJobId(uuid::Uuid::new_v4()),
        name: "disabled-repeatable".to_string(),
        payload: serde_json::json!({
            "test": true
        }),
        schedule: RepeatSchedule::EveryMillis(10_000),
        enabled: true,
        next_run_at: Some(current_timestamp_ms()),
    };

    let id = repeatable.id;

    storage.save_repeatable_job(repeatable.clone()).await?;

    assert!(storage.disable_repeatable_job(id).await?);

    let disabled = storage
        .get_repeatable_job(id)
        .await?
        .expect("repeatable job should exist");

    assert!(!disabled.enabled, "repeatable job should be disabled");

    let run_at = current_timestamp_ms();

    let execution = storage
        .create_repeatable_execution(&disabled, run_at)
        .await?;

    assert!(
        execution.is_none(),
        "disabled repeatable job must not create execution"
    );

    Ok(())
}
