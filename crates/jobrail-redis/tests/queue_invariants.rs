use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use redis::AsyncCommands;
use serial_test::serial;

async fn cleanup(connection: &mut redis::aio::MultiplexedConnection) -> redis::RedisResult<()> {
    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:delayed")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:processing")
        .query_async(connection)
        .await?;

    Ok(())
}

async fn assert_job_membership(
    connection: &mut redis::aio::MultiplexedConnection,
    job_id: &str,
    expected_waiting: bool,
    expected_active: bool,
    expected_delayed: bool,
    expected_scheduled: bool,
    expected_processing: bool,
) -> redis::RedisResult<()> {
    let waiting: Vec<String> = connection.lrange("jobrail:queue:waiting", 0, -1).await?;

    let active: Vec<String> = connection.lrange("jobrail:queue:active", 0, -1).await?;

    let delayed: Option<f64> = connection.zscore("jobrail:queue:delayed", job_id).await?;

    let scheduled: Option<f64> = connection.zscore("jobrail:queue:scheduled", job_id).await?;

    let processing_members: Vec<String> =
        connection.zrange("jobrail:queue:processing", 0, -1).await?;

    let processing = processing_members
        .iter()
        .any(|member| member.starts_with(&format!("{job_id}:")));

    assert_eq!(
        waiting.contains(&job_id.to_string()),
        expected_waiting,
        "waiting invariant failed for {job_id}"
    );

    assert_eq!(
        active.contains(&job_id.to_string()),
        expected_active,
        "active invariant failed for {job_id}"
    );

    assert_eq!(
        delayed.is_some(),
        expected_delayed,
        "delayed invariant failed for {job_id}"
    );

    assert_eq!(
        scheduled.is_some(),
        expected_scheduled,
        "scheduled invariant failed for {job_id}"
    );

    assert_eq!(
        processing, expected_processing,
        "processing invariant failed for {job_id}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn waiting_job_has_only_waiting_membership() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    cleanup(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let job = Job::new(
        "invariant-waiting".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job_id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    let stored = storage.get_job(job_id.clone()).await?.unwrap();

    assert_eq!(stored.state, JobState::Waiting);

    assert_job_membership(
        &mut connection,
        &job_id_string,
        true,
        false,
        false,
        false,
        false,
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn active_job_has_active_and_processing_membership() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    cleanup(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let job = Job::new(
        "invariant-active".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job_id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    let lease = storage.wait_and_claim_job(5_000).await?;

    assert_eq!(lease.job_id, job_id);

    assert_job_membership(
        &mut connection,
        &job_id_string,
        false,
        true,
        false,
        false,
        true,
    )
    .await?;

    assert!(
        storage
            .start_job(job_id.clone(), lease.token.clone())
            .await?
    );

    let stored = storage.get_job(job_id.clone()).await?.unwrap();

    assert_eq!(stored.state, JobState::Active);

    assert_job_membership(
        &mut connection,
        &job_id_string,
        false,
        true,
        false,
        false,
        true,
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn completed_job_has_no_queue_membership() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    cleanup(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let job = Job::new(
        "invariant-completed".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job_id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    let lease = storage.wait_and_claim_job(5_000).await?;

    assert!(
        storage
            .start_job(job_id.clone(), lease.token.clone())
            .await?
    );

    assert!(storage.complete_job(job_id.clone(), lease.token).await?);

    let stored = storage.get_job(job_id.clone()).await?.unwrap();

    assert_eq!(stored.state, JobState::Completed);

    assert_job_membership(
        &mut connection,
        &job_id_string,
        false,
        false,
        false,
        false,
        false,
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn delayed_job_has_only_delayed_membership() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    cleanup(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let job = Job::new(
        "invariant-delayed".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions {
            delay_ms: 60_000,
            ..Default::default()
        },
    );

    let job_id = job.id.clone();
    let job_id_string = job_id.0.to_string();

    storage.save_job(&job).await?;

    assert_eq!(job.state, JobState::Delayed);

    storage.schedule_retry(job_id.clone(), 60_000).await?;

    assert_job_membership(
        &mut connection,
        &job_id_string,
        false,
        false,
        true,
        false,
        false,
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn scheduled_job_has_only_scheduled_membership() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    cleanup(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let run_at = u64::MAX / 2;

    let mut job = Job::new(
        "invariant-scheduled".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions {
            run_at: Some(run_at),
            ..Default::default()
        },
    );

    job.state = JobState::Scheduled;

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    storage.schedule_job(job).await?;

    let stored = storage.get_job(job_id.clone()).await?.unwrap();

    assert_eq!(stored.state, JobState::Scheduled);

    assert_job_membership(
        &mut connection,
        &job_id_string,
        false,
        false,
        false,
        true,
        false,
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn cancelled_job_has_no_queue_membership() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    cleanup(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let job = Job::new(
        "invariant-cancelled".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    let cancelled = storage.cancel_job(job_id.clone()).await?;

    assert_eq!(
        cancelled.expect("job should exist").state,
        JobState::Cancelled
    );

    assert_job_membership(
        &mut connection,
        &job_id_string,
        false,
        false,
        false,
        false,
        false,
    )
    .await?;

    Ok(())
}
