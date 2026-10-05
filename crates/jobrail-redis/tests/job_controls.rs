use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use redis::AsyncCommands;
use serial_test::serial;

async fn cleanup_redis(
    connection: &mut redis::aio::MultiplexedConnection,
) -> redis::RedisResult<()> {
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("jobrail:*")
        .query_async(connection)
        .await?;

    if !keys.is_empty() {
        let _: () = redis::cmd("DEL").arg(keys).query_async(connection).await?;
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn cancelling_active_job_removes_lease_and_blocks_completion() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    cleanup_redis(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let mut job = Job::new(
        "cancel-active-job".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    job.state = JobState::Active;

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    let token = "cancel-test-token".to_string();
    let lease_member = format!("{}:{}", job_id.0, token);

    storage.save_job(&job).await?;

    // Simulate an active job.
    let _: () = connection
        .lpush("jobrail:queue:active", &job_id_string)
        .await?;

    // Simulate the worker's processing lease.
    let _: usize = connection
        .zadd("jobrail:queue:processing", &lease_member, 60_000_i64)
        .await?;

    // Cancel the active job.
    let cancelled = storage.cancel_job(job_id.clone()).await?;
    let cancelled = cancelled.expect("job should exist");

    assert_eq!(cancelled.state, JobState::Cancelled);

    // Job must no longer be active.
    let active_jobs: Vec<String> = connection.lrange("jobrail:queue:active", 0, -1).await?;

    assert!(
        !active_jobs.contains(&job_id_string),
        "cancelled job is still in active queue"
    );

    // Processing lease must be gone.
    let lease_score: Option<f64> = connection
        .zscore("jobrail:queue:processing", &lease_member)
        .await?;

    assert!(
        lease_score.is_none(),
        "cancelled job still owns a processing lease"
    );

    // Stale worker must not complete the cancelled job.
    let completed = storage.complete_job(job_id.clone(), token.clone()).await?;

    assert!(
        !completed,
        "stale worker was able to complete a cancelled job"
    );

    // Stale worker must not fail/retry it either.
    let failed = storage
        .fail_job(job_id.clone(), token.clone(), JobState::Failed, 0)
        .await?;

    assert!(!failed, "stale worker was able to fail a cancelled job");

    // Lease renewal must also fail.
    let renewed = storage
        .renew_lease(job_id.clone(), token.clone(), 30_000)
        .await?;

    assert!(!renewed, "cancelled job lease was renewed");

    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should still exist");

    assert_eq!(final_job.state, JobState::Cancelled);

    cleanup_redis(&mut connection).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn max_attempt_failure_is_terminal() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    cleanup_redis(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let mut job = Job::new(
        "max-attempts".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions {
            max_attempts: 1,
            ..Default::default()
        },
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    let token = "max-attempts-test-token".to_string();
    let lease_member = format!("{}:{}", job_id.0, token);

    storage.save_job(&job).await?;

    // Simulate this specific job's processing lease.
    let _: usize = connection
        .zadd("jobrail:queue:processing", &lease_member, 60_000_i64)
        .await?;

    // Simulate active queue membership.
    let _: () = connection
        .lpush("jobrail:queue:active", &job_id_string)
        .await?;

    assert!(storage.start_job(job_id.clone(), token.clone()).await?);

    // Keep the job representation consistent with the exhausted attempt.
    job.state = JobState::Active;
    job.attempts_started = 1;
    job.attempts_made = 1;

    storage.save_job(&job).await?;

    // Maximum attempts exhausted.
    let failed = storage
        .fail_job(job_id.clone(), token.clone(), JobState::Failed, 0)
        .await?;

    assert!(
        failed,
        "job should transition to Failed when max attempts are exhausted"
    );

    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(final_job.state, JobState::Failed);

    // Must not be delayed.
    let delayed_score: Option<f64> = connection
        .zscore("jobrail:queue:delayed", &job_id_string)
        .await?;

    assert!(
        delayed_score.is_none(),
        "terminal job must not remain in delayed queue"
    );

    // Must not be waiting.
    let waiting_jobs: Vec<String> = connection.lrange("jobrail:queue:waiting", 0, -1).await?;

    assert!(
        !waiting_jobs.contains(&job_id_string),
        "terminal job must not remain in waiting queue"
    );

    // Must not be active.
    let active_jobs: Vec<String> = connection.lrange("jobrail:queue:active", 0, -1).await?;

    assert!(
        !active_jobs.contains(&job_id_string),
        "terminal job must not remain in active queue"
    );

    // Processing lease must be gone.
    let lease_score: Option<f64> = connection
        .zscore("jobrail:queue:processing", &lease_member)
        .await?;

    assert!(
        lease_score.is_none(),
        "terminal job must not retain its processing lease"
    );

    cleanup_redis(&mut connection).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn enqueue_does_not_create_duplicate_waiting_entries() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    cleanup_redis(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let job = Job::new(
        "duplicate-enqueue".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    storage.save_job(&job).await?;

    storage.enqueue(job_id.clone()).await?;
    storage.enqueue(job_id.clone()).await?;
    storage.enqueue(job_id.clone()).await?;

    let waiting_jobs: Vec<String> = connection.lrange("jobrail:queue:waiting", 0, -1).await?;

    let count = waiting_jobs
        .iter()
        .filter(|id| **id == job_id_string)
        .count();

    assert_eq!(count, 1, "job must appear only once in waiting queue");

    cleanup_redis(&mut connection).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn cancellation_wins_over_expired_lease_recovery() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    cleanup_redis(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let mut job = Job::new(
        "cancel-recovery-race".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    let token = "cancel-recovery-test-token".to_string();
    let lease_member = format!("{}:{}", job_id.0, token);

    job.state = JobState::Active;
    storage.save_job(&job).await?;

    // Simulate active queue.
    let _: () = connection
        .lpush("jobrail:queue:active", &job_id_string)
        .await?;

    // Simulate expired lease.
    let _: usize = connection
        .zadd("jobrail:queue:processing", &lease_member, 0_i64)
        .await?;

    // Cancellation wins.
    let cancelled = storage.cancel_job(job_id.clone()).await?;

    assert_eq!(
        cancelled.expect("job should exist").state,
        JobState::Cancelled
    );

    // Recovery happens after cancellation.
    storage.recover_expired_jobs().await?;

    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(
        final_job.state,
        JobState::Cancelled,
        "recovery must not resurrect a cancelled job"
    );

    let waiting_jobs: Vec<String> = connection.lrange("jobrail:queue:waiting", 0, -1).await?;

    assert!(
        !waiting_jobs.contains(&job_id_string),
        "cancelled job was resurrected into waiting queue"
    );

    cleanup_redis(&mut connection).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn cancelled_job_rejects_stale_retry() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    cleanup_redis(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let mut job = Job::new(
        "cancel-retry-race".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    let token = "cancel-retry-test-token".to_string();
    let lease_member = format!("{}:{}", job_id.0, token);

    job.state = JobState::Active;
    storage.save_job(&job).await?;

    // Simulate active queue.
    let _: () = connection
        .lpush("jobrail:queue:active", &job_id_string)
        .await?;

    // Simulate this job's processing lease.
    let _: usize = connection
        .zadd("jobrail:queue:processing", &lease_member, 60_000_i64)
        .await?;

    // Cancel the job.
    assert!(storage.cancel_job(job_id.clone()).await?.is_some());

    // Stale worker attempts retry.
    let retry = storage
        .fail_job(job_id.clone(), token.clone(), JobState::Delayed, 123_456)
        .await?;

    assert!(!retry, "cancelled job must reject stale retry");

    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(final_job.state, JobState::Cancelled);

    let delayed_score: Option<f64> = connection
        .zscore("jobrail:queue:delayed", &job_id_string)
        .await?;

    assert!(
        delayed_score.is_none(),
        "cancelled job must not enter delayed queue"
    );

    let waiting_jobs: Vec<String> = connection.lrange("jobrail:queue:waiting", 0, -1).await?;

    assert!(
        !waiting_jobs.contains(&job_id_string),
        "cancelled job must not enter waiting queue"
    );

    cleanup_redis(&mut connection).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn completed_job_has_no_non_terminal_queue_membership() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    cleanup_redis(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let job = Job::new(
        "invariant-completed".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let id = job_id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    // This queue now contains ONLY our job because we cleaned Redis
    // before the test.
    let lease = storage.wait_and_claim_job(1_000).await?;

    assert_eq!(
        lease.job_id, job_id,
        "test claimed a different job; Redis queue was not isolated"
    );

    assert!(
        storage
            .start_job(job_id.clone(), lease.token.clone())
            .await?
    );

    assert!(
        storage
            .complete_job(job_id.clone(), lease.token.clone())
            .await?
    );

    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(final_job.state, JobState::Completed);

    let waiting: Vec<String> = connection.lrange("jobrail:queue:waiting", 0, -1).await?;

    let active: Vec<String> = connection.lrange("jobrail:queue:active", 0, -1).await?;

    let delayed: Option<f64> = connection.zscore("jobrail:queue:delayed", &id).await?;

    let scheduled: Option<f64> = connection.zscore("jobrail:queue:scheduled", &id).await?;

    assert!(!waiting.contains(&id));
    assert!(!active.contains(&id));
    assert!(delayed.is_none());
    assert!(scheduled.is_none());

    cleanup_redis(&mut connection).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn active_job_has_processing_lease() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    cleanup_redis(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let job = Job::new(
        "invariant-active".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let id = job_id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    // The queue is isolated because we cleaned Redis above.
    let lease = storage.wait_and_claim_job(1_000).await?;

    assert_eq!(
        lease.job_id, job_id,
        "test claimed a different job; Redis queue was not isolated"
    );

    assert!(
        storage
            .start_job(job_id.clone(), lease.token.clone())
            .await?
    );

    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(final_job.state, JobState::Active);

    let lease_member = format!("{}:{}", id, lease.token);

    let lease_score: Option<f64> = connection
        .zscore("jobrail:queue:processing", &lease_member)
        .await?;

    assert!(
        lease_score.is_some(),
        "Active job must have a processing lease"
    );

    let active: Vec<String> = connection.lrange("jobrail:queue:active", 0, -1).await?;

    assert!(
        active.contains(&id),
        "Active job must exist in active queue"
    );

    cleanup_redis(&mut connection).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn cancelled_job_has_no_queue_membership() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    cleanup_redis(&mut connection).await?;

    let mut storage = RedisStorage::new().await?;

    let job = Job::new(
        "invariant-cancelled".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let id = job_id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    let cancelled = storage.cancel_job(job_id.clone()).await?;

    assert_eq!(
        cancelled.expect("job should exist").state,
        JobState::Cancelled
    );

    let waiting: Vec<String> = connection.lrange("jobrail:queue:waiting", 0, -1).await?;

    let active: Vec<String> = connection.lrange("jobrail:queue:active", 0, -1).await?;

    let delayed: Option<f64> = connection.zscore("jobrail:queue:delayed", &id).await?;

    let scheduled: Option<f64> = connection.zscore("jobrail:queue:scheduled", &id).await?;

    assert!(!waiting.contains(&id));
    assert!(!active.contains(&id));
    assert!(delayed.is_none());
    assert!(scheduled.is_none());

    cleanup_redis(&mut connection).await?;

    Ok(())
}
