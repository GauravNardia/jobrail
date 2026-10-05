use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use redis::AsyncCommands;

#[tokio::test]
async fn cancelling_active_job_removes_lease_and_blocks_completion() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

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

    // The job must no longer be in the active queue.
    let active_jobs: Vec<String> = connection.lrange("jobrail:queue:active", 0, -1).await?;

    assert!(
        !active_jobs.contains(&job_id_string),
        "cancelled job is still in active queue"
    );

    // The processing lease must be gone.
    let lease_score: Option<f64> = connection
        .zscore("jobrail:queue:processing", &lease_member)
        .await?;

    assert!(
        lease_score.is_none(),
        "cancelled job still owns a processing lease"
    );

    // A stale worker must not be able to complete the job.
    let completed = storage.complete_job(job_id.clone(), token.clone()).await?;

    assert!(
        !completed,
        "stale worker was able to complete a cancelled job"
    );

    // A stale worker must not be able to fail/retry the job either.
    let failed = storage
        .fail_job(job_id.clone(), token.clone(), JobState::Failed, 0)
        .await?;

    assert!(!failed, "stale worker was able to fail a cancelled job");

    // Lease renewal must also fail.
    let renewed = storage
        .renew_lease(job_id.clone(), token.clone(), 30_000)
        .await?;

    assert!(!renewed, "cancelled job lease was renewed");

    // Final state must still be Cancelled.
    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should still exist");

    assert_eq!(final_job.state, JobState::Cancelled);

    // Cleanup.
    let job_key = format!("jobrail:job:{}", job_id.0);

    let _: () = connection.del(job_key).await?;

    let _: () = connection
        .lrem("jobrail:queue:active", 0, &job_id_string)
        .await?;

    let _: () = connection
        .lrem("jobrail:queue:waiting", 0, &job_id_string)
        .await?;

    let _: () = connection
        .zrem("jobrail:queue:processing", &lease_member)
        .await?;

    Ok(())
}

#[tokio::test]
async fn max_attempt_failure_is_terminal() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

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

    // We do not use wait_and_claim_job() here.
    //
    // That method claims whichever job happens to be in the global
    // waiting queue. Tests should own their specific job directly.
    let token = "max-attempts-test-token".to_string();
    let lease_member = format!("{}:{}", job_id.0, token);

    storage.save_job(&job).await?;

    // Simulate the worker's processing lease for THIS job.
    let _: usize = connection
        .zadd("jobrail:queue:processing", &lease_member, 60_000_i64)
        .await?;

    // Simulate the active queue.
    let _: () = connection
        .lpush("jobrail:queue:active", &job_id_string)
        .await?;

    // start_job() now has a lease that definitely belongs to this job.
    assert!(storage.start_job(job_id.clone(), token.clone()).await?);

    // Keep the local representation in sync with Redis.
    job.state = JobState::Active;
    job.attempts_started = 1;
    job.attempts_made = 1;

    storage.save_job(&job).await?;

    // Max attempts have been exhausted, so this must become terminal Failed.
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

    // Terminal failure must not remain in delayed queue.
    let delayed_score: Option<f64> = connection
        .zscore("jobrail:queue:delayed", &job_id_string)
        .await?;

    assert!(
        delayed_score.is_none(),
        "terminal job must not remain in delayed queue"
    );

    // Terminal failure must not remain in waiting queue.
    let waiting_jobs: Vec<String> = connection.lrange("jobrail:queue:waiting", 0, -1).await?;

    assert!(
        !waiting_jobs.contains(&job_id_string),
        "terminal job must not remain in waiting queue"
    );

    // Terminal failure must not remain in active queue.
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

    // Cleanup.
    let job_key = format!("jobrail:job:{}", job_id.0);

    let _: () = connection.del(job_key).await?;

    let _: () = connection
        .lrem("jobrail:queue:waiting", 0, &job_id_string)
        .await?;

    let _: () = connection
        .lrem("jobrail:queue:active", 0, &job_id_string)
        .await?;

    let _: () = connection
        .zrem("jobrail:queue:delayed", &job_id_string)
        .await?;

    let _: () = connection
        .zrem("jobrail:queue:processing", &lease_member)
        .await?;

    Ok(())
}

#[tokio::test]
async fn enqueue_does_not_create_duplicate_waiting_entries() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

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

    // Cleanup.
    let _: () = connection
        .lrem("jobrail:queue:waiting", 0, &job_id_string)
        .await?;

    let job_key = format!("jobrail:job:{}", job_id.0);

    let _: () = connection.del(job_key).await?;

    Ok(())
}

#[tokio::test]
async fn cancellation_wins_over_expired_lease_recovery() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

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

    storage.save_job(&job).await?;

    // Put the job directly into the active state.
    job.state = JobState::Active;
    storage.save_job(&job).await?;

    // Simulate the active queue.
    let _: () = connection
        .lpush("jobrail:queue:active", &job_id_string)
        .await?;

    // Simulate an already-expired processing lease.
    let _: usize = connection
        .zadd("jobrail:queue:processing", &lease_member, 0_i64)
        .await?;

    // Cancel wins.
    let cancelled = storage.cancel_job(job_id.clone()).await?;

    assert_eq!(
        cancelled.expect("job should exist").state,
        JobState::Cancelled
    );

    // Recovery runs afterwards.
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

    // It must not have been requeued.
    let waiting_jobs: Vec<String> = connection.lrange("jobrail:queue:waiting", 0, -1).await?;

    assert!(
        !waiting_jobs.contains(&job_id_string),
        "cancelled job was resurrected into waiting queue"
    );

    // Cleanup.
    let job_key = format!("jobrail:job:{}", job_id.0);

    let _: () = connection.del(job_key).await?;

    let _: () = connection
        .lrem("jobrail:queue:waiting", 0, &job_id_string)
        .await?;

    let _: () = connection
        .lrem("jobrail:queue:active", 0, &job_id_string)
        .await?;

    let _: () = connection
        .zrem("jobrail:queue:processing", &lease_member)
        .await?;

    Ok(())
}

#[tokio::test]
async fn cancelled_job_rejects_stale_retry() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

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

    storage.save_job(&job).await?;

    // The job must be Active before cancellation.
    job.state = JobState::Active;
    storage.save_job(&job).await?;

    // Simulate the active queue.
    let _: () = connection
        .lpush("jobrail:queue:active", &job_id_string)
        .await?;

    // Simulate this job's processing lease.
    let _: usize = connection
        .zadd("jobrail:queue:processing", &lease_member, 60_000_i64)
        .await?;

    // Cancel the job.
    assert!(storage.cancel_job(job_id.clone()).await?.is_some());

    // The stale worker attempts to retry.
    let retry = storage
        .fail_job(job_id.clone(), token.clone(), JobState::Delayed, 123_456)
        .await?;

    assert!(!retry, "cancelled job must reject stale retry");

    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(final_job.state, JobState::Cancelled);

    // It must not have been placed into delayed queue.
    let delayed_score: Option<f64> = connection
        .zscore("jobrail:queue:delayed", &job_id_string)
        .await?;

    assert!(
        delayed_score.is_none(),
        "cancelled job must not enter delayed queue"
    );

    // It must not have been placed into waiting queue.
    let waiting_jobs: Vec<String> = connection.lrange("jobrail:queue:waiting", 0, -1).await?;

    assert!(
        !waiting_jobs.contains(&job_id_string),
        "cancelled job must not enter waiting queue"
    );

    // Cleanup.
    let job_key = format!("jobrail:job:{}", job_id.0);

    let _: () = connection.del(job_key).await?;

    let _: () = connection
        .lrem("jobrail:queue:waiting", 0, &job_id_string)
        .await?;

    let _: () = connection
        .lrem("jobrail:queue:active", 0, &job_id_string)
        .await?;

    let _: () = connection
        .zrem("jobrail:queue:delayed", &job_id_string)
        .await?;

    let _: () = connection
        .zrem("jobrail:queue:processing", &lease_member)
        .await?;

    Ok(())
}
