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
