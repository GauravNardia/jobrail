use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use redis::AsyncCommands;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn expired_worker_cannot_complete_after_recovery() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;

    let mut connection = client.get_multiplexed_async_connection().await?;

    let mut storage = RedisStorage::new().await?;

    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:delayed")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await?;

    // ---------------------------------------------------------
    // 1. Create a job
    // ---------------------------------------------------------

    let job = Job::new(
        "lease-recovery-race".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    // ---------------------------------------------------------
    // 2. Worker A claims the job
    // ---------------------------------------------------------

    let worker_a = storage.wait_and_claim_job(60_000).await?;

    assert_eq!(
        worker_a.job_id, job_id,
        "Worker A should claim the expected job"
    );

    let worker_a_token = worker_a.token.clone();

    let worker_a_member = format!("{}:{}", worker_a.job_id.0, worker_a.token);

    // Worker A has claimed the job, but claiming alone does not
    // transition the job to Active.
    //
    // The real worker calls start_job() before executing the handler.
    let started = storage
        .start_job(worker_a.job_id.clone(), worker_a_token.clone())
        .await?;

    assert!(started, "Worker A should successfully start the job");

    // Worker A should now own the job.
    let worker_a_lease: Option<f64> = connection
        .zscore("jobrail:queue:processing", &worker_a_member)
        .await?;

    assert!(
        worker_a_lease.is_some(),
        "Worker A should have an active lease"
    );

    // Verify the job is actually Active before simulating
    // the worker crash.
    let active_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(
        active_job.state,
        JobState::Active,
        "Worker A should have transitioned the job to Active"
    );

    // ---------------------------------------------------------
    // 3. Simulate Worker A's lease expiring
    // ---------------------------------------------------------
    //
    // We don't actually wait 60 seconds.
    //
    // Instead we move the lease expiration timestamp into
    // the past. This makes the test fast and deterministic.
    // ---------------------------------------------------------

    let _: usize = connection
        .zadd("jobrail:queue:processing", &worker_a_member, 0_i64)
        .await?;

    // ---------------------------------------------------------
    // 4. Recovery detects the expired lease
    // ---------------------------------------------------------

    storage.recover_expired_jobs().await?;

    // ---------------------------------------------------------
    // 5. Worker A's lease must be gone
    // ---------------------------------------------------------

    let expired_lease: Option<f64> = connection
        .zscore("jobrail:queue:processing", &worker_a_member)
        .await?;

    assert!(
        expired_lease.is_none(),
        "expired Worker A lease should be removed"
    );

    // ---------------------------------------------------------
    // 6. Job should be waiting again
    // ---------------------------------------------------------

    let recovered_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("recovered job should exist");

    assert_eq!(
        recovered_job.state,
        JobState::Waiting,
        "expired job should be moved back to Waiting"
    );

    // ---------------------------------------------------------
    // 7. Worker B claims the recovered job
    // ---------------------------------------------------------

    let worker_b = storage.wait_and_claim_job(60_000).await?;

    assert_eq!(
        worker_b.job_id, job_id,
        "Worker B should reclaim the same job"
    );

    let worker_b_token = worker_b.token.clone();

    // Worker B has claimed the job, but claiming alone does not
    // transition the job to Active. The worker normally calls
    // start_job() immediately after claiming it.
    let started = storage
        .start_job(worker_b.job_id.clone(), worker_b_token.clone())
        .await?;

    assert!(
        started,
        "Worker B should successfully start the recovered job"
    );

    assert_ne!(
        worker_a_token, worker_b_token,
        "Worker B must receive a different ownership token"
    );

    let worker_b_member = format!("{}:{}", worker_b.job_id.0, worker_b.token);

    // ---------------------------------------------------------
    // 8. Verify Worker B owns the lease
    // ---------------------------------------------------------

    let worker_b_lease: Option<f64> = connection
        .zscore("jobrail:queue:processing", &worker_b_member)
        .await?;

    assert!(
        worker_b_lease.is_some(),
        "Worker B should have the new lease"
    );

    // Worker A must NOT have a lease anymore.
    let worker_a_after_recovery: Option<f64> = connection
        .zscore("jobrail:queue:processing", &worker_a_member)
        .await?;

    assert!(
        worker_a_after_recovery.is_none(),
        "Worker A must not regain ownership"
    );

    // ---------------------------------------------------------
    // 9. Worker A finishes late
    // ---------------------------------------------------------
    //
    // This is the important race.
    //
    // Worker A still has its old token.
    // It must NOT be allowed to complete the job.
    // ---------------------------------------------------------

    let stale_completion = storage
        .complete_job(job_id.clone(), worker_a_token.clone())
        .await?;

    assert!(
        !stale_completion,
        "expired Worker A must not complete a job owned by Worker B"
    );

    // ---------------------------------------------------------
    // 10. Job must still be Active
    // ---------------------------------------------------------

    let job_after_stale_completion = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should still exist");

    assert_eq!(
        job_after_stale_completion.state,
        JobState::Active,
        "stale Worker A completion must not change the job state"
    );

    // ---------------------------------------------------------
    // 11. Worker B finishes successfully
    // ---------------------------------------------------------

    let valid_completion = storage
        .complete_job(job_id.clone(), worker_b_token.clone())
        .await?;

    assert!(
        valid_completion,
        "current Worker B owner should be able to complete the job"
    );

    // ---------------------------------------------------------
    // 12. Final state must be Completed
    // ---------------------------------------------------------

    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should still exist");

    assert_eq!(
        final_job.state,
        JobState::Completed,
        "job should finally be completed by Worker B"
    );

    // ---------------------------------------------------------
    // 13. Cleanup
    // ---------------------------------------------------------

    let _: () = connection.del(format!("jobrail:job:{}", job_id.0)).await?;

    let _: () = connection
        .lrem("jobrail:queue:waiting", 0, &job_id_string)
        .await?;

    let _: () = connection
        .lrem("jobrail:queue:active", 0, &job_id_string)
        .await?;

    let _: () = connection
        .zrem("jobrail:queue:processing", &worker_a_member)
        .await?;

    let _: () = connection
        .zrem("jobrail:queue:processing", &worker_b_member)
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn expired_worker_cannot_schedule_retry_after_recovery() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    let mut storage = RedisStorage::new().await?;

    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:delayed")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await?;

    let job = Job::new(
        "retry-recovery-race".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    // Worker A claims.
    let worker_a = storage.wait_and_claim_job(60_000).await?;

    let worker_a_token = worker_a.token.clone();
    let worker_a_member = format!("{}:{}", job_id.0, worker_a_token);

    let started = storage
        .start_job(job_id.clone(), worker_a_token.clone())
        .await?;

    assert!(started);

    // Simulate Worker A crash.
    let _: usize = connection
        .zadd("jobrail:queue:processing", &worker_a_member, 0_i64)
        .await?;

    // Recovery gives ownership back to the queue.
    storage.recover_expired_jobs().await?;

    let recovered = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(recovered.state, JobState::Waiting);

    // Worker B takes ownership.
    let worker_b = storage.wait_and_claim_job(60_000).await?;

    let worker_b_token = worker_b.token.clone();

    let started = storage
        .start_job(job_id.clone(), worker_b_token.clone())
        .await?;

    assert!(started);

    // Worker A wakes up late and tries to schedule a retry.
    let stale_retry = storage
        .fail_job(
            job_id.clone(),
            worker_a_token.clone(),
            JobState::Delayed,
            123_456_789,
        )
        .await?;

    assert!(!stale_retry, "stale Worker A must not schedule a retry");

    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(
        final_job.state,
        JobState::Active,
        "Worker B must remain the owner"
    );

    let delayed_score: Option<f64> = connection
        .zscore("jobrail:queue:delayed", &job_id_string)
        .await?;

    assert!(
        delayed_score.is_none(),
        "stale Worker A created a delayed retry"
    );

    storage
        .complete_job(job_id.clone(), worker_b_token.clone())
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn stale_retry_cannot_override_completed_job() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    let mut storage = RedisStorage::new().await?;

    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:delayed")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await?;

    let job = Job::new(
        "retry-completion-race".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    let worker_a = storage.wait_and_claim_job(60_000).await?;
    let worker_a_token = worker_a.token.clone();

    assert!(
        storage
            .start_job(job_id.clone(), worker_a_token.clone())
            .await?
    );

    let worker_a_member = format!("{}:{}", job_id.0, worker_a_token);

    // Expire A.
    let _: usize = connection
        .zadd("jobrail:queue:processing", &worker_a_member, 0_i64)
        .await?;

    storage.recover_expired_jobs().await?;

    // B reclaims.
    let worker_b = storage.wait_and_claim_job(60_000).await?;
    let worker_b_token = worker_b.token.clone();

    assert!(
        storage
            .start_job(job_id.clone(), worker_b_token.clone())
            .await?
    );

    // B completes first.
    let completed = storage
        .complete_job(job_id.clone(), worker_b_token.clone())
        .await?;

    assert!(completed);

    // A wakes up and tries failure/retry.
    let stale_failure = storage
        .fail_job(
            job_id.clone(),
            worker_a_token.clone(),
            JobState::Delayed,
            123_456,
        )
        .await?;

    assert!(
        !stale_failure,
        "stale Worker A must not retry a completed job"
    );

    let final_job = storage.get_job(job_id).await?.expect("job should exist");

    assert_eq!(final_job.state, JobState::Completed);

    Ok(())
}
