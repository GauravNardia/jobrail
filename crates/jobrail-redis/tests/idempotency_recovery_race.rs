use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::{IdempotencyState, RedisStorage};
use redis::AsyncCommands;

#[tokio::test]
async fn expired_idempotent_job_can_be_reclaimed() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;

    let mut connection = client.get_multiplexed_async_connection().await?;

    let mut storage = RedisStorage::new().await?;

    let idempotency_key = format!("recovery-race-{}", uuid::Uuid::new_v4());

    // ---------------------------------------------------------
    // 1. Create an idempotent job
    // ---------------------------------------------------------

    let job = Job::new(
        "idempotency-recovery-race".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions {
            idempotency_key: Some(idempotency_key.clone()),
            ..Default::default()
        },
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    // ---------------------------------------------------------
    // 2. Worker A claims the job
    // ---------------------------------------------------------

    let worker_a = storage.wait_and_claim_job(60_000).await?;

    assert_eq!(worker_a.job_id, job_id, "Worker A should claim the job");

    let worker_a_token = worker_a.token.clone();

    // ---------------------------------------------------------
    // 3. Worker A starts the job
    // ---------------------------------------------------------

    let started = storage
        .start_job(worker_a.job_id.clone(), worker_a_token.clone())
        .await?;

    assert!(started, "Worker A should start the job");

    // ---------------------------------------------------------
    // 4. Worker A claims the idempotency key
    // ---------------------------------------------------------

    let claimed = storage
        .claim_idempotency_key(idempotency_key.clone(), job_id.clone())
        .await?;

    assert!(claimed, "Worker A should claim the idempotency key");

    let state = storage
        .check_idempotency_key(idempotency_key.clone())
        .await?;

    assert_eq!(
        state,
        IdempotencyState::Processing,
        "idempotency key should be Processing"
    );

    // ---------------------------------------------------------
    // 5. Simulate Worker A crashing.
    //
    // Move its lease into the past.
    // ---------------------------------------------------------

    let worker_a_member = format!("{}:{}", worker_a.job_id.0, worker_a.token);

    let _: usize = connection
        .zadd("jobrail:queue:processing", &worker_a_member, 0_i64)
        .await?;

    // ---------------------------------------------------------
    // 6. Recovery runs
    // ---------------------------------------------------------

    storage.recover_expired_jobs().await?;

    // ---------------------------------------------------------
    // 7. The old lease must be gone
    // ---------------------------------------------------------

    let old_lease: Option<f64> = connection
        .zscore("jobrail:queue:processing", &worker_a_member)
        .await?;

    assert!(
        old_lease.is_none(),
        "expired Worker A lease should be removed"
    );

    // ---------------------------------------------------------
    // 8. The job should be Waiting again
    // ---------------------------------------------------------

    let recovered_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("recovered job should exist");

    assert_eq!(
        recovered_job.state,
        JobState::Waiting,
        "recovered job should be Waiting"
    );

    // ---------------------------------------------------------
    // 9. CRITICAL:
    //
    // The stale Processing idempotency record must be cleared.
    // ---------------------------------------------------------

    let state_after_recovery = storage
        .check_idempotency_key(idempotency_key.clone())
        .await?;

    assert_eq!(
        state_after_recovery,
        IdempotencyState::New,
        "recovered job must release its stale idempotency claim"
    );

    // ---------------------------------------------------------
    // 10. Worker B claims the recovered job
    // ---------------------------------------------------------

    let worker_b = storage.wait_and_claim_job(60_000).await?;

    assert_eq!(
        worker_b.job_id, job_id,
        "Worker B should reclaim the recovered job"
    );

    assert_ne!(
        worker_a_token, worker_b.token,
        "Worker B must receive a new ownership token"
    );

    // ---------------------------------------------------------
    // 11. Worker B starts the job
    // ---------------------------------------------------------

    let started = storage
        .start_job(worker_b.job_id.clone(), worker_b.token.clone())
        .await?;

    assert!(started, "Worker B should start the recovered job");

    // ---------------------------------------------------------
    // 12. Worker B should be able to claim the SAME
    //     idempotency key.
    // ---------------------------------------------------------

    let claimed_by_b = storage
        .claim_idempotency_key(idempotency_key.clone(), job_id.clone())
        .await?;

    assert!(
        claimed_by_b,
        "Worker B should be able to reclaim the idempotency key"
    );

    let state_after_b_claim = storage
        .check_idempotency_key(idempotency_key.clone())
        .await?;

    assert_eq!(
        state_after_b_claim,
        IdempotencyState::Processing,
        "Worker B should own the idempotency claim"
    );

    // ---------------------------------------------------------
    // 13. Worker A is now stale.
    //
    // It must NOT be able to complete the job.
    // ---------------------------------------------------------

    let stale_completion = storage
        .complete_job_with_idempotency(
            job_id.clone(),
            worker_a_token.clone(),
            idempotency_key.clone(),
        )
        .await?;

    assert!(
        !stale_completion,
        "stale Worker A must not complete the recovered job"
    );

    // ---------------------------------------------------------
    // 14. Worker B completes successfully.
    // ---------------------------------------------------------

    let valid_completion = storage
        .complete_job_with_idempotency(
            job_id.clone(),
            worker_b.token.clone(),
            idempotency_key.clone(),
        )
        .await?;

    assert!(
        valid_completion,
        "Worker B should complete the recovered job"
    );

    // ---------------------------------------------------------
    // 15. Final job state
    // ---------------------------------------------------------

    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should still exist");

    assert_eq!(
        final_job.state,
        JobState::Completed,
        "final job state should be Completed"
    );

    // ---------------------------------------------------------
    // 16. Idempotency must also be Completed
    // ---------------------------------------------------------

    let final_idempotency_state = storage
        .check_idempotency_key(idempotency_key.clone())
        .await?;

    assert_eq!(
        final_idempotency_state,
        IdempotencyState::Completed,
        "idempotency key should be Completed"
    );

    // ---------------------------------------------------------
    // 17. Cleanup
    // ---------------------------------------------------------

    let _: () = connection.del(format!("jobrail:job:{}", job_id.0)).await?;

    let _: () = connection
        .del(format!("jobrail:idempotency:{}", idempotency_key))
        .await?;

    let _: () = connection
        .lrem("jobrail:queue:waiting", 0, &job_id_string)
        .await?;

    let _: () = connection
        .lrem("jobrail:queue:active", 0, &job_id_string)
        .await?;

    let worker_b_member = format!("{}:{}", worker_b.job_id.0, worker_b.token);

    let _: () = connection
        .zrem("jobrail:queue:processing", &worker_a_member)
        .await?;

    let _: () = connection
        .zrem("jobrail:queue:processing", &worker_b_member)
        .await?;

    Ok(())
}
