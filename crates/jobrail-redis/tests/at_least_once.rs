use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn expired_job_is_requeued_and_can_execute_again() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    // Separate connection for test setup/cleanup.
    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    // Clean queues.
    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:delayed")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await
        .expect("failed to clean queues");

    // --------------------------------------------------
    // Create a job.
    // --------------------------------------------------

    let job = Job::new(
        "send_email",
        serde_json::json!({
            "email": "test@example.com"
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();

    storage.save_job(&job).await.expect("failed to save job");

    storage
        .enqueue(job_id.clone())
        .await
        .expect("failed to enqueue job");

    // --------------------------------------------------
    // Worker A claims the job.
    // --------------------------------------------------

    let lease_a = storage
        .wait_and_claim_job(1_000)
        .await
        .expect("Worker A failed to claim job");

    assert_eq!(lease_a.job_id, job_id);

    println!("Worker A claimed: {}", job_id.0);

    // --------------------------------------------------
    // Pretend Worker A successfully executed the work.
    //
    // In the real world, this could be:
    //
    // send_email() -> SUCCESS
    //
    // But Worker A crashes before calling complete_job().
    // --------------------------------------------------

    println!("Worker A executed the job successfully");

    // Simulate the worker losing its lease.
    //
    // We directly remove the lease rather than waiting for
    // the 1 second lease to expire. This keeps the test fast
    // and deterministic.
    storage
        .remove_from_processing(job_id.clone(), lease_a.token.clone())
        .await
        .expect("failed to remove Worker A lease");

    // Put an expired lease into processing so the recovery
    // worker has something to recover.
    let expired_lease = format!("{}:{}", job_id.0, lease_a.token);

    let _: () = redis::cmd("ZADD")
        .arg("jobrail:queue:processing")
        .arg(0)
        .arg(&expired_lease)
        .query_async(&mut connection)
        .await
        .expect("failed to create expired lease");

    // Mark the persisted job as Active, just like it would be
    // while Worker A was processing it.
    let mut job = storage
        .get_job(job_id.clone())
        .await
        .expect("failed to get job")
        .expect("job not found");

    job.state = JobState::Active;

    storage
        .save_job(&job)
        .await
        .expect("failed to save active job");

    // --------------------------------------------------
    // Recovery finds the expired lease.
    // --------------------------------------------------

    storage
        .recover_expired_jobs()
        .await
        .expect("recovery failed");

    // The job should now be Waiting again.
    let recovered_job = storage
        .get_job(job_id.clone())
        .await
        .expect("failed to get recovered job")
        .expect("recovered job not found");

    assert_eq!(recovered_job.state, JobState::Waiting);

    // --------------------------------------------------
    // Worker B gets the same job.
    // --------------------------------------------------

    let lease_b = storage
        .wait_and_claim_job(1_000)
        .await
        .expect("Worker B failed to claim recovered job");

    assert_eq!(lease_b.job_id, job_id);

    // Worker B must receive a different token.
    assert_ne!(lease_a.token, lease_b.token);

    println!("Worker B claimed: {}", lease_b.job_id.0);

    // This is the important proof:
    //
    // The SAME job became available again after Worker A
    // lost ownership.
    //
    // Therefore JobRail provides at-least-once execution.
}
