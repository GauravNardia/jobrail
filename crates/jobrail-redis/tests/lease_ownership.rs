use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use redis::AsyncCommands;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn stale_worker_cannot_complete_job() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    // Clean queues before the test.
    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:delayed")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await
        .expect("failed to clean queues");

    // Create and save a job.
    let job = Job::new(
        "test_job",
        serde_json::json!({
            "message": "hello"
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

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

    println!("Worker A token: {}", lease_a.token);

    // --------------------------------------------------
    // Simulate Worker A losing ownership.
    // --------------------------------------------------

    storage
        .remove_from_processing(job_id.clone(), lease_a.token.clone())
        .await
        .expect("failed to remove Worker A lease");

    // Put the job back into Waiting.
    let mut job = storage
        .get_job(job_id.clone())
        .await
        .expect("failed to get job")
        .expect("job not found");

    job.state = JobState::Waiting;

    storage
        .save_job(&job)
        .await
        .expect("failed to save recovered job");

    storage
        .enqueue(job_id.clone())
        .await
        .expect("failed to re-enqueue job");

    // --------------------------------------------------
    // Worker B claims the same job.
    // --------------------------------------------------

    let lease_b = storage
        .wait_and_claim_job(1_000)
        .await
        .expect("Worker B failed to claim job");

    assert_eq!(lease_b.job_id, job_id);

    println!("Worker B token: {}", lease_b.token);

    // Worker A and Worker B must have different tokens.
    assert_ne!(lease_a.token, lease_b.token);

    // --------------------------------------------------
    // Worker A is stale.
    // --------------------------------------------------

    let stale_completion = storage
        .complete_job(job_id.clone(), lease_a.token.clone())
        .await
        .expect("completion request failed");

    assert!(
        !stale_completion,
        "stale Worker A should NOT be allowed to complete the job"
    );

    // --------------------------------------------------
    // Worker B still owns the job.
    // --------------------------------------------------

    let valid_completion = storage
        .complete_job(job_id.clone(), lease_b.token.clone())
        .await
        .expect("completion request failed");

    assert!(
        valid_completion,
        "current Worker B should be allowed to complete the job"
    );

    // Cleanup.
    let job_key = format!("jobrail:job:{}", job_id.0);

    let _: () = connection.del(job_key).await.expect("failed to delete job");

    let _: () = connection
        .lrem("jobrail:queue:waiting", 0, &job_id_string)
        .await
        .expect("failed to clean waiting queue");

    let _: () = connection
        .lrem("jobrail:queue:active", 0, &job_id_string)
        .await
        .expect("failed to clean active queue");

    let _: () = connection
        .zrem("jobrail:queue:processing", &lease_a.token)
        .await
        .expect("failed to clean Worker A lease");

    let _: () = connection
        .zrem("jobrail:queue:processing", &lease_b.token)
        .await
        .expect("failed to clean Worker B lease");
}

#[tokio::test]
#[serial]
async fn stale_worker_cannot_start_job() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

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

    // Create and save a job.
    let job = Job::new(
        "test_job",
        serde_json::json!({
            "message": "hello"
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    storage.save_job(&job).await.expect("failed to save job");

    storage
        .enqueue(job_id.clone())
        .await
        .expect("failed to enqueue job");

    // Worker A claims the job.
    let lease = storage
        .wait_and_claim_job(1_000)
        .await
        .expect("Worker A failed to claim job");

    assert_eq!(lease.job_id, job_id);

    // Simulate Worker A losing ownership.
    storage
        .remove_from_processing(job_id.clone(), lease.token.clone())
        .await
        .expect("failed to remove lease");

    // Worker A now tries to start the job.
    let started = storage
        .start_job(job_id.clone(), lease.token.clone())
        .await
        .expect("start_job failed");

    // It must be rejected.
    assert!(
        !started,
        "stale worker should NOT be allowed to start the job"
    );

    // Verify the job was not modified.
    let job = storage
        .get_job(job_id.clone())
        .await
        .expect("failed to get job")
        .expect("job not found");

    assert_eq!(job.state, JobState::Waiting);
    assert_eq!(job.attempts_started, 0);
    assert_eq!(job.attempts_made, 0);

    // Cleanup.
    let job_key = format!("jobrail:job:{}", job_id.0);

    let _: () = connection.del(job_key).await.expect("failed to delete job");

    let _: () = connection
        .lrem("jobrail:queue:waiting", 0, &job_id_string)
        .await
        .expect("failed to clean waiting queue");

    let _: () = connection
        .lrem("jobrail:queue:active", 0, &job_id_string)
        .await
        .expect("failed to clean active queue");

    let lease_member = format!("{}:{}", job_id.0, lease.token);

    let _: () = connection
        .zrem("jobrail:queue:processing", &lease_member)
        .await
        .expect("failed to clean processing lease");
}

#[tokio::test]
#[serial]
async fn stale_worker_cannot_renew_lease() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    let mut storage = RedisStorage::new().await?;

    // Clean queues.
    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:delayed")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await?;

    let job = Job::new(
        "stale-renew".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    let worker_a = storage.wait_and_claim_job(60_000).await?;

    assert_eq!(worker_a.job_id, job_id);

    let stale_token = worker_a.token.clone();

    // Worker A loses ownership.
    storage
        .remove_from_processing(job_id.clone(), stale_token.clone())
        .await?;

    // Stale Worker A tries to renew.
    let renewed = storage
        .renew_lease(job_id.clone(), stale_token.clone(), 60_000)
        .await?;

    assert!(!renewed, "stale worker must not renew a removed lease");

    // Cleanup.
    let job_key = format!("jobrail:job:{}", job_id.0);

    let _: () = connection.del(job_key).await?;

    let _: () = connection
        .lrem("jobrail:queue:waiting", 0, &job_id_string)
        .await?;

    let _: () = connection
        .lrem("jobrail:queue:active", 0, &job_id_string)
        .await?;

    let lease_member = format!("{}:{}", job_id.0, stale_token);

    let _: () = connection
        .zrem("jobrail:queue:processing", &lease_member)
        .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn stale_worker_cannot_fail_job() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    let mut storage = RedisStorage::new().await?;

    // Clean queues.
    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:delayed")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await?;

    let mut job = Job::new(
        "stale-fail".to_string(),
        serde_json::json!({
            "test": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();
    let job_id_string = job.id.0.to_string();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    // Worker A claims the job.
    let worker = storage.wait_and_claim_job(60_000).await?;

    assert_eq!(worker.job_id, job_id);

    let stale_token = worker.token.clone();

    // Worker A legitimately starts the job first.
    let started = storage
        .start_job(job_id.clone(), stale_token.clone())
        .await?;

    assert!(
        started,
        "Worker A should be able to start the job while it owns the lease"
    );

    // Keep the local representation consistent.
    job.state = JobState::Active;

    storage.save_job(&job).await?;

    // Worker A loses ownership.
    storage
        .remove_from_processing(job_id.clone(), stale_token.clone())
        .await?;

    // Worker A is now stale and tries to fail the job.
    let failed = storage
        .fail_job(job_id.clone(), stale_token.clone(), JobState::Failed, 0)
        .await?;

    assert!(!failed, "stale worker must not fail the job");

    // Since fail_job was rejected, the job must remain Active.
    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(
        final_job.state,
        JobState::Active,
        "stale failure must not change the job state"
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

    let lease_member = format!("{}:{}", job_id.0, stale_token);

    let _: () = connection
        .zrem("jobrail:queue:processing", &lease_member)
        .await?;

    Ok(())
}
