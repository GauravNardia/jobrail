use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn stale_worker_cannot_complete_job() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    // Use a separate Redis connection for test cleanup.
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

    storage.save_job(&job).await.expect("failed to save job");

    // Enqueue only takes JobId.
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
        .get_job(job_id)
        .await
        .expect("failed to get job")
        .expect("job not found");

    assert_eq!(job.state, JobState::Waiting);
    assert_eq!(job.attempts_started, 0);
    assert_eq!(job.attempts_made, 0);
}
