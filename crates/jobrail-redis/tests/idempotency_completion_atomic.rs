use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn job_and_idempotency_key_complete_together() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let idempotency_key = "payment:atomic-test".to_string();

    let _: () = redis::cmd("DEL")
        .arg(format!("jobrail:idempotency:{}", idempotency_key))
        .query_async(&mut connection)
        .await
        .expect("failed to clean idempotency key");

    let job = Job::new(
        "charge_payment",
        serde_json::json!({
            "amount": 1000
        }),
        JobOptions {
            idempotency_key: Some(idempotency_key.clone()),
            ..Default::default()
        },
    );

    let job_id = job.id.clone();

    storage.save_job(&job).await.expect("failed to save job");

    let claimed = storage
        .claim_idempotency_key(idempotency_key.clone(), job_id.clone())
        .await
        .expect("failed to claim idempotency key");

    assert!(claimed);

    // Put the job into the processing/active lifecycle.
    storage
        .enqueue(job_id.clone())
        .await
        .expect("failed to enqueue job");

    let lease = storage
        .wait_and_claim_job(30_000)
        .await
        .expect("failed to claim job");

    assert_eq!(lease.job_id, job_id);

    let started = storage
        .start_job(job_id.clone(), lease.token.clone())
        .await
        .expect("failed to start job");

    assert!(started);

    let completed = storage
        .complete_job_with_idempotency(job_id.clone(), lease.token.clone(), idempotency_key.clone())
        .await
        .expect("failed to complete job");

    assert!(completed);

    let stored_job = storage
        .get_job(job_id)
        .await
        .expect("failed to read job")
        .expect("job not found");

    assert_eq!(stored_job.state, JobState::Completed);

    let idempotency_state = storage
        .check_idempotency_key(idempotency_key)
        .await
        .expect("failed to check idempotency");

    assert_eq!(
        idempotency_state,
        jobrail_redis::IdempotencyState::Completed
    );
}
