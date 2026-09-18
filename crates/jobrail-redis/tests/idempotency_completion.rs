use jobrail_core::job::{Job, JobOptions};
use jobrail_redis::RedisStorage;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn idempotency_key_can_be_completed_by_owner() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let key = "payment:completion-test";

    let redis_key = format!("jobrail:idempotency:{}", key);

    let _: () = redis::cmd("DEL")
        .arg(&redis_key)
        .query_async(&mut connection)
        .await
        .expect("failed to clean idempotency key");

    let job = Job::new(
        "charge_payment",
        serde_json::json!({
            "amount": 1000
        }),
        JobOptions::default(),
    );

    // Reserve the idempotency key.
    let claimed = storage
        .claim_idempotency_key(key.to_string(), job.id.clone())
        .await
        .expect("failed to claim idempotency key");

    assert!(claimed);

    // The same job completes its idempotency operation.
    let completed = storage
        .complete_idempotency_key(key.to_string(), job.id.clone())
        .await
        .expect("failed to complete idempotency key");

    assert!(
        completed,
        "owner should be able to complete its idempotency key"
    );
}

#[tokio::test]
#[serial]
async fn different_job_cannot_complete_idempotency_key() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let key = "payment:wrong-owner-test";

    let redis_key = format!("jobrail:idempotency:{}", key);

    let _: () = redis::cmd("DEL")
        .arg(&redis_key)
        .query_async(&mut connection)
        .await
        .expect("failed to clean idempotency key");

    let job_a = Job::new(
        "charge_payment",
        serde_json::json!({
            "amount": 1000
        }),
        JobOptions::default(),
    );

    let job_b = Job::new(
        "charge_payment",
        serde_json::json!({
            "amount": 1000
        }),
        JobOptions::default(),
    );

    // Job A owns the idempotency key.
    let claimed = storage
        .claim_idempotency_key(key.to_string(), job_a.id.clone())
        .await
        .expect("failed to claim idempotency key");

    assert!(claimed);

    // Job B tries to complete Job A's idempotency key.
    let completed = storage
        .complete_idempotency_key(key.to_string(), job_b.id.clone())
        .await
        .expect("failed to complete idempotency key");

    assert!(
        !completed,
        "different job should NOT be able to complete the idempotency key"
    );
}
