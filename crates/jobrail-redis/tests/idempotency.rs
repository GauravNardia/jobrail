use jobrail_core::job::{Job, JobOptions};
use jobrail_redis::RedisStorage;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn same_idempotency_key_can_only_be_claimed_once() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    // Clean idempotency keys from previous test runs.
    let _: () = redis::cmd("DEL")
        .arg("jobrail:idempotency:payment:123")
        .query_async(&mut connection)
        .await
        .expect("failed to clean idempotency key");

    // Create two different jobs.
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

    // Both jobs represent the SAME logical operation.
    let idempotency_key = "payment:123".to_string();

    // First caller tries to claim the key.
    let first = storage
        .claim_idempotency_key(idempotency_key.clone(), job_a.id.clone())
        .await
        .expect("first idempotency claim failed");

    // First caller should win.
    assert!(
        first,
        "first caller should successfully claim the idempotency key"
    );

    // Second caller tries to claim the SAME key.
    let second = storage
        .claim_idempotency_key(idempotency_key, job_b.id.clone())
        .await
        .expect("second idempotency claim failed");

    // Second caller must lose.
    assert!(
        !second,
        "second caller should NOT be able to claim the same idempotency key"
    );
}
