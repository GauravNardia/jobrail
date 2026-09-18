use jobrail_core::job::{Job, JobOptions};
use jobrail_redis::{IdempotencyStatus, RedisStorage};
use serial_test::serial;

#[tokio::test]
#[serial]
async fn can_read_processing_idempotency_record() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let key = "payment:get-test";

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

    // Create the idempotency record.
    let claimed = storage
        .claim_idempotency_key(key.to_string(), job.id.clone())
        .await
        .expect("failed to claim idempotency key");

    assert!(claimed);

    // Read it back.
    let record = storage
        .get_idempotency_key(key.to_string())
        .await
        .expect("failed to get idempotency record")
        .expect("idempotency record not found");

    assert_eq!(record.job_id, job.id);
    assert!(matches!(record.status, IdempotencyStatus::Processing));
}

#[tokio::test]
#[serial]
async fn missing_idempotency_key_returns_none() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let result = storage
        .get_idempotency_key("this-key-does-not-exist".to_string())
        .await
        .expect("failed to get idempotency record");

    assert!(result.is_none());
}
