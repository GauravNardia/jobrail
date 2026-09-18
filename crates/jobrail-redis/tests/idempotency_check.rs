use jobrail_core::job::{Job, JobOptions};
use jobrail_redis::{IdempotencyState, RedisStorage};
use serial_test::serial;

#[tokio::test]
#[serial]
async fn missing_key_is_new() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let state = storage
        .check_idempotency_key("check:new:test".to_string())
        .await
        .expect("failed to check idempotency key");

    assert_eq!(state, IdempotencyState::New);
}

#[tokio::test]
#[serial]
async fn claimed_key_is_processing() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let key = "check:processing:test";

    let redis_key = format!("jobrail:idempotency:{}", key);

    let _: () = redis::cmd("DEL")
        .arg(&redis_key)
        .query_async(&mut connection)
        .await
        .expect("failed to clean idempotency key");

    let job = Job::new("test_job", serde_json::json!({}), JobOptions::default());

    storage
        .claim_idempotency_key(key.to_string(), job.id)
        .await
        .expect("failed to claim idempotency key");

    let state = storage
        .check_idempotency_key(key.to_string())
        .await
        .expect("failed to check idempotency key");

    assert_eq!(state, IdempotencyState::Processing);
}

#[tokio::test]
#[serial]
async fn completed_key_is_completed() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let key = "check:completed:test";

    let redis_key = format!("jobrail:idempotency:{}", key);

    let _: () = redis::cmd("DEL")
        .arg(&redis_key)
        .query_async(&mut connection)
        .await
        .expect("failed to clean idempotency key");

    let job = Job::new("test_job", serde_json::json!({}), JobOptions::default());

    storage
        .claim_idempotency_key(key.to_string(), job.id.clone())
        .await
        .expect("failed to claim idempotency key");

    storage
        .complete_idempotency_key(key.to_string(), job.id)
        .await
        .expect("failed to complete idempotency key");

    let state = storage
        .check_idempotency_key(key.to_string())
        .await
        .expect("failed to check idempotency key");

    assert_eq!(state, IdempotencyState::Completed);
}
