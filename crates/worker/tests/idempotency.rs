use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use jobrail_worker::{JobHandler, Worker};
use serde_json::Value;
use serial_test::serial;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

struct CountingHandler {
    executions: Arc<AtomicU32>,
}

impl JobHandler for CountingHandler {
    fn execute(&self, _payload: Value) -> Result<(), String> {
        self.executions.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }
}

#[tokio::test]
#[serial]
async fn same_idempotency_key_executes_only_once() {
    // --------------------------------------------------
    // Setup handler
    // --------------------------------------------------

    let executions = Arc::new(AtomicU32::new(0));

    let handler = Arc::new(CountingHandler {
        executions: Arc::clone(&executions),
    });

    // --------------------------------------------------
    // Create worker
    // --------------------------------------------------

    // let worker = Worker::new(1).await.expect("failed to create worker");

    // --------------------------------------------------
    // Redis storage
    // --------------------------------------------------

    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    // --------------------------------------------------
    // Idempotency key
    // --------------------------------------------------

    let idempotency_key = "payment:test:123".to_string();

    // --------------------------------------------------
    // Clean previous test data
    // --------------------------------------------------

    let _: () = redis::cmd("DEL")
        .arg(format!("jobrail:idempotency:{}", idempotency_key))
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await
        .expect("failed to clean Redis");

    // --------------------------------------------------
    // Create Job A
    // --------------------------------------------------

    let job_a = Job::new(
        "charge_payment",
        serde_json::json!({
            "amount": 1000
        }),
        JobOptions {
            idempotency_key: Some(idempotency_key.clone()),
            ..Default::default()
        },
    );

    // --------------------------------------------------
    // Create Job B with SAME idempotency key
    // --------------------------------------------------

    let job_b = Job::new(
        "charge_payment",
        serde_json::json!({
            "amount": 1000
        }),
        JobOptions {
            idempotency_key: Some(idempotency_key.clone()),
            ..Default::default()
        },
    );

    let job_a_id = job_a.id.clone();
    let job_b_id = job_b.id.clone();

    // --------------------------------------------------
    // Save both jobs
    // --------------------------------------------------

    storage
        .save_job(&job_a)
        .await
        .expect("failed to save job A");

    storage
        .save_job(&job_b)
        .await
        .expect("failed to save job B");

    // --------------------------------------------------
    // Enqueue both jobs
    // --------------------------------------------------

    storage
        .enqueue(job_a_id.clone())
        .await
        .expect("failed to enqueue job A");

    storage
        .enqueue(job_b_id.clone())
        .await
        .expect("failed to enqueue job B");

    // --------------------------------------------------
    // First worker execution
    // --------------------------------------------------

    Worker::run_once(1, &mut storage, Arc::clone(&handler))
        .await
        .expect("first worker iteration failed");

    // --------------------------------------------------
    // Job A should execute exactly once
    // --------------------------------------------------

    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "first job should execute exactly once"
    );

    // --------------------------------------------------
    // Verify Job A completed
    // --------------------------------------------------

    let job_a = storage
        .get_job(job_a_id.clone())
        .await
        .expect("failed to read job A")
        .expect("job A not found");

    assert_eq!(
        job_a.state,
        JobState::Completed,
        "job A should be completed"
    );

    // --------------------------------------------------
    // Second worker execution
    // --------------------------------------------------

    Worker::run_once(1, &mut storage, Arc::clone(&handler))
        .await
        .expect("second worker iteration failed");

    // --------------------------------------------------
    // Handler MUST NOT execute again
    // --------------------------------------------------

    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "second job must not execute because the idempotency key is already completed"
    );

    // --------------------------------------------------
    // Verify Job B completed
    // --------------------------------------------------

    let job_b = storage
        .get_job(job_b_id.clone())
        .await
        .expect("failed to read job B")
        .expect("job B not found");

    assert_eq!(
        job_b.state,
        JobState::Completed,
        "duplicate job should be marked as completed"
    );
}
