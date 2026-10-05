use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use jobrail_worker::{JobHandler, Worker};
use redis::AsyncCommands;
use serde_json::Value;
use serial_test::serial;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct SuccessHandler {
    executions: Arc<AtomicUsize>,
}

impl JobHandler for SuccessHandler {
    fn execute(&self, _payload: Value) -> Result<(), String> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
#[serial]
async fn worker_restart_processes_recovered_job() -> redis::RedisResult<()> {
    // --------------------------------------------------
    // Redis connection used for test setup.
    // --------------------------------------------------

    let client = redis::Client::open("redis://127.0.0.1/")?;

    let mut connection = client.get_multiplexed_async_connection().await?;

    // --------------------------------------------------
    // Clean queues from previous test runs.
    // --------------------------------------------------

    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:delayed")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await?;

    // --------------------------------------------------
    // Create storage.
    // --------------------------------------------------

    let mut storage = RedisStorage::new().await?;

    // --------------------------------------------------
    // Create a job.
    // --------------------------------------------------

    let job = Job::new(
        "worker-restart".to_string(),
        serde_json::json!({
            "restart": true
        }),
        JobOptions::default(),
    );

    let job_id = job.id.clone();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    // --------------------------------------------------
    // Worker A claims the job.
    // --------------------------------------------------

    let worker_a = storage.wait_and_claim_job(1_000).await?;

    assert_eq!(worker_a.job_id, job_id);

    // Worker A starts processing the job.
    assert!(
        storage
            .start_job(job_id.clone(), worker_a.token.clone())
            .await?
    );

    // --------------------------------------------------
    // Simulate Worker A crashing.
    //
    // We manually expire Worker A's lease by setting
    // its processing score to 0.
    // --------------------------------------------------

    let worker_a_member = format!("{}:{}", job_id.0, worker_a.token);

    let _: usize = connection
        .zadd("jobrail:queue:processing", &worker_a_member, 0_i64)
        .await?;

    // --------------------------------------------------
    // Recovery detects the expired lease.
    // --------------------------------------------------

    storage.recover_expired_jobs().await?;

    // The recovered job must be Waiting again.
    let recovered = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(
        recovered.state,
        JobState::Waiting,
        "recovered job should return to Waiting"
    );

    // --------------------------------------------------
    // Worker B starts after Worker A crashed.
    // --------------------------------------------------

    let executions = Arc::new(AtomicUsize::new(0));

    let handler = Arc::new(SuccessHandler {
        executions: Arc::clone(&executions),
    });

    let mut worker_b_storage = RedisStorage::new().await?;

    // Worker B should pick up the recovered job and process it.
    Worker::run_once(2, &mut worker_b_storage, handler).await?;

    // --------------------------------------------------
    // Verify final state.
    // --------------------------------------------------

    let final_job = worker_b_storage
        .get_job(job_id)
        .await?
        .expect("job should exist");

    assert_eq!(
        final_job.state,
        JobState::Completed,
        "recovered job should eventually complete"
    );

    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "recovered job should execute exactly once"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn concurrent_workers_process_jobs_without_duplicates() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:delayed")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await?;

    let mut producer = RedisStorage::new().await?;

    const JOB_COUNT: usize = 50;
    const WORKER_COUNT: usize = 10;
    const JOBS_PER_WORKER: usize = 5;

    for index in 0..JOB_COUNT {
        let job = Job::new(
            format!("stress-job-{index}"),
            serde_json::json!({
                "index": index
            }),
            JobOptions::default(),
        );

        let job_id = job.id.clone();

        producer.save_job(&job).await?;
        producer.enqueue(job_id).await?;
    }

    let executions = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for worker_id in 1..=WORKER_COUNT {
        let executions = Arc::clone(&executions);

        let handle = tokio::spawn(async move {
            let mut storage = RedisStorage::new().await?;

            let handler = Arc::new(SuccessHandler { executions });

            for _ in 0..JOBS_PER_WORKER {
                Worker::run_once(worker_id, &mut storage, Arc::clone(&handler)).await?;
            }

            Ok::<(), redis::RedisError>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("worker task panicked")?;
    }

    assert_eq!(
        executions.load(Ordering::SeqCst),
        JOB_COUNT,
        "every job must execute exactly once"
    );

    Ok(())
}

struct PanicHandler;

impl JobHandler for PanicHandler {
    fn execute(&self, _payload: Value) -> Result<(), String> {
        panic!("intentional test panic");
    }
}

#[tokio::test]
#[serial]
async fn worker_survives_handler_panic() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut connection = client.get_multiplexed_async_connection().await?;

    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:delayed")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await?;

    let mut storage = RedisStorage::new().await?;

    let job = Job::new(
        "panic-job".to_string(),
        serde_json::json!({
            "panic": true
        }),
        JobOptions {
            max_attempts: 1,
            ..Default::default()
        },
    );

    let job_id = job.id.clone();

    storage.save_job(&job).await?;
    storage.enqueue(job_id.clone()).await?;

    let handler = Arc::new(PanicHandler);

    Worker::run_once(1, &mut storage, handler).await?;

    let final_job = storage
        .get_job(job_id.clone())
        .await?
        .expect("job should exist");

    assert_eq!(
        final_job.state,
        JobState::Failed,
        "panic should result in terminal failure when max attempts are exhausted"
    );

    let waiting: Vec<String> = connection.lrange("jobrail:queue:waiting", 0, -1).await?;

    assert!(
        !waiting.contains(&job_id.0.to_string()),
        "failed job must not remain in waiting"
    );

    Ok(())
}
