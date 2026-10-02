use jobrail_core::job::{Job, JobOptions};
use jobrail_redis::{IdempotencyState, RedisStorage};
use std::sync::Arc;
use tokio::sync::Barrier;

#[tokio::test]
async fn concurrent_same_idempotency_key_only_one_worker_claims() -> redis::RedisResult<()> {
    const WORKER_COUNT: usize = 20;

    let idempotency_key = "race-test-key".to_string();

    // Create 20 different jobs that all represent the same logical operation.
    //
    // This models multiple requests creating duplicate jobs with the
    // same idempotency key before workers process them.
    let mut jobs = Vec::with_capacity(WORKER_COUNT);

    for index in 0..WORKER_COUNT {
        let job = Job::new(
            format!("idempotency-race-{index}"),
            serde_json::json!({
                "email": "user@example.com",
                "request": index,
            }),
            JobOptions {
                idempotency_key: Some(idempotency_key.clone()),
                ..Default::default()
            },
        );

        jobs.push(job);
    }

    // Clean up any stale idempotency record from a previous test run.
    let cleanup_storage = RedisStorage::new().await?;

    let redis_key = format!("jobrail:idempotency:{idempotency_key}");

    {
        use redis::AsyncCommands;

        let client = redis::Client::open("redis://127.0.0.1/")?;
        let mut connection = client.get_multiplexed_async_connection().await?;

        let _: () = connection.del(&redis_key).await?;
    }

    drop(cleanup_storage);

    // All workers start the claim at approximately the same time.
    let barrier = Arc::new(Barrier::new(WORKER_COUNT));

    let mut handles = Vec::with_capacity(WORKER_COUNT);

    for job in jobs {
        let barrier = Arc::clone(&barrier);
        let idempotency_key = idempotency_key.clone();

        handles.push(tokio::spawn(async move {
            let mut storage = RedisStorage::new().await?;

            barrier.wait().await;

            // This is the important atomic operation.
            //
            // Multiple workers may call this simultaneously,
            // but Redis must allow only one to claim the key.
            let claimed = storage
                .claim_idempotency_key(idempotency_key, job.id.clone())
                .await?;

            Ok::<_, redis::RedisError>((job.id, claimed))
        }));
    }

    let mut successful_claims = 0;
    let mut winning_job_id = None;

    for handle in handles {
        let (job_id, claimed) = handle.await.map_err(|error| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "worker task failed",
                error.to_string(),
            ))
        })??;

        if claimed {
            successful_claims += 1;
            winning_job_id = Some(job_id);
        }
    }

    // Exactly ONE worker must win.
    assert_eq!(
        successful_claims, 1,
        "exactly one worker should claim the idempotency key"
    );

    let winning_job_id = winning_job_id.expect("one worker must win");

    // Verify Redis now reports the idempotency key as Processing.
    let mut storage = RedisStorage::new().await?;

    let state = storage
        .check_idempotency_key(idempotency_key.clone())
        .await?;

    assert_eq!(
        state,
        IdempotencyState::Processing,
        "claimed idempotency key should be Processing"
    );

    // Verify the Redis record points to the winning job.
    let record = storage
        .get_idempotency_key(idempotency_key.clone())
        .await?
        .expect("idempotency record should exist");

    assert_eq!(
        record.job_id, winning_job_id,
        "idempotency record should point to the winning job"
    );

    // Clean up.
    {
        use redis::AsyncCommands;

        let client = redis::Client::open("redis://127.0.0.1/")?;
        let mut connection = client.get_multiplexed_async_connection().await?;

        let _: () = connection.del(redis_key).await?;
    }

    Ok(())
}
