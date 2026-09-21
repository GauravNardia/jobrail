use jobrail_core::job::{JobAttempt, JobAttemptStatus, JobState};
use jobrail_redis::RedisStorage;
use serde_json::Value;
use std::sync::Arc;

pub trait JobHandler: Send + Sync {
    fn execute(&self, payload: Value) -> Result<(), String>;
}

#[derive(Clone)]
pub struct Worker {
    concurrency: usize,
}

impl Worker {
    pub async fn new(concurrency: usize) -> redis::RedisResult<Self> {
        if concurrency == 0 {
            return Err(redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "worker concurrency must be greater than zero",
            )));
        }

        Ok(Self { concurrency })
    }

    pub async fn run<H>(&self, handler: Arc<H>) -> redis::RedisResult<()>
    where
        H: JobHandler + 'static,
    {
        println!("Starting worker pool with {} workers", self.concurrency);

        let mut handles = Vec::new();

        for worker_id in 1..=self.concurrency {
            let handler = Arc::clone(&handler);

            let handle = tokio::spawn(async move {
                println!("Worker {worker_id} started");

                let mut storage = match RedisStorage::new().await {
                    Ok(storage) => storage,
                    Err(error) => {
                        eprintln!("Worker {worker_id} failed to connect to Redis: {error}");
                        return;
                    }
                };

                loop {
                    if let Err(error) =
                        Worker::run_once(worker_id, &mut storage, Arc::clone(&handler)).await
                    {
                        eprintln!("Worker {worker_id} failed: {error}");

                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.map_err(|error| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "worker task stopped",
                    error.to_string(),
                ))
            })?;
        }

        Ok(())
    }

    pub async fn run_once<H>(
        worker_id: usize,
        storage: &mut RedisStorage,
        handler: Arc<H>,
    ) -> redis::RedisResult<()>
    where
        H: JobHandler + 'static,
    {
        // ------------------------------------------------------------
        // 1. Claim a job and receive an ownership token.
        // ------------------------------------------------------------

        let lease = storage.wait_and_claim_job(30_000).await?;

        println!("Worker {worker_id} claimed job: {}", lease.job_id.0);

        // ------------------------------------------------------------
        // 2. Start heartbeat using the SAME token that claimed the job.
        // ------------------------------------------------------------

        let heartbeat_job_id = lease.job_id.clone();
        let heartbeat_token = lease.token.clone();
        let mut heartbeat_storage = storage.clone();

        let heartbeat_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                match heartbeat_storage
                    .renew_lease(heartbeat_job_id.clone(), heartbeat_token.clone(), 30_000)
                    .await
                {
                    Ok(true) => {
                        println!("Heartbeat: renewed lease for job {}", heartbeat_job_id.0);
                    }

                    Ok(false) => {
                        eprintln!(
                            "Heartbeat: lease is no longer owned for job {}",
                            heartbeat_job_id.0
                        );

                        break;
                    }

                    Err(error) => {
                        eprintln!("Failed to renew job lease: {error}");

                        break;
                    }
                }
            }
        });

        // ------------------------------------------------------------
        // 3. Load the job.
        // ------------------------------------------------------------

        let Some(mut job) = storage.get_job(lease.job_id.clone()).await? else {
            println!("Worker {worker_id}: job was not found: {}", lease.job_id.0);

            storage.remove_from_active(lease.job_id.clone()).await?;

            storage
                .remove_from_processing(lease.job_id.clone(), lease.token.clone())
                .await?;

            heartbeat_handle.abort();

            return Ok(());
        };

        println!("Worker {worker_id}: job state before: {:?}", job.state);

        // ------------------------------------------------------------
        // 4. Atomically start the job.
        // ------------------------------------------------------------

        let started = storage
            .start_job(job.id.clone(), lease.token.clone())
            .await?;

        if !started {
            println!(
                "Worker {worker_id}: lost ownership of job {} before starting",
                job.id.0
            );

            heartbeat_handle.abort();

            return Ok(());
        }

        // ------------------------------------------------------------
        // 5. Check idempotency BEFORE creating an attempt.
        //
        // A duplicate idempotency job does not actually execute the
        // handler, so it should NOT create an attempt record.
        // ------------------------------------------------------------

        if let Some(idempotency_key) = job.idempotency_key.clone() {
            let idempotency_state = storage
                .check_idempotency_key(idempotency_key.clone())
                .await?;

            match idempotency_state {
                jobrail_redis::IdempotencyState::New => {
                    let claimed = storage
                        .claim_idempotency_key(idempotency_key, job.id.clone())
                        .await?;

                    if !claimed {
                        println!(
                            "Worker {worker_id}: idempotency key was claimed by another worker"
                        );

                        heartbeat_handle.abort();

                        return Ok(());
                    }

                    println!("Worker {worker_id}: idempotency key reserved");
                }

                jobrail_redis::IdempotencyState::Processing => {
                    println!(
                        "Worker {worker_id}: idempotency key is already processing; skipping execution"
                    );

                    let returned_to_waiting = storage
                        .fail_job(job.id.clone(), lease.token.clone(), JobState::Waiting, 0)
                        .await?;

                    heartbeat_handle.abort();

                    if returned_to_waiting {
                        job.state = JobState::Waiting;

                        println!("Worker {worker_id}: duplicate job returned to Waiting");
                    } else {
                        eprintln!(
                            "Worker {worker_id}: lost ownership of duplicate job {}",
                            job.id.0
                        );
                    }

                    return Ok(());
                }

                jobrail_redis::IdempotencyState::Completed => {
                    println!(
                        "Worker {worker_id}: idempotency key is already completed; skipping execution"
                    );

                    let completed = storage
                        .complete_job(job.id.clone(), lease.token.clone())
                        .await?;

                    heartbeat_handle.abort();

                    if completed {
                        job.state = JobState::Completed;

                        println!("Worker {worker_id}: duplicate job marked as Completed");
                    } else {
                        eprintln!(
                            "Worker {worker_id}: lost ownership of duplicate job {}",
                            job.id.0
                        );
                    }

                    return Ok(());
                }
            }
        }

        // ------------------------------------------------------------
        // 6. THIS IS A REAL EXECUTION.
        //
        // Now create the attempt.
        // ------------------------------------------------------------

        job.attempts_started += 1;
        job.attempts_made += 1;
        job.state = JobState::Active;

        let attempt_number = job.attempts_started;

        let attempt = JobAttempt {
            attempt: attempt_number,
            started_at: current_timestamp_ms(),
            finished_at: None,
            status: JobAttemptStatus::Running,
            error: None,
        };

        storage.start_job_attempt(job.id.clone(), &attempt).await?;

        println!(
            "Worker {worker_id}: started attempt {} for job {}",
            attempt_number, job.id.0
        );

        // ------------------------------------------------------------
        // 7. Execute the actual handler.
        // ------------------------------------------------------------

        let payload = job.payload.clone();
        let handler = Arc::clone(&handler);

        let result = tokio::task::spawn_blocking(move || handler.execute(payload))
            .await
            .map_err(|err| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "job handler task panicked",
                    err.to_string(),
                ))
            })?;

        // ------------------------------------------------------------
        // 8. Job execution finished.
        // Heartbeat is no longer needed.
        // ------------------------------------------------------------

        heartbeat_handle.abort();

        // ------------------------------------------------------------
        // 9. Handle success / failure.
        // ------------------------------------------------------------

        match result {
            Ok(()) => {
                println!("Worker {worker_id}: job executed successfully");

                // Mark the SAME attempt as completed.
                storage
                    .finish_job_attempt(
                        job.id.clone(),
                        attempt_number,
                        JobAttemptStatus::Completed,
                        None,
                    )
                    .await?;

                println!(
                    "Worker {worker_id}: attempt {} marked Completed",
                    attempt_number
                );

                // Now complete the actual job.
                let completed = match job.idempotency_key.clone() {
                    Some(idempotency_key) => {
                        storage
                            .complete_job_with_idempotency(
                                job.id.clone(),
                                lease.token.clone(),
                                idempotency_key,
                            )
                            .await?
                    }

                    None => {
                        storage
                            .complete_job(job.id.clone(), lease.token.clone())
                            .await?
                    }
                };

                if completed {
                    job.state = JobState::Completed;

                    println!("Worker {worker_id}: job completed successfully");
                } else {
                    // The lease expired or another worker owns
                    // the job now. We MUST NOT save our local
                    // job state because we are no longer the owner.
                    eprintln!(
                        "Worker {worker_id}: lost ownership of job {} before completion",
                        job.id.0
                    );
                }
            }

            Err(error) => {
                println!("Worker {worker_id}: job execution failed: {error}");

                // Mark the SAME attempt as failed.
                storage
                    .finish_job_attempt(
                        job.id.clone(),
                        attempt_number,
                        JobAttemptStatus::Failed,
                        Some(error.clone()),
                    )
                    .await?;

                println!(
                    "Worker {worker_id}: attempt {} marked Failed",
                    attempt_number
                );

                // ----------------------------------------------------
                // 10. Decide whether to retry the job.
                // ----------------------------------------------------

                if job.attempts_made >= job.max_attempts {
                    println!("Worker {worker_id}: maximum attempts reached");

                    let failed = storage
                        .fail_job(job.id.clone(), lease.token.clone(), JobState::Failed, 0)
                        .await?;

                    if failed {
                        job.state = JobState::Failed;

                        println!("Worker {worker_id}: job marked as Failed");
                    } else {
                        eprintln!(
                            "Worker {worker_id}: lost ownership of job {} before marking it Failed",
                            job.id.0
                        );
                    }
                } else {
                    println!(
                        "Worker {worker_id}: retrying job. Attempt {}/{}",
                        job.attempts_made, job.max_attempts
                    );

                    let delay_ms = job.retry_delay_ms();

                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|err| {
                            redis::RedisError::from((
                                redis::ErrorKind::UnexpectedReturnType,
                                "system clock is before UNIX epoch",
                                err.to_string(),
                            ))
                        })?;

                    let retry_at = now.as_millis() as u64 + delay_ms;

                    println!("Worker {worker_id}: retrying in {} ms", delay_ms);

                    let delayed = storage
                        .fail_job(
                            job.id.clone(),
                            lease.token.clone(),
                            JobState::Delayed,
                            retry_at,
                        )
                        .await?;

                    if delayed {
                        job.state = JobState::Delayed;

                        println!("Worker {worker_id}: job scheduled for retry");
                    } else {
                        eprintln!(
                            "Worker {worker_id}: lost ownership of job {} before scheduling retry",
                            job.id.0
                        );
                    }
                }
            }
        }

        println!("Worker {worker_id}: job state after: {:?}", job.state);

        Ok(())
    }
}

// ------------------------------------------------------------
// Timestamp helper
// ------------------------------------------------------------

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_millis() as u64
}

// ------------------------------------------------------------
// Recovery worker
// ------------------------------------------------------------

pub async fn run_recovery() -> redis::RedisResult<()> {
    let mut storage = RedisStorage::new().await?;

    println!("Recovery worker started");

    loop {
        if let Err(error) = storage.recover_expired_jobs().await {
            eprintln!("Recovery worker failed: {error}");
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

pub async fn run_scheduled_scheduler() -> redis::RedisResult<()> {
    let mut storage = RedisStorage::new().await?;

    println!("Scheduled job scheduler started");

    loop {
        match run_scheduled_scheduler_once(&mut storage).await {
            Ok(promoted) => {
                if promoted > 0 {
                    println!("Promoted {} scheduled job(s)", promoted);
                }
            }

            Err(error) => {
                eprintln!("Scheduled job scheduler failed: {error}");
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

pub async fn run_scheduled_scheduler_once(storage: &mut RedisStorage) -> redis::RedisResult<usize> {
    let promoted = storage.promote_scheduled_jobs().await?;

    Ok(promoted.len())
}

// ------------------------------------------------------------
// Repeatable job scheduler
// ------------------------------------------------------------

pub async fn run_repeatable_scheduler() -> redis::RedisResult<()> {
    let mut storage = RedisStorage::new().await?;

    println!("Repeatable scheduler started");

    loop {
        match run_repeatable_scheduler_once(&mut storage).await {
            Ok(created) => {
                if created > 0 {
                    println!("Created {} repeatable execution(s)", created);
                }
            }

            Err(error) => {
                eprintln!("Repeatable scheduler failed: {error}");
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

pub async fn run_repeatable_scheduler_once(
    storage: &mut RedisStorage,
) -> redis::RedisResult<usize> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "system clock is before UNIX epoch",
                error.to_string(),
            ))
        })?
        .as_millis() as u64;

    let repeatable_job_ids = storage.get_due_repeatable_job_ids(now_ms).await?;

    let mut created = 0;

    for repeatable_job_id in repeatable_job_ids {
        let id = uuid::Uuid::parse_str(&repeatable_job_id).map_err(|error| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "invalid repeatable job id",
                error.to_string(),
            ))
        })?;

        let repeatable_id = jobrail_core::repeat::RepeatableJobId(id);

        let Some(repeatable_job) = storage.get_repeatable_job(repeatable_id).await? else {
            continue;
        };

        if !repeatable_job.enabled {
            continue;
        }

        let Some(run_at) = repeatable_job.next_run_at else {
            continue;
        };

        if run_at > now_ms {
            continue;
        }

        let result = storage
            .create_repeatable_execution(&repeatable_job, run_at)
            .await?;

        if result.is_some() {
            created += 1;
        }
    }

    Ok(created)
}
