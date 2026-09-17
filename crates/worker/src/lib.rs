use jobrail_core::job::JobState;
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

    async fn run_once<H>(
        worker_id: usize,
        storage: &mut RedisStorage,
        handler: Arc<H>,
    ) -> redis::RedisResult<()>
    where
        H: JobHandler + 'static,
    {
        // Claim the job and receive ownership token.
        let lease = storage.wait_and_claim_job(30_000).await?;

        println!("Worker {worker_id} claimed job: {}", lease.job_id.0);

        // The heartbeat must use the SAME token that claimed the job.
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

        job.attempts_started += 1;
        job.attempts_made += 1;

        job.transition_to(JobState::Active).map_err(|err| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "invalid job state transition",
                format!("{:?} -> {:?}", err.from, err.to),
            ))
        })?;

        storage.save_job(&job).await?;

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

        // Job execution finished, so heartbeat is no longer needed.
        heartbeat_handle.abort();

        match result {
            Ok(()) => {
                println!("Worker {worker_id}: job executed successfully");

                job.transition_to(JobState::Completed).map_err(|err| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "invalid job state transition",
                        format!("{:?} -> {:?}", err.from, err.to),
                    ))
                })?;

                storage.remove_from_active(job.id.clone()).await?;
            }

            Err(error) => {
                println!("Worker {worker_id}: job execution failed: {error}");

                if job.attempts_made >= job.max_attempts {
                    println!("Worker {worker_id}: maximum attempts reached");

                    job.transition_to(JobState::Failed).map_err(|err| {
                        redis::RedisError::from((
                            redis::ErrorKind::UnexpectedReturnType,
                            "invalid job state transition",
                            format!("{:?} -> {:?}", err.from, err.to),
                        ))
                    })?;

                    storage.remove_from_active(job.id.clone()).await?;
                } else {
                    println!(
                        "Worker {worker_id}: retrying job. Attempt {}/{}",
                        job.attempts_made, job.max_attempts
                    );

                    job.transition_to(JobState::Delayed).map_err(|err| {
                        redis::RedisError::from((
                            redis::ErrorKind::UnexpectedReturnType,
                            "invalid job state transition",
                            format!("{:?} -> {:?}", err.from, err.to),
                        ))
                    })?;

                    let delay_ms = job.retry_delay_ms();

                    println!("Worker {worker_id}: retrying in {} ms", delay_ms);

                    storage.schedule_retry(job.id.clone(), delay_ms).await?;

                    storage.remove_from_active(job.id.clone()).await?;
                }
            }
        }

        storage.save_job(&job).await?;

        // Only remove the processing lease belonging to THIS worker.
        storage
            .remove_from_processing(job.id.clone(), lease.token.clone())
            .await?;

        println!("Worker {worker_id}: job state after: {:?}", job.state);

        Ok(())
    }
}

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
