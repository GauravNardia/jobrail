use jobrail_core::job::JobState;
use jobrail_redis::RedisStorage;
use serde_json::Value;
use std::sync::Arc;

pub trait JobHandler: Send + Sync {
    fn execute(&self, payload: Value) -> Result<(), String>;
}

#[derive(Clone)]
pub struct Worker {
    storage: RedisStorage,
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

        let storage = RedisStorage::new().await?;

        Ok(Self {
            storage,
            concurrency,
        })
    }

    pub async fn run<H>(&self, handler: Arc<H>) -> redis::RedisResult<()>
    where
        H: JobHandler + 'static,
    {
        println!("Starting worker pool with {} workers", self.concurrency);

        let mut handles = Vec::new();

        for worker_id in 1..=self.concurrency {
            let worker = self.clone();
            let handler = Arc::clone(&handler);

            let handle = tokio::spawn(async move {
                println!("Worker {worker_id} started");

                loop {
                    match worker.run_once(worker_id, Arc::clone(&handler)).await {
                        Ok(true) => {}

                        Ok(false) => {
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }

                        Err(error) => {
                            eprintln!("Worker {worker_id} failed: {error}");

                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            if let Err(error) = handle.await {
                eprintln!("Worker task stopped: {error}");
            }
        }

        Ok(())
    }

    async fn run_once<H>(&self, worker_id: usize, handler: Arc<H>) -> redis::RedisResult<bool>
    where
        H: JobHandler + 'static,
    {
        let mut storage = self.storage.clone();

        let Some(job_id) = storage.claim_job().await? else {
            return Ok(false);
        };

        println!("Worker {worker_id} claimed job: {}", job_id.0);

        let Some(mut job) = storage.get_job(job_id.clone()).await? else {
            println!("Worker {worker_id}: job was not found: {}", job_id.0);

            storage.remove_from_active(job_id).await?;

            return Ok(true);
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

        println!("Worker {worker_id}: job state after: {:?}", job.state);

        Ok(true)
    }
}
