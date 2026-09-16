use jobrail_core::job::JobState;
use jobrail_redis::RedisStorage;
use serde_json::Value;

pub trait JobHandler: Send + Sync {
    fn execute(&self, payload: Value) -> Result<(), String>;
}

pub struct Worker {
    storage: RedisStorage,
}

impl Worker {
    pub async fn new() -> redis::RedisResult<Self> {
        let storage = RedisStorage::new().await?;
        Ok(Self { storage })
    }

    pub async fn run<H>(&mut self, handler: &H) -> redis::RedisResult<()>
    where
        H: JobHandler,
    {
        loop {
            self.storage.promote_delayed_jobs().await?;

            self.run_once(handler).await?;

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    pub async fn run_once<H>(&mut self, handler: &H) -> redis::RedisResult<()>
    where
        H: JobHandler,
    {
        let Some(job_id) = self.storage.claim_job().await? else {
            println!("No jobs available");

            return Ok(());
        };

        println!("Worker claimed job: {}", job_id.0);

        let Some(mut job) = self.storage.get_job(job_id.clone()).await? else {
            println!("Job was not found: {}", job_id.0);

            return Ok(());
        };

        println!("Job state before: {:?}", job.state);

        job.attempts_started += 1;
        job.attempts_made += 1;

        job.transition_to(JobState::Active).map_err(|err| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "invalid job state transition",
                format!("{:?} -> {:?}", err.from, err.to),
            ))
        })?;

        match handler.execute(job.payload.clone()) {
            Ok(()) => {
                println!("Job executed successfully");

                job.transition_to(JobState::Completed).map_err(|err| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "invalid job state transition",
                        format!("{:?} -> {:?}", err.from, err.to),
                    ))
                })?
            }

            Err(error) => {
                println!("Job execution failed: {error}");

                if job.attempts_made >= job.max_attempts {
                    println!("Maximum attempts reached");

                    job.transition_to(JobState::Failed).map_err(|err| {
                        redis::RedisError::from((
                            redis::ErrorKind::UnexpectedReturnType,
                            "invalid job state transition",
                            format!("{:?} -> {:?}", err.from, err.to),
                        ))
                    })?;
                } else {
                    println!(
                        "Retrying job. Attempt {}/{}",
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
                    println!("Retrying in {} ms", delay_ms);
                    // schedule for retry and add to delayed ZSET
                    self.storage
                        .schedule_retry(job.id.clone(), delay_ms)
                        .await?;

                    // remove from active state
                    self.storage.remove_from_active(job.id.clone()).await?;
                }
            }
        }

        self.storage.save_job(&job).await?;

        println!("Job state after: {:?}", job.state);

        Ok(())
    }
}
