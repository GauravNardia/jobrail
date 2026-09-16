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

                job.transition_to(JobState::Failed).map_err(|err| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "invalid job state transition",
                        format!("{:?} -> {:?}", err.from, err.to),
                    ))
                })?;
            }
        }

        self.storage.save_job(&job).await?;

        println!("Job state after: {:?}", job.state);

        Ok(())
    }
}
