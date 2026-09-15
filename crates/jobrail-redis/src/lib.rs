use jobrail_core::job::{Job, JobId};
use redis::{AsyncCommands, Script};

pub struct RedisStorage {
    connection: redis::aio::MultiplexedConnection,
}

impl RedisStorage {
    pub async fn new() -> redis::RedisResult<Self> {
        let client = redis::Client::open("redis://127.0.0.1/")?;
        let connection = client.get_multiplexed_async_connection().await?;

        Ok(Self { connection })
    }

    pub async fn save_job(&mut self, job: &Job) -> redis::RedisResult<()> {
        let key = format!("jobrail:job:{}", job.id.0);

        let value = serde_json::to_string(job).map_err(|err| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to serialize job",
                err.to_string(),
            ))
        })?;

        let _: () = self.connection.set(key, value).await?;

        Ok(())
    }

    pub async fn get_job(
        &mut self,
        job_id: jobrail_core::job::JobId,
    ) -> redis::RedisResult<Option<Job>> {
        let key = format!("jobrail:job:{}", job_id.0);
        let value: Option<String> = self.connection.get(key).await?;

        match value {
            Some(data) => {
                let job = serde_json::from_str(&data).map_err(|err| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "failed to deserialize job",
                        err.to_string(),
                    ))
                })?;

                Ok(Some(job))
            }
            None => Ok(None),
        }
    }

    pub async fn enqueue(&mut self, job_id: JobId) -> redis::RedisResult<()> {
        let _: () = self
            .connection
            .rpush("jobrail:queue:waiting", job_id.0.to_string())
            .await?;

        Ok(())
    }

    pub async fn dequeue(&mut self) -> redis::RedisResult<Option<JobId>> {
        let job_id: Option<String> = self.connection.lpop("jobrail:queue:waiting", None).await?;

        match job_id {
            Some(job_id) => {
                let job_id = job_id.parse::<uuid::Uuid>().map_err(|err| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "invalid job id in waiting queue",
                        err.to_string(),
                    ))
                })?;

                Ok(Some(JobId(job_id)))
            }
            None => Ok(None),
        }
    }

    pub async fn claim_job(&mut self) -> redis::RedisResult<Option<JobId>> {
        let script = Script::new(include_str!("scripts/claim_job.lua"));

        let result: Option<String> = script
            .key("jobrail:queue:waiting")
            .key("jobrail:queue:active")
            .invoke_async(&mut self.connection)
            .await?;

        match result {
            Some(job_id) => {
                let uuid = job_id.parse::<uuid::Uuid>().map_err(|err| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "invalid job id returned by claim script",
                        err.to_string(),
                    ))
                })?;

                Ok(Some(JobId(uuid)))
            }
            None => Ok(None),
        }
    }
}
