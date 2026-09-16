use jobrail_core::job::{Job, JobId};
use redis::{AsyncCommands, Script};
use std::time::{SystemTime, UNIX_EPOCH};

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

    pub async fn schedule_retry(&mut self, job_id: JobId, delay_ms: u64) -> redis::RedisResult<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "system clock is before UNIX epoch",
                    err.to_string(),
                ))
            })?;

        let retry_at = now.as_millis() as u64 + delay_ms;

        let _: () = self
            .connection
            .zadd("jobrail:queue:delayed", job_id.0.to_string(), retry_at)
            .await?;

        Ok(())
    }

    pub async fn remove_from_active(&mut self, job_id: JobId) -> redis::RedisResult<()> {
        let _: () = self
            .connection
            .lrem("jobrail:queue:active", 0, job_id.0.to_string())
            .await?;

        Ok(())
    }

    pub async fn promote_delayed_jobs(&mut self) -> redis::RedisResult<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "system clock is before UNIX epoch",
                    err.to_string(),
                ))
            })?;

        let now_ms = now.as_millis() as u64;

        let script = Script::new(include_str!("scripts/promote_delayed_jobs.lua"));

        let job_ids: Vec<String> = script
            .key("jobrail:queue:delayed")
            .key("jobrail:queue:waiting")
            .arg(now_ms)
            .invoke_async(&mut self.connection)
            .await?;

        for job_id in job_ids {
            let uuid = job_id.parse::<uuid::Uuid>().map_err(|err| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "invalid job id in delayed queue",
                    err.to_string(),
                ))
            })?;

            let job_id = JobId(uuid);

            let Some(mut job) = self.get_job(job_id.clone()).await? else {
                println!("Delayed job was not found: {:?}", job_id);
                continue;
            };

            job.transition_to(jobrail_core::job::JobState::Waiting)
                .map_err(|err| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "invalid delayed job state transition",
                        format!("{:?} -> {:?}", err.from, err.to),
                    ))
                })?;

            self.save_job(&job).await?;

            println!("Promoted delayed job: {}", job.id.0);
        }

        Ok(())
    }
}
