use jobrail_core::job::{Job, JobId, JobState};
use redis::{AsyncCommands, Script};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct JobLease {
    pub job_id: JobId,
    pub token: String,
}

#[derive(Clone)]
pub struct RedisStorage {
    connection: redis::aio::MultiplexedConnection,
}

impl RedisStorage {
    pub async fn new() -> redis::RedisResult<Self> {
        let client = redis::Client::open("redis://127.0.0.1/")?;

        let config = redis::AsyncConnectionConfig::new().set_response_timeout(None);

        let connection = client
            .get_multiplexed_async_connection_with_config(&config)
            .await?;

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

    pub async fn get_job(&mut self, job_id: JobId) -> redis::RedisResult<Option<Job>> {
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
            .lpush("jobrail:queue:waiting", job_id.0.to_string())
            .await?;

        Ok(())
    }

    pub async fn dequeue(&mut self) -> redis::RedisResult<Option<JobId>> {
        let job_id: Option<String> = self.connection.lpop("jobrail:queue:waiting", None).await?;

        match job_id {
            Some(job_id) => {
                let uuid = job_id.parse::<uuid::Uuid>().map_err(|err| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "invalid job id in waiting queue",
                        err.to_string(),
                    ))
                })?;

                Ok(Some(JobId(uuid)))
            }

            None => Ok(None),
        }
    }

    // Unused -> Will delete later

    // pub async fn claim_job(&mut self) -> redis::RedisResult<Option<JobId>> {
    //     let script = Script::new(include_str!("scripts/claim_job.lua"));

    //     let result: Option<String> = script
    //         .key("jobrail:queue:waiting")
    //         .key("jobrail:queue:active")
    //         .invoke_async(&mut self.connection)
    //         .await?;

    //     match result {
    //         Some(job_id) => {
    //             let uuid = job_id.parse::<uuid::Uuid>().map_err(|err| {
    //                 redis::RedisError::from((
    //                     redis::ErrorKind::UnexpectedReturnType,
    //                     "invalid job id returned by claim script",
    //                     err.to_string(),
    //                 ))
    //             })?;

    //             Ok(Some(JobId(uuid)))
    //         }

    //         None => Ok(None),
    //     }
    // }

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

        // The Lua script returns job IDs.
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

    pub async fn wait_and_claim_job(&mut self, lease_ms: u64) -> redis::RedisResult<JobLease> {
        loop {
            let token = uuid::Uuid::new_v4().to_string();

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|err| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "system clock is before UNIX epoch",
                        err.to_string(),
                    ))
                })?;

            let lease_until = now.as_millis() as u64 + lease_ms;

            let script = Script::new(include_str!("scripts/claim_job_with_lease.lua"));

            let result: Option<String> = script
                .key("jobrail:queue:waiting")
                .key("jobrail:queue:active")
                .key("jobrail:queue:processing")
                .arg(&token)
                .arg(lease_until)
                .invoke_async(&mut self.connection)
                .await?;

            let Some(job_id) = result else {
                // No jobs available.
                //
                // We sleep briefly instead of using BLMOVE because
                // the entire claim + lease operation now happens
                // atomically inside Redis.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                continue;
            };

            let uuid = job_id.parse::<uuid::Uuid>().map_err(|err| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "invalid job id returned by claim script",
                    err.to_string(),
                ))
            })?;

            return Ok(JobLease {
                job_id: JobId(uuid),
                token,
            });
        }
    }

    pub async fn recover_expired_jobs(&mut self) -> redis::RedisResult<()> {
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

        let script = Script::new(include_str!("scripts/recover_expired_jobs.lua"));

        let lease_members: Vec<String> = script
            .key("jobrail:queue:processing")
            .key("jobrail:queue:waiting")
            .arg(now_ms)
            .invoke_async(&mut self.connection)
            .await?;

        for lease_member in lease_members {
            println!("Recovered expired lease: {lease_member}");
        }

        Ok(())
    }

    pub async fn renew_lease(
        &mut self,
        job_id: JobId,
        token: String,
        lease_ms: u64,
    ) -> redis::RedisResult<bool> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "system clock is before UNIX epoch",
                    err.to_string(),
                ))
            })?;

        let lease_until = now.as_millis() as u64 + lease_ms;

        let lease_member = format!("{}:{}", job_id.0, token);

        let script = Script::new(include_str!("scripts/renew_lease.lua"));

        let renewed: i32 = script
            .key("jobrail:queue:processing")
            .arg(lease_member)
            .arg(lease_until)
            .invoke_async(&mut self.connection)
            .await?;

        Ok(renewed == 1)
    }

    pub async fn remove_from_processing(
        &mut self,
        job_id: JobId,
        token: String,
    ) -> redis::RedisResult<()> {
        let lease_member = format!("{}:{}", job_id.0, token);

        let _: () = self
            .connection
            .zrem("jobrail:queue:processing", lease_member)
            .await?;

        Ok(())
    }

    pub async fn complete_job(&mut self, job_id: JobId, token: String) -> redis::RedisResult<bool> {
        let lease_member = format!("{}:{}", job_id.0, token);

        let job_key = format!("jobrail:job:{}", job_id.0);

        let script = Script::new(include_str!("scripts/complete_job.lua"));

        let completed: i32 = script
            .key("jobrail:queue:processing")
            .key(job_key)
            .key("jobrail:queue:active")
            .arg(lease_member)
            .invoke_async(&mut self.connection)
            .await?;

        Ok(completed == 1)
    }

    pub async fn fail_job(
        &mut self,
        job_id: JobId,
        token: String,
        state: JobState,
        retry_at: u64,
    ) -> redis::RedisResult<bool> {
        let lease_member = format!("{}:{}", job_id.0, token);

        let job_key = format!("jobrail:job:{}", job_id.0);

        let state = serde_json::to_string(&state).map_err(|err| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to serialize job state",
                err.to_string(),
            ))
        })?;

        let script = Script::new(include_str!("scripts/fail_job.lua"));

        let result: i32 = script
            .key("jobrail:queue:processing")
            .key(job_key)
            .key("jobrail:queue:active")
            .key("jobrail:queue:delayed")
            .arg(lease_member)
            .arg(state.trim_matches('"'))
            .arg(retry_at)
            .invoke_async(&mut self.connection)
            .await?;

        Ok(result == 1)
    }
}
