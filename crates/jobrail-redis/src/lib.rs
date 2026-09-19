use jobrail_core::job::{Job, JobId, JobState};
use jobrail_core::{job::JobOptions, repeat::RepeatableJob};
use redis::{AsyncCommands, Script};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct JobLease {
    pub job_id: JobId,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    pub job_id: JobId,
    pub status: IdempotencyStatus,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum IdempotencyStatus {
    Processing,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyState {
    New,
    Processing,
    Completed,
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

    pub async fn start_job(&mut self, job_id: JobId, token: String) -> redis::RedisResult<bool> {
        let lease_member = format!("{}:{}", job_id.0, token);

        let job_key = format!("jobrail:job:{}", job_id.0);

        let script = Script::new(include_str!("scripts/start_job.lua"));

        let result: i32 = script
            .key("jobrail:queue:processing")
            .key(job_key)
            .arg(lease_member)
            .invoke_async(&mut self.connection)
            .await?;

        Ok(result == 1)
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

    pub async fn claim_idempotency_key(
        &mut self,
        idempotency_key: String,
        job_id: JobId,
    ) -> redis::RedisResult<bool> {
        let redis_key = format!("jobrail:idempotency:{}", idempotency_key);

        let script = Script::new(include_str!("scripts/claim_idempotency.lua"));

        let claimed: i32 = script
            .key(redis_key)
            .arg(job_id.0.to_string())
            .invoke_async(&mut self.connection)
            .await?;

        Ok(claimed == 1)
    }

    pub async fn complete_idempotency_key(
        &mut self,
        idempotency_key: String,
        job_id: JobId,
    ) -> redis::RedisResult<bool> {
        let redis_key = format!("jobrail:idempotency:{}", idempotency_key);

        let script = Script::new(include_str!("scripts/complete_idempotency.lua"));

        let completed: i32 = script
            .key(redis_key)
            .arg(job_id.0.to_string())
            .invoke_async(&mut self.connection)
            .await?;

        Ok(completed == 1)
    }

    pub async fn get_idempotency_key(
        &mut self,
        idempotency_key: String,
    ) -> redis::RedisResult<Option<IdempotencyRecord>> {
        let redis_key = format!("jobrail:idempotency:{}", idempotency_key);

        let data: Option<String> = self.connection.get(redis_key).await?;

        match data {
            Some(data) => {
                let record = serde_json::from_str::<IdempotencyRecord>(&data).map_err(|err| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "failed to deserialize idempotency record",
                        err.to_string(),
                    ))
                })?;

                Ok(Some(record))
            }

            None => Ok(None),
        }
    }

    pub async fn check_idempotency_key(
        &mut self,
        idempotency_key: String,
    ) -> redis::RedisResult<IdempotencyState> {
        let record = self.get_idempotency_key(idempotency_key).await?;

        match record {
            None => Ok(IdempotencyState::New),

            Some(record) => match record.status {
                IdempotencyStatus::Processing => Ok(IdempotencyState::Processing),

                IdempotencyStatus::Completed => Ok(IdempotencyState::Completed),
            },
        }
    }

    pub async fn complete_job_with_idempotency(
        &mut self,
        job_id: JobId,
        token: String,
        idempotency_key: String,
    ) -> redis::RedisResult<bool> {
        let lease_member = format!("{}:{}", job_id.0, token);

        let job_key = format!("jobrail:job:{}", job_id.0);

        let idempotency_redis_key = format!("jobrail:idempotency:{}", idempotency_key);

        let script = Script::new(include_str!("scripts/complete_job_with_idempotency.lua"));

        let completed: i32 = script
            // KEYS[1] = processing leases
            .key("jobrail:queue:processing")
            // KEYS[2] = job
            .key(job_key)
            // KEYS[3] = active queue
            .key("jobrail:queue:active")
            // KEYS[4] = idempotency record
            .key(idempotency_redis_key)
            // ARGV[1] = lease member
            .arg(lease_member)
            // ARGV[2] = idempotency key
            .arg(idempotency_key)
            .invoke_async(&mut self.connection)
            .await?;

        Ok(completed == 1)
    }

    pub async fn schedule_job(&mut self, job: Job) -> redis::RedisResult<()> {
        let run_at = job.run_at.ok_or_else(|| {
            redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "scheduled job requires run_at",
            ))
        })?;

        let job_id = job.id.0.to_string();

        let job_key = format!("jobrail:job:{}", job.id.0);

        let job_data = serde_json::to_string(&job).map_err(|error| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to serialize scheduled job",
                error.to_string(),
            ))
        })?;

        let script = Script::new(include_str!("scripts/schedule_job.lua"));

        let _: i32 = script
            .key(job_key)
            .key("jobrail:queue:scheduled")
            .arg(job_id)
            .arg(job_data)
            .arg(run_at)
            .invoke_async(&mut self.connection)
            .await?;

        Ok(())
    }

    pub async fn promote_scheduled_jobs(&mut self) -> redis::RedisResult<Vec<JobId>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "system clock is before UNIX epoch",
                    error.to_string(),
                ))
            })?;

        let now_ms = now.as_millis() as u64;

        let script = Script::new(include_str!("scripts/promote_scheduled_jobs.lua"));

        let job_ids: Vec<String> = script
            .key("jobrail:queue:scheduled")
            .key("jobrail:queue:waiting")
            .arg(now_ms)
            .invoke_async(&mut self.connection)
            .await?;

        job_ids
            .into_iter()
            .map(|id| {
                uuid::Uuid::parse_str(&id).map(JobId).map_err(|error| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "invalid scheduled job id",
                        error.to_string(),
                    ))
                })
            })
            .collect()
    }
    pub async fn save_repeatable_job(
        &mut self,
        repeatable_job: RepeatableJob,
    ) -> redis::RedisResult<()> {
        let job_id = repeatable_job.id.0.to_string();

        let redis_key = format!("jobrail:repeat:{}", job_id);

        let data = serde_json::to_string(&repeatable_job).map_err(|error| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to serialize repeatable job",
                error.to_string(),
            ))
        })?;

        let _: () = self.connection.set(redis_key, data).await?;

        let _: () = self
            .connection
            .sadd("jobrail:queue:repeatable", job_id)
            .await?;

        Ok(())
    }

    pub async fn get_repeatable_job(
        &mut self,
        id: jobrail_core::repeat::RepeatableJobId,
    ) -> redis::RedisResult<Option<RepeatableJob>> {
        let redis_key = format!("jobrail:repeat:{}", id.0);

        let data: Option<String> = self.connection.get(redis_key).await?;

        match data {
            Some(data) => {
                let job = serde_json::from_str::<RepeatableJob>(&data).map_err(|error| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "failed to deserialize repeatable job",
                        error.to_string(),
                    ))
                })?;

                Ok(Some(job))
            }

            None => Ok(None),
        }
    }

    pub async fn list_repeatable_jobs(&mut self) -> redis::RedisResult<Vec<RepeatableJob>> {
        let ids: Vec<String> = self.connection.smembers("jobrail:queue:repeatable").await?;

        let mut jobs = Vec::new();

        for id in ids {
            let Some(job) = self
                .get_repeatable_job(jobrail_core::repeat::RepeatableJobId(
                    uuid::Uuid::parse_str(&id).map_err(|error| {
                        redis::RedisError::from((
                            redis::ErrorKind::UnexpectedReturnType,
                            "invalid repeatable job id",
                            error.to_string(),
                        ))
                    })?,
                ))
                .await?
            else {
                continue;
            };

            jobs.push(job);
        }

        Ok(jobs)
    }

    pub async fn schedule_repeatable_execution(
        &mut self,
        repeatable_job: &RepeatableJob,
        run_at: u64,
    ) -> redis::RedisResult<Job> {
        let options = JobOptions {
            priority: 0,
            max_attempts: 3,
            delay_ms: 0,
            run_at: Some(run_at),
            idempotency_key: None,
        };

        let job = Job::new(
            repeatable_job.name.clone(),
            repeatable_job.payload.clone(),
            options,
        );

        self.schedule_job(job.clone()).await?;

        Ok(job)
    }
}
