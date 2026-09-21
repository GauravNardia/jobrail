use jobrail_core::job::{Job, JobAttempt, JobAttemptStatus, JobId, JobState};
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

#[derive(Debug, Clone)]
pub struct JobPage {
    pub jobs: Vec<Job>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
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
        let job_key = format!("jobrail:job:{}", job.id.0);

        let value = serde_json::to_string(job).map_err(|err| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to serialize job",
                err.to_string(),
            ))
        })?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "system clock is before UNIX epoch",
                    err.to_string(),
                ))
            })?;

        let now_ms = now.as_millis() as i64;

        let mut pipe = redis::pipe();

        pipe.atomic()
            .set(job_key, value)
            .zadd("jobrail:jobs:index", job.id.0.to_string(), now_ms);

        let _: () = pipe.query_async(&mut self.connection).await?;

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

    pub async fn list_jobs(
        &mut self,
        limit: usize,
        cursor: Option<String>,
        state: Option<JobState>,
    ) -> redis::RedisResult<JobPage> {
        let limit = limit.clamp(1, 100);

        // Fetch extra records so we can determine whether another page exists.
        let fetch_limit = limit + 1;

        let mut cursor_state = match cursor {
            Some(cursor) => Some(decode_cursor(&cursor)?),
            None => None,
        };

        let mut entries: Vec<(String, f64)> = Vec::new();

        while entries.len() < fetch_limit {
            let remaining = fetch_limit - entries.len();

            let batch: Vec<(String, f64)> = match cursor_state {
                None => {
                    redis::cmd("ZREVRANGE")
                        .arg("jobrail:jobs:index")
                        .arg(0)
                        .arg(remaining - 1)
                        .arg("WITHSCORES")
                        .query_async::<Vec<(String, f64)>>(&mut self.connection)
                        .await?
                }

                Some((cursor_score, ref cursor_member)) => {
                    let mut same_score_members: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                        .arg("jobrail:jobs:index")
                        .arg(cursor_score)
                        .arg(cursor_score)
                        .query_async::<Vec<String>>(&mut self.connection)
                        .await?;

                    same_score_members.reverse();

                    same_score_members.retain(|member| member < cursor_member);

                    let mut result: Vec<(String, f64)> = same_score_members
                        .into_iter()
                        .take(remaining)
                        .map(|member| (member, cursor_score))
                        .collect();

                    if result.len() < remaining {
                        let still_needed = remaining - result.len();

                        let lower_score_entries: Vec<(String, f64)> =
                            redis::cmd("ZREVRANGEBYSCORE")
                                .arg("jobrail:jobs:index")
                                .arg(format!("({cursor_score}"))
                                .arg("-inf")
                                .arg("WITHSCORES")
                                .arg("LIMIT")
                                .arg(0)
                                .arg(still_needed)
                                .query_async::<Vec<(String, f64)>>(&mut self.connection)
                                .await?;

                        result.extend(lower_score_entries);
                    }

                    result
                }
            };

            if batch.is_empty() {
                break;
            }

            for entry in &batch {
                let (job_id, score) = entry;

                let uuid = job_id.parse::<uuid::Uuid>().map_err(|err| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "invalid job id in job index",
                        err.to_string(),
                    ))
                })?;

                let job_id = JobId(uuid);

                if let Some(job) = self.get_job(job_id.clone()).await? {
                    if state.is_none() || state == Some(job.state) {
                        entries.push((job_id.0.to_string(), *score));
                    }
                }

                if entries.len() >= fetch_limit {
                    break;
                }
            }

            if let Some(last) = batch.last() {
                cursor_state = Some((last.1, last.0.clone()));
            }

            if batch.len() < remaining {
                break;
            }
        }

        let has_more = entries.len() > limit;

        let entries = entries.into_iter().take(limit).collect::<Vec<_>>();

        let mut jobs = Vec::with_capacity(entries.len());

        for (job_id, _) in &entries {
            let uuid = job_id.parse::<uuid::Uuid>().map_err(|err| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "invalid job id in job index",
                    err.to_string(),
                ))
            })?;

            let job_id = JobId(uuid);

            if let Some(job) = self.get_job(job_id).await? {
                if state.is_none() || state == Some(job.state) {
                    jobs.push(job);
                }
            }
        }

        let next_cursor = if has_more {
            entries
                .last()
                .map(|(job_id, score)| encode_cursor(*score, job_id))
        } else {
            None
        };

        Ok(JobPage {
            jobs,
            next_cursor,
            has_more,
        })
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
            // KEYS[1]
            .key(job_key)
            // KEYS[2]
            .key("jobrail:queue:scheduled")
            // KEYS[3] ← ADD THIS
            .key("jobrail:jobs:index")
            // ARGV[1]
            .arg(job_id)
            // ARGV[2]
            .arg(job_data)
            // ARGV[3]
            .arg(run_at)
            .invoke_async(&mut self.connection)
            .await?;

        Ok(())
    }
    pub async fn promote_scheduled_jobs(&mut self) -> redis::RedisResult<Vec<JobId>> {
        let now_ms = current_timestamp_ms();

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
        mut repeatable_job: RepeatableJob,
    ) -> redis::RedisResult<()> {
        let job_id = repeatable_job.id.0.to_string();

        let redis_key = format!("jobrail:repeat:{}", job_id);

        if repeatable_job.next_run_at.is_none() {
            let next_run_at = repeatable_job
                .schedule
                .next_run_at_from_now()
                .map_err(|error| {
                    redis::RedisError::from((
                        redis::ErrorKind::UnexpectedReturnType,
                        "failed to calculate next repeatable run",
                        error,
                    ))
                })?;

            repeatable_job.next_run_at = Some(next_run_at);
        }

        let data = serde_json::to_string(&repeatable_job).map_err(|error| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to serialize repeatable job",
                error.to_string(),
            ))
        })?;

        let _: () = self.connection.set(&redis_key, data).await?;

        let _: () = self
            .connection
            .sadd("jobrail:queue:repeatable", &job_id)
            .await?;

        if repeatable_job.enabled {
            let next_run_at = repeatable_job
                .next_run_at
                .expect("next_run_at must exist for enabled repeatable job");

            let _: () = self
                .connection
                .zadd("jobrail:queue:repeatable:schedule", &job_id, next_run_at)
                .await?;
        }

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
    ) -> redis::RedisResult<Option<Job>> {
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

        let claimed = self
            .claim_repeatable_occurrence(repeatable_job.id, run_at, &job)
            .await?;

        if !claimed {
            return Ok(None);
        }

        Ok(Some(job))
    }

    pub async fn get_due_repeatable_job_ids(
        &mut self,
        now_ms: u64,
    ) -> redis::RedisResult<Vec<String>> {
        let job_ids: Vec<String> = self
            .connection
            .zrangebyscore("jobrail:queue:repeatable:schedule", "-inf", now_ms)
            .await?;

        Ok(job_ids)
    }

    pub async fn claim_repeatable_occurrence(
        &mut self,
        repeatable_job_id: jobrail_core::repeat::RepeatableJobId,
        run_at: u64,
        job: &Job,
    ) -> redis::RedisResult<bool> {
        let occurrence_key = format!(
            "jobrail:repeat:occurrence:{}:{}",
            repeatable_job_id.0, run_at
        );

        let job_key = format!("jobrail:job:{}", job.id.0);

        let job_data = serde_json::to_string(job).map_err(|error| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to serialize repeatable execution",
                error.to_string(),
            ))
        })?;

        let script = redis::Script::new(include_str!("scripts/claim_repeatable_occurrence.lua"));

        let claimed: i32 = script
            .key(occurrence_key)
            .key(job_key)
            .key("jobrail:queue:scheduled")
            .arg(job.id.0.to_string())
            .arg(job_data)
            .arg(run_at)
            .invoke_async(&mut self.connection)
            .await?;

        Ok(claimed == 1)
    }

    pub async fn advance_repeatable_job(
        &mut self,
        repeatable_job_id: jobrail_core::repeat::RepeatableJobId,
        previous_run_at: u64,
    ) -> redis::RedisResult<()> {
        let Some(mut repeatable_job) = self.get_repeatable_job(repeatable_job_id).await? else {
            return Ok(());
        };

        let next_run_at = repeatable_job.schedule.next_run_at(previous_run_at);

        repeatable_job.next_run_at = Some(next_run_at);

        self.save_repeatable_job(repeatable_job).await?;

        Ok(())
    }

    pub async fn create_repeatable_execution(
        &mut self,
        repeatable_job: &RepeatableJob,
        run_at: u64,
    ) -> redis::RedisResult<Option<Job>> {
        let next_run_at = repeatable_job.schedule.next_run_at(run_at);

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

        let occurrence_key = format!(
            "jobrail:repeat:occurrence:{}:{}",
            repeatable_job.id.0, run_at
        );

        let job_key = format!("jobrail:job:{}", job.id.0);

        let repeatable_key = format!("jobrail:repeat:{}", repeatable_job.id.0);

        let script = redis::Script::new(include_str!("scripts/create_repeatable_execution.lua"));

        let created: i32 = script
            .key(occurrence_key)
            .key(job_key)
            .key("jobrail:queue:scheduled")
            .key(repeatable_key)
            .key("jobrail:queue:repeatable:schedule")
            .key("jobrail:jobs:index")
            .arg(job.id.0.to_string())
            .arg(serde_json::to_string(&job).map_err(|error| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "failed to serialize repeatable execution",
                    error.to_string(),
                ))
            })?)
            .arg(run_at)
            .arg(next_run_at)
            .arg(repeatable_job.id.0.to_string())
            .arg(86_400)
            .invoke_async(&mut self.connection)
            .await?;

        if created == 0 {
            return Ok(None);
        }

        Ok(Some(job))
    }

    pub async fn disable_repeatable_job(
        &mut self,
        id: jobrail_core::repeat::RepeatableJobId,
    ) -> redis::RedisResult<bool> {
        let Some(mut job) = self.get_repeatable_job(id).await? else {
            return Ok(false);
        };

        job.enabled = false;

        let key = format!("jobrail:repeat:{}", id.0);

        let data = serde_json::to_string(&job).map_err(|error| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to serialize repeatable job",
                error.to_string(),
            ))
        })?;

        let _: () = self.connection.set(&key, data).await?;

        let _: () = self
            .connection
            .zrem("jobrail:queue:repeatable:schedule", id.0.to_string())
            .await?;

        Ok(true)
    }

    pub async fn delete_repeatable_job(
        &mut self,
        id: jobrail_core::repeat::RepeatableJobId,
    ) -> redis::RedisResult<bool> {
        let key = format!("jobrail:repeat:{}", id.0);

        let deleted: i32 = self.connection.del(&key).await?;

        let _: () = self
            .connection
            .srem("jobrail:queue:repeatable", id.0.to_string())
            .await?;

        let _: () = self
            .connection
            .zrem("jobrail:queue:repeatable:schedule", id.0.to_string())
            .await?;

        Ok(deleted == 1)
    }

    pub async fn add_job_attempt(
        &mut self,
        job_id: JobId,
        attempt: &JobAttempt,
    ) -> redis::RedisResult<()> {
        let key = format!("jobrail:job:{}:attempts", job_id.0);

        let data = serde_json::to_string(attempt).map_err(|error| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to serialize job attempt",
                error.to_string(),
            ))
        })?;

        let _: () = self.connection.rpush(key, data).await?;

        Ok(())
    }

    // pub async fn get_job_attempts(&mut self, job_id: JobId) -> redis::RedisResult<Vec<JobAttempt>> {
    //     let key = format!("jobrail:job:{}:attempts", job_id.0);

    //     let values: Vec<String> = self.connection.lrange(key, 0, -1).await?;

    //     values
    //         .into_iter()
    //         .map(|value| {
    //             serde_json::from_str(&value).map_err(|error| {
    //                 redis::RedisError::from((
    //                     redis::ErrorKind::UnexpectedReturnType,
    //                     "failed to deserialize job attempt",
    //                     error.to_string(),
    //                 ))
    //             })
    //         })
    //         .collect()
    // }

    pub async fn cancel_job(&mut self, job_id: JobId) -> redis::RedisResult<Option<Job>> {
        let Some(mut job) = self.get_job(job_id).await? else {
            return Ok(None);
        };

        match job.state {
            JobState::Waiting
            | JobState::Prioritized
            | JobState::Delayed
            | JobState::Scheduled
            | JobState::Active => {}

            JobState::Completed | JobState::Failed | JobState::Cancelled => {
                return Ok(Some(job));
            }
        }

        let job_id_string = job.id.0.to_string();

        let _: () = redis::pipe()
            .atomic()
            .lrem("jobrail:queue:waiting", 0, &job_id_string)
            .lrem("jobrail:queue:active", 0, &job_id_string)
            .zrem("jobrail:queue:delayed", &job_id_string)
            .zrem("jobrail:queue:scheduled", &job_id_string)
            .query_async(&mut self.connection)
            .await?;

        job.state = JobState::Cancelled;

        self.save_job(&job).await?;

        Ok(Some(job))
    }

    pub async fn retry_job(&mut self, job_id: JobId) -> redis::RedisResult<Option<Job>> {
        let Some(mut job) = self.get_job(job_id).await? else {
            return Ok(None);
        };

        if job.state != JobState::Failed {
            return Ok(Some(job));
        }

        job.state = if job.priority > 0 {
            JobState::Prioritized
        } else {
            JobState::Waiting
        };

        job.run_at = None;

        self.save_job(&job).await?;
        self.enqueue(job.id.clone()).await?;

        Ok(Some(job))
    }
    pub async fn start_job_attempt(
        &mut self,
        job_id: JobId,
        attempt: &JobAttempt,
    ) -> redis::RedisResult<()> {
        let key = format!("jobrail:job:{}:attempt:{}", job_id.0, attempt.attempt);

        let data = serde_json::to_string(attempt).map_err(|err| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to serialize job attempt",
                err.to_string(),
            ))
        })?;

        let _: () = self.connection.set(key, data).await?;

        Ok(())
    }

    pub async fn finish_job_attempt(
        &mut self,
        job_id: JobId,
        attempt_number: u32,
        status: JobAttemptStatus,
        error: Option<String>,
    ) -> redis::RedisResult<()> {
        let key = format!("jobrail:job:{}:attempt:{}", job_id.0, attempt_number);

        let Some(data): Option<String> = self.connection.get(&key).await? else {
            return Err(redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "job attempt not found",
            )));
        };

        let mut attempt: JobAttempt = serde_json::from_str(&data).map_err(|err| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to deserialize job attempt",
                err.to_string(),
            ))
        })?;

        attempt.finished_at = Some(current_timestamp_ms());
        attempt.status = status;
        attempt.error = error;

        let updated_data = serde_json::to_string(&attempt).map_err(|err| {
            redis::RedisError::from((
                redis::ErrorKind::UnexpectedReturnType,
                "failed to serialize job attempt",
                err.to_string(),
            ))
        })?;

        let _: () = self.connection.set(&key, updated_data).await?;

        Ok(())
    }

    pub async fn get_job_attempts(&mut self, job_id: JobId) -> redis::RedisResult<Vec<JobAttempt>> {
        let Some(job) = self.get_job(job_id.clone()).await? else {
            return Ok(Vec::new());
        };

        let mut attempts = Vec::new();

        for attempt_number in 1..=job.attempts_started {
            let key = format!("jobrail:job:{}:attempt:{}", job_id.0, attempt_number);

            let Some(data): Option<String> = self.connection.get(&key).await? else {
                continue;
            };

            let attempt: JobAttempt = serde_json::from_str(&data).map_err(|err| {
                redis::RedisError::from((
                    redis::ErrorKind::UnexpectedReturnType,
                    "failed to deserialize job attempt",
                    err.to_string(),
                ))
            })?;

            attempts.push(attempt);
        }

        Ok(attempts)
    }
}

fn encode_cursor(score: f64, member: &str) -> String {
    format!("{score}:{member}")
}

fn decode_cursor(cursor: &str) -> redis::RedisResult<(f64, String)> {
    let (score, member) = cursor.split_once(':').ok_or_else(|| {
        redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "invalid pagination cursor",
        ))
    })?;

    let score = score.parse::<f64>().map_err(|err| {
        redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "invalid pagination cursor score",
            err.to_string(),
        ))
    })?;

    if member.is_empty() {
        return Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "invalid pagination cursor member",
        )));
    }

    Ok((score, member.to_string()))
}

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_millis() as u64
}
