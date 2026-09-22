use axum::{
    Json,
    extract::{Path, Query, State},
};

use jobrail_core::job::{Job, JobOptions};

use crate::{
    dto::jobs::{
        CreateJobRequest, JobAttemptResponse, JobAttemptsResponse, JobPageResponse, JobResponse,
        ListJobsQuery,
    },
    error::ApiError,
    state::AppState,
};

pub async fn create_job(
    State(state): State<AppState>,
    Json(request): Json<CreateJobRequest>,
) -> Result<Json<JobResponse>, ApiError> {
    if let Some(run_at) = request.run_at {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| ApiError::Internal(format!("failed to read system clock: {error}")))?
            .as_millis() as u64;

        if run_at <= now {
            return Err(ApiError::BadRequest(
                "runAt must be in the future".to_string(),
            ));
        }
    }

    let options = JobOptions {
        priority: request.priority,
        max_attempts: request.max_attempts,
        delay_ms: request.delay_ms,
        run_at: request.run_at,
        idempotency_key: request.idempotency_key,
    };

    if request.max_attempts == 0 {
        return Err(ApiError::BadRequest(
            "maxAttempts must be greater than 0".to_string(),
        ));
    }

    let job = Job::new(request.name, request.payload, options);

    let mut storage = state.storage.lock().await;

    /*
     * A scheduled job belongs in the scheduled sorted set.
     */
    if job.run_at.is_some() {
        storage
            .schedule_job(job.clone())
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))?;
    }
    /*
     * A delayed job belongs in the delayed sorted set.
     */
    else if job.delay_ms > 0 {
        storage
            .save_job(&job)
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))?;

        storage
            .schedule_retry(job.id.clone(), job.delay_ms)
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))?;
    }
    /*
     * Normal jobs go directly into the waiting queue.
     */
    else {
        storage
            .save_job(&job)
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))?;

        storage
            .enqueue(job.id.clone())
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))?;
    }

    Ok(Json(job_response(job)))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobResponse>, ApiError> {
    let job_id = uuid::Uuid::parse_str(&id)
        .map_err(|_| ApiError::BadRequest("invalid job id".to_string()))?;

    let job_id = jobrail_core::job::JobId(job_id);

    let mut storage = state.storage.lock().await;

    let job = storage
        .get_job(job_id)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    let Some(job) = job else {
        return Err(ApiError::NotFound("job not found".to_string()));
    };

    Ok(Json(job_response(job)))
}

fn job_response(job: Job) -> JobResponse {
    JobResponse {
        id: job.id.0.to_string(),
        name: job.name,
        state: format!("{:?}", job.state),
        attempts_made: job.attempts_made,
        attempts_started: job.attempts_started,
        run_at: job.run_at,
    }
}

pub async fn list_jobs(
    State(state): State<AppState>,
    Query(query): Query<ListJobsQuery>,
) -> Result<Json<JobPageResponse>, ApiError> {
    let mut storage = state.storage.lock().await;

    let page = storage
        .list_jobs(query.limit, query.cursor, query.state)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    drop(storage);

    let jobs = page.jobs.into_iter().map(job_response).collect();

    Ok(Json(JobPageResponse {
        jobs,
        next_cursor: page.next_cursor,
        has_more: page.has_more,
    }))
}

pub async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobResponse>, ApiError> {
    let job_id = uuid::Uuid::parse_str(&id)
        .map_err(|_| ApiError::BadRequest("invalid job id".to_string()))?;

    let job_id = jobrail_core::job::JobId(job_id);

    let mut storage = state.storage.lock().await;

    let job = storage
        .cancel_job(job_id)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    let Some(job) = job else {
        return Err(ApiError::NotFound("job not found".to_string()));
    };

    Ok(Json(job_response(job)))
}

pub async fn retry_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobResponse>, ApiError> {
    let job_id = uuid::Uuid::parse_str(&id)
        .map_err(|_| ApiError::BadRequest("invalid job id".to_string()))?;

    let job_id = jobrail_core::job::JobId(job_id);

    let mut storage = state.storage.lock().await;

    let job = storage
        .retry_job(job_id)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    let Some(job) = job else {
        return Err(ApiError::NotFound("job not found".to_string()));
    };

    Ok(Json(job_response(job)))
}

pub async fn get_job_attempts(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobAttemptsResponse>, ApiError> {
    let job_id = uuid::Uuid::parse_str(&id)
        .map_err(|_| ApiError::BadRequest("invalid job id".to_string()))?;

    let job_id = jobrail_core::job::JobId(job_id);

    let mut storage = state.storage.lock().await;

    if storage
        .get_job(job_id.clone())
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?
        .is_none()
    {
        return Err(ApiError::NotFound("job not found".to_string()));
    }

    let attempts = storage
        .get_job_attempts(job_id)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    let attempts = attempts
        .into_iter()
        .map(|attempt| JobAttemptResponse {
            attempt: attempt.attempt,
            started_at: attempt.started_at,
            finished_at: attempt.finished_at,
            status: format!("{:?}", attempt.status),
            error: attempt.error,
        })
        .collect();

    Ok(Json(JobAttemptsResponse { attempts }))
}
