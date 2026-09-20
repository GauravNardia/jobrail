use axum::{
    Json,
    extract::{Path, Query, State},
};

use jobrail_core::job::{Job, JobOptions};

use crate::{
    dto::jobs::{CreateJobRequest, JobPageResponse, JobResponse, ListJobsQuery},
    error::ApiError,
    state::AppState,
};

pub async fn create_job(
    State(state): State<AppState>,
    Json(request): Json<CreateJobRequest>,
) -> Result<Json<JobResponse>, ApiError> {
    let options = JobOptions {
        priority: request.priority,
        max_attempts: request.max_attempts,
        delay_ms: request.delay_ms,
        run_at: request.run_at,
        idempotency_key: request.idempotency_key,
    };

    let job = Job::new(request.name, request.payload, options);

    let mut storage = state.storage.lock().await;

    storage
        .save_job(&job)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

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
    }
}

pub async fn list_jobs(
    State(state): State<AppState>,
    Query(query): Query<ListJobsQuery>,
) -> Result<Json<JobPageResponse>, ApiError> {
    let mut storage = state.storage.lock().await;

    let page = storage
        .list_jobs(query.limit, query.cursor)
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
