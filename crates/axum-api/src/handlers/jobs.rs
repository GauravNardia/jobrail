use axum::{
    Json,
    extract::{Path, State},
};

use jobrail_core::job::{Job, JobOptions};

use crate::{
    dto::jobs::{CreateJobRequest, JobResponse},
    error::ApiError,
    state::AppState,
};

pub async fn create_job(
    State(mut state): State<AppState>,
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

    state
        .storage
        .save_job(&job)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    Ok(Json(job_response(job)))
}

pub async fn get_job(
    State(mut state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobResponse>, ApiError> {
    let job_id = uuid::Uuid::parse_str(&id)
        .map_err(|_| ApiError::BadRequest("invalid job id".to_string()))?;

    let job_id = jobrail_core::job::JobId(job_id);

    let job = state
        .storage
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
