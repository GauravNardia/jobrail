use crate::{
    dto::jobs::{CreateJobRequest, JobResponse},
    error::ApiError,
    state::AppState,
};
use axum::{Json, extract::State};
use jobrail_core::job::{Job, JobOptions};

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

    Ok(Json(JobResponse {
        id: job.id.0.to_string(),
        name: job.name,
        state: format!("{:?}", job.state),
        attempts_made: job.attempts_made,
        attempts_started: job.attempts_started,
    }))
}
