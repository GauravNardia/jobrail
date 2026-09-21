use axum::{
    Json,
    extract::{Path, State},
};

use jobrail_core::repeat::{RepeatableJob, RepeatableJobId};

use crate::{
    dto::repeatable::{CreateRepeatableJobRequest, RepeatableJobResponse, RepeatableJobsResponse},
    error::ApiError,
    state::AppState,
};

pub async fn create_repeatable_job(
    State(state): State<AppState>,
    Json(request): Json<CreateRepeatableJobRequest>,
) -> Result<Json<RepeatableJobResponse>, ApiError> {
    let repeatable_job = RepeatableJob {
        id: RepeatableJobId(uuid::Uuid::new_v4()),
        name: request.name,
        payload: request.payload,
        schedule: request.schedule,
        enabled: true,
        next_run_at: None,
    };

    let mut storage = state.storage.lock().await;

    storage
        .save_repeatable_job(repeatable_job.clone())
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    Ok(Json(repeatable_job.into()))
}

pub async fn list_repeatable_jobs(
    State(state): State<AppState>,
) -> Result<Json<RepeatableJobsResponse>, ApiError> {
    let mut storage = state.storage.lock().await;

    let jobs = storage
        .list_repeatable_jobs()
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    let jobs = jobs.into_iter().map(RepeatableJobResponse::from).collect();

    Ok(Json(RepeatableJobsResponse { jobs }))
}

pub async fn get_repeatable_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RepeatableJobResponse>, ApiError> {
    let id = uuid::Uuid::parse_str(&id)
        .map_err(|_| ApiError::BadRequest("invalid repeatable job id".to_string()))?;

    let repeatable_job_id = RepeatableJobId(id);

    let mut storage = state.storage.lock().await;

    let job = storage
        .get_repeatable_job(repeatable_job_id)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    let Some(job) = job else {
        return Err(ApiError::NotFound("repeatable job not found".to_string()));
    };

    Ok(Json(job.into()))
}

pub async fn disable_repeatable_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RepeatableJobResponse>, ApiError> {
    let id = uuid::Uuid::parse_str(&id)
        .map_err(|_| ApiError::BadRequest("invalid repeatable job id".to_string()))?;

    let repeatable_job_id = RepeatableJobId(id);

    let mut storage = state.storage.lock().await;

    let disabled = storage
        .disable_repeatable_job(repeatable_job_id.clone())
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    if !disabled {
        return Err(ApiError::NotFound("repeatable job not found".to_string()));
    }

    let job = storage
        .get_repeatable_job(repeatable_job_id)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    let Some(job) = job else {
        return Err(ApiError::NotFound("repeatable job not found".to_string()));
    };

    Ok(Json(job.into()))
}

pub async fn delete_repeatable_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RepeatableJobResponse>, ApiError> {
    let id = uuid::Uuid::parse_str(&id)
        .map_err(|_| ApiError::BadRequest("invalid repeatable job id".to_string()))?;

    let repeatable_job_id = RepeatableJobId(id);

    let mut storage = state.storage.lock().await;

    let job = storage
        .get_repeatable_job(repeatable_job_id.clone())
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    let Some(job) = job else {
        return Err(ApiError::NotFound("repeatable job not found".to_string()));
    };

    storage
        .delete_repeatable_job(repeatable_job_id)
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    Ok(Json(job.into()))
}
