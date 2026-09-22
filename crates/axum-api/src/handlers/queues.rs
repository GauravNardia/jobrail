use axum::{
    Json,
    extract::{Path, State},
};

use crate::{
    dto::queues::{QueueResponse, QueuesResponse},
    error::ApiError,
    state::AppState,
};

const DEFAULT_QUEUE: &str = "default";

pub async fn list_queues(State(state): State<AppState>) -> Result<Json<QueuesResponse>, ApiError> {
    let mut storage = state.storage.lock().await;

    let stats = storage
        .queue_stat()
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    Ok(Json(QueuesResponse {
        queues: vec![QueueResponse {
            name: DEFAULT_QUEUE.to_string(),
            waiting: stats.waiting,
            active: stats.active,
            delayed: stats.delayed,
            scheduled: stats.scheduled,
            processing: stats.processing,
        }],
    }))
}

pub async fn get_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<QueueResponse>, ApiError> {
    if name != DEFAULT_QUEUE {
        return Err(ApiError::NotFound("queue not found".to_string()));
    }

    let mut storage = state.storage.lock().await;

    let stats = storage
        .queue_stat()
        .await
        .map_err(|error| ApiError::Internal(error.to_string()))?;

    Ok(Json(QueueResponse {
        name,
        waiting: stats.waiting,
        active: stats.active,
        delayed: stats.delayed,
        scheduled: stats.scheduled,
        processing: stats.processing,
    }))
}
