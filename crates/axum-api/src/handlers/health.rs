use axum::{Json, extract::State, http::StatusCode};

use serde::Serialize;

use crate::{error::ApiError, state::AppState};

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

pub async fn ready(
    State(state): State<AppState>,
) -> Result<(StatusCode, Json<HealthResponse>), ApiError> {
    let mut storage = state.storage.lock().await;

    storage
        .ping()
        .await
        .map_err(|error| ApiError::Internal(format!("redis is not ready: {error}")))?;

    Ok((StatusCode::OK, Json(HealthResponse { status: "ready" })))
}
