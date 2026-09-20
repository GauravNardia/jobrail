use axum::{Router, routing::post};

use crate::{handlers::jobs::create_job, state::AppState};

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .nest("/v1", Router::new().route("/jobs", post(create_job)))
        .with_state(state)
}
