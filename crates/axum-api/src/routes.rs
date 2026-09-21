use axum::{
    Router,
    routing::{get, post},
};

use crate::{
    handlers::jobs::{cancel_job, create_job, get_job, get_job_attempts, list_jobs, retry_job},
    state::AppState,
};

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .nest(
            "/v1",
            Router::new()
                .route("/jobs", post(create_job).get(list_jobs))
                .route("/jobs/{id}", get(get_job))
                .route("/jobs/{id}/cancel", post(cancel_job))
                .route("jobs/{id}/retry", post(retry_job))
                .route("/jobs/{id}/attempts", get(get_job_attempts)),
        )
        .with_state(state)
}
