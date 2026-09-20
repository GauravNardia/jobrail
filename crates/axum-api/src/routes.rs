use axum::{
    Router,
    routing::{get, post},
};

use crate::{
    handlers::jobs::{create_job, get_job, list_jobs},
    state::AppState,
};

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .nest(
            "/v1",
            Router::new()
                .route("/jobs", post(create_job).get(list_jobs))
                .route("/jobs/{id}", get(get_job)),
        )
        .with_state(state)
}
