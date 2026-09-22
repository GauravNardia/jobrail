use axum::{
    Router,
    routing::{get, post},
};

use crate::{
    handlers::{
        health::{health, ready},
        jobs::{cancel_job, create_job, get_job, get_job_attempts, list_jobs, retry_job},
        queues::{get_queue, list_queues},
        repeatable::{
            create_repeatable_job, delete_repeatable_job, disable_repeatable_job,
            get_repeatable_job, list_repeatable_jobs,
        },
    },
    state::AppState,
};

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .nest(
            "/v1",
            Router::new()
                .route("/jobs", post(create_job).get(list_jobs))
                .route("/jobs/{id}", get(get_job))
                .route("/jobs/{id}/cancel", post(cancel_job))
                .route("/jobs/{id}/retry", post(retry_job))
                .route("/jobs/{id}/attempts", get(get_job_attempts))
                .route("/queues", get(list_queues))
                .route("/queues/{name}", get(get_queue))
                .route(
                    "/repeatable-jobs",
                    post(create_repeatable_job).get(list_repeatable_jobs),
                )
                .route(
                    "/repeatable-jobs/{id}",
                    get(get_repeatable_job).delete(delete_repeatable_job),
                )
                .route(
                    "/repeatable-jobs/{id}/disable",
                    post(disable_repeatable_job),
                ),
        )
        .with_state(state)
}
