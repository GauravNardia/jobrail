pub mod dto;
pub mod error;
pub mod handlers;
pub mod routes;
pub mod state;

use axum::Router;
use std::sync::Arc;

use crate::{routes::create_router, state::AppState};
use jobrail_redis::RedisStorage;

pub async fn create_app() -> Result<Router, redis::RedisError> {
    let storage = RedisStorage::new().await?;

    let state = AppState {
        storage: Arc::new(tokio::sync::Mutex::new(storage)),
    };

    Ok(create_router(state))
}
