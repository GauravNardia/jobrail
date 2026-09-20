pub mod dto;
pub mod error;
pub mod handlers;
pub mod routes;
pub mod state;

use axum::Router;
use jobrail_redis::RedisStorage;

use crate::{routes::create_router, state::AppState};

pub async fn create_app() -> Result<Router, redis::RedisError> {
    let storage = RedisStorage::new().await?;

    let state = AppState { storage };

    Ok(create_router(state))
}
