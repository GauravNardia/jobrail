use std::sync::Arc;
use tokio::sync::Mutex;

use jobrail_redis::RedisStorage;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<Mutex<RedisStorage>>,
}
