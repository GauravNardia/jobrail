use jobrail_redis::RedisStorage;

#[derive(Clone)]
pub struct AppState {
    pub storage: RedisStorage,
}
