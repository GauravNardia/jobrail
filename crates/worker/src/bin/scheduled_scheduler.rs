use jobrail_worker::run_scheduled_scheduler;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    dotenvy::dotenv().ok();
    run_scheduled_scheduler().await
}
