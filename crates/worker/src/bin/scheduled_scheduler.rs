use jobrail_worker::run_scheduled_scheduler;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    run_scheduled_scheduler().await
}
