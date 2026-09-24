use jobrail_redis::RedisStorage;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    println!("Scheduler started");

    loop {
        match storage.promote_scheduled_jobs().await {
            Ok(job_ids) => {
                if !job_ids.is_empty() {
                    println!("Scheduler promoted {} scheduled jobs", job_ids.len());
                }
            }

            Err(error) => {
                eprintln!("Scheduler failed: {error}");
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
