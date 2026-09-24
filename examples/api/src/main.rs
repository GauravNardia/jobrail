use jobrail_core::job::{Job, JobOptions};
use jobrail_redis::RedisStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut storage = RedisStorage::new().await?;

    let job = Job::new(
        "send-email".to_string(),
        serde_json::json!({
            "to": "user@example.com",
            "subject": "Hello from JobRail"
        }),
        JobOptions::default(),
    );

    storage.save_job(&job).await?;
    storage.enqueue(job.id.clone()).await?;

    println!("Created job: {}", job.id.0);

    Ok(())
}
