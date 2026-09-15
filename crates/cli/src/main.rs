use jobrail_core::job::Job;
use jobrail_redis::RedisStorage;

#[tokio::main]
async fn main() {
    let mut storage = RedisStorage::new()
        .await
        .expect("Failed to connect to Redis");

    let job = Job::new(
        "send_email",
        serde_json::json!({
            "email": "test@example.com"
        }),
        Default::default(),
    );

    println!("Created job: {}", job.id.0);

    storage.save_job(&job).await.expect("Failed to save job");

    println!("Job saved to Redis");

    storage
        .enqueue(job.id.clone())
        .await
        .expect("Failed to enqueue job");

    println!("Job enqueued");

    let dequeued_job_id = storage
        .dequeue()
        .await
        .expect("Failed to dequeue job")
        .expect("Waiting queue was empty");

    println!("Dequeued job: {}", dequeued_job_id.0);

    let loaded_job = storage
        .get_job(job.id)
        .await
        .expect("Failed to get job")
        .expect("Job was not found");

    println!("Job loaded from Redis: {}", loaded_job.name);
}
