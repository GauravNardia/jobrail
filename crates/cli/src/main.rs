use jobrail_core::job::Job;
use jobrail_redis::RedisStorage;
use jobrail_worker::{JobHandler, Worker};

struct SendEmailHandler;

impl JobHandler for SendEmailHandler {
    fn execute(&self, payload: serde_json::Value) -> Result<(), String> {
        println!("Sending email with payload: {payload}");

        Ok(())
    }
}

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

    let mut worker = Worker::new().await.expect("Failed to create worker");

    let handler = SendEmailHandler;

    worker.run_once(&handler).await.expect("Worker failed");

    let loaded_job = storage
        .get_job(job.id)
        .await
        .expect("Failed to get job")
        .expect("Job was not found");

    println!("Job loaded from Redis: {}", loaded_job.name);
}
