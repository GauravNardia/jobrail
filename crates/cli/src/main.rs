use jobrail_core::job::Job;
use jobrail_redis::RedisStorage;
use jobrail_worker::{JobHandler, Worker};
use std::sync::Arc;

struct SendEmailHandler;

impl JobHandler for SendEmailHandler {
    fn execute(&self, payload: serde_json::Value) -> Result<(), String> {
        println!("Starting job: {payload}");

        std::thread::sleep(std::time::Duration::from_secs(40));

        println!("Finished job: {payload}");

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let mut storage = RedisStorage::new()
        .await
        .expect("Failed to connect to Redis");

    let handler = Arc::new(SendEmailHandler);

    let job = Job::new(
        "test_job",
        serde_json::json!({
            "job_number": 1
        }),
        Default::default(),
    );

    println!("Created job: {}", job.id.0);

    storage.save_job(&job).await.expect("Failed to save job");

    storage
        .enqueue(job.id)
        .await
        .expect("Failed to enqueue job");

    println!("Created and enqueued 1 job");

    let worker = Worker::new(3).await.expect("Failed to create worker");

    worker.run(handler).await.expect("Worker failed");
}
