use std::sync::atomic::{AtomicU32, Ordering};

use jobrail_core::job::Job;
use jobrail_redis::RedisStorage;
use jobrail_worker::{JobHandler, Worker};

struct SendEmailHandler {
    attempts: AtomicU32,
}

impl JobHandler for SendEmailHandler {
    fn execute(&self, payload: serde_json::Value) -> Result<(), String> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;

        println!("Sending email with payload: {payload} (handler attempt {attempt})");

        if attempt < 3 {
            Err("Email service failed".to_string())
        } else {
            println!("Email sent successfully!");

            Ok(())
        }
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

    let handler = SendEmailHandler {
        attempts: AtomicU32::new(0),
    };

    worker.run_once(&handler).await.expect("Worker failed");

    println!("Waiting 6 seconds for retry time...");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    storage
        .promote_delayed_jobs()
        .await
        .expect("Failed to promote delayed jobs");

    println!("Retrying job now...");

    worker
        .run_once(&handler)
        .await
        .expect("Retry worker failed");

    println!("Waiting 6 seconds for second retry...");

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    storage
        .promote_delayed_jobs()
        .await
        .expect("Failed to promote delayed jobs");

    println!("Running third attempt...");

    worker
        .run_once(&handler)
        .await
        .expect("Third attempt failed");

    let loaded_job = storage
        .get_job(job.id)
        .await
        .expect("Failed to get job")
        .expect("Job was not found");

    println!(
        "Final job state: {:?}, attempts made: {}",
        loaded_job.state, loaded_job.attempts_made
    );
}
