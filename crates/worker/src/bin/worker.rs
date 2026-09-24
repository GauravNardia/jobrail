use std::sync::Arc;

use jobrail_worker::{JobHandler, Worker};
use serde_json::Value;

struct SendEmailHandler;

impl JobHandler for SendEmailHandler {
    fn execute(&self, payload: Value) -> Result<(), String> {
        println!("Executing job with payload: {payload}");

        Ok(())
    }
}

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    dotenvy::dotenv().ok();

    let worker = Worker::new(1).await?;

    let handler = Arc::new(SendEmailHandler);

    worker.run(handler).await
}
