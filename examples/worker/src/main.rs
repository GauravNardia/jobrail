use jobrail_worker::{JobHandler, Worker};
use serde_json::Value;
use std::sync::Arc;

struct SendEmailHandler;

impl JobHandler for SendEmailHandler {
    fn execute(&self, payload: Value) -> Result<(), String> {
        let to = payload
            .get("to")
            .and_then(Value::as_str)
            .ok_or_else(|| "missing `to` in payload".to_string())?;

        let subject = payload
            .get("subject")
            .and_then(Value::as_str)
            .ok_or_else(|| "missing `subject` in payload".to_string())?;

        let body = payload
            .get("body")
            .and_then(Value::as_str)
            .ok_or_else(|| "missing `body` in payload".to_string())?;

        println!();
        println!("========================================");
        println!("JobHandler: SendEmailHandler");
        println!("========================================");
        println!("To:      {to}");
        println!("Subject: {subject}");
        println!("Body:    {body}");
        println!();
        println!("Pretending to send email...");
        println!("Email sent successfully!");
        println!("========================================");
        println!();

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting JobRail example worker...");
    println!("Handler: SendEmailHandler");
    println!("Concurrency: 1");
    println!();

    let handler = Arc::new(SendEmailHandler);

    let worker = Worker::new(1).await?;

    worker.run(handler).await?;

    Ok(())
}
