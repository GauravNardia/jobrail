use jobrail_core::job::{Job, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use jobrail_worker::{JobHandler, Worker};
use serde_json::Value;
use serial_test::serial;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

struct CountingHandler {
    executions: Arc<AtomicU32>,
}

impl JobHandler for CountingHandler {
    fn execute(&self, _payload: Value) -> Result<(), String> {
        self.executions.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }
}

#[tokio::test]
#[serial]
async fn scheduled_job_is_eventually_executed() {
    let executions = Arc::new(AtomicU32::new(0));

    let handler = Arc::new(CountingHandler {
        executions: Arc::clone(&executions),
    });

    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to create Redis client");

    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    let _: () = redis::cmd("DEL")
        .arg("jobrail:queue:scheduled")
        .arg("jobrail:queue:waiting")
        .arg("jobrail:queue:active")
        .arg("jobrail:queue:processing")
        .query_async(&mut connection)
        .await
        .expect("failed to clean queues");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before UNIX epoch")
        .as_millis() as u64;

    let job = Job::new(
        "scheduled_test",
        serde_json::json!({
            "message": "hello"
        }),
        JobOptions {
            run_at: Some(now),
            ..Default::default()
        },
    );

    let job_id = job.id.clone();

    storage
        .schedule_job(job)
        .await
        .expect("failed to schedule job");

    assert_eq!(
        storage
            .get_job(job_id.clone())
            .await
            .expect("failed to read job")
            .expect("job missing")
            .state,
        JobState::Scheduled
    );

    storage
        .promote_scheduled_jobs()
        .await
        .expect("failed to promote job");

    Worker::run_once(1, &mut storage, Arc::clone(&handler))
        .await
        .expect("worker failed");

    assert_eq!(executions.load(Ordering::SeqCst), 1);

    let final_job = storage
        .get_job(job_id.clone())
        .await
        .expect("failed to read final job")
        .expect("final job missing");

    assert_eq!(final_job.state, JobState::Completed);

    let _: () = redis::cmd("DEL")
        .arg(format!("jobrail:job:{}", job_id.0))
        .query_async(&mut connection)
        .await
        .expect("failed to clean job");
}
