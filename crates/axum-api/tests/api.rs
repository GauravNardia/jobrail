use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use jobrail_core::job::{Job, JobAttempt, JobAttemptStatus, JobOptions, JobState};
use jobrail_redis::RedisStorage;
use serde_json::{Value, json};
use serial_test::serial;
use tower::ServiceExt;

use axum_api::create_app;

async fn request(method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let app = create_app().await.expect("failed to create test app");

    let mut builder = Request::builder().method(method).uri(uri);

    let request_body = match body {
        Some(body) => {
            builder = builder.header("content-type", "application/json");
            Body::from(body.to_string())
        }

        None => Body::empty(),
    };

    let request = builder.body(request_body).expect("failed to build request");

    let response = app.oneshot(request).await.expect("request failed");

    let status = response.status();

    let body = response
        .into_body()
        .collect()
        .await
        .expect("failed to read response body")
        .to_bytes();

    let json: Value = serde_json::from_slice(&body).expect("response was not valid JSON");

    (status, json)
}

async fn create_test_job(name: &str) -> Value {
    let (status, body) = request(
        "POST",
        "/v1/jobs",
        Some(json!({
            "name": name,
            "payload": {
                "message": "integration test"
            }
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);

    body
}

async fn cancel_job(job_id: &str) {
    let (status, _) = request("POST", &format!("/v1/jobs/{job_id}/cancel"), None).await;

    assert!(
        status == StatusCode::OK || status == StatusCode::NOT_FOUND,
        "unexpected cleanup status: {status}"
    );
}

fn future_timestamp_ms(seconds: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_millis() as u64
        + seconds * 1_000
}

// ------------------------------------------------------------
// Health / readiness
// ------------------------------------------------------------

#[tokio::test]
#[serial]
async fn health_endpoint_returns_ok() {
    let (status, body) = request("GET", "/health", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
#[serial]
async fn ready_endpoint_returns_ready() {
    let (status, body) = request("GET", "/ready", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ready");
}

// ------------------------------------------------------------
// Job creation / retrieval
// ------------------------------------------------------------

#[tokio::test]
#[serial]
async fn create_and_get_job() {
    let created = create_test_job("create-and-get").await;

    let job_id = created["id"].as_str().expect("missing job id");

    assert_eq!(created["name"], "create-and-get");
    assert_eq!(created["state"], "Waiting");
    assert_eq!(created["attemptsMade"], 0);
    assert_eq!(created["attemptsStarted"], 0);

    let (status, body) = request("GET", &format!("/v1/jobs/{job_id}"), None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], job_id);
    assert_eq!(body["name"], "create-and-get");
    assert_eq!(body["state"], "Waiting");

    cancel_job(job_id).await;
}

#[tokio::test]
#[serial]
async fn create_delayed_job() {
    let (status, body) = request(
        "POST",
        "/v1/jobs",
        Some(json!({
            "name": "delayed-test",
            "payload": {
                "message": "delayed"
            },
            "delayMs": 60_000
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "Delayed");

    let job_id = body["id"].as_str().expect("missing job id");

    cancel_job(job_id).await;
}

#[tokio::test]
#[serial]
async fn create_scheduled_job() {
    let run_at = future_timestamp_ms(60);

    let (status, body) = request(
        "POST",
        "/v1/jobs",
        Some(json!({
            "name": "scheduled-test",
            "payload": {
                "message": "scheduled"
            },
            "runAt": run_at
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "Scheduled");
    assert_eq!(body["runAt"], run_at);

    let job_id = body["id"].as_str().expect("missing job id");

    cancel_job(job_id).await;
}

#[tokio::test]
#[serial]
async fn run_at_in_the_past_returns_bad_request() {
    let past = future_timestamp_ms(0) - 60_000;

    let (status, body) = request(
        "POST",
        "/v1/jobs",
        Some(json!({
            "name": "invalid-scheduled-test",
            "payload": {},
            "runAt": past
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "BAD_REQUEST");
}

// ------------------------------------------------------------
// Job listing / pagination / filtering
// ------------------------------------------------------------

#[tokio::test]
#[serial]
async fn list_jobs_returns_created_jobs() {
    let first = create_test_job("list-test-1").await;
    let second = create_test_job("list-test-2").await;

    let first_id = first["id"].as_str().expect("missing first id");

    let second_id = second["id"].as_str().expect("missing second id");

    let (status, body) = request("GET", "/v1/jobs?limit=100", None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["jobs"].is_array());

    let jobs = body["jobs"].as_array().expect("jobs should be an array");

    assert!(jobs.iter().any(|job| job["id"] == first_id));
    assert!(jobs.iter().any(|job| job["id"] == second_id));

    cancel_job(first_id).await;
    cancel_job(second_id).await;
}

#[tokio::test]
#[serial]
async fn list_jobs_supports_pagination() {
    let first = create_test_job("pagination-1").await;
    let second = create_test_job("pagination-2").await;
    let third = create_test_job("pagination-3").await;

    let (status, first_page) = request("GET", "/v1/jobs?limit=2", None).await;

    assert_eq!(status, StatusCode::OK);

    let jobs = first_page["jobs"]
        .as_array()
        .expect("jobs should be an array");

    assert_eq!(jobs.len(), 2);

    assert_eq!(first_page["hasMore"], true);

    let cursor = first_page["nextCursor"]
        .as_str()
        .expect("nextCursor should exist");

    let (status, second_page) =
        request("GET", &format!("/v1/jobs?limit=2&cursor={cursor}"), None).await;

    assert_eq!(status, StatusCode::OK);

    let second_jobs = second_page["jobs"]
        .as_array()
        .expect("jobs should be an array");

    assert!(!second_jobs.is_empty());

    let first_ids: Vec<&str> = jobs.iter().filter_map(|job| job["id"].as_str()).collect();

    let overlaps = second_jobs.iter().any(|job| {
        job["id"]
            .as_str()
            .map(|id| first_ids.contains(&id))
            .unwrap_or(false)
    });

    assert!(
        !overlaps,
        "second page should not contain jobs from first page"
    );

    cancel_job(first["id"].as_str().expect("missing first id")).await;

    cancel_job(second["id"].as_str().expect("missing second id")).await;

    cancel_job(third["id"].as_str().expect("missing third id")).await;
}

#[tokio::test]
#[serial]
async fn list_jobs_supports_state_filter() {
    let job = create_test_job("state-filter-test").await;

    let job_id = job["id"].as_str().expect("missing job id");

    let (cancel_status, cancelled) =
        request("POST", &format!("/v1/jobs/{job_id}/cancel"), None).await;

    assert_eq!(cancel_status, StatusCode::OK);
    assert_eq!(cancelled["state"], "Cancelled");

    let (status, body) = request("GET", "/v1/jobs?state=Cancelled&limit=100", None).await;

    assert_eq!(status, StatusCode::OK);

    let jobs = body["jobs"].as_array().expect("jobs should be an array");

    assert!(jobs.iter().any(|candidate| candidate["id"] == job_id));
}

// ------------------------------------------------------------
// Cancellation
// ------------------------------------------------------------

#[tokio::test]
#[serial]
async fn cancel_job_changes_state() {
    let job = create_test_job("cancel-test").await;

    let job_id = job["id"].as_str().expect("missing job id");

    let (status, body) = request("POST", &format!("/v1/jobs/{job_id}/cancel"), None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "Cancelled");

    let (status, body) = request("GET", &format!("/v1/jobs/{job_id}"), None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "Cancelled");
}

// ------------------------------------------------------------
// Retry
// ------------------------------------------------------------

#[tokio::test]
#[serial]
async fn retry_failed_job_changes_state_to_waiting() {
    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let mut job = Job::new(
        "retry-test",
        json!({
            "message": "retry"
        }),
        JobOptions::default(),
    );

    job.state = JobState::Failed;

    storage
        .save_job(&job)
        .await
        .expect("failed to save failed job");

    let job_id = job.id.0.to_string();

    let (status, body) = request("POST", &format!("/v1/jobs/{job_id}/retry"), None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], "Waiting");

    cancel_job(&job_id).await;
}

// ------------------------------------------------------------
// Attempts
// ------------------------------------------------------------

#[tokio::test]
#[serial]
async fn get_job_attempts_returns_attempt_history() {
    let job = create_test_job("attempt-test").await;

    let job_id_string = job["id"].as_str().expect("missing job id").to_string();

    let uuid = uuid::Uuid::parse_str(&job_id_string).expect("invalid job id");

    let job_id = jobrail_core::job::JobId(uuid);

    let mut storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let mut stored_job = storage
        .get_job(job_id.clone())
        .await
        .expect("failed to get job")
        .expect("job should exist");

    stored_job.attempts_started = 1;

    storage
        .save_job(&stored_job)
        .await
        .expect("failed to update job");

    let attempt = JobAttempt {
        attempt: 1,
        started_at: future_timestamp_ms(0),
        finished_at: None,
        status: JobAttemptStatus::Running,
        error: None,
    };

    storage
        .start_job_attempt(job_id.clone(), &attempt)
        .await
        .expect("failed to start attempt");

    storage
        .finish_job_attempt(job_id, 1, JobAttemptStatus::Completed, None)
        .await
        .expect("failed to finish attempt");

    let (status, body) = request("GET", &format!("/v1/jobs/{job_id_string}/attempts"), None).await;

    assert_eq!(status, StatusCode::OK);

    let attempts = body["attempts"]
        .as_array()
        .expect("attempts should be an array");

    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0]["attempt"], 1);
    assert_eq!(attempts[0]["status"], "Completed");
    assert!(attempts[0]["finishedAt"].is_number());

    cancel_job(&job_id_string).await;
}

// ------------------------------------------------------------
// Queue inspection
// ------------------------------------------------------------

#[tokio::test]
#[serial]
async fn list_queues_returns_default_queue() {
    let (status, body) = request("GET", "/v1/queues", None).await;

    assert_eq!(status, StatusCode::OK);

    let queues = body["queues"]
        .as_array()
        .expect("queues should be an array");

    assert!(!queues.is_empty());
    assert_eq!(queues[0]["name"], "default");
    assert!(queues[0]["waiting"].is_number());
    assert!(queues[0]["active"].is_number());
    assert!(queues[0]["delayed"].is_number());
    assert!(queues[0]["scheduled"].is_number());
    assert!(queues[0]["processing"].is_number());
}

#[tokio::test]
#[serial]
async fn get_default_queue_returns_stats() {
    let (status, body) = request("GET", "/v1/queues/default", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "default");
    assert!(body["waiting"].is_number());
    assert!(body["active"].is_number());
    assert!(body["delayed"].is_number());
    assert!(body["scheduled"].is_number());
    assert!(body["processing"].is_number());
}

#[tokio::test]
#[serial]
async fn unknown_queue_returns_not_found() {
    let (status, body) = request("GET", "/v1/queues/does-not-exist", None).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
}

// ------------------------------------------------------------
// Repeatable jobs
// ------------------------------------------------------------

#[tokio::test]
#[serial]
async fn repeatable_job_crud_works() {
    let (status, created) = request(
        "POST",
        "/v1/repeatable-jobs",
        Some(json!({
            "name": "repeatable-test",
            "payload": {
                "message": "hello"
            },
            "schedule": {
                "EveryMillis": 60_000
            }
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["name"], "repeatable-test");
    assert_eq!(created["enabled"], true);
    assert!(created["id"].is_string());
    assert!(created["nextRunAt"].is_null());

    let id = created["id"].as_str().expect("missing repeatable job id");

    let (status, listed) = request("GET", "/v1/repeatable-jobs", None).await;

    assert_eq!(status, StatusCode::OK);

    let jobs = listed["jobs"].as_array().expect("jobs should be an array");

    assert!(jobs.iter().any(|job| job["id"] == id));

    let (status, fetched) = request("GET", &format!("/v1/repeatable-jobs/{id}"), None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["id"], id);

    let (status, disabled) =
        request("POST", &format!("/v1/repeatable-jobs/{id}/disable"), None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(disabled["enabled"], false);

    let (status, deleted) = request("DELETE", &format!("/v1/repeatable-jobs/{id}"), None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(deleted["id"], id);
}

// ------------------------------------------------------------
// Error contract
// ------------------------------------------------------------

#[tokio::test]
#[serial]
async fn invalid_job_id_returns_bad_request() {
    let (status, body) = request("GET", "/v1/jobs/not-a-uuid", None).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "BAD_REQUEST");
    assert!(body["error"]["message"].is_string());
}

#[tokio::test]
#[serial]
async fn missing_job_returns_not_found() {
    let id = uuid::Uuid::new_v4();

    let (status, body) = request("GET", &format!("/v1/jobs/{id}"), None).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
    assert_eq!(body["error"]["message"], "job not found");
}

#[tokio::test]
#[serial]
async fn zero_max_attempts_returns_bad_request() {
    let (status, body) = request(
        "POST",
        "/v1/jobs",
        Some(json!({
            "name": "invalid-attempts",
            "payload": {},
            "maxAttempts": 0
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "BAD_REQUEST");
}
