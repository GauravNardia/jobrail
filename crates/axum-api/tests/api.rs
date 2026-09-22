use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use jobrail_redis::RedisStorage;
use serde_json::Value;
use tokio::sync::Mutex;
use tower::ServiceExt;

use axum_api::{routes::create_router, state::AppState};

async fn app() -> axum::Router {
    let storage = RedisStorage::new()
        .await
        .expect("failed to connect to Redis");

    let state = AppState {
        storage: Arc::new(Mutex::new(storage)),
    };

    create_router(state)
}

#[tokio::test]
async fn health_endpoint_returns_ok() {
    let app = app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();

    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn ready_endpoint_returns_ready() {
    let app = app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();

    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "ready");
}

#[tokio::test]
async fn queues_endpoint_returns_default_queue() {
    let app = app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/queues")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();

    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json["queues"].is_array());
    assert_eq!(json["queues"][0]["name"], "default");
}

#[tokio::test]
async fn unknown_queue_returns_not_found() {
    let app = app().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/queues/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn create_job_puts_job_into_waiting_queue() {
    let app = app().await;

    let request = Request::builder()
        .method("POST")
        .uri("/v1/jobs")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "name": "integration-test",
                "payload": {
                    "message": "hello"
                }
            })
            .to_string(),
        ))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();

    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json["id"].is_string());
    assert_eq!(json["name"], "integration-test");
    assert_eq!(json["state"], "Waiting");
}
