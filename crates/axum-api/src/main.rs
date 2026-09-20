use axum_api::create_app;

#[tokio::main]
async fn main() {
    let app = create_app().await.expect("failed to create JobRail API");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001")
        .await
        .expect("failed to bind API server");

    println!("JobRail API running on http://localhost:3001");

    axum::serve(listener, app).await.expect("API server failed");
}
