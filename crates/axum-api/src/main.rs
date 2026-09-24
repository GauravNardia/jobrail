use axum_api::create_app;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let app = create_app().await.expect("failed to create JobRail API");

    let host = std::env::var("API_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());

    let port = std::env::var("API_PORT").unwrap_or_else(|_| "3001".to_string());

    let address = format!("{host}:{port}");

    let listener = tokio::net::TcpListener::bind(&address).await?;

    println!("JobRail API listening on {address}");

    axum::serve(listener, app).await.expect("API server failed");

    Ok(())
}
