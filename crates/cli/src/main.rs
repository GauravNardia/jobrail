#[tokio::main]
async fn main() {
    jobrail_redis::test_connection()
        .await
        .expect("Failed to connect to Redis");
}
