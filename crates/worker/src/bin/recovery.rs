use jobrail_worker::run_recovery;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    run_recovery().await.expect("Recovery worker failed");
}
