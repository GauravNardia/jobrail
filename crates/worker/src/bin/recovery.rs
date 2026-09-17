use jobrail_worker::run_recovery;

#[tokio::main]
async fn main() {
    run_recovery().await.expect("Recovery worker failed");
}
