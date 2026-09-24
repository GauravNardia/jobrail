use jobrail_worker::run_repeatable_scheduler;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    run_repeatable_scheduler()
        .await
        .expect("Repeatable scheduler failed");
}
