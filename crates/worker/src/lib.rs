use jobrail_redis::RedisStorage;

pub struct Worker {
    storage: RedisStorage,
}

impl Worker {
    pub async fn new() -> redis::RedisResult<Self> {
        let storage = RedisStorage::new().await?;
        Ok(Self { storage })
    }

    pub async fn run_once(&mut self) -> redis::RedisResult<()> {
        let Some(job) = self.storage.claim_job().await? else {
            println!("No jobs available");

            return Ok(());
        };

        println!("Worker claimed job: {}", job.id.0);
        println!("Job state: {:?}", job.state);

        Ok(())
    }
}
