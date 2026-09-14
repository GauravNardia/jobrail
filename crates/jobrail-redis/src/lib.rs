use redis::AsyncCommands;

pub async fn test_connection() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;

    let mut connection = client.get_multiplexed_async_connection().await?;

    let _: () = connection.set("jobrail:test", "hello").await?;

    let value: String = connection.get("jobrail:test").await?;

    println!("Redis returned: {}", value);

    Ok(())
}
