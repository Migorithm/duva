use redis_starter_rust::{config, start_up};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    start_up(config::config().bind_addr().as_str()).await
}
