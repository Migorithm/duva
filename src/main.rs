use redis_starter_rust::{
    adapters::{cancellation_token::CancellationToken, io::tokio_stream::AppStream},
    services::config::config_actor::Config,
    start_up,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let config = Config::default();
    start_up::<CancellationToken, AppStream, tokio::net::TcpStream>(config, ()).await
}
