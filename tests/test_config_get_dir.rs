/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use common::{init_config_manager_with_free_port, start_test_server, TestStreamHandler};

use crate::common::{array, config_command};
use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory, services::cluster::actor::ClusterActor,
};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_config_get_dir() {
    // GIVEN
    //TODO test config should be dynamically configured
    let config = init_config_manager_with_free_port().await;
    let cluster_actor = ClusterActor::new();
    let _ = start_test_server(CancellationTokenFactory, config.clone(), cluster_actor).await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();

    let mut h: TestStreamHandler = client_stream.split().into();

    // WHEN
    h.send(config_command("GET", "dir").as_slice()).await;

    // THEN
    assert_eq!(h.get_response().await, array(vec!["dir", "."]));
}
