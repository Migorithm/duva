/// After three-way handshake, client will receive peers from the master server
mod common;
use std::time::Duration;

use common::{find_free_port_in_range, start_test_server, threeway_handshake_helper};
use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::{
        config::{config_actor::Config, config_manager::ConfigManager},
        stream_manager::interface::TStream,
    },
};
use tokio::{net::TcpStream, time::timeout};

#[tokio::test]
async fn test_heartbeat() {
    // GIVEN
    // run the random server on a random port
    let config = Config::default();
    let mut manager = ConfigManager::new(config);
    manager.port = find_free_port_in_range(6000, 6553).await.unwrap();
    let master_cluster_bind_addr = manager.peer_bind_addr();
    let _ = start_test_server(CancellationTokenFactory, manager).await;

    // run the slave stream on a random port
    let slave_port = 6788;
    // find_free_port_in_range(6000, 6553).await.unwrap();

    // run the client bind stream on a random port so it can later get connection request from server
    let handler = tokio::spawn(async move {
        let slave_cluster_bind_addr = format!("localhost:{}", slave_port + 10000);
        let listener = tokio::net::TcpListener::bind(&slave_cluster_bind_addr)
            .await
            .unwrap();
        while let Ok((mut stream, _)) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
        {
            let mut count = 0;
            while count < 5 {
                let values = stream.read_value().await.unwrap();
                // TODO PING may not be used. It is just a placeholder
                assert_eq!(values.serialize(), "+PING\r\n");
                count += 1;
            }
            break;
        }
    });

    // WHEN making three-way handshake, server will connect to the client's server which is in this case
    let mut client_stream = TcpStream::connect(master_cluster_bind_addr).await.unwrap();
    threeway_handshake_helper(&mut client_stream, slave_port).await;

    //WHEN we await on handler, it will receive 5 PING messages
    handler.await.unwrap();
}
