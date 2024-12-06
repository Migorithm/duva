use common::{init_config_with_free_port, init_slave_config_with_free_port, start_test_server};
use redis_starter_rust::adapters::cancellation_token::CancellationToken;
mod common;

#[tokio::test]
async fn test_handshake_ping() {
    // GIVEN
    //TODO test config should be dynamically configured

    let master_config = init_config_with_free_port().await;
    start_test_server::<CancellationToken>(master_config.clone()).await;

    let slave_config = init_slave_config_with_free_port(master_config.port).await;
    start_test_server::<CancellationToken>(slave_config).await;

    let slave_config = init_slave_config_with_free_port(master_config.port).await;
    start_test_server::<CancellationToken>(slave_config).await;

    // THEN
    assert_eq!(master_config.replication.connected_slaves, 2);
}
