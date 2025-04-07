/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
use crate::common::{ServerEnv, array, spawn_server_process};

use duva::clients::ClientStreamHandler;

#[tokio::test]
async fn test_replication_info() {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env);
    let mut h = ClientStreamHandler::new(process.bind_addr()).await;

    // WHEN
    let res = h.send_and_get(&array(vec!["INFO", "replication"])).await;

    // THEN
    let info: Vec<&str> = res.split("\r\n").collect();

    // THEN
    assert_eq!(info[1], "role:leader");
    assert!(info[2].starts_with("leader_repl_id:"));
    assert_eq!(info[3], "high_watermark:0");
    assert_eq!(info[4], format!("self_identifier:127.0.0.1:{}", env.port));
}
