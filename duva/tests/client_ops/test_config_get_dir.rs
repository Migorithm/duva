/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
use std::time::Duration;

use tokio::time::sleep;

use crate::common::{Client, ServerEnv, spawn_server_process};

#[tokio::test]
async fn test_config_get_dir() -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env).await?;

    sleep(Duration::from_millis(500)).await;
    let mut h = Client::new(process.port);

    // WHEN
    let res = h.send_and_get("CONFIG get dir", 1).await;

    // THEN
    assert_eq!(res.first().unwrap(), "dir .");

    Ok(())
}
