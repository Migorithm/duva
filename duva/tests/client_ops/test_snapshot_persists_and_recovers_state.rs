use crate::common::Client;
use crate::common::{ServerEnv, spawn_server_process};
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

async fn run_snapshot_persists_and_recovers_state(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let mut leader_process = spawn_server_process(&env).await?;

    let mut h = Client::new(leader_process.port);

    // WHEN
    // set without expiry time
    let res = h.send_and_get("SET foo bar", 1).await;
    assert_eq!(res, vec!["OK"]);

    // set with expiry time
    assert_eq!(h.send_and_get("SET foo2 bar2 PX 9999999999", 1).await, vec!["OK"]);

    // check keys
    assert_eq!(h.send_and_get("KEYS *", 2).await, vec!["0) \"foo2\"", "1) \"foo\""]);

    // check replication info
    let info = h.send_and_get("INFO replication", 4).await;

    // WHEN
    assert_eq!(h.send_and_get("SAVE", 1).await, vec!["(nil)"]);

    // kill leader process
    let _ = leader_process.terminate().await;

    // run server with the same file name
    let new_process = spawn_server_process(&env).await?;

    let mut client = Client::new(new_process.port);

    assert_eq!(client.send_and_get("KEYS *", 2).await, vec!["0) \"foo2\"", "1) \"foo\""]);

    // replication info
    let info2 = client.send_and_get("INFO replication", 4).await;

    // THEN
    assert_eq!(info, info2);

    Ok(())
}

fn create_unique_file_name(function_name: &str) -> String {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();

    format!("test_{}_{}.rdb", function_name, timestamp)
}

#[tokio::test]
async fn test_snapshot_persists_and_recovers_state() -> anyhow::Result<()> {
    for env in [
        ServerEnv::default().with_file_name(create_unique_file_name("test_save_dump")),
        ServerEnv::default()
            .with_file_name(create_unique_file_name("test_save_dump"))
            .with_append_only(true),
    ] {
        run_snapshot_persists_and_recovers_state(env).await?;
    }

    Ok(())
}
