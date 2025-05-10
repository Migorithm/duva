use crate::common::Client;
use crate::common::{ServerEnv, spawn_server_process};
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

fn run_snapshot_persists_and_recovers_state(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let mut leader_process = spawn_server_process(&env, false)?;

    let mut h = Client::new(leader_process.port);

    // WHEN
    let res = h.send_and_get("SET foo bar", 1);
    assert_eq!(res, vec!["OK"]);
    assert_eq!(h.send_and_get("SET foo2 bar2 PX 9999999999", 1), vec!["OK"]);
    assert_eq!(h.send_and_get("KEYS *", 2), vec!["0) \"foo2\"", "1) \"foo\""]);

    // pre load replication info for comparison
    let old_info = h.send_and_get("INFO replication", 4);

    // WHEN
    assert_eq!(h.send_and_get("SAVE", 1), vec!["(nil)"]);

    // kill leader process
    let _ = leader_process.terminate();

    // run server with the same file name
    let new_process = spawn_server_process(&env, false)?;

    let mut client = Client::new(new_process.port);
    assert_eq!(client.send_and_get("KEYS *", 2), vec!["0) \"foo2\"", "1) \"foo\""]);

    // replication info
    let new_info = client.send_and_get("INFO replication", 4);

    // THEN
    assert_eq!(old_info, new_info);

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
        run_snapshot_persists_and_recovers_state(env)?;
    }

    Ok(())
}
