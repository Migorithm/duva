/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.
/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_replication_info(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;
    let mut h = Client::new(process.port);

    // WHEN
    let res = h.send_and_get_vec("INFO replication", 4);

    // THEN
    assert_eq!(res[0], "role:leader");
    assert!(res[1].starts_with("leader_repl_id:"));
    assert_eq!(res[2], "last_log_index:0");
    assert_eq!(res[3], format!("self_identifier:127.0.0.1:{}", env.port));

    Ok(())
}

#[test]
fn test_replication_info() -> anyhow::Result<()> {
    for env in [ServerEnv::default().with_append_only(true)] {
        run_replication_info(env)?;
    }

    Ok(())
}
