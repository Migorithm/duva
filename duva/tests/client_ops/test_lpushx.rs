// Inserts specified values at the head of the list stored at key, only if key already exists and holds a list.

use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_lpushx(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);

    //WHEN & THEN
    assert_eq!(h.send_and_get(format!("LPUSH test 1")), "(integer) 1");
    assert_eq!(h.send_and_get(format!("LPUSHX test 2")), "(integer) 2");
    assert_eq!(h.send_and_get(format!("LPUSHX test2 1")), "(integer) 0");

    Ok(())
}

#[test]
fn test_lpushx() -> anyhow::Result<()> {
    for env in [ ServerEnv::default().with_append_only(true)] {
        run_lpushx(env)?;
    }

    Ok(())
}
