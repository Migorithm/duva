/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
use std::{thread::sleep, time::Duration};

use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_config_get_dir(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env, false)?;

    sleep(Duration::from_millis(500));
    let mut h = Client::new(process.port);

    // WHEN
    let res = h.send_and_get("CONFIG get dir");

    // THEN
    assert_eq!(res, format!("dir {}", env.dir.path().display()));

    Ok(())
}

#[test]
fn test_config_get_dir() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_config_get_dir(env)?;
    }

    Ok(())
}
