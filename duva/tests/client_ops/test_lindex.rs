use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_lindex(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);
    let res = h.send_and_get(format!("RPUSH x 1 2 3"));
    assert_eq!(res, "(integer) 3");
    //WHEN & ASSERT
    assert_eq!(h.send_and_get(format!("LINDEX x 0 ")), "(integer) 1");

    assert_eq!(h.send_and_get(format!("LINDEX x 1")), "(integer) 2");
    assert_eq!(h.send_and_get(format!("LINDEX x -1")), "(integer) 3");

    Ok(())
}

#[test]
fn test_lindex() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_lindex(env)?;
    }

    Ok(())
}
