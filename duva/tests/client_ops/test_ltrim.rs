use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_ltrim(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);
    let res = h.send_and_get(format!("RPUSH x 1 2 3"));
    assert_eq!(res, "(integer) 3");

    //WHEN & ASSERT
    assert_eq!(h.send_and_get(format!("LTRIM x 1 -1")), "OK");
    assert_eq!(h.send_and_get_vec(format!("LRANGE x 0 -1"), 2), vec!["1) \"2\"", "2) \"3\""]);

    Ok(())
}

#[test]
fn ltrim() -> anyhow::Result<()> {
    for env in [ ServerEnv::default().with_append_only(true)] {
        run_ltrim(env)?;
    }

    Ok(())
}
