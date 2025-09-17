use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_lrange(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);
    let res = h.send_and_get(format!("RPUSH x 1 2 3"));
    assert_eq!(res, "(integer) 3");
    //WHEN & ASSERT
    assert_eq!(h.send_and_get(format!("LRANGE x 0 0")), "1) \"1\"");

    assert_eq!(
        h.send_and_get_vec(format!("LRANGE x -3 2"), 3),
        vec!["1) \"1\"", "2) \"2\"", "3) \"3\""]
    );

    assert_eq!(h.send_and_get(format!("LRANGE y 1 5")), "(empty array)"); // when accessing a non-existing key

    Ok(())
}

#[test]
fn lrange() -> anyhow::Result<()> {
    for env in [ServerEnv::default().with_append_only(true)] {
        run_lrange(env)?;
    }

    Ok(())
}
