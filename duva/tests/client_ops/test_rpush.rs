use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_rpush(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);

    //WHEN
    let res = h.send_and_get(format!("RPUSH test 1 2 3 4 5 6 7 8 9 10"));

    //ASSERT
    assert_eq!(res, "(integer) 10");

    Ok(())
}

#[test]
fn test_rpush() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_rpush(env)?;
    }

    Ok(())
}
