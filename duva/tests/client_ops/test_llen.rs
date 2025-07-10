use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_llen(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);
    assert_eq!(h.send_and_get(format!("lpush test 1 2 3 4 5 6 7 8 9 10"),), "(integer) 10");

    //WHEN & ASSERT
    assert_eq!(h.send_and_get(format!("LLEN test"),), "(integer) 10");
    assert_eq!(h.send_and_get(format!("LLEN test2"),), "(integer) 0");

    Ok(())
}

#[test]
fn test_llen() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_llen(env)?;
    }

    Ok(())
}
