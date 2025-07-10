use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_llen(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);
    h.send_and_get(format!("set setkey 1"));
    assert_eq!(h.send_and_get(format!("lpush x 1 2 3 4 5 6 7 8 9 10"),), "(integer) 10");

    //WHEN & ASSERT
    assert_eq!(h.send_and_get(format!("LLEN x"),), "(integer) 10");
    assert_eq!(h.send_and_get(format!("LLEN y"),), "(integer) 0"); // when accessing a non-existing key

    assert_eq!(h.send_and_get(format!("LLEN setkey"),), ""); // when accessing a key that is not a list

    Ok(())
}

#[test]
fn test_llen() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_llen(env)?;
    }

    Ok(())
}
