use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_lpop(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);
    let res = h.send_and_get(format!("LPUSH test 1 2 3 4 5"));
    assert_eq!(res, "(integer) 5");

    //WHEN & ASSERT
    assert_eq!(h.send_and_get(format!("LPOP test")), "1) \"5\"");
    assert_eq!(h.send_and_get(format!("LPOP test")), "1) \"4\"");
    assert_eq!(h.send_and_get(format!("LPOP test")), "1) \"3\"");
    assert_eq!(h.send_and_get(format!("LPOP test")), "1) \"2\"");
    assert_eq!(h.send_and_get(format!("LPOP test")), "1) \"1\"");
    assert_eq!(h.send_and_get(format!("LPOP test")), "(nil)");

    // GIVEN 2
    let res = h.send_and_get(format!("LPUSH test2 1 2 3 4 5"));
    assert_eq!(res, "(integer) 5");

    //WHEN & ASSERT - 2 at a time
    assert_eq!(h.send_and_get_vec(format!("LPOP test2 2"), 2), vec!["1) \"5\"", "2) \"4\""]);
    assert_eq!(h.send_and_get_vec(format!("LPOP test2 2"), 2), vec!["1) \"3\"", "2) \"2\""]);

    Ok(())
}

#[test]
fn test_lpop() -> anyhow::Result<()> {
    for env in [ ServerEnv::default().with_append_only(true)] {
        run_lpop(env)?;
    }

    Ok(())
}
