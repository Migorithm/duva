use crate::common::{Client, ServerEnv, form_cluster, spawn_server_process};

fn run_decr(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut env = ServerEnv::default().with_append_only(with_append_only);
    let mut env2 = ServerEnv::default().with_append_only(with_append_only);

    let [leader_p, follower_p] = form_cluster([&mut env, &mut env2], false);

    let mut h = Client::new(leader_p.port);
    let mut h2: Client = Client::new(follower_p.port);

    // WHEN & THEN - happy case
    assert_eq!(h.send_and_get("DECR a", 1), vec!["(integer) -1"]);
    assert_eq!(h.send_and_get("DECR a", 1), vec!["(integer) -2"]);
    assert_eq!(h.send_and_get("DECR a", 1), vec!["(integer) -3"]);
    assert_eq!(h.send_and_get("GET a", 1), vec!["-3"]);

    // THEN & THEN - increase and decrease
    assert_eq!(h.send_and_get("INCR b", 1), vec!["(integer) 1"]);
    assert_eq!(h.send_and_get("DECR b", 1), vec!["(integer) 0"]);

    // WHEN & THEN- operation on string
    assert_eq!(h.send_and_get("SET c adsds", 1), vec!["OK"]);
    assert_eq!(
        h.send_and_get("DECR c", 1),
        vec!["(error) ERR value is not an integer or out of range"]
    );

    // WHEN & THEN- out of range
    assert_eq!(h.send_and_get("SET d 92233720368547332375808", 1), vec!["OK"]);
    assert_eq!(
        h.send_and_get("DECR d", 1),
        vec!["(error) ERR value is not an integer or out of range"]
    );

    // WHEN & THEN- getting values from follower
    assert_eq!(h2.send_and_get("GET a", 1), vec!["-3"]);
    assert_eq!(h2.send_and_get("GET b", 1), vec!["0"]);
    assert_eq!(h2.send_and_get("GET c", 1), vec!["adsds"]);
    assert_eq!(h2.send_and_get("GET d", 1), vec!["92233720368547332375808"]);

    Ok(())
}

#[test]
fn test_decr() -> anyhow::Result<()> {
    run_decr(false)?;
    run_decr(true)?;

    Ok(())
}
