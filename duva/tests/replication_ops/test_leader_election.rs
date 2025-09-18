use crate::{
    common::{ServerEnv, form_cluster},
    replication_ops::panic_if_election_not_done,
};

fn run_leader_election(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut leader_env = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env1 = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env2 = ServerEnv::default().with_append_only(with_append_only);

    let [leader_p, follower_p1, follower_p2] =
        form_cluster([&mut leader_env, &mut follower_env1, &mut follower_env2]);

    // WHEN
    drop(leader_p);

    // THEN
    panic_if_election_not_done("first", follower_p1.port, follower_p2.port);

    Ok(())
}

#[test]
fn test_leader_election() -> anyhow::Result<()> {
    run_leader_election(true)?;

    Ok(())
}
