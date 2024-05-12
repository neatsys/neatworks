use tokio::task::JoinSet;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let output = cover_control::terraform_output().await?;
    let mut sessions = JoinSet::new();
    for instance in output
        .regions
        .values()
        .flat_map(|region| region.mutex.iter().chain(&region.cops))
    {
        sessions.spawn(cover_control::ssh(
            format!("ec2-user@{}", instance.public_dns),
            "nitro-cli terminate-enclave --all",
        ));
    }

    let mut the_result = Ok(());
    while let Some(result) = sessions.join_next().await {
        the_result = the_result.and_then(|_| anyhow::Ok(result??))
    }
    the_result
}
