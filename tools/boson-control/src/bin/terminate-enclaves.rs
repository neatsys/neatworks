use tokio::task::JoinSet;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let output = boson_control::terraform_output().await?;
    let mut sessions = JoinSet::new();
    for instance in [&output.ap, &output.us, &output.eu, &output.sa, &output.af]
        .into_iter()
        .flat_map(|region| region.mutex.iter().chain(&region.cops))
    {
        sessions.spawn(boson_control::ssh(
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
