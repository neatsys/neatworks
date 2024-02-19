use std::{
    env::args, future::Future, iter::repeat_with, net::SocketAddr, pin::Pin, time::Duration,
};

use augustus::{
    app::Null,
    event::{erased, Session},
    net::Udp,
    replication::{Concurrent, ReplicaNet},
    unreplicated::{
        self, to_client_on_buf, Client, Replica, ToClientMessageNet, ToReplicaMessageNet,
    },
};
use tokio::{net::UdpSocket, runtime, signal::ctrl_c, task::JoinSet, time::sleep};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let role = args().nth(1);
    if role.as_deref() == Some("replica") {
        // replica mode
        let socket = UdpSocket::bind("0.0.0.0:4000").await?;
        let raw_net = Udp(socket.into());
        let net = ToClientMessageNet::new(raw_net.clone());

        let mode = args().nth(2);
        let recv_session;
        let state_session = if mode.as_deref() == Some("erased") {
            let mut state = unreplicated::erased::Replica::new(Null, Box::new(net));
            let mut state_session = erased::Session::new();
            let mut state_sender = state_session.erased_sender();
            recv_session = Box::pin(raw_net.recv_session(move |buf| {
                unreplicated::erased::to_replica_on_buf(buf, &mut state_sender)
            })) as Pin<Box<dyn Future<Output = anyhow::Result<()>>>>;
            Box::pin(async move { state_session.erased_run(&mut state).await })
                as Pin<Box<dyn Future<Output = anyhow::Result<()>>>>
        } else {
            let mut state = Replica::new(Null, net);
            let mut state_session = Session::new();
            let mut state_sender = state_session.sender();
            recv_session =
                Box::pin(raw_net.recv_session(move |buf| {
                    unreplicated::to_replica_on_buf(buf, &mut state_sender)
                }));
            Box::pin(async move { state_session.run(&mut state).await })
        };
        'select: {
            tokio::select! {
                result = recv_session => result?,
                result = state_session => result?,
                result = ctrl_c() => break 'select result?,
            }
            anyhow::bail!("unexpected exit")
        }
        return Ok(());
    };

    let replica_addrs = vec![SocketAddr::new([10, 0, 0, 1].into(), 4000)];
    let mut concurrent = Concurrent::new();
    let mut concurrent_session = erased::Session::new();
    let mut state_sessions = JoinSet::new();
    let state_runtime = runtime::Builder::new_multi_thread().enable_all().build()?;
    for id in repeat_with(rand::random).take(1) {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let addr = SocketAddr::new([10, 0, 0, 2].into(), socket.local_addr()?.port());
        let raw_net = Udp(socket.into());
        let mut state = Client::new(
            id,
            addr,
            ToReplicaMessageNet::new(ReplicaNet::new(
                raw_net.clone(),
                replica_addrs.clone(),
                None,
            )),
            concurrent_session.erased_sender(),
        );
        let mut state_session = Session::new();
        concurrent.insert_client_sender(id, state_session.sender())?;
        let mut state_sender = state_session.sender();
        state_sessions.spawn_on(
            async move {
                raw_net
                    .recv_session(|buf| to_client_on_buf(buf, &mut state_sender))
                    .await
            },
            state_runtime.handle(),
        );
        state_sessions.spawn_on(
            async move { state_session.run(&mut state).await },
            state_runtime.handle(),
        );
    }
    concurrent.launch()?;
    let concurrent_session = concurrent_session.erased_run(&mut concurrent);
    'select: {
        tokio::select! {
            Some(result) = state_sessions.join_next() => result??,
            result = concurrent_session => result?,
            () = sleep(Duration::from_secs(10)) => break 'select,
        }
    }
    state_runtime.shutdown_background();
    // TODO
    Ok(())
}
