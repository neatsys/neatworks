use std::{
    env::args, future::Future, iter::repeat_with, net::SocketAddr, pin::Pin, time::Duration,
};

use augustus::{
    app::Null,
    event::{
        erased::{self, Blanket},
        Session,
    },
    net::{tokio::Udp, IndexNet},
    unreplicated::{
        self, to_client_on_buf, to_replica_on_buf, Client, Replica, ToClientMessageNet,
        ToReplicaMessageNet,
    },
    workload::{CloseLoop, Iter, OpLatency},
};
use tokio::{
    net::UdpSocket, runtime, signal::ctrl_c, sync::mpsc::unbounded_channel, task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;

// #[cfg(not(target_env = "msvc"))]
// use tikv_jemallocator::Jemalloc;

// #[cfg(not(target_env = "msvc"))]
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

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
        let state_session = if mode.as_deref() == Some("boxed") {
            println!("Starting replica with boxed events and net");
            let mut state = Blanket(Replica::new(Null, Box::new(net)));
            let mut state_session = erased::Session::new();
            let mut state_sender = state_session.erased_sender();
            recv_session = Box::pin(raw_net.recv_session(move |buf| {
                unreplicated::erased::to_replica_on_buf(buf, &mut state_sender)
            })) as Pin<Box<dyn Future<Output = anyhow::Result<()>>>>;
            Box::pin(async move { state_session.erased_run(&mut state).await })
                as Pin<Box<dyn Future<Output = anyhow::Result<()>>>>
        } else {
            let mut state = Replica::new(Null, net);
            let mut state_session = Session::<unreplicated::ReplicaEvent<_>>::new();
            let mut state_sender = state_session.sender();
            recv_session = Box::pin(
                raw_net.recv_session(move |buf| to_replica_on_buf(buf, &mut state_sender)),
            );
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

    let replica_addrs = vec![SocketAddr::new([127, 0, 0, 1].into(), 4000)];
    let mut sessions = JoinSet::new();
    let runtime = runtime::Builder::new_multi_thread().enable_all().build()?;
    let (count_sender, mut count_receiver) = unbounded_channel();
    let cancel = CancellationToken::new();
    for id in repeat_with(rand::random).take(40) {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let addr = SocketAddr::new([127, 0, 0, 101].into(), socket.local_addr()?.port());
        let raw_net = Udp(socket.into());

        let mut state_session = Session::<unreplicated::ClientEvent>::new();
        let mut close_loop_session = erased::Session::new();

        let mut state = Client::new(
            id,
            addr,
            ToReplicaMessageNet::new(IndexNet::new(raw_net.clone(), replica_addrs.clone(), None)),
            close_loop_session.erased_sender(),
        );
        let mut close_loop = Blanket(CloseLoop::new(
            state_session.sender(),
            OpLatency::new(Iter(repeat_with(Default::default))),
        ));
        let mut state_sender = state_session.sender();
        sessions.spawn_on(
            async move {
                raw_net
                    .recv_session(|buf| to_client_on_buf(buf, &mut state_sender))
                    .await
            },
            runtime.handle(),
        );
        sessions.spawn_on(
            async move { state_session.run(&mut state).await },
            runtime.handle(),
        );
        let cancel = cancel.clone();
        let count_sender = count_sender.clone();
        sessions.spawn_on(
            async move {
                close_loop.launch()?;
                tokio::select! {
                    result = close_loop_session.erased_run(&mut close_loop) => result?,
                    () = cancel.cancelled() => {}
                }
                let _ = count_sender.send(close_loop.workload.latencies.len());
                Ok(())
            },
            runtime.handle(),
        );
    }
    'select: {
        tokio::select! {
            Some(result) = sessions.join_next() => result??,
            () = sleep(Duration::from_secs(10)) => break 'select,
        }
        anyhow::bail!("unexpected shutdown")
    }
    cancel.cancel();
    drop(count_sender);
    let mut total_count = 0;
    while let Some(count) = count_receiver.recv().await {
        total_count += count
    }
    runtime.shutdown_background();
    println!("{} ops/sec", total_count as f32 / 10.);
    Ok(())
}
