use std::{
    iter::repeat_with,
    mem::take,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use augustus::{
    app::ycsb,
    event::{
        erased::{events::Init, session::Sender, Blanket, Buffered, Session, Unify},
        SendEvent,
    },
    net::{session::Udp, IndexNet, Payload},
    pbft, unreplicated,
    workload::{
        events::{Invoke, InvokeOk},
        CloseLoop, Iter, Json, OpLatency, Workload,
    },
};
use rand::{rngs::StdRng, SeedableRng as _};
use replication_control_messages::{App, BenchmarkResult, ClientConfig, Protocol};
use tokio::{
    net::UdpSocket,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Barrier,
    },
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;

pub async fn session(
    config: ClientConfig,
    benchmark_result: Arc<Mutex<Option<BenchmarkResult>>>,
) -> anyhow::Result<()> {
    let mut sessions = JoinSet::new();
    let barrier = Arc::new(Barrier::new(config.num_close_loop + 1));
    let stop = CancellationToken::new();
    let latencies = Arc::new(Mutex::new(Vec::new()));

    #[allow(clippy::large_enum_variant)]
    enum Workload {
        Null,
        Ycsb(ycsb::Workload<StdRng>),
    }
    let workload = match &config.app {
        App::Null => Workload::Null,
        App::Ycsb(config) => {
            use replication_control_messages::YcsbProfile::*;
            let workload = ycsb::Workload::new(
                StdRng::seed_from_u64(117418),
                match &config.profile {
                    A => ycsb::WorkloadSettings::new_a,
                    B => ycsb::WorkloadSettings::new_b,
                    C => ycsb::WorkloadSettings::new_c,
                    D => ycsb::WorkloadSettings::new_d,
                    E => ycsb::WorkloadSettings::new_e,
                    F => ycsb::WorkloadSettings::new_f,
                }(config.record_count),
            )?;
            Workload::Ycsb(workload)
        }
    };

    for client_id in repeat_with(rand::random).take(config.num_close_loop) {
        let socket = UdpSocket::bind("localhost:0").await?; // TODO
        let addr = socket.local_addr()?;
        let net = Udp(socket.into());
        let (invoke_sender, invoke_receiver) = unbounded_channel();
        let (upcall_sender, upcall_receiver) = unbounded_channel();
        match config.protocol {
            Protocol::Unreplicated => sessions.spawn(unreplicated_session(
                config.clone(),
                invoke_receiver,
                upcall_sender,
                client_id,
                addr,
                net,
            )),
            Protocol::Pbft => sessions.spawn(pbft_session(
                config.clone(),
                invoke_receiver,
                upcall_sender,
                client_id,
                addr,
                net,
            )),
        };
        match &workload {
            Workload::Null => sessions.spawn(close_loop_session(
                OpLatency::new(Iter::from(repeat_with(Default::default))),
                |workload| workload.as_mut(),
                upcall_receiver,
                invoke_sender,
                barrier.clone(),
                stop.clone(),
                latencies.clone(),
            )),
            Workload::Ycsb(workload) => sessions.spawn(close_loop_session(
                Json(ycsb::Destruct::from(OpLatency::new(
                    workload.clone_reseed(StdRng::seed_from_u64(client_id as _)),
                ))),
                |workload| workload.as_mut(),
                upcall_receiver,
                invoke_sender,
                barrier.clone(),
                stop.clone(),
                latencies.clone(),
            )),
        };
    }

    let task = async {
        barrier.wait().await;
        sleep(Duration::from_millis(3000)).await
    };
    'select: {
        tokio::select! {
            () = task => break 'select,
            Some(result) = sessions.join_next() => result??,
        }
        anyhow::bail!("unreachable")
    }
    stop.cancel();
    while let Some(result) = sessions.join_next().await {
        result??
    }
    let latencies = Arc::into_inner(latencies)
        .unwrap()
        .into_inner()
        .map_err(|err| anyhow::format_err!("{err}"))?;
    let result = BenchmarkResult {
        throughput: latencies.len() as f32 / 3.,
        latency: latencies.iter().sum::<Duration>() / latencies.len() as u32,
    };
    let replaced = benchmark_result
        .lock()
        .map_err(|err| anyhow::format_err!("{err}"))?
        .replace(result);
    anyhow::ensure!(replaced.is_none());
    Ok(())
}

async fn close_loop_session<W: Workload<Op = Payload, Result = Payload> + 'static>(
    workload: W,
    as_latencies: impl Fn(&mut W) -> &mut Vec<Duration>,
    mut upcall: UnboundedReceiver<InvokeOk>,
    invoke: UnboundedSender<Invoke>,
    barrier: Arc<Barrier>,
    stop: CancellationToken,
    latencies: Arc<Mutex<Vec<Duration>>>,
) -> anyhow::Result<()> {
    let mut session = Session::new();

    let mut close_loop = Blanket(Unify(CloseLoop::new(invoke, workload)));

    let mut init_sender = Sender::from(session.sender());
    let upcall_session = {
        let mut sender = Sender::from(session.sender());
        async move {
            while let Some(invoke_ok) = upcall.recv().await {
                sender.send(invoke_ok)?
            }
            anyhow::Ok(())
        }
    };
    let session = session.run(&mut close_loop);

    barrier.wait().await;
    init_sender.send(Init)?;
    'select: {
        tokio::select! {
            () = stop.cancelled() => break 'select,
            result = upcall_session => result?,
            result = session => result?,
        }
        anyhow::bail!("unreachable")
    }
    latencies
        .lock()
        .map_err(|err| anyhow::format_err!("{err}"))?
        .extend(take(as_latencies(&mut close_loop.workload)));
    Ok(())
}

async fn unreplicated_session(
    config: ClientConfig,
    mut invoke: UnboundedReceiver<Invoke>,
    upcall: UnboundedSender<InvokeOk>,
    id: u32,
    addr: SocketAddr,
    net: Udp,
) -> anyhow::Result<()> {
    let mut state_session = Session::new();

    let mut state = Blanket(Unify(unreplicated::Client::new(
        id,
        addr,
        unreplicated::ToReplicaMessageNet::new(IndexNet::new(
            net.clone(),
            config.replica_addrs,
            None,
        )),
        upcall,
    )));

    let recv_session = net.recv_session({
        let mut state_sender = Sender::from(state_session.sender());
        move |buf: &_| unreplicated::erased::to_client_on_buf(buf, &mut state_sender)
    });
    let invoke_session = {
        let mut state_sender = Sender::from(state_session.sender());
        async move {
            while let Some(invoke) = invoke.recv().await {
                state_sender.send(invoke)?
            }
            anyhow::Ok(())
        }
    };
    let state_session = state_session.run(&mut state);

    tokio::select! {
        result = recv_session => result?,
        result = invoke_session => return result,
        result = state_session => result?,
    }
    anyhow::bail!("unreachable")
}

async fn pbft_session(
    config: ClientConfig,
    mut invoke: UnboundedReceiver<Invoke>,
    upcall: UnboundedSender<InvokeOk>,
    id: u32,
    addr: SocketAddr,
    net: Udp,
) -> anyhow::Result<()> {
    let mut state_session = Session::new();

    let mut state = Blanket(Buffered::from(pbft::Client::new(
        id,
        addr,
        pbft::ToReplicaMessageNet::new(IndexNet::new(net.clone(), config.replica_addrs, None)),
        upcall,
        config.num_replica,
        config.num_faulty,
    )));

    let recv_session = net.recv_session({
        let mut state_sender = Sender::from(state_session.sender());
        move |buf: &_| pbft::to_client_on_buf(buf, &mut state_sender)
    });
    let invoke_session = {
        let mut state_sender = Sender::from(state_session.sender());
        async move {
            while let Some(invoke) = invoke.recv().await {
                state_sender.send(invoke)?
            }
            anyhow::Ok(())
        }
    };
    let state_session = state_session.run(&mut state);

    tokio::select! {
        result = recv_session => result?,
        result = invoke_session => return result,
        result = state_session => result?,
    }
    anyhow::bail!("unreachable")
}
