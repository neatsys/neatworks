// taskset -c 0 bench-unreplicated
// => 384321 ops/sec
// taskset -c 0 bench-unreplicated dyn
// => 374281.9 ops/sec
// taskset -c 0 bench-unreplicated blocking
// => 449670.1 ops/sec
// bench-unreplicated blocking dual
// => 617318.2 ops/sec
// taskset -c 0 bench-unreplicated tcp (with bench-unreplicated client tcp)
// => 178405.7 ops/sec

use std::{
    collections::HashSet,
    env::args,
    future::{pending, Future},
    iter::repeat_with,
    net::SocketAddr,
    time::Duration,
};

use augustus::{
    app::Null,
    event::{
        blocking,
        erased::{self, Blanket},
        ordered::Timer,
        Inline, OnTimer, Session, Unify,
    },
    net::{
        session::{tcp_accept_session, Tcp, TcpControl, Udp},
        IndexNet,
    },
    unreplicated::{
        self, to_client_on_buf, to_replica_on_buf, Client, Replica, ReplicaEvent,
        ToClientMessageNet, ToReplicaMessageNet,
    },
    workload::{CloseLoop, Iter, OpLatency},
};
use tokio::{
    net::{TcpListener, UdpSocket},
    runtime,
    signal::ctrl_c,
    sync::mpsc::unbounded_channel,
    task::{spawn_blocking, JoinSet},
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
    tracing_subscriber::fmt::init();
    let mut args = args().skip(1).collect::<HashSet<_>>();
    let flag_client = args.remove("client");
    let flag_tcp = args.remove("tcp");
    let flag_dyn = args.remove("dyn");
    let flag_blocking = args.remove("blocking");
    let flag_dual = args.remove("dual");
    if !args.is_empty() {
        anyhow::bail!("unknown arguments {args:?}")
    }
    #[allow(clippy::nonminimal_bool)]
    if flag_client && (flag_dyn || flag_blocking)
        || flag_tcp && (flag_blocking || flag_dyn)
        || flag_dual && !flag_blocking
    {
        anyhow::bail!("invalid argument combination")
    }

    if flag_client {
        let replica_addrs = vec![SocketAddr::new([10, 0, 0, 7].into(), 4000)];
        let mut sessions = JoinSet::new();
        let (count_sender, mut count_receiver) = unbounded_channel();
        let cancel = CancellationToken::new();
        let mut runtimes = Vec::new();
        for _ in 0..5 {
            let runtime = runtime::Builder::new_multi_thread().enable_all().build()?;
            for id in repeat_with(rand::random).take(8) {
                let mut state_session = Session::<unreplicated::ClientEvent>::new();
                let mut close_loop_session = erased::Session::new();

                let mut close_loop = Blanket(erased::Unify(CloseLoop::new(
                    state_session.sender(),
                    OpLatency::new(Iter(repeat_with(Default::default))),
                )));

                if flag_tcp {
                    let listener = runtime.spawn(TcpListener::bind("10.0.0.8:0")).await??;
                    let mut tcp_session = erased::Session::new();
                    let raw_net = Tcp(erased::session::Sender::from(tcp_session.sender()));
                    let mut state = Unify(Client::new(
                        id,
                        listener.local_addr()?,
                        ToReplicaMessageNet::new(IndexNet::new(
                            raw_net.clone(),
                            replica_addrs.clone(),
                            None,
                        )),
                        erased::session::Sender::from(close_loop_session.sender()),
                    ));
                    let mut state_sender = state_session.sender();
                    let mut tcp_control = Blanket(erased::Unify(TcpControl::new(
                        move |buf: &_| to_client_on_buf(buf, &mut state_sender),
                        listener.local_addr()?,
                    )));

                    let tcp_sender = erased::session::Sender::from(tcp_session.sender());
                    sessions.spawn_on(tcp_accept_session(listener, tcp_sender), runtime.handle());
                    sessions.spawn_on(
                        async move { tcp_session.run(&mut tcp_control).await },
                        runtime.handle(),
                    );
                    sessions.spawn_on(
                        async move { state_session.run(&mut state).await },
                        runtime.handle(),
                    );
                } else {
                    let socket = runtime.spawn(UdpSocket::bind("10.0.0.8:0")).await??;
                    let addr = socket.local_addr()?;
                    let raw_net = Udp(socket.into());
                    let mut state = Unify(Client::new(
                        id,
                        addr,
                        ToReplicaMessageNet::new(IndexNet::new(
                            raw_net.clone(),
                            replica_addrs.clone(),
                            None,
                        )),
                        erased::session::Sender::from(close_loop_session.sender()),
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
                }

                let cancel = cancel.clone();
                let count_sender = count_sender.clone();
                sessions.spawn_on(
                    async move {
                        close_loop.launch()?;
                        tokio::select! {
                            result = close_loop_session.run(&mut close_loop) => result?,
                            () = cancel.cancelled() => {}
                        }
                        count_sender.send(close_loop.workload.latencies.len())?;
                        Ok(())
                    },
                    runtime.handle(),
                );
            }
            runtimes.push(runtime)
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
        for runtime in runtimes {
            runtime.shutdown_background();
        }
        println!("{} ops/sec", total_count as f32 / 10.);
        return Ok(());
    };

    if flag_blocking {
        let socket = std::net::UdpSocket::bind("0.0.0.0:4000")?;
        let raw_net = augustus::net::blocking::Udp(socket.into());
        let net = ToClientMessageNet::new(raw_net.clone());

        let mut state = Unify(Replica::new(Null, net));
        if !flag_dual {
            let recv_session = spawn_blocking(move || {
                let mut timer = Timer::new();
                loop {
                    let deadline = timer.deadline();
                    raw_net.recv(
                        |buf| to_replica_on_buf(buf, &mut Inline(&mut state, &mut timer)),
                        deadline,
                    )?;
                    state.on_timer(timer.advance()?, &mut timer)?
                }
            });
            return run(async { recv_session.await? }, pending()).await;
        }

        let (mut state_sender, state_receiver) = std::sync::mpsc::channel::<ReplicaEvent<_>>();
        let recv_session = spawn_blocking(move || {
            let mut cpu_set = rustix::process::CpuSet::new();
            cpu_set.set(0);
            rustix::process::sched_setaffinity(None, &cpu_set)?;
            raw_net.recv(move |buf| to_replica_on_buf(buf, &mut state_sender), None)
        });
        let state_session = spawn_blocking(move || {
            let mut cpu_set = rustix::process::CpuSet::new();
            cpu_set.set(1);
            rustix::process::sched_setaffinity(None, &cpu_set)?;
            blocking::run(state_receiver, &mut state)
        });
        return run(async { recv_session.await? }, async {
            state_session.await?
        })
        .await;
    }

    if flag_tcp {
        let mut tcp_session = erased::Session::new();
        let raw_net = Tcp(erased::session::Sender::from(tcp_session.sender()));
        let mut state = Unify(Replica::new(Null, ToClientMessageNet::new(raw_net)));
        let mut state_session = Session::new();
        let listener = TcpListener::bind("0.0.0.0:4000").await?;
        let mut state_sender = state_session.sender();
        let mut tcp_control = Blanket(erased::Unify(TcpControl::new(
            move |buf: &_| to_replica_on_buf(buf, &mut state_sender),
            listener.local_addr()?,
        )));

        let accept_session = tcp_accept_session(
            listener,
            erased::session::Sender::from(tcp_session.sender()),
        );
        let tcp_session = tcp_session.run(&mut tcp_control);
        let state_session = state_session.run(&mut state);
        return run(
            async {
                tokio::select! {
                    result = accept_session => result,
                    result = tcp_session => result,
                }
            },
            state_session,
        )
        .await;
    }

    let socket = UdpSocket::bind("0.0.0.0:4000").await?;
    let raw_net = Udp(socket.into());
    let net = ToClientMessageNet::new(raw_net.clone());
    if flag_dyn {
        println!("Starting replica with dynamically dispatched events and net");
        let mut state = Blanket(erased::Unify(Replica::new(Null, Box::new(net))));
        let mut state_session = erased::Session::new();
        let mut state_sender = erased::session::Sender::from(state_session.sender());
        let recv_session = raw_net.recv_session(move |buf| {
            unreplicated::erased::to_replica_on_buf(buf, &mut state_sender)
        });
        let state_session = state_session.run(&mut state);
        run(recv_session, state_session).await
    } else {
        let mut state = Unify(Replica::new(Null, net));
        let mut state_session = Session::<unreplicated::ReplicaEvent<_>>::new();
        let mut state_sender = state_session.sender();
        let recv_session =
            raw_net.recv_session(move |buf| to_replica_on_buf(buf, &mut state_sender));
        let state_session = state_session.run(&mut state);
        run(recv_session, state_session).await
    }
}

async fn run(
    recv_session: impl Future<Output = anyhow::Result<()>>,
    state_session: impl Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    tokio::select! {
        result = recv_session => result?,
        result = state_session => result?,
        result = ctrl_c() => return Ok(result?),
    }
    anyhow::bail!("unexpected exit")
}
