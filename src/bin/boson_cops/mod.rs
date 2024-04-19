use std::{mem::take, net::SocketAddr, ops::Range, time::Duration};

use augustus::{
    app::{self, ycsb, App},
    boson::{self, QuorumClient, QuorumClock, VerifyQuorumClock},
    cops::{self, DefaultVersion, DefaultVersionService},
    event::{
        self,
        erased::{events::Init, session::Sender, Blanket, Buffered, Session, Unify},
        Once, SendEvent,
    },
    net::{
        deserialize, dispatch,
        events::Recv,
        session::{tcp, Tcp},
        Dispatch, IndexNet,
    },
    pbft,
    worker::{spawning_backend, Submit},
    workload::{CloseLoop, Iter, Json, Upcall, Workload},
};
use boson_control_messages::{CopsClient, CopsServer, Variant};
use rand::{rngs::StdRng, thread_rng, SeedableRng};
use rand_distr::Uniform;
use tokio::{net::TcpListener, task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;

fn create_workload<R>(
    rng: R,
    do_put: bool,
    record_count: usize,
    put_range: Range<usize>,
) -> anyhow::Result<ycsb::Workload<R>> {
    let mut settings = if do_put {
        ycsb::WorkloadSettings::new_a
    } else {
        ycsb::WorkloadSettings::new_c
    }(record_count);
    settings.request_distr = ycsb::SettingsDistr::Uniform;
    settings.field_count = 1;
    let mut workload = ycsb::Workload::new(rng, settings)?;
    if do_put {
        workload.key_num = ycsb::Gen::Uniform(Uniform::from(put_range))
    }
    Ok(workload)
}

pub async fn pbft_client_session(
    config: CopsClient,
    upcall: impl Clone + SendEvent<(f32, Duration)> + Send + Sync + 'static,
) -> anyhow::Result<()> {
    let CopsClient {
        addrs,
        ip,
        num_concurrent,
        num_concurrent_put,
        record_count,
        put_range,
        variant: Variant::Replicated(config),
        ..
    } = config
    else {
        anyhow::bail!("unimplemented")
    };
    let num_replica = addrs.len();
    let num_faulty = config.num_faulty;

    let mut sessions = JoinSet::new();
    for index in 0..num_concurrent {
        // let tcp_listener = TcpListener::bind((ip, 0)).await?;
        let tcp_listener = TcpListener::bind(SocketAddr::from(([0; 4], 0))).await?;
        let addr = SocketAddr::from((ip, tcp_listener.local_addr()?.port()));
        // println!("client {addr} listening {:?}", tcp_listener.local_addr());
        let workload = create_workload(
            StdRng::seed_from_u64(117418 + index as u64),
            index < num_concurrent_put,
            record_count,
            put_range.clone(),
        )?;

        let mut dispatch_session = event::Session::new();
        let mut client_session = Session::new();
        let mut close_loop_session = Session::new();

        let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
            Tcp::new(addr)?,
            {
                let mut sender = Sender::from(client_session.sender());
                move |buf: &_| pbft::to_client_on_buf(buf, &mut sender)
            },
            Once(dispatch_session.sender()),
        )?));
        let mut client = Blanket(Buffered::from(pbft::Client::new(
            rand::random(),
            addr,
            pbft::ToReplicaMessageNet::new(IndexNet::new(
                dispatch::Net::from(dispatch_session.sender()),
                addrs.clone(),
                None,
            )),
            Box::new(Sender::from(close_loop_session.sender())) as Box<dyn Upcall + Send + Sync>,
            num_replica,
            num_faulty,
        )));
        let mut close_loop = Blanket(Unify(CloseLoop::new(
            Sender::from(client_session.sender()),
            Json(workload),
        )));

        let mut upcall = upcall.clone();
        sessions.spawn(async move {
            let tcp_accept_session = tcp::accept_session(tcp_listener, dispatch_session.sender());
            let dispatch_session = dispatch_session.run(&mut dispatch);
            let client_session = client_session.run(&mut client);
            // wanted to reuse this below, but cannot (easily), because the `close_loop`s are
            // substantially different: this one is
            // `CloseLoop<impl Workload<Op = Payload, Result = Payload, ...>, ...>`, while the later
            // one is `CloseLoop<impl Workload<Op = ycsb::Op, Result = ycsb::Result, ...>, ...>`
            let close_loop_session = async move {
                Sender::from(close_loop_session.sender()).send(Init)?;
                tokio::select! {
                    () = sleep(Duration::from_millis(5000)) => {}
                    result = close_loop_session.run(&mut close_loop) => result?,
                }
                close_loop.workload.as_mut().clear();
                tokio::select! {
                    () = sleep(Duration::from_millis(10000)) => {}
                    result = close_loop_session.run(&mut close_loop) => result?,
                }
                let latencies = take(close_loop.workload.as_mut());
                tokio::select! {
                    () = sleep(Duration::from_millis(5000)) => {}
                    result = close_loop_session.run(&mut close_loop) => result?,
                }
                upcall.send((
                    latencies.len() as f32 / 10.,
                    latencies.iter().sum::<Duration>() / latencies.len().max(1) as u32,
                ))
            };
            tokio::select! {
                result = tcp_accept_session => result?,
                result = dispatch_session => result?,
                result = client_session => result?,
                result = close_loop_session => return result,
            }
            anyhow::bail!("unreachable")
        });
    }
    while let Some(result) = sessions.join_next().await {
        result??
    }
    Ok(())
}

pub async fn pbft_server_session(
    config: CopsServer,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    use augustus::crypto::{Crypto, CryptoFlavor};

    let CopsServer {
        addrs,
        id,
        record_count,
        variant: Variant::Replicated(config),
    } = config
    else {
        anyhow::bail!("unimplemented")
    };
    let addr = addrs[id as usize];
    let num_replica = addrs.len();
    let num_faulty = config.num_faulty;
    let crypto = Crypto::new_hardcoded(num_replica, id, CryptoFlavor::Schnorrkel)?;
    let mut app = app::BTreeMap::new();
    {
        let mut workload =
            create_workload(StdRng::seed_from_u64(117418), false, record_count, 0..1)?;
        let mut workload = Json(Iter::<_, _, ()>::from(workload.startup_ops()));
        while let Some((op, ())) = workload.next_op()? {
            app.execute(&op)?;
        }
    }

    // let tcp_listener = TcpListener::bind(addr).await?;
    let tcp_listener = TcpListener::bind(SocketAddr::from(([0; 4], addr.port()))).await?;
    let mut dispatch_session = event::Session::new();
    let mut replica_session = Session::new();
    let (crypto_worker, mut crypto_executor) = spawning_backend();

    let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
        Tcp::new(addr)?,
        {
            let mut sender = Sender::from(replica_session.sender());
            move |buf: &_| pbft::to_replica_on_buf(buf, &mut sender)
        },
        Once(dispatch_session.sender()),
    )?));
    let mut replica = Blanket(Buffered::from(pbft::Replica::new(
        id,
        app,
        pbft::ToReplicaMessageNet::new(IndexNet::new(
            dispatch::Net::from(dispatch_session.sender()),
            addrs,
            id as usize,
        )),
        pbft::ToClientMessageNet::new(dispatch::Net::from(dispatch_session.sender())),
        Box::new(pbft::CryptoWorker::from(crypto_worker))
            as Box<dyn Submit<Crypto, dyn pbft::SendCryptoEvent<SocketAddr>> + Send + Sync>,
        num_replica,
        num_faulty,
    )));

    let crypto_session = crypto_executor.run(crypto, Sender::from(replica_session.sender()));
    let tcp_accept_session = tcp::accept_session(tcp_listener, dispatch_session.sender());
    let dispatch_session = dispatch_session.run(&mut dispatch);
    let replica_session = replica_session.run(&mut replica);
    tokio::select! {
        () = cancel.cancelled() => return Ok(()),
        result = crypto_session => result?,
        result = tcp_accept_session => result?,
        result = dispatch_session => result?,
        result = replica_session => result?,
    }
    anyhow::bail!("unreachable")
}

pub async fn untrusted_client_session(
    config: CopsClient,
    upcall: impl Clone + SendEvent<(f32, Duration)> + Send + Sync + 'static,
) -> anyhow::Result<()> {
    let CopsClient {
        addrs,
        ip,
        index,
        num_concurrent,
        num_concurrent_put,
        record_count,
        put_range,
        variant: Variant::Untrusted,
    } = config
    else {
        anyhow::bail!("unimplemented")
    };
    let replica_addr = addrs[index];

    let mut sessions = JoinSet::new();
    for index in 0..num_concurrent {
        // let tcp_listener = TcpListener::bind((ip, 0)).await?;
        let tcp_listener = TcpListener::bind(SocketAddr::from(([0; 4], 0))).await?;
        let addr = SocketAddr::from((ip, tcp_listener.local_addr()?.port()));
        let workload = create_workload(
            StdRng::seed_from_u64(117418 + index as u64),
            index < num_concurrent_put,
            record_count,
            put_range.clone(),
        )?;

        let mut dispatch_session = event::Session::new();
        let mut client_session = Session::new();
        let mut close_loop_session = Session::new();

        let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
            Tcp::new(addr)?,
            {
                let mut sender = Sender::from(client_session.sender());
                move |buf: &_| cops::to_client_on_buf(buf, &mut sender)
            },
            Once(dispatch_session.sender()),
        )?));
        let mut client = Blanket(Buffered::from(
            cops::Client::<_, _, DefaultVersion, _>::new(
                addr,
                replica_addr,
                cops::ToReplicaMessageNet::new(dispatch::Net::from(dispatch_session.sender())),
                Box::new(Sender::from(close_loop_session.sender()))
                    as Box<dyn cops::Upcall + Send + Sync>,
            ),
        ));
        let mut close_loop = Blanket(Unify(CloseLoop::new(
            Sender::from(client_session.sender()),
            workload,
        )));

        let mut upcall = upcall.clone();
        sessions.spawn(async move {
            let tcp_accept_session = tcp::accept_session(tcp_listener, dispatch_session.sender());
            let dispatch_session = dispatch_session.run(&mut dispatch);
            let client_session = client_session.run(&mut client);
            let close_loop_session = async move {
                Sender::from(close_loop_session.sender()).send(Init)?;
                tokio::select! {
                    () = sleep(Duration::from_millis(5000)) => {}
                    result = close_loop_session.run(&mut close_loop) => result?,
                }
                close_loop.workload.as_mut().clear();
                tokio::select! {
                    () = sleep(Duration::from_millis(10000)) => {}
                    result = close_loop_session.run(&mut close_loop) => result?,
                }
                let latencies = take(close_loop.workload.as_mut());
                tokio::select! {
                    () = sleep(Duration::from_millis(5000)) => {}
                    result = close_loop_session.run(&mut close_loop) => result?,
                }
                upcall.send((
                    latencies.len() as f32 / 10.,
                    latencies.iter().sum::<Duration>() / latencies.len().max(1) as u32,
                ))
            };
            tokio::select! {
                result = tcp_accept_session => result?,
                result = dispatch_session => result?,
                result = client_session => result?,
                result = close_loop_session => return result,
            }
            anyhow::bail!("unreachable")
        });
    }
    while let Some(result) = sessions.join_next().await {
        result??
    }
    Ok(())
}

pub async fn untrusted_server_session(
    config: CopsServer,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let CopsServer {
        addrs,
        id,
        record_count,
        variant: Variant::Untrusted,
    } = config
    else {
        anyhow::bail!("unimplemented")
    };
    let addr = addrs[id as usize];

    // let tcp_listener = TcpListener::bind(addr).await?;
    let tcp_listener = TcpListener::bind(SocketAddr::from(([0; 4], addr.port()))).await?;
    let mut dispatch_session = event::Session::new();
    let mut replica_session = Session::new();

    let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
        Tcp::new(addr)?,
        {
            let mut sender = Sender::from(replica_session.sender());
            move |buf: &_| cops::to_replica_on_buf(buf, &mut sender)
        },
        Once(dispatch_session.sender()),
    )?));
    let mut replica = Blanket(Buffered::from(
        cops::Replica::<_, _, _, _, SocketAddr>::new(
            DefaultVersion::default(),
            cops::ToReplicaMessageNet::<_, _, SocketAddr>::new(IndexNet::new(
                dispatch::Net::from(dispatch_session.sender()),
                addrs,
                id as usize,
            )),
            cops::ToClientMessageNet::new(dispatch::Net::from(dispatch_session.sender())),
            DefaultVersionService(Box::new(Sender::from(replica_session.sender()))
                as Box<dyn SendEvent<cops::events::UpdateOk<DefaultVersion>> + Send + Sync>),
        ),
    ));
    {
        let mut workload =
            create_workload(StdRng::seed_from_u64(117418), false, record_count, 0..1)?;
        let mut workload = Iter::<_, _, ()>::from(workload.startup_ops());
        while let Some((op, ())) = workload.next_op()? {
            replica.startup_insert(op)?
        }
    }

    let tcp_accept_session = tcp::accept_session(tcp_listener, dispatch_session.sender());
    let dispatch_session = dispatch_session.run(&mut dispatch);
    let replica_session = replica_session.run(&mut replica);
    tokio::select! {
        () = cancel.cancelled() => return Ok(()),
        result = tcp_accept_session => result?,
        result = dispatch_session => result?,
        result = replica_session => result?,
    }
    anyhow::bail!("unreachable")
}

pub async fn quorum_client_session(
    config: CopsClient,
    upcall: impl Clone + SendEvent<(f32, Duration)> + Send + Sync + 'static,
) -> anyhow::Result<()> {
    // use augustus::crypto::peer::Crypto;

    let CopsClient {
        addrs,
        ip,
        index,
        num_concurrent,
        num_concurrent_put,
        record_count,
        put_range,
        // variant: Variant::Quorum(config),
        variant: Variant::Quorum(_),
    } = config
    else {
        anyhow::bail!("unimplemented")
    };
    let replica_addr = addrs[index];
    // let crypto = Crypto::new_random(&mut thread_rng());

    let mut sessions = JoinSet::new();
    for index in 0..num_concurrent {
        // let tcp_listener = TcpListener::bind((ip, 0)).await?;
        let tcp_listener = TcpListener::bind(SocketAddr::from(([0; 4], 0))).await?;
        let addr = SocketAddr::from((ip, tcp_listener.local_addr()?.port()));
        let workload = create_workload(
            StdRng::seed_from_u64(117418 + index as u64),
            index < num_concurrent_put,
            record_count,
            put_range.clone(),
        )?;

        let mut dispatch_session = event::Session::new();
        let mut client_session = Session::new();
        let mut close_loop_session = Session::new();
        // let (crypto_worker, mut crypto_executor) = spawning_backend();

        let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
            Tcp::new(addr)?,
            {
                // let mut sender = boson::VerifyQuorumClock::new(config.num_faulty, crypto_worker);
                let mut sender = Sender::from(client_session.sender());
                move |buf: &_| cops::to_client_on_buf(buf, &mut sender)
            },
            Once(dispatch_session.sender()),
        )?));
        let mut client = Blanket(Buffered::from(
            cops::Client::<_, _, boson::QuorumClock, _>::new(
                addr,
                replica_addr,
                cops::ToReplicaMessageNet::new(dispatch::Net::from(dispatch_session.sender())),
                Box::new(Sender::from(close_loop_session.sender()))
                    as Box<dyn cops::Upcall + Send + Sync>,
            ),
        ));
        let mut close_loop = Blanket(Unify(CloseLoop::new(
            Sender::from(client_session.sender()),
            workload,
        )));

        let mut upcall = upcall.clone();
        // let crypto = crypto.clone();
        sessions.spawn(async move {
            let tcp_accept_session = tcp::accept_session(tcp_listener, dispatch_session.sender());
            // let crypto_session = crypto_executor.run(crypto, Sender::from(client_session.sender()));
            let dispatch_session = dispatch_session.run(&mut dispatch);
            let client_session = client_session.run(&mut client);
            let close_loop_session = async move {
                Sender::from(close_loop_session.sender()).send(Init)?;
                tokio::select! {
                    () = sleep(Duration::from_millis(5000)) => {}
                    result = close_loop_session.run(&mut close_loop) => result?,
                }
                close_loop.workload.as_mut().clear();
                tokio::select! {
                    () = sleep(Duration::from_millis(10000)) => {}
                    result = close_loop_session.run(&mut close_loop) => result?,
                }
                let latencies = take(close_loop.workload.as_mut());
                tokio::select! {
                    () = sleep(Duration::from_millis(5000)) => {}
                    result = close_loop_session.run(&mut close_loop) => result?,
                }
                upcall.send((
                    latencies.len() as f32 / 10.,
                    latencies.iter().sum::<Duration>() / latencies.len().max(1) as u32,
                ))
            };
            tokio::select! {
                result = tcp_accept_session => result?,
                // result = crypto_session => result?,
                result = dispatch_session => result?,
                result = client_session => result?,
                result = close_loop_session => return result,
            }
            anyhow::bail!("unreachable")
        });
    }
    while let Some(result) = sessions.join_next().await {
        result??
    }
    Ok(())
}

pub async fn quorum_server_session(
    config: CopsServer,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    use augustus::crypto::peer::{Crypto, Verifiable};

    let CopsServer {
        addrs,
        id,
        record_count,
        variant: Variant::Quorum(config),
    } = config
    else {
        anyhow::bail!("unimplemented")
    };
    let addr = addrs[id as usize];
    // replica also don't sign any clock
    let crypto = Crypto::new_random(&mut thread_rng());

    // let tcp_listener = TcpListener::bind(addr).await?;
    let tcp_listener = TcpListener::bind(SocketAddr::from(([0; 4], addr.port()))).await?;
    // let clock_tcp_listener = TcpListener::bind((addr.ip(), 0)).await?;
    let clock_tcp_listener = TcpListener::bind(SocketAddr::from(([0; 4], 0))).await?;
    let clock_addr = SocketAddr::from((addr.ip(), clock_tcp_listener.local_addr()?.port()));

    let mut dispatch_session = event::Session::new();
    let mut clock_dispatch_session = event::Session::new();
    let mut replica_session = Session::new();
    let mut clock_session = Session::new();
    let (client_crypto_worker, mut client_crypto_executor) = spawning_backend();
    let (crypto_worker, mut crypto_executor) = spawning_backend();

    let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
        Tcp::new(addr)?,
        {
            let mut sender = VerifyQuorumClock::new(config.num_faulty, client_crypto_worker);
            move |buf: &_| cops::to_replica_on_buf(buf, &mut sender)
        },
        Once(dispatch_session.sender()),
    )?));
    let mut clock_dispatch = event::Unify(event::Buffered::from(Dispatch::new(
        Tcp::new(clock_addr)?,
        {
            let mut sender = Sender::from(clock_session.sender());
            move |buf: &_| sender.send(Recv(deserialize::<Verifiable<boson::AnnounceOk>>(buf)?))
        },
        Once(clock_dispatch_session.sender()),
    )?));
    let mut replica = Blanket(Buffered::from(
        cops::Replica::<_, _, _, _, SocketAddr>::new(
            QuorumClock::default(),
            cops::ToReplicaMessageNet::<_, _, SocketAddr>::new(IndexNet::new(
                dispatch::Net::from(dispatch_session.sender()),
                addrs,
                id as usize,
            )),
            cops::ToClientMessageNet::new(dispatch::Net::from(dispatch_session.sender())),
            boson::Cops(Sender::from(clock_session.sender())),
        ),
    ));
    {
        let mut workload =
            create_workload(StdRng::seed_from_u64(117418), false, record_count, 0..1)?;
        let mut workload = Iter::<_, _, ()>::from(workload.startup_ops());
        while let Some((op, ())) = workload.next_op()? {
            replica.startup_insert(op)?
        }
    }
    let mut clock = Blanket(Unify(QuorumClient::new(
        clock_addr,
        crypto.public_key(),
        config.num_faulty,
        Box::new(boson::quorum_client::CryptoWorker::from(crypto_worker))
            as Box<
                dyn Submit<Crypto, dyn boson::quorum_client::SendCryptoEvent<SocketAddr>>
                    + Send
                    + Sync,
            >,
        boson::Cops(Box::new(Sender::from(replica_session.sender()))
            as Box<
                dyn SendEvent<cops::events::UpdateOk<QuorumClock>> + Send + Sync,
            >),
        augustus::net::MessageNet::<_, Verifiable<boson::Announce<SocketAddr>>>::new(
            IndexNet::new(
                dispatch::Net::from(clock_dispatch_session.sender()),
                config.addrs,
                None,
            ),
        ),
    )));

    let tcp_accept_session = tcp::accept_session(tcp_listener, dispatch_session.sender());
    let clock_tcp_accept_session =
        tcp::accept_session(clock_tcp_listener, clock_dispatch_session.sender());
    let client_crypto_session =
        client_crypto_executor.run(crypto.clone(), Sender::from(replica_session.sender()));
    let crypto_session = crypto_executor.run(crypto, Sender::from(clock_session.sender()));
    let dispatch_session = dispatch_session.run(&mut dispatch);
    let clock_dispatch_session = clock_dispatch_session.run(&mut clock_dispatch);
    let replica_session = replica_session.run(&mut replica);
    let clock_session = clock_session.run(&mut clock);
    tokio::select! {
        () = cancel.cancelled() => return Ok(()),
        result = tcp_accept_session => result?,
        result = clock_tcp_accept_session => result?,
        result = client_crypto_session => result?,
        result = crypto_session => result?,
        result = dispatch_session => result?,
        result = clock_dispatch_session => result?,
        result = clock_session => result?,
        result = replica_session => result?,
    }
    anyhow::bail!("unreachable")
}

// cSpell:words pbft upcall
