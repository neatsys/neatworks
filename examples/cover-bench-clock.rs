use std::{
    fmt::Write,
    future::pending,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use augustus::{
    cover::{
        self, nitro_enclaves_portal_session, NitroEnclavesClock, QuorumClient, QuorumClock, Update,
        UpdateOk,
    },
    cops::OrdinaryVersion,
    crypto::peer::{Crypto, Verifiable},
    event::{
        self,
        erased::{session::Sender, Blanket, Session, Unify},
        Once, SendEvent as _,
    },
    net::{
        deserialize, dispatch,
        events::Recv,
        session::{tcp, Tcp},
        Dispatch, IndexNet,
    },
    worker::{spawning_backend, Submit},
};
use cover_control_messages::{Microbench, Quorum, Variant};
use rand::thread_rng;
use tokio::{
    io::AsyncReadExt as _,
    net::TcpListener,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::{sleep, timeout, Instant},
};

const CID: u32 = 16;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let (mut stream, _) = TcpListener::bind("0.0.0.0:3000").await?.accept().await?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;

    let config = serde_json::from_slice::<Microbench>(&buf)?;

    let (portal_session, session) = match config.variant {
        Variant::NitroEnclaves => {
            let (update_sender, update_receiver) = unbounded_channel();
            let (update_ok_sender, mut update_ok_receiver) = unbounded_channel::<UpdateOk<_>>();
            tokio::spawn({
                let update_sender = update_sender.clone();
                async move {
                    pending::<()>().await;
                    drop(update_sender)
                }
            });
            (
                tokio::spawn(nitro_enclaves_portal_session(
                    CID,
                    update_receiver,
                    update_ok_sender,
                )),
                tokio::spawn(async move {
                    let verify = |clock: NitroEnclavesClock| {
                        let document = clock.verify()?;
                        anyhow::ensure!(document.is_some());
                        Ok(())
                    };
                    let mut lines = String::new();
                    if let Some(num_concurrent) = config.num_concurrent {
                        stress_bench_session(
                            1 << 10,
                            0,
                            num_concurrent,
                            &update_sender,
                            &mut update_ok_receiver,
                            &mut lines,
                        )
                        .await?;
                        println!("{lines}")
                    } else {
                        for size in (0..=16).step_by(2).map(|n| 1 << n) {
                            bench_session(
                                size,
                                0,
                                &update_sender,
                                &mut update_ok_receiver,
                                verify,
                                &mut lines,
                            )
                            .await?
                        }
                        for num_merged in 0..=15 {
                            bench_session(
                                1 << 10,
                                num_merged,
                                &update_sender,
                                &mut update_ok_receiver,
                                verify,
                                &mut lines,
                            )
                            .await?
                        }
                        println!("{lines}")
                    }
                    anyhow::Ok(())
                }),
            )
        }
        Variant::Quorum(quorum_config) => {
            let crypto = Crypto::new_random(&mut thread_rng());
            let num_faulty = quorum_config.num_faulty;
            let (update_sender, update_receiver) = unbounded_channel();
            let (update_ok_sender, mut update_ok_receiver) = unbounded_channel::<UpdateOk<_>>();
            tokio::spawn({
                let update_sender = update_sender.clone();
                async move {
                    pending::<()>().await;
                    drop(update_sender)
                }
            });
            (
                tokio::spawn(quorum_client_session(
                    quorum_config,
                    config.ip,
                    crypto.clone(),
                    update_receiver,
                    update_ok_sender,
                )),
                tokio::spawn(async move {
                    let mut lines = String::new();
                    bench_session(
                        1 << 10,
                        0,
                        &update_sender,
                        &mut update_ok_receiver,
                        |clock| clock.verify(num_faulty, &crypto),
                        &mut lines,
                    )
                    .await?;
                    if num_faulty == 1 {
                        for size in (0..=16).step_by(2).map(|n| 1 << n) {
                            if size == 1 << 10 {
                                continue;
                            }
                            bench_session(
                                size,
                                0,
                                &update_sender,
                                &mut update_ok_receiver,
                                |clock| clock.verify(num_faulty, &crypto),
                                &mut lines,
                            )
                            .await?
                        }
                        for num_merged in 1..=15 {
                            bench_session(
                                1 << 10,
                                num_merged,
                                &update_sender,
                                &mut update_ok_receiver,
                                |clock| clock.verify(num_faulty, &crypto),
                                &mut lines,
                            )
                            .await?
                        }
                    }
                    let lines = lines
                        .split_whitespace()
                        .map(|line| format!("{num_faulty},{line}"))
                        .collect::<Vec<_>>()
                        .join("\n");
                    println!("{lines}");
                    anyhow::Ok(())
                }),
            )
        }
        _ => anyhow::bail!("unimplemented"),
    };
    'select: {
        tokio::select! {
            result = session => break 'select result??,
            result = portal_session => result??,
        }
        anyhow::bail!("unreachable")
    }
    Ok(())
}

async fn bench_session<C: TryFrom<OrdinaryVersion> + Clone + Send + Sync + 'static>(
    size: usize,
    num_merged: usize,
    update_sender: &UnboundedSender<Update<C>>,
    update_ok_receiver: &mut UnboundedReceiver<UpdateOk<C>>,
    verify: impl Fn(C) -> anyhow::Result<()>,
    lines: &mut String,
) -> anyhow::Result<()>
where
    C::Error: Into<anyhow::Error>,
{
    let clock = C::try_from(OrdinaryVersion((0..size).map(|i| (i as _, 0)).collect()))
        .map_err(Into::into)?;
    update_sender.send(Update(clock, Default::default(), 0))?;
    let Some((_, clock)) = update_ok_receiver.recv().await else {
        anyhow::bail!("missing UpdateOk")
    };
    for _ in 0..10 {
        sleep(Duration::from_millis(100)).await;
        let update = Update(clock.clone(), vec![clock.clone(); num_merged], 0);
        let start = Instant::now();
        update_sender.send(update)?;
        let Some((_, clock)) = update_ok_receiver.recv().await else {
            anyhow::bail!("missing UpdateOk")
        };
        let elapsed = start.elapsed();
        eprintln!("{size:8} {num_merged:3} {elapsed:?}");
        writeln!(lines, "{size},{num_merged},{}", elapsed.as_secs_f32())?;
        verify(clock)?
    }
    Ok(())
}

async fn stress_bench_session<C: TryFrom<OrdinaryVersion> + Clone + Send + Sync + 'static>(
    size: usize,
    num_merged: usize,
    num_concurrent: usize,
    update_sender: &UnboundedSender<Update<C>>,
    update_ok_receiver: &mut UnboundedReceiver<UpdateOk<C>>,
    lines: &mut String,
) -> anyhow::Result<()>
where
    C::Error: Into<anyhow::Error>,
{
    let clock = C::try_from(OrdinaryVersion((0..size).map(|i| (i as _, 0)).collect()))
        .map_err(Into::into)?;
    for i in 0..num_concurrent {
        update_sender.send(Update(clock.clone(), Default::default(), i as _))?;
    }
    let mut count = 0;
    let close_loops_session = async {
        while let Some((id, clock)) = update_ok_receiver.recv().await {
            count += 1;
            let update = Update(clock.clone(), vec![clock.clone(); num_merged], id);
            update_sender.send(update)?
        }
        anyhow::Ok(())
    };
    match timeout(Duration::from_secs(10), close_loops_session).await {
        Err(_) => {}
        Ok(result) => {
            result?;
            anyhow::bail!("unreachable")
        }
    }
    eprintln!("concurrent {num_concurrent} count {count}");
    writeln!(
        lines,
        "{size},{num_merged},{num_concurrent},{}",
        count as f32 / 10.
    )?;
    Ok(())
}

async fn quorum_client_session(
    config: Quorum,
    ip: IpAddr,
    crypto: Crypto,
    mut update_receiver: UnboundedReceiver<Update<QuorumClock>>,
    update_ok_sender: UnboundedSender<UpdateOk<QuorumClock>>,
) -> anyhow::Result<()> {
    let clock_tcp_listener = TcpListener::bind(SocketAddr::from(([0; 4], 0))).await?;
    let clock_addr = SocketAddr::new(ip, clock_tcp_listener.local_addr()?.port());

    let mut clock_dispatch_session = event::Session::new();
    let mut clock_session = Session::new();
    let (crypto_worker, mut crypto_executor) = spawning_backend();

    let mut clock_dispatch = event::Unify(event::Buffered::from(Dispatch::new(
        Tcp::new(clock_addr)?,
        {
            let mut sender = Sender::from(clock_session.sender());
            move |buf: &_| sender.send(Recv(deserialize::<Verifiable<cover::AnnounceOk>>(buf)?))
        },
        Once(clock_dispatch_session.sender()),
    )?));
    let mut clock = Blanket(Unify(QuorumClient::new(
        clock_addr,
        crypto.public_key(),
        config.num_faulty,
        Box::new(cover::quorum_client::CryptoWorker::from(crypto_worker))
            as Box<
                dyn Submit<Crypto, dyn cover::quorum_client::SendCryptoEvent<SocketAddr>>
                    + Send
                    + Sync,
            >,
        update_ok_sender,
        augustus::net::MessageNet::<_, Verifiable<cover::Announce<SocketAddr>>>::new(
            IndexNet::new(
                dispatch::Net::from(clock_dispatch_session.sender()),
                config.addrs,
                None,
            ),
        ),
    )));

    let event_session = {
        let mut sender = Sender::from(clock_session.sender());
        async move {
            while let Some(event) = update_receiver.recv().await {
                sender.send(event)?
            }
            anyhow::Ok(())
        }
    };
    let clock_tcp_accept_session =
        tcp::accept_session(clock_tcp_listener, clock_dispatch_session.sender());
    let crypto_session = crypto_executor.run(crypto.clone(), Sender::from(clock_session.sender()));
    let clock_dispatch_session = clock_dispatch_session.run(&mut clock_dispatch);
    let clock_session = clock_session.run(&mut clock);

    tokio::select! {
        result = event_session => result?,
        result = clock_tcp_accept_session => result?,
        result = clock_dispatch_session => result?,
        result = crypto_session => result?,
        result = clock_session => result?,
    }
    anyhow::bail!("unreachable")
}