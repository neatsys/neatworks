use std::{future::Future, net::SocketAddr};

use augustus::{
    app::{self, ycsb, App, Null},
    crypto::{Crypto, CryptoFlavor},
    event::erased::{session::Sender, Blanket, Buffered, Session, Unify},
    net::{session::Udp, IndexNet, Payload},
    pbft, unreplicated,
    worker::{spawning_backend, Submit},
    workload::{Iter, Json, Workload},
};
use rand::{rngs::StdRng, SeedableRng};
use replication_control_messages::{Protocol, ReplicaConfig, YcsbBackend};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

pub fn session(
    config: ReplicaConfig,
    cancel: CancellationToken,
) -> anyhow::Result<impl Future<Output = anyhow::Result<()>> + Send + Sync + 'static> {
    enum App {
        Null,
        BTreeMap(app::BTreeMap),
        Sqlite(app::Sqlite),
    }
    let app = match &config.app {
        replication_control_messages::App::Null => App::Null,
        replication_control_messages::App::Ycsb(ycsb_config) => {
            let settings = ycsb::WorkloadSettings::new(ycsb_config.record_count);
            match &ycsb_config.backend {
                YcsbBackend::BTree => {
                    let mut app = app::BTreeMap::new();
                    app_startup(&mut app, settings)?;
                    App::BTreeMap(app)
                }
                YcsbBackend::Sqlite => {
                    let mut app = app::Sqlite::new(settings.field_count)?;
                    app_startup(&mut app, settings)?;
                    App::Sqlite(app)
                }
            }
        }
    };
    Ok(async move {
        match app {
            App::Null => app_session(config, Null, cancel).await,
            App::BTreeMap(app) => app_session(config, app, cancel).await,
            App::Sqlite(app) => app_session(config, app, cancel).await,
        }
    })
}

fn app_startup(app: &mut impl App, settings: ycsb::WorkloadSettings) -> anyhow::Result<()> {
    let mut workload = ycsb::Workload::new(StdRng::seed_from_u64(117418), settings)?;
    let mut workload = Json(Iter::<_, _, ()>::from(workload.startup_ops()));
    while let Some((Payload(op), _)) = workload.next_op()? {
        app.execute(&op)?;
    }
    Ok(())
}

async fn app_session(
    config: ReplicaConfig,
    app: impl App + Send + 'static,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let socket = UdpSocket::bind(config.replica_addrs[config.replica_id as usize]).await?;
    let net = Udp(socket.into());
    let mut session = match &config.protocol {
        Protocol::Unreplicated => tokio::spawn(unreplicated_session(config, app, net)),
        Protocol::Pbft => tokio::spawn(pbft_session(config, app, net)),
    };
    'select: {
        tokio::select! {
            () = cancel.cancelled() => break 'select,
            result = &mut session => result??
        }
        anyhow::bail!("unreachable")
    }
    session.abort();
    anyhow::ensure!(session.await.is_err_and(|err| err.is_cancelled()));
    Ok(())
}

async fn unreplicated_session(
    config: ReplicaConfig,
    app: impl App + 'static,
    net: Udp,
) -> anyhow::Result<()> {
    anyhow::ensure!(config.replica_id == 0);
    let mut state_session = Session::new();

    let mut state = Blanket(Unify(unreplicated::Replica::<_, _, SocketAddr>::new(
        app,
        unreplicated::ToClientMessageNet::new(net.clone()),
    )));

    let recv_session = net.recv_session({
        let mut state_sender = Sender::from(state_session.sender());
        move |buf: &_| unreplicated::erased::to_replica_on_buf(buf, &mut state_sender)
    });
    let state_session = state_session.run(&mut state);

    tokio::select! {
        result = recv_session => result?,
        result = state_session => result?,
    }
    anyhow::bail!("unreachable")
}

async fn pbft_session(
    config: ReplicaConfig,
    app: impl App + Send + 'static,
    net: Udp,
) -> anyhow::Result<()> {
    let crypto = Crypto::new_hardcoded(
        config.num_replica,
        config.replica_id,
        CryptoFlavor::Schnorrkel,
    )?;

    let mut state_session = Session::new();
    let (crypto_worker, mut crypto_executor) = spawning_backend();

    let mut state = Blanket(Buffered::from(pbft::Replica::new(
        config.replica_id,
        app,
        pbft::ToReplicaMessageNet::new(IndexNet::new(
            net.clone(),
            config.replica_addrs,
            config.replica_id as usize,
        )),
        pbft::ToClientMessageNet::new(net.clone()),
        Box::new(pbft::CryptoWorker::from(crypto_worker))
            as Box<dyn Submit<Crypto, dyn pbft::SendCryptoEvent<SocketAddr>> + Send>,
        config.num_replica,
        config.num_faulty,
    )));

    let recv_session = net.recv_session({
        let mut state_sender = Sender::from(state_session.sender());
        move |buf: &_| pbft::to_replica_on_buf(buf, &mut state_sender)
    });
    let crypto_session = crypto_executor.run(crypto, Sender::from(state_session.sender()));
    let state_session = state_session.run(&mut state);

    tokio::select! {
        result = recv_session => result?,
        result = crypto_session => result?,
        result = state_session => result?,
    }
    anyhow::bail!("unreachable")
}

// cSpell:words unreplicated pbft upcall ycsb seedable schnorrkel secp256k1
