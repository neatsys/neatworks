use std::{env::args, net::SocketAddr, path::Path, sync::Arc, time::Duration};

use augustus::{
    blob,
    crypto::{Crypto, H256},
    event::erased::Session,
    kademlia::{self, Buckets, PeerId, PeerRecord},
    net::{
        kademlia::{Control, PeerNet},
        Udp,
    },
    worker::erased::spawn_backend,
};
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};

use entropy::{GetOk, MessageNet, Peer, PutOk};
use entropy_control_messages::StartPeersConfig;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use tokio::{
    fs::create_dir,
    net::UdpSocket,
    signal::ctrl_c,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    task::JoinSet,
    time::timeout,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let app = Router::new()
        .route("/ok", get(ok))
        .route("/start-peers", post(start_peers));
    let (upcall_sender, upcall_receiver) = unbounded_channel();
    let app = app.with_state(AppState {
        sessions: Default::default(),
        upcall_sender,
        upcall_receiver: Arc::new(Mutex::new(upcall_receiver)),
    });
    let url = args().nth(1).ok_or(anyhow::anyhow!("not specify url"))?;
    let listener = tokio::net::TcpListener::bind(&url).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async { ctrl_c().await.unwrap() })
        .await?;
    Ok(())
}

#[derive(Debug, Clone)]
struct AppState {
    sessions: Arc<Mutex<JoinSet<anyhow::Result<()>>>>,
    upcall_sender: UnboundedSender<Upcall>,
    upcall_receiver: Arc<Mutex<UnboundedReceiver<Upcall>>>,
}

async fn ok(State(state): State<AppState>) {
    let mut sessions = state.sessions.lock().await;
    match timeout(Duration::ZERO, sessions.join_next()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(result)) => result.unwrap().unwrap(),
    }
}

async fn start_peers(State(state): State<AppState>, Json(config): Json<StartPeersConfig>) {
    let mut sessions = state.sessions.lock().await;
    assert!(sessions.is_empty());
    let mut rng = StdRng::seed_from_u64(117418);
    let mut records = Vec::new();
    let mut local_peers = Vec::new();
    for (i, ip) in config.ips.into_iter().enumerate() {
        for j in 0..config.num_peer_per_ip {
            let crypto = Crypto::new_random(&mut rng);
            let record =
                PeerRecord::new(crypto.public_key(), SocketAddr::from((ip, 4000 + j as u16)));
            records.push(record.clone());
            if i == config.ip_index {
                local_peers.push((record, crypto, StdRng::from_rng(rng.clone()).unwrap()))
            }
        }
    }
    for (record, crypto, rng) in local_peers {
        sessions.spawn(start_peer(
            record,
            crypto,
            rng,
            records.clone(),
            state.upcall_sender.clone(),
        ));
    }
}

#[derive(Debug, Clone, derive_more::From)]
enum Upcall {
    PutOk(PutOk),
    GetOk(GetOk),
}

async fn start_peer(
    record: PeerRecord<SocketAddr>,
    crypto: Crypto<PeerId>,
    mut rng: StdRng,
    mut records: Vec<PeerRecord<SocketAddr>>,
    upcall_sender: UnboundedSender<Upcall>,
) -> anyhow::Result<()> {
    let peer_id = record.id;
    let path = format!("/tmp/entropy-{:x}", H256(peer_id));
    let path = Path::new(&path);
    create_dir(path).await?;
    let socket_net = Udp(UdpSocket::bind(record.addr).await?.into());

    let ip = record.addr.ip();
    let mut buckets = Buckets::new(record);
    records.shuffle(&mut rng);
    for record in records {
        buckets.insert(record)
    }

    let mut kademlia_session = Session::new();
    let mut control_session = Session::new();
    let (mut blob_sender, blob_receiver) = unbounded_channel();
    let (fs_sender, fs_receiver) = unbounded_channel();
    let (crypto_worker, mut crypto_session) = spawn_backend(crypto.clone());
    let (codec_worker, mut codec_session) = spawn_backend(());
    let mut peer_session = Session::new();

    let mut kademlia_peer = kademlia::Peer::new(
        buckets,
        MessageNet::new(socket_net.clone()),
        control_session.sender(),
        crypto_worker,
    );
    let mut control = Control::new(socket_net.clone(), kademlia_session.sender());
    let mut peer = Peer::new(
        peer_id,
        crypto,
        0,
        1.try_into().unwrap(),
        1.try_into().unwrap(),
        1.try_into().unwrap(),
        MessageNet::<_, SocketAddr>::new(PeerNet(control_session.sender())),
        blob_sender.clone(),
        upcall_sender,
        codec_worker,
        fs_sender,
    );

    let socket_session = socket_net.recv_session({
        let mut peer_sender = peer_session.sender();
        let mut kademlia_sender = kademlia_session.sender();
        move |buf| {
            entropy::on_buf(
                buf,
                &mut peer_sender,
                &mut kademlia_sender,
                &mut blob_sender,
            )
        }
    });
    let crypto_session = crypto_session.run(kademlia::SendCrypto(kademlia_session.sender()));
    let kademlia_session = kademlia_session.run(&mut kademlia_peer);
    let blob_session = blob::session(
        ip,
        blob_receiver,
        MessageNet::<_, SocketAddr>::new(PeerNet(control_session.sender())),
        peer_session.sender(),
    );
    let control_session = control_session.run(&mut control);
    let fs_session = entropy::fs::session(path, fs_receiver, peer_session.sender());
    let codec_session = codec_session.run(entropy::SendCodec(peer_session.sender()));
    let peer_session = peer_session.run(&mut peer);

    tokio::select! {
        result = socket_session => result?,
        result = kademlia_session => result?,
        result = control_session => result?,
        result = blob_session => result?,
        result = fs_session => result?,
        result = crypto_session => result?,
        result = codec_session => result?,
        result = peer_session => result?,
    }
    Err(anyhow::anyhow!("unexpected shutdown"))
}
