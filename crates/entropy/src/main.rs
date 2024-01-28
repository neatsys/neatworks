use std::{env::args, net::SocketAddr, sync::Arc, time::Duration};

use augustus::{
    crypto::Crypto,
    kademlia::{Buckets, PeerId, PeerRecord},
    net::Udp,
};
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};

use entropy_control_messages::StartPeersConfig;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use tokio::{net::UdpSocket, signal::ctrl_c, sync::Mutex, task::JoinSet, time::timeout};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let app = Router::new()
        .route("/ok", get(ok))
        .route("/start-peers", post(start_peers))
        .with_state(AppState {
            sessions: Default::default(),
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
}

async fn ok(State(state): State<AppState>) {
    let mut sessions = state.sessions.lock().await;
    match timeout(Duration::ZERO, sessions.join_next()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(result)) => result.unwrap().unwrap(),
    }
}

async fn start_peers(State(state): State<AppState>, Json(config): Json<StartPeersConfig>) {
    let sessions = state.sessions.lock().await;
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
    //
}

async fn start_peer(
    record: PeerRecord<SocketAddr>,
    _crypto: Crypto<PeerId>,
    mut rng: StdRng,
    mut records: Vec<PeerRecord<SocketAddr>>,
) {
    let _socket_net = Udp(UdpSocket::bind(record.addr).await.unwrap().into());
    let mut buckets = Buckets::new(record);
    records.shuffle(&mut rng);
    for record in records {
        buckets.insert(record)
    }
    // let control_session = Session::new();
    // let (crypto_worker, crypto_session) = spawn_backend(crypto.clone());
    // let kademlia_peer = kademlia::Peer::new(
    //     buckets,
    //     MessageNet::new(socket_net.clone()),
    //     control_session.sender(),
    //     crypto_worker,
    // );
}
