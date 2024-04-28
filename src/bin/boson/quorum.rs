use std::net::SocketAddr;

use augustus::{
    boson::{self, QuorumServer},
    crypto::peer::{Crypto, Verifiable},
    event::{
        self,
        erased::{session::Sender, Blanket, Session, Unify},
        Once, SendEvent,
    },
    net::{
        deserialize, dispatch,
        events::Recv,
        session::{tcp, Tcp},
        Dispatch, MessageNet,
    },
    worker::spawning_backend,
};
use rand::thread_rng;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

fn adjust(addr: SocketAddr) -> SocketAddr {
    if addr.ip().is_loopback() {
        addr
    } else {
        SocketAddr::from(([0; 4], addr.port()))
    }
}

pub async fn session(
    config: boson_control_messages::QuorumServer,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let addr = config.quorum.addrs[config.index];
    let crypto = Crypto::new_random(&mut thread_rng());
    let tcp_listener = TcpListener::bind(adjust(addr)).await?;

    let mut dispatch_session = event::Session::new();
    let mut server_session = Session::new();
    let (recv_crypto_worker, mut recv_crypto_executor) = spawning_backend();
    let (send_crypto_worker, mut send_crypto_executor) = spawning_backend();

    let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
        Tcp::new(addr)?,
        {
            let mut sender =
                boson::VerifyClock::new(config.quorum.num_faulty, recv_crypto_worker);
            move |buf: &_| {
                sender.send(Recv(
                    deserialize::<Verifiable<boson::Announce<SocketAddr>>>(buf)?,
                ))
            }
        },
        Once(dispatch_session.sender()),
    )?));
    let mut server = Blanket(Unify(QuorumServer::new(
        crypto.public_key(),
        send_crypto_worker,
    )));

    let tcp_accept_session = tcp::accept_session(tcp_listener, dispatch_session.sender());
    let recv_crypto_session =
        recv_crypto_executor.run(crypto.clone(), Sender::from(server_session.sender()));
    let send_crypto_session = send_crypto_executor.run(
        crypto,
        MessageNet::<_, Verifiable<boson::AnnounceOk>>::new(dispatch::Net::from(
            dispatch_session.sender(),
        )),
    );
    let dispatch_session = dispatch_session.run(&mut dispatch);
    let server_session = server_session.run(&mut server);

    tokio::select! {
        () = cancel.cancelled() => return Ok(()),
        result = tcp_accept_session => result?,
        result = recv_crypto_session => result?,
        result = send_crypto_session => result?,
        result = dispatch_session => result?,
        result = server_session => result?,
    }
    anyhow::bail!("unreachable")
}
