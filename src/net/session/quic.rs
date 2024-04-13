use std::{fmt::Debug, net::SocketAddr, sync::Arc};

use quinn::ConnectionError;
use rustls::RootCertStore;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{warn, Instrument};

use crate::{
    event::{SendEvent, SendEventOnce},
    net::{
        dispatch::{CloseGuard, Closed, Incoming, Protocol},
        Buf, MAX_BUF_LEN,
    },
};

#[derive(Debug, Clone)]
pub struct Quic(pub quinn::Endpoint);

fn quic_config() -> anyhow::Result<(quinn::ServerConfig, quinn::ClientConfig)> {
    let issuer_key = rcgen::KeyPair::from_pem(include_str!("../issuer_key.pem"))?;
    let issuer = rcgen::Certificate::generate_self_signed(
        rcgen::CertificateParams::from_ca_cert_pem(include_str!("../issuer_cert.pem"))?,
        &issuer_key,
    )?;
    let priv_key = rcgen::KeyPair::generate()?;
    let cert = rcgen::Certificate::generate(
        rcgen::CertificateParams::new(vec!["neatworks.quic".into()])?,
        &priv_key,
        &issuer,
        &issuer_key,
    )?;
    let priv_key = rustls::PrivateKey(priv_key.serialize_der());
    let cert_chain = vec![rustls::Certificate(cert.der().to_vec())];

    let mut server_config = quinn::ServerConfig::with_single_cert(cert_chain, priv_key)?;
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    // transport_config.max_concurrent_uni_streams(0_u8.into());
    transport_config.max_concurrent_uni_streams(4096u32.into());
    // transport_config.max_idle_timeout(Some(Duration::from_millis(2500).try_into()?));

    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(&[issuer.der()]);
    let client_config = quinn::ClientConfig::with_root_certificates(roots);
    Ok((server_config, client_config))
}

impl Quic {
    pub fn new(addr: SocketAddr) -> anyhow::Result<Self> {
        let (server_config, client_config) = quic_config()?;
        let mut endpoint = quinn::Endpoint::server(server_config, addr)?;
        endpoint.set_default_client_config(client_config);
        Ok(Self(endpoint))
    }

    async fn read_task(
        connection: quinn::Connection,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
    ) {
        let remote_addr = connection.remote_address();
        loop {
            let mut stream = match connection.accept_uni().await {
                Ok(stream) => stream,
                // TODO
                Err(
                    quinn::ConnectionError::ConnectionClosed(_)
                    | quinn::ConnectionError::LocallyClosed
                    | quinn::ConnectionError::TimedOut,
                ) => break,
                Err(err) => {
                    warn!("<<< {remote_addr} {err}");
                    break;
                }
            };
            let mut on_buf = on_buf.clone();
            tokio::spawn(async move {
                if let Err(err) = async {
                    // determine incomplete stream?
                    on_buf(&stream.read_to_end(MAX_BUF_LEN).await?)?;
                    anyhow::Ok(())
                }
                .await
                {
                    warn!("<<< {remote_addr} {err}")
                }
            });
        }
    }

    async fn write_task<B: Buf, E: SendEventOnce<Closed>>(
        connection: quinn::Connection,
        mut receiver: UnboundedReceiver<B>,
        close_guard: CloseGuard<E>,
    ) {
        loop {
            enum Select<B> {
                Recv(Option<B>),
                Closed(ConnectionError),
            }
            match tokio::select! {
                recv = receiver.recv() => Select::Recv(recv),
                closed = connection.closed() => Select::Closed(closed),
            } {
                Select::Recv(None) => return, // skip close, we are outliving dispatcher
                Select::Recv(Some(buf)) => {
                    let connection = connection.clone();
                    tokio::spawn(async move {
                        if let Err(err) = async {
                            connection.open_uni().await?.write_all(buf.as_ref()).await?;
                            anyhow::Ok(())
                        }
                        .await
                        {
                            warn!(">>> {} {err}", connection.remote_address())
                        }
                    });
                }
                Select::Closed(err) => {
                    if !matches!(err, ConnectionError::TimedOut) {
                        warn!(">> {:?} {err}", connection.remote_address())
                    }
                    break;
                }
            }
        }
        if let Err(err) = close_guard.close(connection.remote_address()) {
            warn!("close {:?} {err}", connection.remote_address())
        }
    }
}

impl<B: Buf> Protocol<B> for Quic {
    type Sender = UnboundedSender<B>;

    fn connect<E: SendEventOnce<Closed> + Send + 'static>(
        &self,
        remote: SocketAddr,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
        close_guard: CloseGuard<E>,
    ) -> Self::Sender {
        let endpoint = self.0.clone();
        // tracing::debug!("{:?} connect {remote}", endpoint.local_addr());
        let (sender, receiver) = unbounded_channel();
        tokio::spawn(async move {
            let task = async {
                let span = tracing::debug_span!("connecting", local = ?endpoint.local_addr(), remote = ?remote).entered();
                let connecting = {
                    let _s = tracing::debug_span!(parent: None, "dummy detached").entered();
                    endpoint.connect(remote, "neatworks.quic")
                }?;
                drop(span.exit());
                anyhow::Ok(
                    connecting
                        .instrument(tracing::debug_span!("connect", local = ?endpoint.local_addr(), remote = ?remote))
                        .await?,
                )
            };
            let connection = match task.await {
                Ok(connection) => connection,
                Err(err) => {
                    warn!(">>> {remote} {err}");
                    return;
                }
            };
            tokio::spawn(Self::read_task(connection.clone(), on_buf));
            tokio::spawn(Self::write_task(connection, receiver, close_guard));
        });
        sender
    }

    type Incoming = quinn::Connection;

    fn accept<E: SendEventOnce<Closed> + Send + 'static>(
        connection: Self::Incoming,
        on_buf: impl FnMut(&[u8]) -> anyhow::Result<()> + Clone + Send + 'static,
        close_guard: CloseGuard<E>,
    ) -> Option<(SocketAddr, Self::Sender)> {
        let remote = connection.remote_address();
        tokio::spawn(Self::read_task(connection.clone(), on_buf));
        // tracing::debug!("{remote}");
        if remote.ip().is_unspecified() {
            // theoretically the connection is still writable, but `Dispatch` probably don't known
            // what should write to this connection and will not write anything, so just skip
            // spawning the write task
            // i don't know when this case would happen though
            None
        } else {
            let (sender, receiver) = unbounded_channel();
            tokio::spawn(Self::write_task(connection, receiver, close_guard));
            Some((remote, sender))
        }
    }
}

pub async fn accept_session(
    Quic(endpoint): Quic,
    sender: impl SendEvent<Incoming<quinn::Connection>> + Clone + Send + 'static,
) -> anyhow::Result<()> {
    while let Some(conn) = endpoint.accept().await {
        let remote_addr = conn.remote_address();
        tracing::trace!(
            "remote {remote_addr} >>> {:?} accept",
            endpoint.local_addr()
        );
        let mut sender = sender.clone();
        tokio::spawn(async move {
            if let Err(err) = async {
                sender.send(Incoming(conn.await?))?;
                anyhow::Ok(())
            }
            .await
            {
                warn!("<<< {remote_addr} {err}")
            }
        });
    }
    Ok(())
}

// cSpell:words quic bincode rustls libp2p kademlia oneshot rcgen unreplicated
// cSpell:words neatworks
// cSpell:ignore nodelay reuseaddr
