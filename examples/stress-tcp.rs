use std::{
    env::args,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        Arc,
    },
    time::Duration,
};

use augustus::{
    event::{
        erased::{events::Init, session::Sender, Blanket, Session, Unify},
        SendEvent,
    },
    net::{
        session::{tcp_accept_session, Dispatch, DispatchNet, Tcp},
        SendMessage,
    },
};

use tokio::{
    net::TcpListener,
    task::JoinSet,
    time::{sleep, timeout_at, Instant},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let num_peer = args().nth(1).map(|n| n.parse::<u16>()).unwrap_or(Ok(50))?;
    let multiplier = args().nth(2).map(|n| n.parse::<u8>()).unwrap_or(Ok(200))?;
    let expected = num_peer as u32 * num_peer as u32 * multiplier as u32;
    println!("expect {expected} messages");

    let count = Arc::new(AtomicU32::new(0));
    let mut sessions = JoinSet::new();
    for i in 0..num_peer {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", 3000 + i)).await?;

        let mut control = Blanket(Unify(Dispatch::new(Tcp::new(None)?, {
            let count = count.clone();
            move |_: &_| {
                count.fetch_add(1, SeqCst);
                Ok(())
            }
        })?));

        let mut control_session = Session::new();
        sessions.spawn(tcp_accept_session(
            listener,
            Sender::from(control_session.sender()),
        ));
        let mut net = DispatchNet(Sender::from(control_session.sender()));
        sessions.spawn(async move {
            Sender::from(control_session.sender()).send(Init)?;
            control_session.run(&mut control).await
        });
        sessions.spawn(async move {
            for j in 0..multiplier {
                for k in 0..num_peer {
                    net.send(
                        SocketAddr::from(([127, 0, 0, j + 1], 3000 + k)),
                        bytes::Bytes::from(b"hello".to_vec()),
                    )?;
                    sleep(Duration::from_millis(1)).await
                }
            }
            Ok(())
        });
    }

    let mut finished = 0;
    let mut deadline = Instant::now() + Duration::from_secs(1);
    while finished < expected {
        match timeout_at(deadline, sessions.join_next()).await {
            Ok(None) => break,
            Ok(Some(Ok(Ok(())))) => {}
            Ok(Some(result)) => {
                println!("{result:?}");
                break;
            }
            Err(_) => {
                let count = count.swap(0, SeqCst);
                finished += count;
                println!("{finished:6}/{expected:6} {count} messages/s");
                deadline += Duration::from_secs(1)
            }
        }
    }
    Ok(())
    // std::future::pending().await
}
