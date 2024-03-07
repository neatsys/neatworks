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
    event::erased::{session::Sender, Blanket, Session, Unify},
    net::{
        session::{tcp_accept_session, Tcp, TcpControl},
        SendMessage,
    },
};
use rand::{thread_rng, Rng};
use tokio::{
    net::TcpListener,
    task::JoinSet,
    time::{sleep, timeout},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let num_peer = args().nth(1).map(|n| n.parse::<u16>()).unwrap_or(Ok(100))?;
    let multiplier = args().nth(2).map(|n| n.parse::<u8>()).unwrap_or(Ok(100))?;

    let count = Arc::new(AtomicU32::new(0));
    let mut sessions = JoinSet::new();
    for i in 0..num_peer {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", 3000 + i)).await?;

        let mut control = Blanket(Unify(TcpControl::new(
            {
                let count = count.clone();
                move |_: &_| {
                    count.fetch_add(1, SeqCst);
                    Ok(())
                }
            },
            None,
        )?));

        let mut control_session = Session::new();
        sessions.spawn(tcp_accept_session(
            listener,
            Sender::from(control_session.sender()),
        ));
        let mut net = Tcp(Sender::from(control_session.sender()));
        sessions.spawn(async move { control_session.run(&mut control).await });
        sessions.spawn(async move {
            loop {
                for j in 0..multiplier {
                    for k in 0..num_peer {
                        net.send(
                            SocketAddr::from(([127, 0, 0, j + 1], 3000 + k)),
                            bytes::Bytes::from(b"hello".to_vec()),
                        )?;
                        let delay = 10 + thread_rng().gen_range(0..10);
                        sleep(Duration::from_millis(delay)).await
                    }
                }
            }
        });
    }

    for _ in 0..100 {
        if let Ok(result) = timeout(Duration::from_secs(1), sessions.join_next()).await {
            println!("{result:?}");
            break;
        }
        println!("{} messages/s", count.swap(0, SeqCst))
    }
    Ok(())
}
