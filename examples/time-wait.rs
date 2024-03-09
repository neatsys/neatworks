use std::net::SocketAddr;

use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    spawn,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 8000))).await?;
    let listen_session = spawn(async move {
        let mut buf = [0; 1];
        // let mut conns = Vec::new();
        loop {
            let (mut conn, _) = listener.accept().await?;
            let _ = conn.read(&mut buf).await?;
            // conns.push(conn)
        }
    });
    let connect_session = async {
        // let mut streams = Vec::new();
        for i in 0.. {
            // for j in 0..2 {
            let stream = TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], 8000))).await?;
            // let stream = tokio::net::TcpSocket::new_v4()?
            //     .connect(([127, 0, 0, j + 1], 8000).into())
            //     .await?;
            println!("{i} {stream:?}");
            drop(stream)
            // streams.push(stream)
            // }
        }
        Ok(())
    };
    tokio::select! {
        result = listen_session => result?,
        result = connect_session => result,
    }
}
