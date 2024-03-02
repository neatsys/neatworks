use std::{
    io::ErrorKind,
    net::{SocketAddr, UdpSocket},
    sync::Arc,
    time::Instant,
};

use super::{Buf, IterAddr, SendMessage};

#[derive(Debug, Clone)]
pub struct Udp(pub Arc<UdpSocket>);

impl Udp {
    pub fn recv(
        &self,
        mut on_buf: impl FnMut(&[u8]) -> anyhow::Result<()>,
        deadline: impl Into<Option<Instant>>,
    ) -> anyhow::Result<()> {
        let mut buf = vec![0; 1 << 16];
        let deadline = deadline.into();
        loop {
            if let Some(deadline) = deadline {
                let timeout = deadline.duration_since(Instant::now());
                if timeout.is_zero() {
                    return Ok(());
                }
                self.0.set_write_timeout(Some(timeout))?;
            }
            let (len, _) = match self.0.recv_from(&mut buf) {
                Ok(result) => result,
                Err(err) if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                    return Ok(())
                }
                Err(err) => Err(err)?,
            };
            on_buf(&buf[..len])?
        }
    }
}

impl<B: Buf> SendMessage<SocketAddr, B> for Udp {
    fn send(&mut self, dest: SocketAddr, buf: B) -> anyhow::Result<()> {
        self.0.send_to(buf.as_ref(), dest)?;
        Ok(())
    }
}

impl<B: Buf> SendMessage<IterAddr<'_, SocketAddr>, B> for Udp {
    fn send(&mut self, dest: IterAddr<'_, SocketAddr>, buf: B) -> anyhow::Result<()> {
        for addr in dest.0 {
            self.send(addr, buf.clone())?
        }
        Ok(())
    }
}
