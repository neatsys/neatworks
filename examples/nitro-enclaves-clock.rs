use augustus::cops::DepOrd;
use byteorder::{ByteOrder, LittleEndian};
use nix::errno::Errno::EINTR;
use nix::sys::socket::listen as listen_vsock;
use nix::sys::socket::MsgFlags;
use nix::sys::socket::{accept, bind, connect, shutdown, socket};
use nix::sys::socket::{recv, send};
use nix::sys::socket::{AddressFamily, Shutdown, SockAddr, SockFlag, SockType};
use nix::unistd::close;
use std::convert::TryInto;
use std::mem::size_of;
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Instant;

use augustus::{boson::NitroEnclaves, cops::DefaultVersion};

const VMADDR_CID_ANY: u32 = 0xFFFFFFFF;
const BUF_MAX_LEN: usize = 8192;
// Maximum number of outstanding connections in the socket's
// listen queue
const BACKLOG: usize = 128;
// Maximum number of connection attempts
const MAX_CONNECTION_ATTEMPTS: usize = 5;

fn main() -> anyhow::Result<()> {
    server().map_err(anyhow::Error::msg)
}

/// Accept connections on a certain port and print
/// the received data
pub fn server() -> Result<(), String> {
    let socket_fd = socket(
        AddressFamily::Vsock,
        SockType::Stream,
        SockFlag::empty(),
        None,
    )
    .map_err(|err| format!("Create socket failed: {:?}", err))?;

    let sockaddr = SockAddr::new_vsock(VMADDR_CID_ANY, 5005);

    bind(socket_fd, &sockaddr).map_err(|err| format!("Bind failed: {:?}", err))?;

    listen_vsock(socket_fd, BACKLOG).map_err(|err| format!("Listen failed: {:?}", err))?;

    loop {
        let fd = accept(socket_fd).map_err(|err| format!("Accept failed: {:?}", err))?;
        // let msg = b"connected";
        // send_loop(fd, msg, msg.len() as _)?;

        // TODO: Replace this with your server code
        let zero = DefaultVersion::default();
        NitroEnclaves::issue(zero.clone(), 0).map_err(|err| err.to_string())?;

        let mut working_clock = zero.clone();
        for i in 0..1 << 12 {
            working_clock = working_clock.update([].into_iter(), i);
            assert_eq!(working_clock.deps().count(), i as usize + 1);
            if (i + 1).is_power_of_two() {
                let start = Instant::now();
                let clock = NitroEnclaves::issue(zero.clone(), i).map_err(|err| err.to_string())?;
                let msg = format!("{},{:?},{}\n", i + 1, start.elapsed(), clock.document.len())
                    .into_bytes();
                send_loop(fd, &msg, msg.len() as _)?;
            }
        }

        close(fd).map_err(|err| format!("Close failed: {:?}", err))?
    }
}

pub fn send_u64(fd: RawFd, val: u64) -> Result<(), String> {
    let mut buf = [0u8; size_of::<u64>()];
    LittleEndian::write_u64(&mut buf, val);
    send_loop(fd, &buf, size_of::<u64>().try_into().unwrap())?;
    Ok(())
}

pub fn recv_u64(fd: RawFd) -> Result<u64, String> {
    let mut buf = [0u8; size_of::<u64>()];
    recv_loop(fd, &mut buf, size_of::<u64>().try_into().unwrap())?;
    let val = LittleEndian::read_u64(&buf);
    Ok(val)
}

/// Send `len` bytes from `buf` to a connection-oriented socket
pub fn send_loop(fd: RawFd, buf: &[u8], len: u64) -> Result<(), String> {
    let len: usize = len.try_into().map_err(|err| format!("{:?}", err))?;
    let mut send_bytes = 0;

    while send_bytes < len {
        let size = match send(fd, &buf[send_bytes..len], MsgFlags::empty()) {
            Ok(size) => size,
            Err(nix::Error::Sys(EINTR)) => 0,
            Err(err) => return Err(format!("{:?}", err)),
        };
        send_bytes += size;
    }

    Ok(())
}

/// Receive `len` bytes from a connection-orriented socket
pub fn recv_loop(fd: RawFd, buf: &mut [u8], len: u64) -> Result<(), String> {
    let len: usize = len.try_into().map_err(|err| format!("{:?}", err))?;
    let mut recv_bytes = 0;

    while recv_bytes < len {
        let size = match recv(fd, &mut buf[recv_bytes..len], MsgFlags::empty()) {
            Ok(size) => size,
            Err(nix::Error::Sys(EINTR)) => 0,
            Err(err) => return Err(format!("{:?}", err)),
        };
        recv_bytes += size;
    }

    Ok(())
}
