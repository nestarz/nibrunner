use std::os::fd::{AsRawFd, OwnedFd};

use nix::sys::socket::{
    accept, bind, connect, getpeername, listen, socket, AddressFamily, Backlog, SockFlag, SockType, VsockAddr,
};

const BACKLOG: usize = 4;

const CID_ANY: u32 = 0xFFFF_FFFF;

const CID_HOST: u32 = 2;

pub(crate) fn listener(port: u32) -> nix::Result<OwnedFd> {
    let socket = socket(
        AddressFamily::Vsock,
        SockType::Stream,
        SockFlag::SOCK_CLOEXEC,
        None,
    )?;
    bind(socket.as_raw_fd(), &VsockAddr::new(CID_ANY, port))?;
    listen(&socket, Backlog::new(BACKLOG as i32)?)?;
    Ok(socket)
}

pub(crate) fn dial(port: u32) -> nix::Result<OwnedFd> {
    let socket = socket(
        AddressFamily::Vsock,
        SockType::Stream,
        SockFlag::SOCK_CLOEXEC,
        None,
    )?;
    connect(socket.as_raw_fd(), &VsockAddr::new(CID_HOST, port))?;
    Ok(socket)
}

/// The tenant cannot reach a guest port while the kernel carries no vsock loopback. A connection
/// from anywhere but the host is closed unanswered, so that stays true of a kernel that does.
pub(crate) fn accept_from_host(listener: &OwnedFd) -> nix::Result<OwnedFd> {
    loop {
        let connection = accept(listener.as_raw_fd())?;
        let connection = unsafe { <OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(connection) };
        if getpeername::<VsockAddr>(connection.as_raw_fd()).is_ok_and(|peer| peer.cid() == CID_HOST) {
            return Ok(connection);
        }
    }
}
