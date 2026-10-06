use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;

use guest_contract::channels::{Channel, ChannelTransport, PROCESS_CHANNEL_DIRECTORY};
use nix::sys::socket::{
    accept4, bind, connect, listen, socket, AddressFamily, Backlog, SockFlag, SockType, UnixAddr, VsockAddr,
};

const BACKLOG: i32 = 4;
const CID_ANY: u32 = 0xffff_ffff;
const CID_HOST: u32 = 2;

pub(crate) fn listener(transport: ChannelTransport, channel: Channel) -> nix::Result<OwnedFd> {
    listen_at(transport, channel, Path::new(PROCESS_CHANNEL_DIRECTORY))
}

fn listen_at(transport: ChannelTransport, channel: Channel, directory: &Path) -> nix::Result<OwnedFd> {
    let socket = socket(family(transport), SockType::Stream, SockFlag::SOCK_CLOEXEC, None)?;
    match transport {
        ChannelTransport::Vsock => bind(socket.as_raw_fd(), &VsockAddr::new(CID_ANY, channel.port()))?,
        ChannelTransport::Unix => {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::symlink_metadata(directory)
                .map_err(|error| nix::errno::Errno::from_raw(error.raw_os_error().unwrap_or(libc::EIO)))?;
            if !metadata.is_dir()
                || metadata.uid() != nix::unistd::geteuid().as_raw()
                || metadata.mode() & 0o077 != 0
            {
                return Err(nix::errno::Errno::EACCES);
            }
            let path = transport.endpoint(directory, channel).path;
            bind(socket.as_raw_fd(), &UnixAddr::new(&path)?)?;
            nix::sys::stat::fchmodat(
                nix::fcntl::AT_FDCWD,
                &path,
                nix::sys::stat::Mode::from_bits_truncate(0o600),
                nix::sys::stat::FchmodatFlags::FollowSymlink,
            )?;
        }
    }
    listen(&socket, Backlog::new(BACKLOG)?)?;
    Ok(socket)
}

pub(crate) fn dial(transport: ChannelTransport, channel: Channel) -> nix::Result<OwnedFd> {
    dial_at(transport, channel, Path::new(PROCESS_CHANNEL_DIRECTORY))
}

fn dial_at(transport: ChannelTransport, channel: Channel, directory: &Path) -> nix::Result<OwnedFd> {
    let socket = socket(family(transport), SockType::Stream, SockFlag::SOCK_CLOEXEC, None)?;
    match transport {
        ChannelTransport::Vsock => connect(socket.as_raw_fd(), &VsockAddr::new(CID_HOST, channel.port()))?,
        ChannelTransport::Unix => connect(
            socket.as_raw_fd(),
            &UnixAddr::new(&transport.endpoint(directory, channel).path)?,
        )?,
    }
    Ok(socket)
}

fn family(transport: ChannelTransport) -> AddressFamily {
    match transport {
        ChannelTransport::Vsock => AddressFamily::Vsock,
        ChannelTransport::Unix => AddressFamily::Unix,
    }
}

pub(crate) fn accept_one(listener: &OwnedFd) -> nix::Result<OwnedFd> {
    let connection = accept4(listener.as_raw_fd(), SockFlag::SOCK_CLOEXEC)?;
    Ok(unsafe { OwnedFd::from_raw_fd(connection) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::fs::PermissionsExt;

    fn private_directory() -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        directory
    }

    #[test]
    fn a_unix_channel_requires_a_private_directory_owned_by_its_runtime() {
        let directory = private_directory();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(
            listen_at(ChannelTransport::Unix, Channel::Control, directory.path()).unwrap_err(),
            nix::errno::Errno::EACCES
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn unix_channels_keep_stream_bytes_private_and_do_not_inherit_across_exec() {
        let directory = private_directory();
        for channel in [Channel::Control, Channel::Filesystem, Channel::Logs] {
            let listener = listen_at(ChannelTransport::Unix, channel, directory.path()).unwrap();
            let endpoint = ChannelTransport::Unix.endpoint(directory.path(), channel);
            assert_eq!(
                std::fs::metadata(&endpoint.path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                listen_at(ChannelTransport::Unix, channel, directory.path()).unwrap_err(),
                nix::errno::Errno::EADDRINUSE
            );
            let client = dial_at(ChannelTransport::Unix, channel, directory.path()).unwrap();
            let accepted = accept_one(&listener).unwrap();
            assert_ne!(
                nix::fcntl::fcntl(&accepted, nix::fcntl::FcntlArg::F_GETFD).unwrap() & libc::FD_CLOEXEC,
                0
            );
            let mut client = std::fs::File::from(client);
            let mut server = std::fs::File::from(accepted);
            client.write_all(b"request").unwrap();
            let mut buffer = [0; 7];
            server.read_exact(&mut buffer).unwrap();
            assert_eq!(&buffer, b"request");
            server.write_all(b"reply").unwrap();
            let mut buffer = [0; 5];
            client.read_exact(&mut buffer).unwrap();
            assert_eq!(&buffer, b"reply");
        }
    }

    #[test]
    fn unix_channels_never_replace_existing_files_or_follow_socket_symlinks() {
        let directory = private_directory();
        let endpoint = ChannelTransport::Unix.endpoint(directory.path(), Channel::Control);
        std::fs::write(&endpoint.path, b"keep").unwrap();
        assert_eq!(
            listen_at(ChannelTransport::Unix, Channel::Control, directory.path()).unwrap_err(),
            nix::errno::Errno::EADDRINUSE
        );
        assert_eq!(std::fs::read(&endpoint.path).unwrap(), b"keep");
        std::fs::remove_file(&endpoint.path).unwrap();
        std::os::unix::fs::symlink("elsewhere", &endpoint.path).unwrap();
        assert_eq!(
            listen_at(ChannelTransport::Unix, Channel::Control, directory.path()).unwrap_err(),
            nix::errno::Errno::EADDRINUSE
        );
        assert!(endpoint.path.is_symlink());
    }
}
