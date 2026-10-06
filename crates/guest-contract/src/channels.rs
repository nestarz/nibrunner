use std::path::{Path, PathBuf};

use crate::vsock;

pub const PROCESS_CHANNEL_DIRECTORY: &str = "/run/nibrunner/channels";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    Logs,
    Control,
    Filesystem,
}

impl Channel {
    pub fn port(self) -> u32 {
        match self {
            Self::Logs => vsock::TENANT_LOG_VSOCK_PORT,
            Self::Control => vsock::GUEST_CONTROL_VSOCK_PORT,
            Self::Filesystem => vsock::GUEST_FILESYSTEM_VSOCK_PORT,
        }
    }

    fn socket_name(self) -> &'static str {
        match self {
            Self::Logs => "logs.sock",
            Self::Control => "control.sock",
            Self::Filesystem => "filesystem.sock",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChannelTransport {
    #[default]
    Vsock,
    Unix,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelEndpoint {
    pub path: PathBuf,
    pub vsock_port: Option<u32>,
}

impl ChannelTransport {
    pub fn endpoint(self, directory: &Path, channel: Channel) -> ChannelEndpoint {
        match (self, channel) {
            (Self::Vsock, Channel::Logs) => ChannelEndpoint {
                path: directory.join(vsock::tenant_log_socket_name()),
                vsock_port: None,
            },
            (Self::Vsock, channel) => ChannelEndpoint {
                path: directory.join(vsock::GUEST_VSOCK_FILENAME),
                vsock_port: Some(channel.port()),
            },
            (Self::Unix, channel) => ChannelEndpoint {
                path: directory.join(channel.socket_name()),
                vsock_port: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn firecracker_multiplexes_requests_and_keeps_its_existing_log_listener() {
        let root = Path::new("/app");
        let control = ChannelTransport::Vsock.endpoint(root, Channel::Control);
        let files = ChannelTransport::Vsock.endpoint(root, Channel::Filesystem);
        assert_eq!(control.path, Path::new("/app/logs.vsock"));
        assert_eq!(files.path, control.path);
        assert_eq!(control.vsock_port, Some(51001));
        assert_eq!(files.vsock_port, Some(51002));
        let logs = ChannelTransport::Vsock.endpoint(root, Channel::Logs);
        assert_eq!(logs.path, Path::new("/app/logs.vsock_51000"));
        assert_eq!(logs.vsock_port, None);
    }

    #[test]
    fn unix_channels_have_distinct_paths_and_need_no_hypervisor_handshake() {
        let root = Path::new(PROCESS_CHANNEL_DIRECTORY);
        let paths: std::collections::BTreeSet<_> = [Channel::Logs, Channel::Control, Channel::Filesystem]
            .into_iter()
            .map(|channel| {
                let endpoint = ChannelTransport::Unix.endpoint(root, channel);
                assert_eq!(endpoint.vsock_port, None);
                assert_eq!(endpoint.path.parent(), Some(root));
                endpoint.path
            })
            .collect();
        assert_eq!(paths.len(), 3);
    }
}
