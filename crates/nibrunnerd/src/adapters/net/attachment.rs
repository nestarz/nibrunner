use async_trait::async_trait;
use protocol::Ipv4Address;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostInterface {
    pub interface_name: String,
    pub host_ipv4: Ipv4Address,
    pub subnet_prefix_length: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceInterface {
    pub host: HostInterface,
    pub guest_ipv4: Ipv4Address,
    pub guest_mac: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Neighbour {
    pub guest_ipv4: Ipv4Address,
    pub guest_mac: String,
    pub interface_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{what} could not be done to {device}: {reason}")]
pub struct NetworkError {
    pub what: &'static str,
    pub device: String,
    pub reason: String,
}

impl NetworkError {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait HostNetwork: Send + Sync {
    async fn ensure_tap(&self, tap: &HostInterface) -> Result<(), NetworkError>;

    async fn ensure_namespace(&self, interface: &NamespaceInterface) -> Result<PathBuf, NetworkError>;

    async fn refresh_neighbour(&self, neighbour: &Neighbour) -> Result<(), NetworkError>;

    // Attachments outlive the runtime and the daemon; releasing a slot must reclaim both
    // its interface and any persistent namespace before another app receives that slot.
    async fn delete_attachment(&self, interface_name: &str) -> Result<(), NetworkError>;

    async fn attachment_names(&self) -> Vec<String>;
}

#[cfg(target_os = "linux")]
pub use linux::KernelNetwork;

#[cfg(target_os = "linux")]
mod linux {
    use std::fs::{File, OpenOptions};
    use std::net::{IpAddr, Ipv4Addr};
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;
    use std::path::{Path, PathBuf};

    use async_trait::async_trait;
    use futures::TryStreamExt;
    use netlink_packet_route::link::{InfoData, InfoKind, InfoVeth, LinkAttribute, LinkInfo, LinkMessage};
    use netlink_packet_route::neighbour::NeighbourState;
    use rtnetlink::{Handle, LinkUnspec, LinkVeth, RouteMessageBuilder};

    use super::{HostInterface, HostNetwork, NamespaceInterface, Neighbour, NetworkError};

    pub struct KernelNetwork {
        handle: Handle,
        namespace_dir: PathBuf,
    }

    const PEER: &str = "eth0";
    const NAMESPACE_ALIAS: &str = "nibrunner namespace";

    impl KernelNetwork {
        pub fn open(namespace_dir: PathBuf) -> Result<Self, NetworkError> {
            let (connection, handle, _) = rtnetlink::new_connection().map_err(|error| NetworkError {
                what: "a netlink socket",
                device: "netlink".into(),
                reason: error.to_string(),
            })?;
            tokio::spawn(connection);
            Ok(Self {
                handle,
                namespace_dir,
            })
        }

        async fn link(&self, name: &str) -> Result<Option<LinkMessage>, NetworkError> {
            match self
                .handle
                .link()
                .get()
                .match_name(name.to_owned())
                .execute()
                .try_next()
                .await
            {
                Err(rtnetlink::Error::NetlinkError(error)) if error.raw_code() == -libc::ENODEV => Ok(None),
                result => result.map_err(|error| failed("reading a network interface", name, error)),
            }
        }

        async fn index_of(&self, name: &str) -> Result<u32, NetworkError> {
            self.link(name)
                .await?
                .map(|link| link.header.index)
                .ok_or_else(|| failed("a network interface", name, "it did not appear"))
        }

        async fn address_host(&self, interface: &HostInterface, index: u32) -> Result<(), NetworkError> {
            self.handle
                .address()
                .add(
                    index,
                    IpAddr::V4(interface.host_ipv4.addr()),
                    interface.subnet_prefix_length,
                )
                .replace()
                .execute()
                .await
                .map_err(|error| failed("an address", &interface.interface_name, error))?;
            self.handle
                .link()
                .set(LinkUnspec::new_with_index(index).up().build())
                .execute()
                .await
                .map_err(|error| failed("bringing the link up", &interface.interface_name, error))?;
            allow_route_localnet(&interface.interface_name)
        }
    }

    fn managed_kind(link: &LinkMessage, name: &str) -> Option<InfoKind> {
        let kind = link.attributes.iter().find_map(|attribute| match attribute {
            LinkAttribute::LinkInfo(info) => info.iter().find_map(|info| match info {
                LinkInfo::Kind(kind) => Some(kind.clone()),
                _ => None,
            }),
            _ => None,
        })?;
        match kind {
            InfoKind::Tun => {
                let flags = std::fs::read_to_string(format!("/sys/class/net/{name}/tun_flags")).ok()?;
                let flags = u32::from_str_radix(flags.trim().trim_start_matches("0x"), 16).ok()?;
                (flags & 2 != 0).then_some(InfoKind::Tun)
            }
            InfoKind::Veth if link.attributes.iter().any(|attribute| matches!(attribute, LinkAttribute::IfAlias(alias) if alias == NAMESPACE_ALIAS)) => Some(InfoKind::Veth),
            _ => None,
        }
    }

    fn validate_name(name: &str) -> Result<(), NetworkError> {
        if !nft_render::is_interface_name(name) || name.len() >= libc::IFNAMSIZ {
            return Err(failed(
                "a managed network attachment",
                name,
                "the name is not an app slot",
            ));
        }
        Ok(())
    }

    fn mac_bytes(mac: &str, name: &str) -> Result<Vec<u8>, NetworkError> {
        let octets = mac
            .split(':')
            .map(|part| {
                if part.len() == 2 {
                    u8::from_str_radix(part, 16).ok()
                } else {
                    None
                }
            })
            .collect::<Option<Vec<_>>>();
        octets
            .filter(|octets| octets.len() == 6)
            .ok_or_else(|| failed("a network address", name, "invalid MAC address"))
    }

    fn failed(what: &'static str, device: &str, reason: impl std::fmt::Display) -> NetworkError {
        NetworkError {
            what,
            device: device.to_string(),
            reason: reason.to_string(),
        }
    }

    #[allow(unsafe_code)]
    fn create_persistent_tap(interface_name: &str) -> Result<(), NetworkError> {
        use std::os::fd::AsRawFd;

        const IFF_TAP: libc::c_short = 0x0002;
        const IFF_NO_PI: libc::c_short = 0x1000;
        const TUNSETIFF: libc::Ioctl = 0x400454ca;
        const TUNSETPERSIST: libc::Ioctl = 0x400454cb;

        #[repr(C)]
        struct InterfaceRequest {
            name: [libc::c_char; libc::IFNAMSIZ],
            flags: libc::c_short,
            padding: [u8; 22],
        }

        let device = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/net/tun")
            .map_err(|error| failed("a tap device", interface_name, error))?;
        let mut request = InterfaceRequest {
            name: [0; libc::IFNAMSIZ],
            flags: IFF_TAP | IFF_NO_PI,
            padding: [0; 22],
        };
        for (index, byte) in interface_name
            .as_bytes()
            .iter()
            .take(libc::IFNAMSIZ - 1)
            .enumerate()
        {
            request.name[index] = *byte as libc::c_char;
        }
        let created = unsafe { libc::ioctl(device.as_raw_fd(), TUNSETIFF, &mut request) };
        if created < 0 {
            return Err(failed(
                "a tap device",
                interface_name,
                std::io::Error::last_os_error(),
            ));
        }
        let persisted = unsafe { libc::ioctl(device.as_raw_fd(), TUNSETPERSIST, 1) };
        if persisted < 0 {
            return Err(failed(
                "a persistent tap device",
                interface_name,
                std::io::Error::last_os_error(),
            ));
        }
        Ok(())
    }

    fn allow_route_localnet(interface_name: &str) -> Result<(), NetworkError> {
        let path = format!("/proc/sys/net/ipv4/conf/{interface_name}/route_localnet");
        std::fs::write(&path, b"1").map_err(|error| failed("route_localnet", interface_name, error))
    }

    #[async_trait]
    impl HostNetwork for KernelNetwork {
        async fn ensure_tap(&self, tap: &HostInterface) -> Result<(), NetworkError> {
            validate_name(&tap.interface_name)?;
            match self.link(&tap.interface_name).await? {
                None => create_persistent_tap(&tap.interface_name)?,
                Some(link) if managed_kind(&link, &tap.interface_name) == Some(InfoKind::Tun) => {}
                Some(_) => {
                    return Err(failed(
                        "a tap device",
                        &tap.interface_name,
                        "a different interface already owns this name",
                    ))
                }
            }
            let index = self.index_of(&tap.interface_name).await?;
            self.address_host(tap, index).await
        }

        async fn ensure_namespace(&self, interface: &NamespaceInterface) -> Result<PathBuf, NetworkError> {
            let name = &interface.host.interface_name;
            validate_name(name)?;
            let mac = mac_bytes(&interface.guest_mac, name)?;
            let existing = self.link(name).await?;
            if existing
                .as_ref()
                .is_some_and(|link| managed_kind(link, name) != Some(InfoKind::Veth))
            {
                return Err(failed(
                    "a namespace interface",
                    name,
                    "a different interface already owns this name",
                ));
            }
            crate::json_store::make_directory(&self.namespace_dir, 0o700)
                .map_err(|error| failed("a namespace directory", name, error))?;
            let namespace_path = self.namespace_dir.join(name);
            if existing.is_some() && !is_namespace(&namespace_path)? {
                return Err(failed(
                    "a namespace interface",
                    name,
                    "its namespace handle is missing",
                ));
            }
            namespace_work(NamespaceWork::Create(namespace_path.clone())).await?;
            let namespace =
                File::open(&namespace_path).map_err(|error| failed("a namespace handle", name, error))?;
            if existing.is_none() {
                let peer = LinkUnspec::new_with_name(PEER)
                    .setns_by_fd(namespace.as_raw_fd())
                    .address(mac)
                    .build();
                self.handle
                    .link()
                    .add(
                        LinkVeth::new(name, PEER)
                            .alias(NAMESPACE_ALIAS)
                            .set_info_data(InfoData::Veth(InfoVeth::Peer(peer)))
                            .build(),
                    )
                    .execute()
                    .await
                    .map_err(|error| failed("a namespace interface", name, error))?;
            }
            let index = self.index_of(name).await?;
            self.address_host(&interface.host, index).await?;
            namespace_work(NamespaceWork::Configure(
                namespace_path.clone(),
                interface.clone(),
            ))
            .await?;
            Ok(namespace_path)
        }

        async fn refresh_neighbour(&self, neighbour: &Neighbour) -> Result<(), NetworkError> {
            validate_name(&neighbour.interface_name)?;
            let link = self
                .link(&neighbour.interface_name)
                .await?
                .filter(|link| managed_kind(link, &neighbour.interface_name).is_some())
                .ok_or_else(|| {
                    failed(
                        "a neighbour entry",
                        &neighbour.interface_name,
                        "the interface is not managed by nibrunner",
                    )
                })?;
            let index = link.header.index;
            let mac = mac_bytes(&neighbour.guest_mac, &neighbour.interface_name)?;
            self.handle
                .neighbours()
                .add(index, IpAddr::V4(neighbour.guest_ipv4.addr()))
                .link_layer_address(&mac)
                .state(NeighbourState::Reachable)
                .replace()
                .execute()
                .await
                .map_err(|error| failed("a neighbour entry", &neighbour.interface_name, error))
        }

        async fn delete_attachment(&self, name: &str) -> Result<(), NetworkError> {
            validate_name(name)?;
            let namespace = self.namespace_dir.join(name);
            let mounted = is_namespace(&namespace)?;
            if !mounted && std::fs::metadata(&namespace).is_ok_and(|metadata| metadata.len() != 0) {
                return Err(failed(
                    "removing a namespace handle",
                    name,
                    "the path contains an unrelated file",
                ));
            }
            if let Some(link) = self.link(name).await? {
                if managed_kind(&link, name).is_none() {
                    return Err(failed(
                        "removing an attachment",
                        name,
                        "the interface is not managed by nibrunner",
                    ));
                }
                self.handle
                    .link()
                    .del(link.header.index)
                    .execute()
                    .await
                    .map_err(|error| failed("removing an attachment", name, error))?;
            }
            if mounted {
                nix::mount::umount2(&namespace, nix::mount::MntFlags::MNT_DETACH)
                    .map_err(|error| failed("removing a namespace", name, error))?;
            }
            match std::fs::remove_file(namespace) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    Err(failed("removing a namespace handle", name, error))
                }
                _ => Ok(()),
            }
        }

        async fn attachment_names(&self) -> Vec<String> {
            let mut names = std::collections::BTreeSet::new();
            let mut links = self.handle.link().get().execute();
            while let Ok(Some(message)) = links.try_next().await {
                for attribute in &message.attributes {
                    if let LinkAttribute::IfName(name) = attribute {
                        if nft_render::is_interface_name(name) && managed_kind(&message, name).is_some() {
                            names.insert(name.clone());
                        }
                    }
                }
            }
            // The namespace can survive a failed veth creation or an interrupted removal.
            for entry in std::fs::read_dir(&self.namespace_dir)
                .into_iter()
                .flatten()
                .flatten()
            {
                if let Some(name) = entry
                    .file_name()
                    .to_str()
                    .filter(|name| nft_render::is_interface_name(name))
                {
                    names.insert(name.to_owned());
                }
            }
            names.into_iter().collect()
        }
    }

    fn is_namespace(path: &Path) -> Result<bool, NetworkError> {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(failed(
                    "a namespace handle",
                    &path.display().to_string(),
                    "symbolic links are not namespace handles",
                ));
            }
            Ok(metadata) if !metadata.is_file() => {
                return Err(failed(
                    "a namespace handle",
                    &path.display().to_string(),
                    "the path is not a regular namespace handle",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(failed("a namespace handle", &path.display().to_string(), error)),
            _ => {}
        }
        let file = File::open(path)
            .map_err(|error| failed("a namespace handle", &path.display().to_string(), error))?;
        let stat = nix::sys::statfs::fstatfs(file)
            .map_err(|error| failed("a namespace handle", &path.display().to_string(), error))?;
        Ok(stat.filesystem_type() == nix::sys::statfs::NSFS_MAGIC)
    }

    enum NamespaceWork {
        Create(PathBuf),
        Configure(PathBuf, NamespaceInterface),
    }

    impl NamespaceWork {
        fn run(self) -> Result<(), NetworkError> {
            use nix::sched::{setns, unshare, CloneFlags};
            match self {
                Self::Create(path) => {
                    if is_namespace(&path)? {
                        return Ok(());
                    }
                    let name = path.display().to_string();
                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .mode(0o600)
                        .custom_flags(libc::O_NOFOLLOW)
                        .open(&path)
                        .map_err(|error| failed("creating a namespace handle", &name, error))?;
                    if file
                        .metadata()
                        .map_err(|error| failed("reading a namespace handle", &name, error))?
                        .len()
                        != 0
                    {
                        return Err(failed(
                            "creating a namespace handle",
                            &name,
                            "the path contains an unrelated file",
                        ));
                    }
                    unshare(CloneFlags::CLONE_NEWNET)
                        .map_err(|error| failed("creating a network namespace", &name, error))?;
                    nix::mount::mount(
                        Some("/proc/thread-self/ns/net"),
                        &path,
                        None::<&str>,
                        nix::mount::MsFlags::MS_BIND,
                        None::<&str>,
                    )
                    .map_err(|error| failed("persisting a network namespace", &name, error))
                }
                Self::Configure(path, interface) => {
                    let name = &interface.host.interface_name;
                    let namespace =
                        File::open(path).map_err(|error| failed("a namespace handle", name, error))?;
                    setns(namespace, CloneFlags::CLONE_NEWNET)
                        .map_err(|error| failed("entering a network namespace", name, error))?;
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| failed("configuring a network namespace", name, error))?;
                    runtime.block_on(configure_namespace(&interface))
                }
            }
        }
    }

    async fn namespace_work(work: NamespaceWork) -> Result<(), NetworkError> {
        let (send, receive) = tokio::sync::oneshot::channel();
        // setns changes the calling thread. A fresh OS thread must exit here, never return
        // to Tokio's shared worker pool with a tenant's network namespace still selected.
        std::thread::Builder::new()
            .name("nibrunner-netns".into())
            .spawn(move || {
                let _ = send.send(work.run());
            })
            .map_err(|error| failed("starting a namespace worker", "netns", error))?;
        receive
            .await
            .map_err(|error| failed("finishing a namespace worker", "netns", error))?
    }

    async fn configure_namespace(interface: &NamespaceInterface) -> Result<(), NetworkError> {
        let network = KernelNetwork::open(PathBuf::new())?;
        let name = &interface.host.interface_name;
        let peer = network.index_of(PEER).await?;
        let loopback = network.index_of("lo").await?;
        for link in [
            LinkUnspec::new_with_index(loopback).up().build(),
            LinkUnspec::new_with_index(peer)
                .address(mac_bytes(&interface.guest_mac, name)?)
                .up()
                .build(),
        ] {
            network
                .handle
                .link()
                .set(link)
                .execute()
                .await
                .map_err(|error| failed("bringing a namespace link up", name, error))?;
        }
        network
            .handle
            .address()
            .add(
                peer,
                IpAddr::V4(interface.guest_ipv4.addr()),
                interface.host.subnet_prefix_length,
            )
            .replace()
            .execute()
            .await
            .map_err(|error| failed("a namespace address", name, error))?;
        network
            .handle
            .route()
            .add(
                RouteMessageBuilder::<Ipv4Addr>::new()
                    .destination_prefix(Ipv4Addr::UNSPECIFIED, 0)
                    .gateway(interface.host.host_ipv4.addr())
                    .output_interface(peer)
                    .build(),
            )
            .replace()
            .execute()
            .await
            .map_err(|error| failed("a namespace default route", name, error))
    }

    pub(super) fn _assert_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}
        is_send_sync::<KernelNetwork>();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::mocks;

    #[tokio::test]
    async fn what_a_boot_asks_of_the_network_is_recorded_in_order() {
        let (network, spy) = mocks::network();
        let slot = nft_render::describe_slot(0, protocol::AppId::parse("app-1").unwrap());
        network
            .ensure_tap(&HostInterface {
                interface_name: slot.interface_name.clone(),
                host_ipv4: slot.host_ipv4.clone(),
                subnet_prefix_length: slot.subnet_prefix_length,
            })
            .await
            .unwrap();
        network
            .refresh_neighbour(&Neighbour {
                guest_ipv4: slot.guest_ipv4.clone(),
                guest_mac: slot.guest_mac.clone(),
                interface_name: slot.interface_name.clone(),
            })
            .await
            .unwrap();
        assert_eq!(network.attachment_names().await, vec!["nbr0".to_string()]);
        assert_eq!(spy.neighbours()[0].guest_mac, "02:00:0a:c9:00:02");
    }

    #[tokio::test]
    async fn a_kernel_that_will_not_give_this_host_a_tap_says_which_device_and_why() {
        let refusal = NetworkError {
            what: "a tap device",
            device: "nbr0".into(),
            reason: "operation not permitted".into(),
        };
        let network = mocks::network_refusing(refusal.clone());
        let slot = nft_render::describe_slot(0, protocol::AppId::parse("app-1").unwrap());
        let refused = network
            .ensure_tap(&HostInterface {
                interface_name: slot.interface_name.clone(),
                host_ipv4: slot.host_ipv4.clone(),
                subnet_prefix_length: slot.subnet_prefix_length,
            })
            .await
            .unwrap_err();
        assert_eq!(refused, refusal);
        assert_eq!(
            refused.message(),
            "a tap device could not be done to nbr0: operation not permitted"
        );
        assert!(network
            .refresh_neighbour(&Neighbour {
                guest_ipv4: slot.guest_ipv4.clone(),
                guest_mac: slot.guest_mac.clone(),
                interface_name: slot.interface_name.clone(),
            })
            .await
            .is_err());
        assert!(network.attachment_names().await.is_empty());
    }

    #[tokio::test]
    async fn a_tap_asked_for_twice_is_the_same_device_asked_for_twice() {
        let (network, spy) = mocks::network();
        let slot = nft_render::describe_slot(0, protocol::AppId::parse("app-1").unwrap());
        let tap = HostInterface {
            interface_name: slot.interface_name.clone(),
            host_ipv4: slot.host_ipv4.clone(),
            subnet_prefix_length: slot.subnet_prefix_length,
        };
        network.ensure_tap(&tap).await.unwrap();
        network.ensure_tap(&tap).await.unwrap();
        assert_eq!(spy.taps(), vec![tap.clone(), tap]);
    }
}
