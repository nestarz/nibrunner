use async_trait::async_trait;
use protocol::Ipv4Address;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TapInterface {
    pub tap_name: String,
    pub host_ipv4: Ipv4Address,
    pub subnet_prefix_length: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Neighbour {
    pub guest_ipv4: Ipv4Address,
    pub guest_mac: String,
    pub tap_name: String,
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
    async fn ensure_tap(&self, tap: &TapInterface) -> Result<(), NetworkError>;

    async fn refresh_neighbour(&self, neighbour: &Neighbour) -> Result<(), NetworkError>;

    async fn set_tap_owner(&self, tap_name: &str, _uid: u32) -> Result<(), NetworkError> {
        Err(NetworkError {
            what: "tap ownership",
            device: tap_name.into(),
            reason: "this network adapter cannot assign an owner".into(),
        })
    }

    // A tap outlives the process that made it, which is what lets a daemon restart leave every
    // tenant serving. The same persistence means nothing reclaims one, so an app that leaves has
    // to say so.
    async fn delete_tap(&self, tap_name: &str) -> Result<(), NetworkError>;

    async fn tap_names(&self) -> Vec<String>;
}

#[cfg(target_os = "linux")]
pub use linux::KernelNetwork;

#[cfg(target_os = "linux")]
mod linux {
    use std::net::IpAddr;

    use async_trait::async_trait;
    use futures::TryStreamExt;
    use netlink_packet_route::link::{InfoKind, LinkAttribute, LinkInfo, LinkMessage};
    use netlink_packet_route::neighbour::NeighbourState;
    use rtnetlink::{Handle, LinkUnspec};

    use super::{HostNetwork, Neighbour, NetworkError, TapInterface};

    pub struct KernelNetwork {
        handle: Handle,
    }

    // IFF_TAP in /sys/class/net/<name>/tun_flags: a tun device in tun mode is not a tap a VMM can use.
    const TUN_FLAG_TAP: u32 = 0x0002;

    impl KernelNetwork {
        pub fn open() -> Result<Self, NetworkError> {
            let (connection, handle, _) = rtnetlink::new_connection().map_err(|error| NetworkError {
                what: "a netlink socket",
                device: "netlink".into(),
                reason: error.to_string(),
            })?;
            tokio::spawn(connection);
            Ok(Self { handle })
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

        async fn index_of(&self, tap_name: &str) -> Option<u32> {
            self.link(tap_name)
                .await
                .ok()
                .flatten()
                .map(|message| message.header.index)
        }

        async fn own_tap(&self, tap_name: &str) -> Result<Option<LinkMessage>, NetworkError> {
            match self.link(tap_name).await? {
                Some(link) if !is_a_tap(&link, tap_name) => Err(failed(
                    "a tap device",
                    tap_name,
                    "a different interface already owns this name",
                )),
                link => Ok(link),
            }
        }
    }

    // Only a tap (a tun device in tap mode) is one this host may address, re-own or delete.
    // Anything else under a slot's name is not ours, whatever it is called.
    fn is_a_tap(link: &LinkMessage, name: &str) -> bool {
        let is_tun = link.attributes.iter().any(|attribute| {
            matches!(attribute, LinkAttribute::LinkInfo(info)
                if info.iter().any(|info| matches!(info, LinkInfo::Kind(InfoKind::Tun))))
        });
        is_tun
            && std::fs::read_to_string(format!("/sys/class/net/{name}/tun_flags"))
                .ok()
                .and_then(|flags| tun_flags_are_tap(&flags))
                .unwrap_or(false)
    }

    fn tun_flags_are_tap(flags: &str) -> Option<bool> {
        let flags = u32::from_str_radix(flags.trim().trim_start_matches("0x"), 16).ok()?;
        Some(flags & TUN_FLAG_TAP != 0)
    }

    fn validate_name(name: &str) -> Result<(), NetworkError> {
        if !nft_render::is_tap_name(name) || name.len() >= libc::IFNAMSIZ {
            return Err(failed("a tap device", name, "the name is not an app slot's"));
        }
        Ok(())
    }

    fn mac_bytes(mac: &str, name: &str) -> Result<Vec<u8>, NetworkError> {
        mac.split(':')
            .map(|part| {
                (part.len() == 2)
                    .then(|| u8::from_str_radix(part, 16).ok())
                    .flatten()
            })
            .collect::<Option<Vec<_>>>()
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
    fn create_persistent_tap(tap_name: &str) -> Result<(), NetworkError> {
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
            .map_err(|error| failed("a tap device", tap_name, error))?;
        let mut request = InterfaceRequest {
            name: [0; libc::IFNAMSIZ],
            flags: IFF_TAP | IFF_NO_PI,
            padding: [0; 22],
        };
        for (index, byte) in tap_name.as_bytes().iter().take(libc::IFNAMSIZ - 1).enumerate() {
            request.name[index] = *byte as libc::c_char;
        }
        let created = unsafe { libc::ioctl(device.as_raw_fd(), TUNSETIFF, &mut request) };
        if created < 0 {
            return Err(failed("a tap device", tap_name, std::io::Error::last_os_error()));
        }
        let persisted = unsafe { libc::ioctl(device.as_raw_fd(), TUNSETPERSIST, 1) };
        if persisted < 0 {
            return Err(failed(
                "a persistent tap device",
                tap_name,
                std::io::Error::last_os_error(),
            ));
        }
        Ok(())
    }

    #[allow(
        unsafe_code,
        reason = "assigning a persistent TAP to its jailed VMM requires TUNSETOWNER"
    )]
    fn set_tap_owner(tap_name: &str, uid: u32) -> Result<(), NetworkError> {
        use std::os::fd::AsRawFd;
        const TUNSETIFF: libc::Ioctl = 0x400454ca;
        const TUNSETOWNER: libc::Ioctl = 0x400454cc;
        const IFF_TAP: libc::c_short = 0x0002;
        const IFF_NO_PI: libc::c_short = 0x1000;
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
            .map_err(|error| failed("tap ownership", tap_name, error))?;
        let mut request = InterfaceRequest {
            name: [0; libc::IFNAMSIZ],
            flags: IFF_TAP | IFF_NO_PI,
            padding: [0; 22],
        };
        for (index, byte) in tap_name.as_bytes().iter().take(libc::IFNAMSIZ - 1).enumerate() {
            request.name[index] = *byte as libc::c_char;
        }
        if unsafe { libc::ioctl(device.as_raw_fd(), TUNSETIFF, &mut request) } < 0
            || unsafe { libc::ioctl(device.as_raw_fd(), TUNSETOWNER, uid) } < 0
        {
            return Err(failed("tap ownership", tap_name, std::io::Error::last_os_error()));
        }
        Ok(())
    }

    fn allow_route_localnet(tap_name: &str) -> Result<(), NetworkError> {
        let path = format!("/proc/sys/net/ipv4/conf/{tap_name}/route_localnet");
        std::fs::write(&path, b"1").map_err(|error| failed("route_localnet", tap_name, error))
    }

    #[async_trait]
    impl HostNetwork for KernelNetwork {
        async fn ensure_tap(&self, tap: &TapInterface) -> Result<(), NetworkError> {
            validate_name(&tap.tap_name)?;
            if self.own_tap(&tap.tap_name).await?.is_none() {
                create_persistent_tap(&tap.tap_name)?;
            }
            let index = self
                .index_of(&tap.tap_name)
                .await
                .ok_or_else(|| failed("a tap device", &tap.tap_name, "it did not appear"))?;
            self.handle
                .address()
                .add(index, IpAddr::V4(tap.host_ipv4.addr()), tap.subnet_prefix_length)
                .replace()
                .execute()
                .await
                .map_err(|error| failed("an address", &tap.tap_name, error))?;
            self.handle
                .link()
                .set(LinkUnspec::new_with_index(index).up().build())
                .execute()
                .await
                .map_err(|error| failed("bringing the link up", &tap.tap_name, error))?;
            allow_route_localnet(&tap.tap_name)
        }

        async fn set_tap_owner(&self, tap_name: &str, uid: u32) -> Result<(), NetworkError> {
            set_tap_owner(tap_name, uid)
        }

        async fn refresh_neighbour(&self, neighbour: &Neighbour) -> Result<(), NetworkError> {
            let mac = mac_bytes(&neighbour.guest_mac, &neighbour.tap_name)?;
            let index = self
                .own_tap(&neighbour.tap_name)
                .await?
                .map(|link| link.header.index)
                .ok_or_else(|| failed("a neighbour entry", &neighbour.tap_name, "the tap is not there"))?;
            self.handle
                .neighbours()
                .add(index, IpAddr::V4(neighbour.guest_ipv4.addr()))
                .link_layer_address(&mac)
                .state(NeighbourState::Reachable)
                .replace()
                .execute()
                .await
                .map_err(|error| failed("a neighbour entry", &neighbour.tap_name, error))
        }

        async fn delete_tap(&self, tap_name: &str) -> Result<(), NetworkError> {
            validate_name(tap_name)?;
            // A tap that is not there is the state being asked for, not a failure: a daemon that
            // died between stopping an app and reclaiming its slot comes back to this line.
            let Some(link) = self.own_tap(tap_name).await? else {
                return Ok(());
            };
            self.handle
                .link()
                .del(link.header.index)
                .execute()
                .await
                .map_err(|error| failed("removing a tap device", tap_name, error))
        }

        async fn tap_names(&self) -> Vec<String> {
            let mut names = Vec::new();
            let mut links = self.handle.link().get().execute();
            while let Ok(Some(message)) = links.try_next().await {
                for attribute in &message.attributes {
                    if let LinkAttribute::IfName(name) = attribute {
                        if nft_render::is_tap_name(name) && is_a_tap(&message, name) {
                            names.push(name.clone());
                        }
                    }
                }
            }
            names
        }
    }

    pub(super) fn _assert_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}
        is_send_sync::<KernelNetwork>();
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn only_a_tun_device_in_tap_mode_reads_as_a_tap() {
            assert_eq!(tun_flags_are_tap("0x1002\n"), Some(true));
            assert_eq!(tun_flags_are_tap("0x1001\n"), Some(false));
            assert_eq!(tun_flags_are_tap("not flags"), None);
        }

        #[test]
        fn a_mac_address_is_six_two_digit_octets_or_it_is_refused() {
            assert_eq!(
                mac_bytes("02:00:0a:c9:00:02", "nbr0").unwrap(),
                [0x02, 0x00, 0x0a, 0xc9, 0x00, 0x02]
            );
            for invalid in [
                "1:2:3:4:5:6",
                "02:00:0a:c9:00",
                "02:00:0a:c9:00:02:03",
                "02:00:0a:c9:00:zz",
            ] {
                assert!(mac_bytes(invalid, "nbr0").is_err(), "{invalid}");
            }
        }

        #[test]
        fn a_name_that_is_not_an_app_slot_is_never_a_tap_to_touch() {
            assert!(validate_name("nbr0").is_ok());
            for name in ["../outside", "nbr1234567890123456", "lo", "eth0"] {
                assert!(validate_name(name).is_err(), "{name}");
            }
        }
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
            .ensure_tap(&TapInterface {
                tap_name: slot.tap_name.clone(),
                host_ipv4: slot.host_ipv4.clone(),
                subnet_prefix_length: slot.subnet_prefix_length,
            })
            .await
            .unwrap();
        network
            .refresh_neighbour(&Neighbour {
                guest_ipv4: slot.guest_ipv4.clone(),
                guest_mac: slot.guest_mac.clone(),
                tap_name: slot.tap_name.clone(),
            })
            .await
            .unwrap();
        assert_eq!(network.tap_names().await, vec!["nbr0".to_string()]);
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
            .ensure_tap(&TapInterface {
                tap_name: slot.tap_name.clone(),
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
                tap_name: slot.tap_name.clone(),
            })
            .await
            .is_err());
        assert!(network.tap_names().await.is_empty());
    }

    #[tokio::test]
    async fn a_tap_asked_for_twice_is_the_same_device_asked_for_twice() {
        let (network, spy) = mocks::network();
        let slot = nft_render::describe_slot(0, protocol::AppId::parse("app-1").unwrap());
        let tap = TapInterface {
            tap_name: slot.tap_name.clone(),
            host_ipv4: slot.host_ipv4.clone(),
            subnet_prefix_length: slot.subnet_prefix_length,
        };
        network.ensure_tap(&tap).await.unwrap();
        network.ensure_tap(&tap).await.unwrap();
        assert_eq!(spy.taps(), vec![tap.clone(), tap]);
    }
}
