use protocol::{AppId, HostPort, Ipv4Address};

/// Where the layout starts. How far it goes is the configuration's to say — `max_apps` in
/// config.toml — and nothing here holds a copy of that number.
pub const FIRST_SLOT: u32 = 0;

fn nbd_device_path(minor: u32) -> String {
    format!("/dev/nbd{minor}")
}

/// Slot N takes /dev/nbdN, so the export reader holds the minor past the last slot a host laid
/// out for `max_apps` could hand out.
pub fn export_reader_device_path(max_apps: u32) -> String {
    nbd_device_path(max_apps)
}

/// The first loopback port a slot reserves: where the ports are, not how many a host takes.
pub const HOST_PORT_BASE: u16 = 21_000;

/// How many host ports a slot reserves, whether or not an app asks for them.
///
/// A host port is derived from the slot rather than stored, so this stride is what an app's ports
/// are *at*: changing it moves every app on the host at once, which is the one thing the slot
/// table exists to prevent. So it is set wide enough to outlast the number of ports a host allows
/// an app to declare — `proxy.raw.max_ports_per_app`, which this bounds — and raising that limit is an
/// edit rather than a migration. A reserved port is not an open one: nothing binds or forwards a
/// port no document named.
pub const PORTS_PER_SLOT: u32 = 8;

const _: () = assert!(
    PORTS_PER_SLOT > 1,
    "a slot with no room beside the HTTP port could offer no way in"
);

/// The most apps any host can be laid out for: the last port of the last slot has to be a port.
/// The guest network fits more, so this is the bound `max_apps` is held to.
pub const fn most_apps_the_ports_fit() -> u32 {
    (u16::MAX as u32 - HOST_PORT_BASE as u32 + 1) / PORTS_PER_SLOT
}

/// The host ports no listener of an operator's may take, first and last inclusive, on a host
/// laid out for `max_apps`.
pub fn reserved_port_range(max_apps: u32) -> (u16, u16) {
    let last = u32::from(HOST_PORT_BASE) + max_apps * PORTS_PER_SLOT - 1;
    (
        HOST_PORT_BASE,
        u16::try_from(last).expect("max_apps is held to what the ports fit"),
    )
}

/// Where the guests are: a /30 per slot, so the /16 has room for more slots than the ports do.
pub const GUEST_NETWORK_CIDR: &str = "10.201.0.0/16";
const GUEST_SUBNET_PREFIX_LENGTH: u8 = 30;
const ADDRESSES_PER_SLOT: u32 = 4;
const GUEST_NETWORK_ADDRESSES: u32 = 1 << 16;

const _: () = assert!(
    most_apps_the_ports_fit() <= GUEST_NETWORK_ADDRESSES / ADDRESSES_PER_SLOT,
    "max_apps is held to what the ports fit, so the guest network has to fit at least as many"
);
const GUEST_NETWORK_FIRST_OCTET: u32 = 10;
const GUEST_NETWORK_SECOND_OCTET: u32 = 201;
const OCTET_SIZE: u32 = 256;
const HOST_ADDRESS_OFFSET: u32 = 1;
const GUEST_ADDRESS_OFFSET: u32 = 2;

pub const INTERFACE_NAME_PREFIX: &str = "nbr";

const MAC_PREFIX: &str = "02:00";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppSlot {
    pub slot: u32,
    pub app_id: AppId,
    pub host_port: HostPort,
    pub host_ipv4: Ipv4Address,
    pub guest_ipv4: Ipv4Address,
    pub guest_mac: String,
    pub interface_name: String,
    pub nbd_device_path: String,
    pub subnet_prefix_length: u8,
}

fn address_at(index: u32) -> Ipv4Address {
    Ipv4Address::parse(format!(
        "{GUEST_NETWORK_FIRST_OCTET}.{GUEST_NETWORK_SECOND_OCTET}.{}.{}",
        index / OCTET_SIZE,
        index % OCTET_SIZE
    ))
    .expect("a slot address is always an address")
}

fn mac_for(address: &Ipv4Address) -> String {
    let octets = address
        .as_str()
        .split('.')
        .map(|octet| format!("{:02x}", octet.parse::<u8>().unwrap_or(0)))
        .collect::<Vec<_>>()
        .join(":");
    format!("{MAC_PREFIX}:{octets}")
}

/// The `index`th host port of a slot, or nothing when the slot reserves no such port.
pub fn host_port_at(slot: u32, index: u32) -> Option<HostPort> {
    if index >= PORTS_PER_SLOT {
        return None;
    }
    HostPort::try_from(u32::from(HOST_PORT_BASE) + slot * PORTS_PER_SLOT + index).ok()
}

pub fn describe_slot(slot: u32, app_id: AppId) -> AppSlot {
    let base = slot * ADDRESSES_PER_SLOT;
    let guest_ipv4 = address_at(base + GUEST_ADDRESS_OFFSET);
    AppSlot {
        slot,
        app_id,
        host_port: host_port_at(slot, 0).expect("the first port of a slot is always in range"),
        host_ipv4: address_at(base + HOST_ADDRESS_OFFSET),
        guest_mac: mac_for(&guest_ipv4),
        guest_ipv4,
        interface_name: format!("{INTERFACE_NAME_PREFIX}{slot}"),
        nbd_device_path: nbd_device_path(slot),
        subnet_prefix_length: GUEST_SUBNET_PREFIX_LENGTH,
    }
}

impl AppSlot {
    /// The `index`th host port this slot reserves. Index 0 is [`AppSlot::host_port`].
    pub fn host_port_at(&self, index: u32) -> Option<HostPort> {
        host_port_at(self.slot, index)
    }
}

pub fn is_interface_name(name: &str) -> bool {
    name.strip_prefix(INTERFACE_NAME_PREFIX)
        .is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(name: &str) -> AppId {
        AppId::parse(format!("app-{name}")).unwrap()
    }

    #[test]
    fn every_per_app_resource_comes_from_the_one_number() {
        let slot = describe_slot(0, app("0"));
        assert_eq!(slot.host_port.get(), HOST_PORT_BASE);
        assert_eq!(slot.host_ipv4.as_str(), "10.201.0.1");
        assert_eq!(slot.guest_ipv4.as_str(), "10.201.0.2");
        assert_eq!(slot.guest_mac, "02:00:0a:c9:00:02");
        assert_eq!(slot.interface_name, "nbr0");
        assert_eq!(slot.nbd_device_path, "/dev/nbd0");
        assert_eq!(slot.subnet_prefix_length, 30);
    }

    #[test]
    fn slots_do_not_overlap_and_carry_past_an_octet_boundary() {
        let first = describe_slot(0, app("0"));
        let second = describe_slot(1, app("1"));
        assert_eq!(second.host_ipv4.as_str(), "10.201.0.5");
        assert_eq!(second.guest_ipv4.as_str(), "10.201.0.6");
        assert_eq!(
            second.host_port.get(),
            first.host_port.get() + u16::try_from(PORTS_PER_SLOT).unwrap(),
            "a slot's ports are its own, so the next slot starts past the whole stride"
        );
        assert_eq!(describe_slot(64, app("64")).guest_ipv4.as_str(), "10.201.1.2");
    }

    #[test]
    fn the_export_reader_holds_the_minor_past_the_last_slot() {
        assert_eq!(export_reader_device_path(63), "/dev/nbd63");
        assert_eq!(describe_slot(62, app("last")).nbd_device_path, "/dev/nbd62");
        assert_eq!(export_reader_device_path(1000), "/dev/nbd1000");
    }

    #[test]
    fn a_slot_reaches_every_port_it_reserves_and_never_the_next_slots() {
        let first = describe_slot(0, app("0"));
        let second = describe_slot(1, app("1"));
        assert_eq!(first.host_port_at(0), Some(first.host_port));
        assert_eq!(
            first.host_port_at(PORTS_PER_SLOT - 1).unwrap().get(),
            second.host_port.get() - 1
        );
        assert_eq!(
            first.host_port_at(PORTS_PER_SLOT),
            None,
            "the port after the last one this slot reserves belongs to the next slot"
        );
    }

    #[test]
    fn the_reserved_range_covers_every_port_every_slot_could_hand_out() {
        for max_apps in [1, 63, 1000, most_apps_the_ports_fit()] {
            let (base, end) = reserved_port_range(max_apps);
            assert_eq!(base, HOST_PORT_BASE);
            let last = describe_slot(max_apps - 1, app("last"));
            assert_eq!(last.host_port_at(PORTS_PER_SLOT - 1).unwrap().get(), end);
        }
        assert_eq!(reserved_port_range(1000), (21_000, 28_999));
    }

    #[test]
    fn the_most_apps_the_ports_fit_ends_on_the_last_port_there_is() {
        assert_eq!(most_apps_the_ports_fit(), 5_567);
        assert_eq!(reserved_port_range(most_apps_the_ports_fit()).1, u16::MAX);
        let last = describe_slot(most_apps_the_ports_fit() - 1, app("last"));
        assert_eq!(last.host_port_at(PORTS_PER_SLOT - 1).unwrap().get(), u16::MAX);
        assert_eq!(last.guest_ipv4.as_str(), "10.201.86.250");
    }

    #[test]
    fn attachment_names_are_the_prefix_and_a_number() {
        assert!(is_interface_name("nbr12"));
        assert!(!is_interface_name("nbr"));
        assert!(!is_interface_name("eth0"));
    }
}
