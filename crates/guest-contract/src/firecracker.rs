use protocol::{InstanceResources, Ipv4Address};
use serde::{Deserialize, Serialize};

/// The drives every guest has, in the order they are attached: the host's root, the config
/// image, the app's volume. The layers follow, one drive each, so these three sit at the same
/// device however many there are.
pub const FIXED_DRIVE_IDS: [&str; 3] = ["rootfs", "config", "data"];

pub fn layer_drive_id(index: usize) -> String {
    format!("layer{index}")
}

const BASE_KERNEL_ARGS: &str = "console=ttyS0 quiet reboot=k panic=1 pci=off i8042.noaux i8042.nomux i8042.dumbkbd clocksource=kvm-clock root=/dev/vda ro init=/init";

pub fn netmask_for(prefix_length: u8) -> String {
    let mask: u32 = if prefix_length == 0 {
        0
    } else {
        u32::MAX << (32 - u32::from(prefix_length.min(32)))
    };
    std::net::Ipv4Addr::from(mask).to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmNetwork {
    pub tap_name: String,
    pub guest_mac: String,
    pub guest_ipv4: Ipv4Address,
    pub host_ipv4: Ipv4Address,
    pub subnet_prefix_length: u8,
}

pub fn render_kernel_args(network: &VmNetwork) -> String {
    format!(
        "{BASE_KERNEL_ARGS} ip={}::{}:{}::eth0:off",
        network.guest_ipv4,
        network.host_ipv4,
        netmask_for(network.subnet_prefix_length)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheType {
    Unsafe,
    Writeback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IoEngine {
    Sync,
    Async,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirecrackerDrive {
    pub drive_id: String,
    pub path_on_host: String,
    pub is_root_device: bool,
    pub is_read_only: bool,
    pub cache_type: CacheType,
    pub io_engine: IoEngine,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootSource {
    pub kernel_image_path: String,
    pub boot_args: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineConfig {
    pub vcpu_count: u32,
    pub mem_size_mib: u32,
    pub smt: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkInterface {
    pub iface_id: String,
    pub host_dev_name: String,
    pub guest_mac: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VsockDevice {
    pub guest_cid: u32,
    pub uds_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalloonConfig {
    pub amount_mib: u32,
    pub deflate_on_oom: bool,
    pub stats_polling_interval_s: u16,
    pub free_page_reporting: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirecrackerConfig {
    #[serde(rename = "boot-source")]
    pub boot_source: BootSource,
    pub drives: Vec<FirecrackerDrive>,
    #[serde(rename = "machine-config")]
    pub machine_config: MachineConfig,
    #[serde(rename = "network-interfaces")]
    pub network_interfaces: Vec<NetworkInterface>,
    pub vsock: VsockDevice,
    pub balloon: BalloonConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmPaths {
    pub kernel_path: String,
    pub rootfs_path: String,
    pub instance_config_image_path: String,
    pub data_device_path: String,
    /// Bottom layer first, as the document listed them.
    pub layer_image_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmVsock {
    pub guest_cid: u32,
    pub path: String,
}

const NETWORK_INTERFACE_ID: &str = "eth0";

fn read_only_drive(drive_id: &str, path_on_host: &str, is_root_device: bool) -> FirecrackerDrive {
    FirecrackerDrive {
        drive_id: drive_id.to_string(),
        path_on_host: path_on_host.to_string(),
        is_root_device,
        is_read_only: true,
        cache_type: CacheType::Unsafe,
        io_engine: IoEngine::Sync,
    }
}

pub fn render_firecracker_config(
    resources: InstanceResources,
    paths: &VmPaths,
    network: &VmNetwork,
    vsock: &VmVsock,
) -> FirecrackerConfig {
    FirecrackerConfig {
        boot_source: BootSource {
            kernel_image_path: paths.kernel_path.clone(),
            boot_args: render_kernel_args(network),
        },
        drives: [
            read_only_drive(FIXED_DRIVE_IDS[0], &paths.rootfs_path, true),
            read_only_drive(FIXED_DRIVE_IDS[1], &paths.instance_config_image_path, false),
            FirecrackerDrive {
                drive_id: FIXED_DRIVE_IDS[2].to_string(),
                path_on_host: paths.data_device_path.clone(),
                is_root_device: false,
                is_read_only: false,
                cache_type: CacheType::Writeback,
                io_engine: IoEngine::Sync,
            },
        ]
        .into_iter()
        .chain(
            paths
                .layer_image_paths
                .iter()
                .enumerate()
                .map(|(index, path)| read_only_drive(&layer_drive_id(index), path, false)),
        )
        .collect(),
        balloon: BalloonConfig {
            amount_mib: 0,
            deflate_on_oom: false,
            stats_polling_interval_s: 0,
            free_page_reporting: true,
        },
        machine_config: MachineConfig {
            vcpu_count: resources.vcpu_count,
            mem_size_mib: resources.memory_mib,
            smt: false,
        },
        network_interfaces: vec![NetworkInterface {
            iface_id: NETWORK_INTERFACE_ID.to_string(),
            host_dev_name: network.tap_name.clone(),
            guest_mac: network.guest_mac.clone(),
        }],
        vsock: VsockDevice {
            guest_cid: vsock.guest_cid,
            uds_path: vsock.path.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::DEFAULT_INSTANCE_RESOURCES;

    fn network() -> VmNetwork {
        VmNetwork {
            tap_name: "nbr3".into(),
            guest_mac: "02:00:0a:c9:00:0e".into(),
            guest_ipv4: Ipv4Address::parse("10.201.0.14").unwrap(),
            host_ipv4: Ipv4Address::parse("10.201.0.13").unwrap(),
            subnet_prefix_length: 30,
        }
    }

    fn paths() -> VmPaths {
        VmPaths {
            kernel_path: "/opt/nibrun/bin/guest-image/vmlinux".into(),
            rootfs_path: "/opt/nibrun/bin/guest-image/rootfs.ext4".into(),
            instance_config_image_path: "/var/lib/nibrun/vm/inst-1/config.squashfs".into(),
            data_device_path: "/dev/nbd3".into(),
            layer_image_paths: vec![
                "/var/lib/nibrun/artifacts/base/layer.img".into(),
                "/var/lib/nibrun/artifacts/abc/packed-0123456789abcdef.squashfs".into(),
            ],
        }
    }

    fn config() -> FirecrackerConfig {
        render_firecracker_config(
            DEFAULT_INSTANCE_RESOURCES,
            &paths(),
            &network(),
            &VmVsock {
                guest_cid: 6,
                path: "logs.vsock".into(),
            },
        )
    }

    #[test]
    fn vda_vdb_vdc_are_rootfs_config_data_and_the_layers_follow_in_document_order() {
        let rendered = config();
        let ids: Vec<&str> = rendered.drives.iter().map(|d| d.drive_id.as_str()).collect();
        assert_eq!(ids, ["rootfs", "config", "data", "layer0", "layer1"]);
        let paths_on_host: Vec<String> = rendered.drives.iter().map(|d| d.path_on_host.clone()).collect();
        let p = paths();
        assert_eq!(
            paths_on_host,
            vec![
                p.rootfs_path,
                p.instance_config_image_path,
                p.data_device_path,
                p.layer_image_paths[0].clone(),
                p.layer_image_paths[1].clone(),
            ]
        );
        assert_eq!(rendered.drives.iter().filter(|d| d.is_root_device).count(), 1);
        assert!(rendered.drives[0].is_root_device);
        for (index, drive) in rendered.drives[FIXED_DRIVE_IDS.len()..].iter().enumerate() {
            assert_eq!(drive.drive_id, layer_drive_id(index));
        }
    }

    #[test]
    fn the_tenant_data_drive_is_the_only_writable_one_and_is_writeback_so_a_guest_fsync_reaches_the_host() {
        let drives = config().drives;
        let data = &drives[2];
        assert_eq!(data.cache_type, CacheType::Writeback);
        assert!(!data.is_read_only);
        for drive in drives.iter().filter(|d| d.drive_id != "data") {
            assert!(drive.is_read_only, "{}", drive.drive_id);
            assert_eq!(drive.cache_type, CacheType::Unsafe);
        }
        assert!(drives.iter().all(|d| d.io_engine == IoEngine::Sync));
    }

    #[test]
    fn the_kernel_command_line_carries_what_a_snapshot_and_a_graceful_stop_need() {
        let args = render_kernel_args(&network());
        assert!(args.contains("console=ttyS0 quiet"));
        for flag in ["i8042.noaux", "i8042.nomux", "i8042.dumbkbd"] {
            assert!(args.contains(flag));
        }
        assert!(args.contains("clocksource=kvm-clock"));
        assert!(!args.contains("i8042.nopnp"));
        assert!(args.contains("ip=10.201.0.14::10.201.0.13:255.255.255.252::eth0:off"));
        assert!(args.contains("root=/dev/vda ro init=/init"));
        assert!(args.contains("panic=1"));
    }

    #[test]
    fn netmasks_render_as_dotted_quads() {
        assert_eq!(netmask_for(30), "255.255.255.252");
        assert_eq!(netmask_for(24), "255.255.255.0");
        assert_eq!(netmask_for(16), "255.255.0.0");
        assert_eq!(netmask_for(32), "255.255.255.255");
        assert_eq!(netmask_for(0), "0.0.0.0");
    }

    #[test]
    fn unused_pages_are_reported_without_inflating_the_balloon_or_polling_the_guest() {
        let json = serde_json::to_value(config()).unwrap();
        assert_eq!(
            json["balloon"],
            serde_json::json!({
                "amount_mib": 0,
                "deflate_on_oom": false,
                "stats_polling_interval_s": 0,
                "free_page_reporting": true,
            })
        );
    }

    #[test]
    fn machine_network_and_vsock_come_from_the_config_and_the_slot() {
        let rendered = config();
        assert_eq!(
            rendered.machine_config,
            MachineConfig {
                vcpu_count: 1,
                mem_size_mib: 256,
                smt: false
            }
        );
        assert_eq!(rendered.network_interfaces[0].host_dev_name, "nbr3");
        assert_eq!(
            rendered.vsock,
            VsockDevice {
                guest_cid: 6,
                uds_path: "logs.vsock".into()
            }
        );
        let json = serde_json::to_value(&rendered).unwrap();
        assert!(json.get("boot-source").is_some());
        assert!(json.get("machine-config").is_some());
        assert!(json.get("network-interfaces").is_some());
        assert_eq!(json["drives"][2]["cache_type"], "Writeback");
    }
}
