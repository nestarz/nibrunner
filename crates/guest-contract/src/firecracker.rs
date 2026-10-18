use protocol::{InstanceResources, Ipv4Address};
use serde::{Deserialize, Serialize};

/// The drives every guest has, in the order they are attached: the host's root and the config
/// image. The app's volume, or its scratch on disk, is the data drive after them; the layers follow,
/// one drive each. A scratch in memory is no drive at all, so a guest that has one finds its layers
/// one drive sooner, and the config drive tells it so.
pub const FIXED_DRIVE_IDS: [&str; 2] = ["rootfs", "config"];
pub const DATA_DRIVE_ID: &str = "data";

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
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub discard: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blk_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology: Option<BlockTopology>,
}

/// In logical blocks: a physical block of one, aligned at the start, at least one per I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockTopology {
    pub physical_block_exp: u8,
    pub alignment_offset: u32,
    pub min_io_size: u32,
    pub opt_io_size: u32,
}

/// The block size of every filesystem nibrunner formats (`mke2fs -b 4096`), and of the host's
/// pages and btrfs blocks under it.
pub const WRITABLE_BLOCK_BYTES: u32 = 4096;
const WRITABLE_TOPOLOGY: BlockTopology = BlockTopology {
    physical_block_exp: 0,
    alignment_offset: 0,
    min_io_size: 1,
    opt_io_size: 0,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HugePages {
    None,
    Transparent,
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
    pub huge_pages: HugePages,
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
    /// Absent for a guest whose writable root is a scratch in its own memory.
    pub data_device_path: Option<String>,
    /// The block size of the filesystem on the data device, when it has one already: a volume an
    /// earlier release formatted with 1 KiB blocks cannot sit on a device of 4 KiB sectors.
    pub data_block_bytes: Option<u32>,
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
        discard: false,
        blk_size: None,
        topology: None,
    }
}

/// The one drive the guest writes: discard lets the blocks it frees go back to the host file
/// (Sync is the only engine Firecracker discards on), and 4 KiB sectors align the filesystem to
/// the host's blocks, unless the filesystem already on it has smaller ones.
fn writable_drive(path_on_host: &str, block_bytes: Option<u32>) -> FirecrackerDrive {
    let aligned = block_bytes.is_none_or(|bytes| bytes >= WRITABLE_BLOCK_BYTES);
    FirecrackerDrive {
        drive_id: DATA_DRIVE_ID.to_string(),
        path_on_host: path_on_host.to_string(),
        is_root_device: false,
        is_read_only: false,
        cache_type: CacheType::Writeback,
        io_engine: IoEngine::Sync,
        discard: true,
        blk_size: aligned.then_some(WRITABLE_BLOCK_BYTES),
        topology: aligned.then_some(WRITABLE_TOPOLOGY),
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
        ]
        .into_iter()
        .chain(
            paths
                .data_device_path
                .as_ref()
                .map(|path| writable_drive(path, paths.data_block_bytes)),
        )
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
            huge_pages: HugePages::Transparent,
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
            data_device_path: Some("/dev/nbd3".into()),
            data_block_bytes: None,
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
                p.data_device_path.unwrap(),
                p.layer_image_paths[0].clone(),
                p.layer_image_paths[1].clone(),
            ]
        );
        assert_eq!(rendered.drives.iter().filter(|d| d.is_root_device).count(), 1);
        assert!(rendered.drives[0].is_root_device);
        for (index, drive) in rendered.drives[FIXED_DRIVE_IDS.len() + 1..].iter().enumerate() {
            assert_eq!(drive.drive_id, layer_drive_id(index));
        }
    }

    #[test]
    fn a_guest_with_a_scratch_in_memory_has_no_data_drive_and_its_layers_come_one_drive_sooner() {
        let rendered = render_firecracker_config(
            DEFAULT_INSTANCE_RESOURCES,
            &VmPaths {
                data_device_path: None,
                ..paths()
            },
            &network(),
            &VmVsock {
                guest_cid: 6,
                path: "logs.vsock".into(),
            },
        );
        let ids: Vec<&str> = rendered.drives.iter().map(|d| d.drive_id.as_str()).collect();
        assert_eq!(ids, ["rootfs", "config", "layer0", "layer1"]);
        assert!(rendered.drives.iter().all(|d| d.is_read_only));
    }

    #[test]
    fn the_tenant_data_drive_is_the_only_writable_one_and_is_writeback_so_a_guest_fsync_reaches_the_host() {
        let drives = config().drives;
        let data = &drives[2];
        assert_eq!(data.cache_type, CacheType::Writeback);
        assert!(!data.is_read_only);
        for drive in drives.iter().filter(|d| d.drive_id != DATA_DRIVE_ID) {
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
                smt: false,
                huge_pages: HugePages::Transparent,
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
        assert_eq!(json["machine-config"]["huge_pages"], "Transparent");
    }

    #[test]
    fn only_the_writable_drive_discards_and_it_has_4_kib_sectors_unless_its_filesystem_has_smaller_blocks() {
        let json = serde_json::to_value(config()).unwrap();
        assert_eq!(json["drives"][2]["discard"], true);
        assert_eq!(json["drives"][2]["blk_size"], 4096);
        assert_eq!(
            json["drives"][2]["topology"],
            serde_json::json!({"physical_block_exp": 0, "alignment_offset": 0, "min_io_size": 1, "opt_io_size": 0})
        );
        for (index, drive) in json["drives"].as_array().unwrap().iter().enumerate() {
            if index != 2 {
                for key in ["discard", "blk_size", "topology"] {
                    assert!(drive.get(key).is_none(), "{} has {key}", drive["drive_id"]);
                }
            }
        }
        let older = render_firecracker_config(
            DEFAULT_INSTANCE_RESOURCES,
            &VmPaths {
                data_block_bytes: Some(1024),
                ..paths()
            },
            &network(),
            &VmVsock {
                guest_cid: 6,
                path: "logs.vsock".into(),
            },
        );
        assert!(older.drives[2].discard);
        assert_eq!((older.drives[2].blk_size, older.drives[2].topology), (None, None));
    }
}
