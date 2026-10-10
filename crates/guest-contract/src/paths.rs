pub const CONFIG_DEVICE: &str = "/dev/vdb";
pub const VOLUME_DEVICE: &str = "/dev/vdc";

// vda and vdb are the two every guest has, and vdc is the volume when there is a drive for it; the
// layers are the drives after whichever came last.
const FIRST_LAYER_DEVICE_INDEX: u8 = 3;

/// Where the writable root comes from, and so where the layers start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritableRoot {
    /// The volume drive at `VOLUME_DEVICE`: an app's volume, or a scratch on the host's disk.
    VolumeDrive,
    /// A tmpfs of this many MiB, and no drive for it.
    Memory { mib: u32 },
}

pub fn layer_device(index: usize, writable: WritableRoot) -> String {
    let first = match writable {
        WritableRoot::VolumeDrive => FIRST_LAYER_DEVICE_INDEX,
        WritableRoot::Memory { .. } => FIRST_LAYER_DEVICE_INDEX - 1,
    };
    let letter = (b'a' + first + index as u8) as char;
    format!("/dev/vd{letter}")
}

pub const CONFIG_MOUNT: &str = "/run/config";
pub const CONFIG_FILE: &str = "/run/config/instance.env";

/// The guest's own root, bound here without its submounts, is the bottom of every stack: the
/// Debian this image is, under whatever the document layered on it.
pub const BASE_MOUNT: &str = "/mnt/base";
pub const LAYERS_MOUNT_DIR: &str = "/mnt/layers";

pub fn layer_mount(index: usize) -> String {
    format!("{LAYERS_MOUNT_DIR}/{index}")
}

/// The app's volume, and the two directories overlayfs keeps on it. `upper` is every write the
/// tenant ever made to its root, and so the only thing worth exporting; `work` is overlayfs's own.
pub const VOLUME_MOUNT: &str = "/mnt/volume";
pub const VOLUME_UPPER_DIR: &str = "/mnt/volume/upper";
pub const VOLUME_WORK_DIR: &str = "/mnt/volume/work";
pub const VOLUME_UPPER_NAME: &str = "upper";

/// The stacked root the program runs in.
pub const ROOT_MOUNT: &str = "/mnt/root";
pub const RESOLV_CONF: &str = "/mnt/root/etc/resolv.conf";

pub const TENANT_UID: u32 = 65534;
pub const TENANT_GID: u32 = 65534;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layers_follow_the_volume_drive_and_take_its_place_when_the_root_is_written_to_memory() {
        assert_eq!(layer_device(0, WritableRoot::VolumeDrive), "/dev/vdd");
        assert_eq!(layer_device(1, WritableRoot::VolumeDrive), "/dev/vde");
        assert_eq!(layer_device(0, WritableRoot::Memory { mib: 64 }), VOLUME_DEVICE);
        assert_eq!(layer_device(1, WritableRoot::Memory { mib: 64 }), "/dev/vdd");
    }
}
