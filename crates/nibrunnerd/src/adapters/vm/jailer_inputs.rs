use std::path::{Path, PathBuf};

use guest_contract::firecracker::FirecrackerConfig;
use serde::{Deserialize, Serialize};

use crate::json_store::{write_json, StoreError};

pub(crate) const INPUTS_FILENAME: &str = "jailer-inputs.json";

#[derive(Serialize, Deserialize)]
pub(crate) struct Input {
    pub(crate) source: PathBuf,
    pub(crate) name: String,
    pub(crate) read_only: bool,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Inputs {
    pub(crate) slot: u32,
    pub(crate) files: Vec<Input>,
    pub(crate) config: FirecrackerConfig,
}

pub(crate) fn stage(directory: &Path, slot: u32, mut config: FirecrackerConfig) -> Result<(), StoreError> {
    let mut files = vec![Input {
        source: PathBuf::from(&config.boot_source.kernel_image_path),
        name: "kernel".into(),
        read_only: true,
    }];
    config.boot_source.kernel_image_path = "/assets/kernel".into();
    for drive in &mut config.drives {
        files.push(Input {
            source: PathBuf::from(&drive.path_on_host),
            name: drive.drive_id.clone(),
            read_only: drive.is_read_only,
        });
        drive.path_on_host = format!("/assets/{}", drive.drive_id);
    }
    write_json(&directory.join(INPUTS_FILENAME), &Inputs { slot, files, config })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::app_id;
    use guest_contract::firecracker::{render_firecracker_config, VmNetwork, VmPaths, VmVsock};

    fn config() -> FirecrackerConfig {
        config_writing_to(Some("/dev/nbd0".into()))
    }

    fn config_writing_to(data_device_path: Option<String>) -> FirecrackerConfig {
        let slot = nft_render::describe_slot(0, app_id());
        render_firecracker_config(
            protocol::DEFAULT_INSTANCE_RESOURCES,
            &VmPaths {
                kernel_path: "/guest/kernel".into(),
                rootfs_path: "/guest/rootfs".into(),
                instance_config_image_path: "/vm/config".into(),
                data_device_path,
                data_block_bytes: None,
                layer_image_paths: vec!["/cache/layer".into()],
            },
            &VmNetwork {
                tap_name: slot.tap_name,
                guest_mac: slot.guest_mac,
                guest_ipv4: slot.guest_ipv4,
                host_ipv4: slot.host_ipv4,
                subnet_prefix_length: slot.subnet_prefix_length,
            },
            &VmVsock {
                guest_cid: 3,
                path: guest_contract::vsock::GUEST_VSOCK_FILENAME.into(),
            },
        )
    }

    #[test]
    fn staged_inputs_preserve_host_assets_and_translate_the_config_without_sharing_another_volume() {
        let directory = tempfile::tempdir().unwrap();
        let host_config = config();
        stage(directory.path(), 7, host_config.clone()).unwrap();
        let inputs: Inputs = crate::json_store::read_json(&directory.path().join(INPUTS_FILENAME))
            .unwrap()
            .unwrap();
        assert_eq!(inputs.slot, 7);
        assert_eq!(inputs.config.boot_source.kernel_image_path, "/assets/kernel");
        assert_eq!(host_config.boot_source.kernel_image_path, "/guest/kernel");
        assert_eq!(inputs.files.iter().filter(|input| !input.read_only).count(), 1);
        let volume = inputs.files.iter().find(|input| !input.read_only).unwrap();
        assert_eq!(volume.source, Path::new("/dev/nbd0"));
        assert_eq!(volume.name, "data");
        for drive in &inputs.config.drives {
            let input = inputs
                .files
                .iter()
                .find(|input| input.name == drive.drive_id)
                .unwrap();
            let original = host_config
                .drives
                .iter()
                .find(|original| original.drive_id == drive.drive_id)
                .unwrap();
            assert_eq!(input.source, Path::new(&original.path_on_host));
            assert_eq!(input.read_only, original.is_read_only);
            assert_eq!(drive.path_on_host, format!("/assets/{}", input.name));
        }
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(directory.path().join(INPUTS_FILENAME))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn a_guest_writing_to_memory_is_staged_with_no_writable_input() {
        let directory = tempfile::tempdir().unwrap();
        stage(directory.path(), 7, config_writing_to(None)).unwrap();
        let inputs: Inputs = crate::json_store::read_json(&directory.path().join(INPUTS_FILENAME))
            .unwrap()
            .unwrap();
        assert!(inputs.files.iter().all(|input| input.read_only));
        assert!(inputs.config.drives.iter().all(|drive| drive.drive_id != "data"));
    }
}
