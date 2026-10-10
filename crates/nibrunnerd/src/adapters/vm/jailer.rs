#[cfg(target_os = "linux")]
use std::ffi::CString;
use std::path::{Path, PathBuf};

use protocol::AppId;

use super::jailer_inputs::{Inputs, INPUTS_FILENAME};
use super::mount_namespace::Mount;
#[cfg(target_os = "linux")]
use super::snapshot::{exchange, EXCHANGE_BOOT_ID_FILENAME};
use crate::install::jailer_identities::Identities;
use crate::json_store::read_json;
#[cfg(target_os = "linux")]
use crate::json_store::{make_directory, write_json};
use crate::unix_socket::own;

#[cfg(target_os = "linux")]
const PRIVATE_MODE: u32 = 0o700;
#[cfg(target_os = "linux")]
const ASSETS_MODE: u32 = 0o555;

pub(crate) struct Jailer {
    pub(crate) binary: PathBuf,
    pub(crate) identities: Identities,
    #[cfg(test)]
    fake_preparation: bool,
}

pub(crate) struct Jail {
    pub(crate) uid: u32,
    pub(crate) gid: u32,
    pub(crate) root: PathBuf,
    mounts: Vec<Mount>,
    pub(crate) limits: super::limits::VmLimits,
    #[cfg(test)]
    fake_mounts: bool,
}

fn io_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

pub(crate) fn jail_id(app_id: &AppId) -> String {
    use sha2::Digest;
    hex::encode(&sha2::Sha256::digest(app_id.as_str().as_bytes())[..16])
}

pub(crate) fn root(base: &Path, app_id: &AppId) -> PathBuf {
    base.join("firecracker").join(jail_id(app_id)).join("root")
}

impl Jailer {
    pub(crate) fn new(binary: PathBuf, identities: Identities) -> Self {
        Self {
            binary,
            identities,
            #[cfg(test)]
            fake_preparation: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_testing(binary: PathBuf) -> Self {
        let mut jailer = Self::new(
            binary,
            Identities {
                uid_base: 100_000,
                gid_base: 100_000,
                slots: 65_536,
            },
        );
        jailer.fake_preparation = true;
        jailer
    }

    fn identities(&self, slot: u32) -> std::io::Result<(u32, u32)> {
        if slot >= self.identities.slots {
            return Err(io_error(format!(
                "slot {slot} is past the {} jailer identities reserved in /etc/subuid and /etc/subgid",
                self.identities.slots
            )));
        }
        let identity = |base: u32| {
            base.checked_add(slot)
                .filter(|id| *id > 0 && *id < u32::MAX)
                .ok_or_else(|| io_error("the jail identity is outside its reserved range"))
        };
        Ok((
            identity(self.identities.uid_base)?,
            identity(self.identities.gid_base)?,
        ))
    }

    pub(crate) fn prepare(
        &self,
        directory: &Path,
        base: &Path,
        snapshot_dir: &Path,
        app_id: &AppId,
        host_boot_id: &str,
    ) -> std::io::Result<Jail> {
        #[cfg(test)]
        if self.fake_preparation {
            let root = root(base, app_id);
            crate::json_store::make_directory(&root, 0o700)?;
            return Ok(Jail::for_testing(root));
        }
        let inputs: Inputs = read_json(&directory.join(INPUTS_FILENAME))
            .map_err(io_error)?
            .ok_or_else(|| io_error("the jail has no staged inputs"))?;
        let (uid, gid) = self.identities(inputs.slot)?;
        let limits = super::limits::VmLimits::for_machine(&inputs.config.machine_config)?;
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (
                inputs,
                limits,
                uid,
                gid,
                directory,
                base,
                snapshot_dir,
                app_id,
                host_boot_id,
            );
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "the Firecracker jailer requires Linux",
            ))
        }
        #[cfg(target_os = "linux")]
        {
            let root = root(base, app_id);
            if root.exists() {
                std::fs::remove_dir_all(&root)?;
            }
            make_directory(&root, PRIVATE_MODE)?;
            let assets = root.join("assets");
            make_directory(&assets, PRIVATE_MODE)?;
            let mut mounts = Vec::new();
            for input in &inputs.files {
                let target = assets.join(&input.name);
                let metadata = std::fs::metadata(&input.source)?;
                use std::os::unix::fs::{FileTypeExt, MetadataExt};
                if metadata.file_type().is_block_device() && !input.read_only {
                    make_device(&target, metadata.rdev(), uid, gid)?;
                    continue;
                }
                if !metadata.is_file() {
                    return Err(io_error(format!("{} is not a VM image", input.source.display())));
                }
                if input.read_only && metadata.mode() & 0o004 == 0 {
                    std::fs::copy(&input.source, &target)?;
                    set_mode(&target, 0o444)?;
                    continue;
                }
                if !input.read_only {
                    grant_volume_group(&input.source, gid)?;
                }
                std::fs::File::create(&target)?;
                mounts.push(Mount::new(&input.source, &target, input.read_only)?);
            }
            set_mode(&assets, ASSETS_MODE)?;
            let snapshots = exchange(snapshot_dir, app_id);
            let exchange_base = snapshots
                .parent()
                .expect("an exchange belongs to its snapshot directory");
            make_directory(exchange_base, PRIVATE_MODE)?;
            crate::json_store::write_text(
                &exchange_base.join(EXCHANGE_BOOT_ID_FILENAME),
                host_boot_id,
                0o600,
            )
            .map_err(io_error)?;
            make_directory(&snapshots, PRIVATE_MODE)?;
            own(&snapshots, uid, gid)?;
            make_directory(&root.join("snapshots"), PRIVATE_MODE)?;
            mounts.push(Mount::new(&snapshots, &root.join("snapshots"), false)?);
            write_json(
                &root.join(super::manager::FIRECRACKER_CONFIG_FILENAME),
                &inputs.config,
            )
            .map_err(io_error)?;
            set_mode(&root.join(super::manager::FIRECRACKER_CONFIG_FILENAME), 0o444)?;
            Ok(Jail {
                uid,
                gid,
                root,
                mounts,
                limits,
                #[cfg(test)]
                fake_mounts: false,
            })
        }
    }
}

impl Jail {
    #[cfg(test)]
    pub(crate) fn for_testing(root: PathBuf) -> Self {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(&root).expect("the fake jail directory exists");
        Self {
            uid: metadata.uid(),
            gid: metadata.gid(),
            root,
            mounts: Vec::new(),
            limits: super::limits::VmLimits::for_machine(&guest_contract::firecracker::MachineConfig {
                vcpu_count: 1,
                mem_size_mib: 256,
                smt: false,
            })
            .expect("valid test resources"),
            fake_mounts: true,
        }
    }

    pub(crate) fn grant_socket(&self, path: &Path) -> std::io::Result<()> {
        own(path, self.uid, self.gid)
    }

    pub(crate) fn configure_command(&self, command: &mut tokio::process::Command) -> std::io::Result<()> {
        #[cfg(test)]
        if self.fake_mounts {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        {
            super::mount_namespace::configure(command, self.mounts.clone());
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (command, &self.mounts);
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "the jailer requires Linux",
            ))
        }
    }
}

#[cfg(target_os = "linux")]
fn set_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(target_os = "linux")]
fn grant_volume_group(path: &Path, gid: u32) -> std::io::Result<()> {
    std::os::unix::fs::chown(path, None, Some(gid))?;
    set_mode(path, 0o660)
}

#[cfg(target_os = "linux")]
#[allow(
    unsafe_code,
    reason = "an NBD node inside the jail must refer to the host device without changing its ownership"
)]
fn make_device(path: &Path, device: u64, uid: u32, gid: u32) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let name = CString::new(path.as_os_str().as_bytes()).map_err(io_error)?;
    if unsafe { libc::mknod(name.as_ptr(), libc::S_IFBLK | 0o600, device) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    own(path, uid, gid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slot_outside_the_reserved_identity_range_is_refused() {
        let jailer = Jailer::new(
            "/jailer".into(),
            Identities {
                uid_base: u32::MAX,
                gid_base: 100_000,
                slots: 8,
            },
        );
        assert!(jailer.identities(1).is_err());
    }

    #[test]
    fn a_slot_past_the_reservation_is_refused_rather_than_given_someone_elses_identity() {
        let jailer = Jailer::new(
            "/jailer".into(),
            Identities {
                uid_base: 100_000,
                gid_base: 200_000,
                slots: 8,
            },
        );
        assert_eq!(jailer.identities(7).unwrap(), (100_007, 200_007));
        let refused = jailer.identities(8).unwrap_err().to_string();
        assert!(refused.contains("past the 8 jailer identities"), "{refused}");
    }

    #[test]
    fn app_identifiers_with_underscores_have_distinct_valid_jailer_identifiers() {
        let underscored = AppId::parse("app_one").unwrap();
        let hyphenated = AppId::parse("app-one").unwrap();
        let id = jail_id(&underscored);
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|character| character.is_ascii_alphanumeric()));
        assert_ne!(id, jail_id(&hyphenated));
        assert!(root(Path::new("/jails"), &underscored).ends_with(format!("{id}/root")));
    }
}
