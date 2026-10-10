//! One-time host setup: everything between a machine with the packages on it and a machine
//! `nibrunnerd start` works on.
//!
//! It is a subcommand rather than something the daemon does on the way up, and that is the whole
//! of its design. Laying ZeroFS down is not the same act as supervising it: there is exactly one
//! read-write `zerofs run` per storage prefix fleet-wide, a second writer is fenced by SlateDB's
//! epoch only after a window of acknowledging writes it then discards, and the thing that holds
//! that lock is a single-instance unit. So this writes the unit and never becomes it — `start`
//! asks systemd for it.

pub mod guest_image;
pub mod kernel;
pub mod max_apps;
pub mod prerequisites;
pub mod render;
pub mod secrets;
pub mod service_user;
pub mod zerofs;

use std::path::{Path, PathBuf};

use protocol::HostVersions;

use crate::config::{HostConfig, VolumeBackend, STARTER_STATE_DIR};
use crate::json_store::{make_directory, write_text};

const MEBIBYTES_PER_GIBIBYTE: u64 = 1024;

const DIRECTORY_MODE: u32 = 0o700;
const PUBLIC_DIRECTORY_MODE: u32 = 0o755;
const READABLE_FILE_MODE: u32 = 0o644;

pub const SYSTEMD_DIR: &str = "/etc/systemd/system";

const CONFIG_DOCS_URL: &str = "https://nibrunner.dev/docs/reference/config";

#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    #[error("this host is not ready:\n{0}")]
    Unready(String),
    #[error("{0}")]
    Refused(String),
}

impl InstallError {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

/// What one run changed, said as it happens — a fetch or a hash takes long enough that silence
/// reads as a hang — and kept, so `start` knows which unit read something that changed. What was
/// found already right is not in here.
#[derive(Debug, Default)]
pub struct Laid {
    pub done: Vec<String>,
    pub written: Vec<PathBuf>,
    /// What this host holds against the `max_apps` it is laid out for: measured rather than
    /// changed, so beside `done` rather than in it, and printed after it.
    pub measured: Option<String>,
}

impl Laid {
    fn did(&mut self, what: impl Into<String>) {
        let what = what.into();
        println!("  {what}");
        self.done.push(what);
    }

    fn changed(&mut self, path: &Path) {
        self.written.push(path.to_path_buf());
    }

    fn wrote(&mut self, path: &Path) {
        self.did(format!("{} written", path.display()));
        self.changed(path);
    }
}

/// Whether this host was laid out from a configuration somebody wrote, or from the starting point
/// this binary carries — which is the difference between having nothing left to decide and having
/// every decision still ahead of you.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Operator,
    Starter,
}

pub async fn run(
    config: &HostConfig,
    config_file: &Path,
    force: bool,
    release: Option<&Path>,
) -> Result<Laid, InstallError> {
    let mut laid = Laid::default();

    // Before the checks rather than after them, because the guest image is one of the things they
    // refuse a host for — and a release was named precisely so this host would not have to have it
    // already. What it verified is handed on, so the image is hashed once rather than once more.
    let verified = match release {
        None => None,
        Some(release) => Some(match guest_image::ensure(&config.guest_image_dir, release)? {
            guest_image::Laid::AlreadyThere(version) => version,
            guest_image::Laid::Taken(version) => {
                laid.did(format!("guest image {version} laid down"));
                version
            }
            guest_image::Laid::Replaced { was, now } => {
                laid.did(format!("guest image {was} replaced by {now}"));
                now
            }
        }),
    };
    let guest_image = refuse_unready(config, verified)?;
    laid.measured = Some(max_apps::said(
        config.max_apps,
        &max_apps::measure(&config.snapshot_dir, &config.volumes),
    ));

    let environment_file = environment_file(config_file);

    // Written before they are applied, so a host that has to reboot to finish reboots into the
    // settings rather than back out of them.
    for (path, rendered) in kernel::files(config, config_file) {
        write_generated(&path, &rendered, force, &mut laid)?;
    }
    kernel::apply(config, &mut laid)?;

    for directory in directories(config) {
        make_directory(&directory, DIRECTORY_MODE).map_err(|error| {
            InstallError::Refused(format!("{} could not be made: {error}", directory.display()))
        })?;
    }

    if let Some(settings) = config.volumes.zerofs() {
        if let service_user::Made::Created = service_user::ensure(service_user::ZEROFS_USER)? {
            laid.did(format!("{} account created", service_user::ZEROFS_USER));
        }
        // The cache is the one thing the live server writes that systemd does not create for it,
        // so it is the one thing whose ownership has to be handed over.
        let owner = service_user::ids(service_user::ZEROFS_USER).ok_or_else(|| {
            InstallError::Refused(format!(
                "the {} account could not be read",
                service_user::ZEROFS_USER
            ))
        })?;
        service_user::own(&settings.cache_dir, owner)?;

        if !zerofs::installed(&settings.binary) {
            laid.did(format!("fetching zerofs {}", zerofs::VERSION));
            zerofs::fetch(&settings.binary).await?;
            laid.changed(&settings.binary);
        }

        write_generated(
            &settings.config_file,
            &render::zerofs_config(settings, config_file, &environment_file),
            force,
            &mut laid,
        )?;
        write_generated(
            &settings.checkpoint_config_file,
            &render::checkpoint_config(settings, config_file),
            force,
            &mut laid,
        )?;
        write_generated(
            &unit_path(render::ZEROFS_UNIT),
            &render::zerofs_unit(settings, config_file, &environment_file),
            force,
            &mut laid,
        )?;
        write_generated(
            &unit_path(render::MOUNT_UNIT),
            &render::mount_unit(settings, config_file),
            force,
            &mut laid,
        )?;
    }

    if let Ok(binary) = std::env::current_exe() {
        write_generated(
            &unit_path(render::DAEMON_UNIT),
            &render::daemon_unit(config_file, &binary),
            force,
            &mut laid,
        )?;
    }
    write_generated(
        &unit_path(render::DAEMON_DROP_IN),
        &render::daemon_drop_in(config, config_file, &environment_file),
        force,
        &mut laid,
    )?;

    if secrets::ensure_file(&environment_file, config)? {
        laid.did(format!("{} created", environment_file.display()));
    }

    stamp_versions(config, guest_image)?;
    Ok(laid)
}

/// What only a person can do before `nibrunnerd start`, numbered, said by the thing that knows:
/// whether the configuration is theirs yet, and which secret it needs that is not there.
pub fn next_steps(config: &HostConfig, config_file: &Path, origin: Origin) -> String {
    let environment_file = environment_file(config_file);
    let mut steps = Vec::new();
    match origin {
        Origin::Starter => {
            steps.push(format!(
                "edit {}\n     \
                 written just now: volumes as files on this disk, plain HTTP on :80, nothing in an\n     \
                 object store. Every key: {CONFIG_DOCS_URL}",
                config_file.display()
            ));
            steps.push(format!(
                "edit {}\n     the secrets that configuration needs, each named in the file",
                environment_file.display()
            ));
        }
        Origin::Operator => {
            let missing = secrets::missing(config, &environment_file);
            if !missing.is_empty() {
                steps.push(format!(
                    "set {} in {}",
                    missing.join(", "),
                    environment_file.display()
                ));
            }
        }
    }
    steps.push("nibrunnerd start".to_string());

    let mut said = String::from("\nLaid out; nothing started. What is left:\n\n");
    for (number, step) in steps.iter().enumerate() {
        said.push_str(&format!("  {}. {step}\n", number + 1));
    }
    said
}

/// The starting point this daemon carries, rendered by the code that reads it rather than carried
/// as a file that would have to be kept abreast of that code by hand. It is not one of the files
/// `write_generated` writes and must never read as one: this is the file the others are rendered
/// *from*, and a re-run leaves it alone. `max_apps` is measured on this machine, and the line
/// above it says so, so that nobody reads a measurement as a policy.
pub fn write_starter_configuration(path: &Path) -> Result<(), crate::json_store::StoreError> {
    let bounds = max_apps::measure(Path::new(STARTER_STATE_DIR), &VolumeBackend::LocalFile);
    write_text(path, &starter_configuration(&bounds), READABLE_FILE_MODE)
}

fn starter_configuration(bounds: &max_apps::Bounds) -> String {
    let (max_apps, derived) = max_apps::starter(bounds);
    format!(
        "# This host had no configuration, so this is the starting point: volumes as files on its own\n\
         # disk, stores as directories on it, plain HTTP on :80. It is yours from here — edit it, then\n\
         # `nibrunnerd start` — and nothing writes it for you again.\n\
         # What every key means: {CONFIG_DOCS_URL}\n\
         \n\
         # {derived}\n\
         {}",
        HostConfig::starter(max_apps).to_toml()
    )
}

pub fn unit_path(unit: &str) -> PathBuf {
    Path::new(SYSTEMD_DIR).join(unit)
}

pub fn environment_file(config_file: &Path) -> PathBuf {
    config_file
        .parent()
        .unwrap_or(Path::new("/etc/nibrunner"))
        .join(render::ENVIRONMENT_FILENAME)
}

/// Every directory this host writes into that is not created on the way past. `runtime_dir` is
/// deliberately absent: systemd makes it, and one made here would carry the wrong ownership on the
/// first boot after a reboot cleared it.
fn directories(config: &HostConfig) -> Vec<PathBuf> {
    let mut directories = vec![
        config.state_dir.clone(),
        config.snapshot_dir.clone(),
        config.guest_image_dir.clone(),
    ];
    if let Some(exports) = &config.exports {
        directories.push(exports.staging_dir.clone());
    }
    if let Some(settings) = config.volumes.zerofs() {
        directories.push(settings.cache_dir.clone());
        directories.push(settings.checkpoint_cache_dir.clone());
        directories.push(settings.mount_path.clone());
    }
    directories
}

/// What this command cannot put right: a machine without hardware virtualisation, a tool it does
/// not install, a guest image nobody laid down. The kernel settings used to be in here and are not
/// any more — those it sets. Hands back the guest image's version, verified here unless the caller
/// already had.
fn refuse_unready(config: &HostConfig, verified: Option<String>) -> Result<String, InstallError> {
    let guest_image = verified.map_or_else(|| prerequisites::guest_image(config), Ok);
    let mut missing: Vec<String> = prerequisites::check(config)
        .iter()
        .filter(|check| !check.met)
        .map(|check| format!("  {} is missing — {}", check.what, check.remedy))
        .collect();
    if let Err(reason) = &guest_image {
        missing.push(format!(
            "  guest image is missing — {reason}; run install.sh again, which lays vmlinux, rootfs.ext4 and manifest.json down in {} from the release it fetches",
            config.guest_image_dir.display()
        ));
    }
    if !missing.is_empty() {
        return Err(InstallError::Unready(missing.join("\n")));
    }
    guest_image.map_err(InstallError::Refused)
}

/// A file this command wrote is one it may write again. A file without its marker was written by
/// somebody, and replacing it is their decision rather than one a re-run makes for them.
fn write_generated(path: &Path, rendered: &str, force: bool, laid: &mut Laid) -> Result<(), InstallError> {
    match std::fs::read_to_string(path) {
        Ok(existing) if existing == rendered => return Ok(()),
        Ok(existing) if !existing.starts_with(render::GENERATED_MARKER) && !force => {
            return Err(InstallError::Refused(format!(
                "{} was not written by `nibrunnerd install` and will not be replaced by it. Move it aside, or pass --force to overwrite it.",
                path.display()
            )));
        }
        _ => {}
    }
    if let Some(parent) = path.parent() {
        make_directory(parent, PUBLIC_DIRECTORY_MODE).map_err(|error| {
            InstallError::Refused(format!("{} could not be made: {error}", parent.display()))
        })?;
    }
    write_text(path, rendered, READABLE_FILE_MODE).map_err(|error| InstallError::Refused(error.message()))?;
    laid.wrote(path);
    Ok(())
}

/// The file `paths.versions_file` was always for: what the installer laid down, read back by the
/// daemon so a report names the host rather than the build.
fn stamp_versions(config: &HostConfig, guest_image: String) -> Result<(), InstallError> {
    let versions = HostVersions {
        agent: env!("CARGO_PKG_VERSION").to_string(),
        guest_image,
        zerofs: match config.volumes {
            VolumeBackend::LocalFile => "none".to_string(),
            VolumeBackend::Zerofs(_) => zerofs::VERSION.to_string(),
        },
        firecracker: crate::adapters::vm::process::FIRECRACKER_VERSION.to_string(),
    };
    crate::json_store::write_json(&config.versions_file, &versions)
        .map_err(|error| InstallError::Refused(error.message()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn measured(memory: u32, disk: u32) -> max_apps::Bounds {
        max_apps::Bounds {
            memory: Ok(memory),
            disk: Ok(disk),
            ports: nft_render::most_apps_the_ports_fit(),
        }
    }

    // It is written to a host that then has to load it, and the next-steps block describes it. A
    // starting point this daemon refuses, or that serves nothing, leaves a fresh machine no better
    // off than having no configuration at all.
    #[test]
    fn the_configuration_this_binary_carries_is_one_it_would_accept_and_serve_on() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        std::fs::write(&path, starter_configuration(&measured(29, 320))).unwrap();
        let config = HostConfig::from_file(&path).expect("the starting configuration must load");
        assert_eq!(config, HostConfig::starter(29));
        assert!(matches!(config.volumes, VolumeBackend::LocalFile));
        assert!(secrets::of(&config).iter().all(|secret| !secret.needed));
        let http = config
            .proxy
            .http
            .expect("plain HTTP, so the README's document is served");
        assert_eq!(http.port, 80);
        assert!(http.tls.is_none());
    }

    // The marker is what lets a re-run replace a file, and this is the one file a re-run reads
    // rather than writes — so it had better not be carrying it.
    #[test]
    fn the_configuration_this_binary_carries_is_not_one_a_rerun_would_replace() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        write_starter_configuration(&path).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(!written.starts_with(render::GENERATED_MARKER), "{written}");
    }

    // The number is this machine's, so the line above it has to say it was measured — and it is
    // above the key, not somewhere in a header the key is three sections away from.
    #[test]
    fn the_number_a_starter_is_laid_out_for_is_written_with_where_it_came_from_above_it() {
        let written = starter_configuration(&measured(29, 320));
        let lines: Vec<&str> = written.lines().collect();
        let key = lines
            .iter()
            .position(|line| *line == "max_apps = 29")
            .expect("the least bound is the key");
        assert_eq!(
            lines[key - 1],
            "# To start, install assumed an app to be 1 vCPU, 256 MiB and 8 GiB, and measured how many this machine holds: memory 29, disk 320, ports 5567."
        );
        assert!(lines[key - 2].is_empty(), "{written}");
    }

    // What this machine measures is its own, so only what every machine's starter has in common is
    // held to: a number the daemon accepts, whatever the disk under /var/lib turned out to be.
    #[test]
    fn a_host_with_no_configuration_is_written_one_it_accepts_whatever_it_measured() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        write_starter_configuration(&path).unwrap();
        let config = HostConfig::from_file(&path).expect("the starting configuration must load");
        assert_eq!(config, HostConfig::starter(config.max_apps));
        assert!(config.max_apps >= 1);
        assert!(config.max_apps <= nft_render::most_apps_the_ports_fit());
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(written.contains("install assumed an app to be"), "{written}");
    }

    #[test]
    fn a_file_this_command_wrote_is_one_it_writes_again() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        let first = format!("{}\nfirst\n", render::GENERATED_MARKER);
        let second = format!("{}\nsecond\n", render::GENERATED_MARKER);

        write_generated(&path, &first, false, &mut Laid::default()).unwrap();
        write_generated(&path, &second, false, &mut Laid::default()).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), second);
    }

    #[test]
    fn a_file_somebody_else_wrote_is_refused_by_name_rather_than_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        std::fs::write(&path, "# mine\ndisk_size_gb = 9000\n").unwrap();

        let error = write_generated(&path, "# theirs\n", false, &mut Laid::default()).unwrap_err();
        assert!(error.message().contains("--force"), "{}", error.message());
        assert!(error.message().contains(&path.display().to_string()));
        assert!(std::fs::read_to_string(&path).unwrap().contains("9000"));
    }

    #[test]
    fn force_is_what_replaces_it() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        std::fs::write(&path, "# mine\n").unwrap();
        write_generated(&path, "# theirs\n", true, &mut Laid::default()).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "# theirs\n");
    }

    // Re-running is how a host is brought up to a new release, so it has to be readable as having
    // done nothing when there was nothing to do — and `start` restarts what read a file in
    // `written`, so a file that did not change must not be in it.
    #[test]
    fn a_rerun_that_changes_nothing_records_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        let rendered = format!("{}\nsame\n", render::GENERATED_MARKER);
        let mut laid = Laid::default();
        write_generated(&path, &rendered, false, &mut laid).unwrap();
        write_generated(&path, &rendered, false, &mut laid).unwrap();
        assert_eq!(laid.done.len(), 1, "{:?}", laid.done);
        assert!(laid.done[0].ends_with("written"), "{:?}", laid.done);
        assert_eq!(laid.written, [path]);
    }

    #[test]
    fn the_environment_file_sits_beside_the_configuration_that_named_it() {
        assert_eq!(
            environment_file(Path::new("/etc/nibrunner/config.toml")),
            PathBuf::from("/etc/nibrunner/host.env")
        );
        assert_eq!(
            environment_file(Path::new("/srv/other/host.toml")),
            PathBuf::from("/srv/other/host.env")
        );
    }

    // Every choice about what the host serves is still ahead of whoever ran this, and the secrets
    // the choice will need are in a file they have not opened yet.
    #[test]
    fn a_host_laid_out_from_the_starter_is_told_to_edit_both_files_then_start() {
        let config_file = Path::new("/etc/nibrunner/config.toml");
        let said = next_steps(
            &HostConfig::under(Path::new("/srv/nibrunner")),
            config_file,
            Origin::Starter,
        );
        let steps: Vec<&str> = said
            .lines()
            .filter(|line| line.trim_start().starts_with(char::is_numeric))
            .collect();
        assert_eq!(steps.len(), 3, "{said}");
        assert!(steps[0].contains("edit /etc/nibrunner/config.toml"), "{said}");
        assert!(steps[1].contains("edit /etc/nibrunner/host.env"), "{said}");
        assert!(steps[2].ends_with("nibrunnerd start"), "{said}");
        assert!(said.contains(CONFIG_DOCS_URL), "{said}");
        assert!(said.contains("plain HTTP on :80"), "{said}");
    }

    #[test]
    fn a_host_with_its_own_configuration_is_asked_only_for_what_is_missing() {
        let directory = tempfile::tempdir().unwrap();
        let config_file = directory.path().join("config.toml");
        let mut config = HostConfig::under(Path::new("/srv/nibrunner"));
        config.artifact_store_url = "s3://nibrunner-artifacts/artifacts".to_string();

        let said = next_steps(&config, &config_file, Origin::Operator);
        assert!(
            said.contains("1. set AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY in"),
            "{said}"
        );
        assert!(said.contains("2. nibrunnerd start"), "{said}");
        assert!(!said.contains("edit"), "{said}");

        std::fs::write(
            directory.path().join("host.env"),
            "AWS_REGION=eu-west-2\nAWS_ACCESS_KEY_ID=a\nAWS_SECRET_ACCESS_KEY=b\n",
        )
        .unwrap();
        let said = next_steps(&config, &config_file, Origin::Operator);
        assert!(said.contains("1. nibrunnerd start"), "{said}");
        assert!(!said.contains("2."), "{said}");
    }

    #[test]
    fn a_local_file_host_is_given_no_directory_only_zerofs_would_want() {
        let config = HostConfig::under(Path::new("/srv/nibrunner"));
        let directories = directories(&config);
        assert!(directories.contains(&config.snapshot_dir));
        assert!(
            !directories.iter().any(|path| path.ends_with("zerofs")),
            "{directories:?}"
        );
        // systemd owns it, and one made here comes back with the wrong owner after a reboot.
        assert!(!directories.contains(&config.runtime_dir), "{directories:?}");
    }

    #[test]
    fn a_zerofs_host_gets_the_caches_and_the_mount_point() {
        let mut config = HostConfig::under(Path::new("/srv/nibrunner"));
        let settings = crate::test_support::zerofs_settings(|_| {});
        config.volumes = VolumeBackend::Zerofs(Box::new(settings.clone()));
        let directories = directories(&config);
        for wanted in [
            &settings.cache_dir,
            &settings.checkpoint_cache_dir,
            &settings.mount_path,
        ] {
            assert!(directories.contains(wanted), "{directories:?}");
        }
    }

    #[test]
    fn an_unready_host_is_told_everything_that_is_missing_at_once() {
        let mut config = HostConfig::under(Path::new("/nibrunner-nowhere"));
        config.volumes = VolumeBackend::Zerofs(Box::new(crate::test_support::zerofs_settings(|_| {})));
        let Err(error) = refuse_unready(&config, None) else {
            // A host that genuinely has all of this is a Linux box with the tools on it, and the
            // guest image check still fails against a directory that is not there.
            panic!("a directory that does not exist cannot be a ready host");
        };
        assert!(error.message().contains("guest image"), "{}", error.message());
    }
}
