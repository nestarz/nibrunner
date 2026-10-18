//! The files this host would otherwise be asked to write by hand: ZeroFS's two configs and the
//! two units that supervise it. Every one of them is rendered from `config.toml`, so the answer
//! an operator gave once is the answer all four carry.

use std::path::Path;

use crate::config::{HostConfig, ZerofsSettings, ZEROFS_PROMETHEUS_PORT};
use crate::domain::exports::reader::{CHECKPOINT_VARIABLE, NBD_SOCKET_FILENAME};
use crate::install::service_user::ZEROFS_USER;

/// The first line of everything this module writes. A file carrying it is one `install` may
/// replace; a file without it was written by somebody, and replacing it is a decision they make
/// rather than one a re-run makes for them.
pub const GENERATED_MARKER: &str = "# Written by `nibrunnerd install`";

/// Where ZeroFS reads the password and the AWS credentials from. `install` never writes a value
/// into this file — it only makes sure there is one, and names it in the units it renders.
pub const ENVIRONMENT_FILENAME: &str = "host.env";

pub const PASSWORD_VARIABLE: &str = "ZEROFS_ENCRYPTION_PASSWORD";
pub const REGION_VARIABLE: &str = "AWS_REGION";

/// A checkpoint reads one filesystem once, roughly in inode order, and is then gone: a cache only
/// earns its size on bytes read twice, which here is the metadata `debugfs` walks. Constants
/// rather than keys because nothing about a host changes what one export costs.
const CHECKPOINT_CACHE_DISK_GB: &str = "4.0";
const CHECKPOINT_CACHE_MEMORY_GB: &str = "0.25";

pub(super) fn header(purpose: &str, config_file: &Path) -> String {
    format!(
        "{GENERATED_MARKER}.\n\
         # {purpose}\n\
         #\n\
         # Do not edit: the next `nibrunnerd install` rewrites this file from\n\
         # {}, which is where these values are decided. Editing here makes a\n\
         # second copy of an answer that already lives there, and the two drift.\n",
        config_file.display()
    )
}

fn gibibytes(mib: u64) -> String {
    format!("{}.0", mib / super::MEBIBYTES_PER_GIBIBYTE)
}

/// The live server: one per storage prefix, fleet-wide, and the only writer.
pub fn zerofs_config(settings: &ZerofsSettings, config_file: &Path, environment_file: &Path) -> String {
    format!(
        "{}\n\
         # Every ${{VAR}} below is expanded by ZeroFS itself from the environment its unit loads\n\
         # from {}.\n\
         \n\
         [cache]\n\
         dir = \"{}\"\n\
         disk_size_gb = {}\n\
         memory_size_gb = {}\n\
         warm_metadata = \"filters_index\"\n\
         \n\
         [storage]\n\
         url = \"{}\"\n\
         encryption_password = \"${{{PASSWORD_VARIABLE}}}\"\n\
         \n\
         # Region only. Credentials resolve through the default chain, which on a machine with no\n\
         # instance role is the same environment file this unit already loads.\n\
         [aws]\n\
         region = \"${{{REGION_VARIABLE}}}\"\n\
         \n\
         [filesystem]\n\
         # Honouring fsync costs ~244 ms a call, which is unusable. With this it is ~1.24 ms, and\n\
         # the guest's fsync becomes a no-op all the way down: NBD FLUSH and FUA stop meaning\n\
         # anything, and [lsm] flush_interval_secs below is the entire durability guarantee.\n\
         ignore_fsync = true\n\
         compression = \"zstd-3\"\n\
         \n\
         [lsm]\n\
         # The accepted RPO. Worst-case loss is this plus the time the seal and upload take, so it\n\
         # is 5-15s in practice. Five is the enforced minimum and less is clamped, not refused.\n\
         flush_interval_secs = 5\n\
         \n\
         # Unix sockets rather than TCP: everything that talks to ZeroFS is on this host.\n\
         [servers.nbd]\n\
         unix_socket = \"{}\"\n\
         \n\
         # How a volume is created and sized: the device files live in a .nbd directory inside the\n\
         # filesystem itself, so managing them is a filesystem operation rather than an RPC one.\n\
         [servers.ninep]\n\
         unix_socket = \"{}\"\n\
         \n\
         # The admin RPC every `zerofs` subcommand this daemon runs goes over — flush and the\n\
         # checkpoint verbs. Without this section each of them exits \"RPC server not configured\",\n\
         # and a flush that fails stops the tenant it was flushing for.\n\
         [servers.rpc]\n\
         unix_socket = \"{}\"\n\
         \n\
         [prometheus]\n\
         addresses = [\"127.0.0.1:{ZEROFS_PROMETHEUS_PORT}\"]\n\
         \n\
         [telemetry]\n\
         enabled = false\n",
        header("ZeroFS, backing every tenant disk on this host.", config_file),
        environment_file.display(),
        settings.cache_dir.display(),
        gibibytes(settings.cache_disk_mib),
        gibibytes(settings.cache_memory_mib),
        settings.storage_url,
        settings.nbd_socket_path.display(),
        settings.ninep_socket_path.display(),
        settings.rpc_socket_path.display(),
    )
}

/// One checkpoint, read-only, for the length of one export's read. The daemon starts these itself,
/// so the variable this interpolates is the one the daemon sets when it does.
pub fn checkpoint_config(settings: &ZerofsSettings, config_file: &Path) -> String {
    format!(
        "{}\n\
         # ${CHECKPOINT_VARIABLE} is set by this daemon on the process it spawns, one per export.\n\
         # It is what keeps two of them off each other's socket and cache.\n\
         \n\
         [cache]\n\
         dir = \"{}/${{{CHECKPOINT_VARIABLE}}}\"\n\
         disk_size_gb = {CHECKPOINT_CACHE_DISK_GB}\n\
         memory_size_gb = {CHECKPOINT_CACHE_MEMORY_GB}\n\
         \n\
         [storage]\n\
         url = \"{}\"\n\
         encryption_password = \"${{{PASSWORD_VARIABLE}}}\"\n\
         \n\
         [aws]\n\
         region = \"${{{REGION_VARIABLE}}}\"\n\
         \n\
         # No [filesystem], [lsm] or [gc]: every setting in them is about writing, and a checkpoint\n\
         # server never writes. No [servers.rpc] either — checkpoints are created, listed and\n\
         # deleted through the live server, which is the only process that can. No [prometheus]:\n\
         # the live server holds that address, and a second binding is a startup failure.\n\
         [servers.nbd]\n\
         unix_socket = \"{}/${{{CHECKPOINT_VARIABLE}}}/{NBD_SOCKET_FILENAME}\"\n\
         \n\
         [telemetry]\n\
         enabled = false\n",
        header(
            "ZeroFS serving one checkpoint of this host's filesystems, read-only.",
            config_file
        ),
        settings.checkpoint_cache_dir.display(),
        settings.storage_url,
        settings.checkpoint_runtime_dir.display(),
    )
}

pub const ZEROFS_UNIT: &str = "nibrunner-zerofs.service";
pub const MOUNT_UNIT: &str = "nibrunner-zerofs-mount.service";

/// The unit is the lock. There is exactly one read-write `zerofs run` per storage prefix,
/// fleet-wide — a second writer is fenced by SlateDB's epoch only after a window of acknowledging
/// writes it then discards, so it loses a tenant's data rather than failing to start. This daemon
/// never starts one, which is what makes a single-instance unit able to hold that.
pub fn zerofs_unit(settings: &ZerofsSettings, config_file: &Path, environment_file: &Path) -> String {
    format!(
        "{}\n\
         [Unit]\n\
         Description=ZeroFS, backing every tenant disk on this host\n\
         Documentation=https://www.zerofs.net\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=exec\n\
         # Not root. This process holds the object-store credentials and the key every tenant's\n\
         # blocks are encrypted under, and `nibrunnerd install` creates the account for it.\n\
         User={ZEROFS_USER}\n\
         Group={ZEROFS_USER}\n\
         ExecStart={} run --config {}\n\
         EnvironmentFile={}\n\
         \n\
         # Any failed write to the object store terminates the process by design, and a process\n\
         # fenced by another writer exits cleanly. `always` covers both, where `on-failure` would\n\
         # leave a fenced host quietly serving nothing.\n\
         Restart=always\n\
         RestartSec=5s\n\
         \n\
         # Shutdown seals and uploads the open segment and flushes metadata. A SIGKILL part-way\n\
         # through loses the unflushed tail, which is exactly what ignore_fsync already risks.\n\
         TimeoutStopSec=120\n\
         \n\
         RuntimeDirectory={}\n\
         RuntimeDirectoryMode=0750\n\
         LimitNOFILE=1048576\n\
         \n\
         NoNewPrivileges=true\n\
         ProtectSystem=strict\n\
         ProtectHome=true\n\
         PrivateTmp=true\n\
         ProtectKernelTunables=true\n\
         ProtectKernelModules=true\n\
         RestrictRealtime=true\n\
         RestrictSUIDSGID=true\n\
         LockPersonality=true\n\
         ReadWritePaths={}\n\
         \n\
         LogExtraFields=SOURCE=zerofs\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        header("The one writer this host's storage prefix may have.", config_file),
        settings.binary.display(),
        settings.config_file.display(),
        environment_file.display(),
        runtime_directory(&settings.nbd_socket_path),
        settings.cache_dir.display(),
    )
}

/// This host's own view of the filesystem, which is where a volume's device file is created.
pub fn mount_unit(settings: &ZerofsSettings, config_file: &Path) -> String {
    format!(
        "{}\n\
         [Unit]\n\
         Description=The host's own view of the ZeroFS filesystem\n\
         Documentation=https://www.zerofs.net\n\
         After={ZEROFS_UNIT}\n\
         Requires={ZEROFS_UNIT}\n\
         # The socket goes with ZeroFS, so this mount goes with it: a restart of the server leaves\n\
         # a mount pointing at a socket that no longer exists.\n\
         PartOf={ZEROFS_UNIT}\n\
         # A start job for this unit waits out every auto-restart of the server, and a server that\n\
         # fails at once restarts for ever — so the job would too, and whoever asked for it with\n\
         # it. A minute is longer than any first boot the server takes to open its socket.\n\
         JobTimeoutSec=60\n\
         Before=nibrunnerd.service\n\
         \n\
         [Service]\n\
         Type=exec\n\
         ExecStartPre=/usr/bin/mkdir -p {}\n\
         # ZeroFS execs well before it answers, so `After=` alone would let this race an unopened\n\
         # socket and fail its first start. Waiting for the socket is the readiness signal.\n\
         ExecStartPre=/bin/sh -c 'for _ in $(seq 100); do [ -S {} ] && exit 0; sleep 0.1; done; exit 1'\n\
         # --writeback false: the NBD server is a second client of the same filesystem, so a write\n\
         # buffered in this client's page cache is a device the server cannot see yet. One file is\n\
         # created per app, once, so throughput is irrelevant and coherence is the whole point.\n\
         ExecStart={} mount --writeback false unix:{} {}\n\
         ExecStartPost=/bin/sh -c 'for _ in $(seq 100); do mountpoint -q {} && exit 0; sleep 0.1; done; exit 1'\n\
         # The two waits above are ten seconds each, but a stat on a FUSE mount whose server has\n\
         # stopped answering does not return, and neither would the loop around it. This is what\n\
         # turns that into a failed unit with a status, rather than a start job that never ends.\n\
         TimeoutStartSec=30\n\
         ExecStopPost=-/usr/bin/fusermount3 -u {}\n\
         Restart=always\n\
         RestartSec=2s\n\
         \n\
         LogExtraFields=SOURCE=zerofs\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        header(
            "Where this host creates a volume's device file, inside the filesystem itself.",
            config_file
        ),
        settings.mount_path.display(),
        settings.ninep_socket_path.display(),
        settings.binary.display(),
        settings.ninep_socket_path.display(),
        settings.mount_path.display(),
        settings.mount_path.display(),
        settings.mount_path.display(),
    )
}

pub const DAEMON_UNIT: &str = "nibrunnerd.service";
pub const DAEMON_DROP_IN: &str = "nibrunnerd.service.d/install.conf";

/// The daemon's own unit, from the one copy of it in this repository, with the path it is started
/// from replaced by the path this binary is actually at — so a host that put it somewhere other
/// than `/usr/local/bin` gets a unit that names where its binary really is.
pub fn daemon_unit(config_file: &Path, binary: &Path) -> String {
    const UNIT: &str = include_str!("../../../../deploy/nibrunnerd.service");
    let started = UNIT.replace(
        "ExecStart=/usr/local/bin/nibrunnerd",
        &format!("ExecStart={}", binary.display()),
    );
    format!(
        "{}\n{started}",
        header("What supervises this daemon.", config_file)
    )
}

/// The daemon reads the same environment file, and for two reasons rather than one: its artifact
/// and export stores resolve AWS credentials from their own process environment, and the
/// checkpoint servers it spawns inherit that environment to expand ZeroFS's `${VAR}` references.
pub fn daemon_drop_in(config: &HostConfig, config_file: &Path, environment_file: &Path) -> String {
    let ordering = match config.volumes.zerofs() {
        // Deliberately not Requires= or BindsTo=: this daemon must be restartable, and stoppable,
        // without that being an event for anything it manages.
        Some(_) => format!("\n[Unit]\nAfter={MOUNT_UNIT}\nWants={MOUNT_UNIT}\n"),
        None => String::new(),
    };
    format!(
        "{}{ordering}\n[Service]\nEnvironmentFile={}\n",
        header(
            "What this daemon needs beyond its own configuration.",
            config_file
        ),
        environment_file.display(),
    )
}

/// systemd names a `RuntimeDirectory=` relative to /run, and the sockets are what live in it.
fn runtime_directory(socket: &Path) -> String {
    socket
        .parent()
        .and_then(|parent| parent.strip_prefix("/run").ok())
        .map_or_else(|| "zerofs".to_string(), |rest| rest.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::zerofs_settings;

    fn settings() -> ZerofsSettings {
        zerofs_settings(|_| {})
    }

    fn config_file() -> &'static Path {
        Path::new("/etc/nibrunner/config.toml")
    }

    fn environment_file() -> &'static Path {
        Path::new("/etc/nibrunner/host.env")
    }

    #[test]
    fn the_rendered_config_is_the_toml_zerofs_reads() {
        let rendered = zerofs_config(&settings(), config_file(), environment_file());
        let parsed: toml::Value = toml::from_str(&rendered).expect("a config zerofs could parse");
        assert!(parsed.get("cache").is_some());
        assert!(parsed.get("storage").is_some());
    }

    #[test]
    fn the_admin_rpc_is_configured_because_a_flush_that_fails_stops_a_tenant() {
        let rendered = zerofs_config(&settings(), config_file(), environment_file());
        let parsed: toml::Value = toml::from_str(&rendered).expect("valid toml");
        assert_eq!(
            parsed["servers"]["rpc"]["unix_socket"].as_str(),
            Some(settings().rpc_socket_path.display().to_string().as_str())
        );
    }

    // The reservation this daemon holds back from every guest is read out of the file it just
    // wrote, so the number that goes in has to be the number that comes back out.
    #[test]
    fn the_cache_this_host_reserves_against_is_the_cache_it_wrote() {
        let settings = zerofs_settings(|settings| {
            settings.cache_disk_mib = 200 * 1024;
            settings.cache_memory_mib = 2 * 1024;
        });
        let rendered = zerofs_config(&settings, config_file(), environment_file());
        assert_eq!(
            crate::adapters::volumes::zerofs::cache_gigabytes(&rendered, "disk_size_gb"),
            Some(200)
        );
        assert_eq!(
            crate::adapters::volumes::zerofs::cache_gigabytes(&rendered, "memory_size_gb"),
            Some(2)
        );
    }

    #[test]
    fn no_secret_is_written_into_a_file_that_is_only_configuration() {
        let settings = settings();
        for rendered in [
            zerofs_config(&settings, config_file(), environment_file()),
            checkpoint_config(&settings, config_file()),
        ] {
            assert!(
                rendered.contains(&format!("${{{PASSWORD_VARIABLE}}}")),
                "{rendered}"
            );
            assert!(
                rendered.contains(&format!("${{{REGION_VARIABLE}}}")),
                "{rendered}"
            );
        }
    }

    // The daemon spawns these itself and sets this variable on the process it spawns. If the two
    // ever disagreed, every checkpoint server would open the same cache and the same socket.
    #[test]
    fn a_checkpoint_is_told_apart_by_the_variable_the_daemon_actually_sets() {
        let rendered = checkpoint_config(&settings(), config_file());
        assert!(
            rendered.contains(&format!("${{{CHECKPOINT_VARIABLE}}}")),
            "{rendered}"
        );
        assert!(rendered.contains(NBD_SOCKET_FILENAME), "{rendered}");
    }

    #[test]
    fn a_checkpoint_server_is_never_told_to_write() {
        let rendered = checkpoint_config(&settings(), config_file());
        let parsed: toml::Value = toml::from_str(&rendered).expect("valid toml");
        for writing in ["lsm", "filesystem", "gc", "prometheus"] {
            assert!(parsed.get(writing).is_none(), "[{writing}] is in {rendered}");
        }
        assert!(parsed["servers"].get("rpc").is_none(), "{rendered}");
    }

    #[test]
    fn the_unit_runs_the_one_writer_and_the_mount_waits_for_it() {
        let settings = settings();
        let unit = zerofs_unit(&settings, config_file(), environment_file());
        assert!(unit.contains(&format!("ExecStart={} run --config", settings.binary.display())));
        assert!(!unit.contains("--checkpoint"), "{unit}");

        let mount = mount_unit(&settings, config_file());
        assert!(mount.contains(&format!("Requires={ZEROFS_UNIT}")), "{mount}");
        assert!(mount.contains("--writeback false"), "{mount}");
        assert!(
            mount.contains(&format!("[ -S {} ]", settings.ninep_socket_path.display())),
            "the mount must wait for the socket rather than race it: {mount}"
        );
        assert!(
            mount.contains("TimeoutStartSec=30"),
            "a stat on an unanswering FUSE mount never returns, and a start attempt must: {mount}"
        );
        assert!(
            mount.contains("JobTimeoutSec=60"),
            "a job waits out every auto-restart of a server that fails at once, and must not for ever: {mount}"
        );
    }

    // It holds this host's object-store credentials and the key every tenant's blocks are
    // encrypted under, so the one thing this unit must never say is nothing about who runs it.
    #[test]
    fn the_one_writer_does_not_run_as_root() {
        let unit = zerofs_unit(&settings(), config_file(), environment_file());
        assert!(unit.contains(&format!("User={ZEROFS_USER}")), "{unit}");
        assert!(unit.contains(&format!("Group={ZEROFS_USER}")), "{unit}");
        assert!(unit.contains("NoNewPrivileges=true"), "{unit}");
    }

    #[test]
    fn a_local_file_host_gets_no_ordering_on_a_unit_it_does_not_run() {
        let config = HostConfig::under(Path::new("/srv/nibrunner"));
        let rendered = daemon_drop_in(&config, config_file(), environment_file());
        assert!(!rendered.contains(MOUNT_UNIT), "{rendered}");
        assert!(
            rendered.contains("EnvironmentFile=/etc/nibrunner/host.env"),
            "{rendered}"
        );
    }

    #[test]
    fn everything_written_here_says_so_on_its_first_line() {
        let settings = settings();
        let config = HostConfig::under(Path::new("/srv/nibrunner"));
        for rendered in [
            zerofs_config(&settings, config_file(), environment_file()),
            checkpoint_config(&settings, config_file()),
            zerofs_unit(&settings, config_file(), environment_file()),
            mount_unit(&settings, config_file()),
            daemon_drop_in(&config, config_file(), environment_file()),
        ] {
            assert!(rendered.starts_with(GENERATED_MARKER), "{rendered}");
            assert!(rendered.contains("/etc/nibrunner/config.toml"), "{rendered}");
        }
    }

    #[test]
    fn a_runtime_directory_is_named_the_way_systemd_names_one() {
        assert_eq!(runtime_directory(Path::new("/run/zerofs/nbd.sock")), "zerofs");
        assert_eq!(
            runtime_directory(Path::new("/run/nibrunner/zerofs/nbd.sock")),
            "nibrunner/zerofs"
        );
        assert_eq!(runtime_directory(Path::new("/var/lib/nbd.sock")), "zerofs");
    }
}
