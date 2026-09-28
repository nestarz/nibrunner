//! `nibrunnerd start`: the host `install` laid out, asked of systemd. Everything before this point
//! writes files; this is the one place that asks for them to be run, and it asks only after
//! checking what a start would otherwise find out too late and say from too far away — a secret
//! still empty in `host.env`, a certificate the configuration names that is not there or that
//! holds fewer certificates than it marks. With those checked, a start that fails is one the
//! configuration got wrong.

use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime};

use crate::adapters::proxy::pem::read_certificates;
use crate::config::HostConfig;
use crate::install::render::{DAEMON_UNIT, MOUNT_UNIT, ZEROFS_UNIT};
use crate::install::{secrets, unit_path, Laid};

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("this host cannot be started:\n{0}\nnothing was started")]
    Unstartable(String),
    #[error("{0}")]
    Systemd(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Started,
    Restarted,
    Reloaded,
    /// Left as it was, because nothing it reads changed.
    Running,
    Failed,
}

impl Outcome {
    pub fn said(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Restarted => "restarted",
            Self::Reloaded => "reloaded",
            Self::Running => "running",
            Self::Failed => "failed",
        }
    }
}

pub struct Report {
    pub units: Vec<(&'static str, Outcome)>,
    /// `host.env` was edited after ZeroFS last started, and ZeroFS was left running.
    pub kept_credentials: Option<KeptCredentials>,
}

/// ZeroFS expands the object-store key and the encryption password out of its environment once,
/// on the way up, and a start that leaves it running — every start that changed nothing it reads —
/// leaves it with the ones it started with. Not a reason to restart it from here: that drops
/// every NBD device under every guest, and only the operator knows when no guest needs its disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptCredentials {
    pub environment_file: PathBuf,
}

impl KeptCredentials {
    pub fn said(&self) -> String {
        format!(
            "{} changed after {ZEROFS_UNIT} last started, and ZeroFS reads it only on the way up: it keeps the credentials it started with until `systemctl restart nibrunner-zerofs` is run. Run that at a moment no guest needs its disk, because the restart drops every device under every guest.",
            self.environment_file.display()
        )
    }
}

impl Report {
    pub fn all_up(&self) -> bool {
        self.units.iter().all(|(_, outcome)| *outcome != Outcome::Failed)
    }

    /// One command that shows every unit this host has, because the one that failed is rarely
    /// the one that says why: the mount fails because the server it requires did.
    pub fn journal(&self) -> String {
        let named: Vec<String> = self.units.iter().map(|(unit, _)| format!("-u {unit}")).collect();
        format!("journalctl {} -n 60 --no-pager", named.join(" "))
    }
}

pub fn run(config: &HostConfig, environment_file: &Path, laid: &Laid) -> Result<Report, StartError> {
    refuse_unstartable(config, environment_file)?;
    must(&["daemon-reload"])?;

    let plan = plan(config, laid);
    let mut enable = vec!["enable"];
    enable.extend(plan.iter().map(|(unit, _)| *unit));
    must(&enable)?;

    let mut units = Vec::new();
    for (unit, restart) in plan {
        let was_active = is_active(unit);
        if unit == DAEMON_UNIT && was_active && permits_reload(laid) {
            match crate::reload::request(config).map_err(|error| {
                StartError::Systemd(format!(
                    "the running host could not confirm its configuration: {error}"
                ))
            })? {
                Some(crate::reload::Applied::Unchanged) => {
                    units.push((unit, Outcome::Running));
                    continue;
                }
                Some(crate::reload::Applied::Grown) => {
                    units.push((unit, Outcome::Reloaded));
                    continue;
                }
                None => {}
            }
        }
        let verb = if restart { "restart" } else { "start" };
        let outcome = match systemctl(&[verb, unit])? {
            true if !is_active(unit) => Outcome::Failed,
            true if !was_active => Outcome::Started,
            true if restart => Outcome::Restarted,
            true => Outcome::Running,
            false => Outcome::Failed,
        };
        units.push((unit, outcome));
    }
    // `Type=exec` calls a unit started the moment it execs, and one that exits right after is
    // read as started by the check above — and as auto-restarting by this one, once the units
    // after it have taken their time coming up.
    for (unit, outcome) in &mut units {
        if *outcome != Outcome::Failed && !is_active(unit) {
            *outcome = Outcome::Failed;
        }
    }
    let kept_credentials = zerofs_kept_its_credentials(&units, || {
        let edited = std::fs::metadata(environment_file).ok()?.modified().ok()?;
        Some((edited, active_since(ZEROFS_UNIT)?))
    })
    .then(|| KeptCredentials {
        environment_file: environment_file.to_path_buf(),
    });
    Ok(Report {
        units,
        kept_credentials,
    })
}

/// Whether ZeroFS was left running on an environment file edited after it came up. Only a unit
/// left as it was can be: one this run started or restarted read the file on its way up. The
/// clocks are asked for only then, so a host without ZeroFS never asks systemd about it.
fn zerofs_kept_its_credentials(
    units: &[(&'static str, Outcome)],
    clocks: impl FnOnce() -> Option<(SystemTime, SystemTime)>,
) -> bool {
    let left_running = units
        .iter()
        .any(|(unit, outcome)| *unit == ZEROFS_UNIT && *outcome == Outcome::Running);
    if !left_running {
        return false;
    }
    clocks().is_some_and(|(edited, active_since)| edited > active_since)
}

/// When the unit last became active, on the wall clock. systemd keeps it on the monotonic clock,
/// and a file's mtime is on the other one, so the two are brought together through now on both.
fn active_since(unit: &str) -> Option<SystemTime> {
    let output = Command::new("systemctl")
        .args(["show", "-p", "ActiveEnterTimestampMonotonic", "--value", unit])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    let since_boot = monotonic_since_boot(&String::from_utf8_lossy(&output.stdout))?;
    let now = nix::time::clock_gettime(nix::time::ClockId::CLOCK_MONOTONIC).ok()?;
    let now_since_boot = Duration::new(now.tv_sec().try_into().ok()?, now.tv_nsec().try_into().ok()?);
    wall_clock_of(since_boot, now_since_boot, SystemTime::now())
}

/// systemd prints microseconds since boot, and `0` for a unit that has never been active.
fn monotonic_since_boot(shown: &str) -> Option<Duration> {
    let microseconds: u64 = shown.trim().parse().ok()?;
    (microseconds > 0).then(|| Duration::from_micros(microseconds))
}

fn wall_clock_of(since_boot: Duration, now_since_boot: Duration, now: SystemTime) -> Option<SystemTime> {
    now.checked_sub(now_since_boot.checked_sub(since_boot)?)
}

/// Which units this host has, in the order they come up, and whether each is restarted or merely
/// started. The daemon falls back to a restart when it cannot apply the configuration in place.
/// ZeroFS only when something it reads changed:
/// its restart drops every NBD device under every live guest, and the mount goes with it.
fn plan(config: &HostConfig, laid: &Laid) -> Vec<(&'static str, bool)> {
    let mut plan = Vec::new();
    if let Some(settings) = config.volumes.zerofs() {
        let rewritten = |paths: &[PathBuf]| laid.written.iter().any(|path| paths.contains(path));
        let zerofs = rewritten(&[
            settings.binary.clone(),
            settings.config_file.clone(),
            unit_path(ZEROFS_UNIT),
        ]);
        let mount = zerofs || rewritten(&[unit_path(MOUNT_UNIT)]);
        plan.push((ZEROFS_UNIT, zerofs));
        plan.push((MOUNT_UNIT, mount));
    }
    plan.push((DAEMON_UNIT, true));
    plan
}

fn permits_reload(laid: &Laid) -> bool {
    laid.written
        .iter()
        .all(|path| path == Path::new(crate::install::kernel::SYSCTL_FILE))
}

fn refuse_unstartable(config: &HostConfig, environment_file: &Path) -> Result<(), StartError> {
    let mut reasons: Vec<String> = secrets::missing(config, environment_file)
        .into_iter()
        .map(|name| format!("  {name} is not set in {}", environment_file.display()))
        .collect();
    for (what, path, holds) in tls_material(config) {
        if !path.is_file() {
            reasons.push(format!(
                "  [proxy.http.tls] {what} {} is not there",
                path.display()
            ));
        } else if holds == Holds::Certificates {
            if let Err(error) = read_certificates(path) {
                reasons.push(format!("  [proxy.http.tls] {what} {error}"));
            }
        }
    }
    for (section, address) in listeners(config) {
        if !this_host_has(address) {
            reasons.push(format!(
                "  [{section}] listen_address {address} is not an address this host has"
            ));
        }
    }
    if reasons.is_empty() {
        return Ok(());
    }
    Err(StartError::Unstartable(reasons.join("\n")))
}

fn listeners(config: &HostConfig) -> Vec<(&'static str, IpAddr)> {
    let mut listeners = Vec::new();
    if let Some(http) = &config.proxy.http {
        listeners.push(("proxy.http", http.listen_address));
    }
    if let Some(raw) = &config.proxy.raw {
        listeners.push(("proxy.raw", raw.listen_address));
    }
    if let Some(metrics) = &config.metrics {
        listeners.push(("metrics", metrics.listen_address));
    }
    listeners
}

/// Asked of the kernel the way the daemon will ask it, on a port it hands out: binding an address
/// this host does not have fails the same way whichever port is named, and this way needs no
/// privilege and takes nothing the daemon will want.
fn this_host_has(address: IpAddr) -> bool {
    address.is_unspecified() || UdpSocket::bind(SocketAddr::new(address, 0)).is_ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Holds {
    /// Read here the way the daemon reads it, because a file that is there can still be short.
    Certificates,
    Key,
}

fn tls_material(config: &HostConfig) -> Vec<(&'static str, &Path, Holds)> {
    let Some(tls) = config.proxy.http.as_ref().and_then(|http| http.tls.as_ref()) else {
        return Vec::new();
    };
    let mut material = vec![
        ("certificate", tls.certificate.as_path(), Holds::Certificates),
        ("key", tls.key.as_path(), Holds::Key),
    ];
    if let Some(client_ca) = &tls.client_ca {
        material.push(("client_ca.certificate", client_ca.as_path(), Holds::Certificates));
    }
    material
}

/// Longer than any unit here can take to start or stop — the mount waits up to twenty seconds,
/// ZeroFS is given two minutes to stop — so reaching it means a job that is not going to finish,
/// which is a thing to be told about rather than waited on.
const SYSTEMCTL_DEADLINE: Duration = Duration::from_secs(300);

fn must(arguments: &[&str]) -> Result<(), StartError> {
    if systemctl(arguments)? {
        return Ok(());
    }
    Err(StartError::Systemd(format!(
        "systemctl {} failed",
        arguments.join(" ")
    )))
}

/// Said before it is run, so a call that blocks is seen blocking on the command it is. systemd's
/// own stderr comes straight through: "Job for X failed, see journalctl -xeu X" is the message,
/// and rewording it would only lose the unit's name.
fn systemctl(arguments: &[&str]) -> Result<bool, StartError> {
    println!("  systemctl {}", arguments.join(" "));
    let could_not =
        |error: std::io::Error| StartError::Systemd(format!("systemctl could not be run: {error}"));
    let mut child = Command::new("systemctl")
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(could_not)?;
    let deadline = Instant::now() + SYSTEMCTL_DEADLINE;
    loop {
        if let Some(status) = child.try_wait().map_err(could_not)? {
            return Ok(status.success());
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            return Err(StartError::Systemd(format!(
                "systemctl {} has not returned after {} minutes. The job is still queued: `systemctl list-jobs` shows it, and `systemctl status` on the unit says what it is waiting on",
                arguments.join(" "),
                SYSTEMCTL_DEADLINE.as_secs() / 60
            )));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn is_active(unit: &str) -> bool {
    Command::new("systemctl")
        .args(["is-active", "--quiet", unit])
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HttpListener, TlsMaterial, VolumeBackend};
    use crate::test_support::zerofs_settings;

    fn zerofs_host() -> HostConfig {
        let mut config = HostConfig::under(Path::new("/srv/nibrunner"));
        config.volumes = VolumeBackend::Zerofs(Box::new(zerofs_settings(|_| {})));
        config
    }

    fn written(paths: &[PathBuf]) -> Laid {
        Laid {
            written: paths.to_vec(),
            ..Laid::default()
        }
    }

    #[test]
    fn applied_kernel_settings_allow_reload_but_rewritten_service_files_do_not() {
        assert!(permits_reload(&written(&[])));
        assert!(permits_reload(&written(&[PathBuf::from(
            crate::install::kernel::SYSCTL_FILE
        )])));
        assert!(!permits_reload(&written(&[unit_path(DAEMON_UNIT)])));
    }

    #[test]
    fn a_local_file_host_has_one_unit_with_a_restart_fallback() {
        let plan = plan(&HostConfig::under(Path::new("/srv/nibrunner")), &Laid::default());
        assert_eq!(plan, [(DAEMON_UNIT, true)]);
    }

    #[test]
    fn zerofs_is_left_running_when_nothing_it_reads_changed() {
        let plan = plan(
            &zerofs_host(),
            &written(&[unit_path("nibrunnerd.service.d/install.conf")]),
        );
        assert_eq!(
            plan,
            [(ZEROFS_UNIT, false), (MOUNT_UNIT, false), (DAEMON_UNIT, true)]
        );
    }

    // Its config, its unit and its binary are what it reads on the way up. A restart drops every
    // device under every guest, so each is named rather than "anything at all changed".
    #[test]
    fn zerofs_is_restarted_when_what_it_reads_was_rewritten_and_the_mount_follows() {
        let config = zerofs_host();
        let settings = config.volumes.zerofs().unwrap();
        for changed in [
            settings.config_file.clone(),
            settings.binary.clone(),
            unit_path(ZEROFS_UNIT),
        ] {
            let plan = plan(&config, &written(std::slice::from_ref(&changed)));
            assert_eq!(
                plan,
                [(ZEROFS_UNIT, true), (MOUNT_UNIT, true), (DAEMON_UNIT, true)],
                "{}",
                changed.display()
            );
        }
        // Read by the checkpoint servers the daemon spawns, never by the live server.
        let plan = plan(
            &config,
            &written(std::slice::from_ref(&settings.checkpoint_config_file)),
        );
        assert_eq!(plan[0], (ZEROFS_UNIT, false));
    }

    #[test]
    fn the_mount_alone_is_restarted_when_only_its_unit_changed() {
        let plan = plan(&zerofs_host(), &written(&[unit_path(MOUNT_UNIT)]));
        assert_eq!(
            plan,
            [(ZEROFS_UNIT, false), (MOUNT_UNIT, true), (DAEMON_UNIT, true)]
        );
    }

    #[test]
    fn what_went_wrong_is_read_from_every_unit_the_host_has_not_only_the_one_that_failed() {
        let report = Report {
            units: vec![
                (ZEROFS_UNIT, Outcome::Failed),
                (MOUNT_UNIT, Outcome::Failed),
                (DAEMON_UNIT, Outcome::Started),
            ],
            kept_credentials: None,
        };
        assert!(!report.all_up());
        assert_eq!(
            report.journal(),
            "journalctl -u nibrunner-zerofs.service -u nibrunner-zerofs-mount.service -u nibrunnerd.service -n 60 --no-pager"
        );
        assert!(Report {
            units: vec![(DAEMON_UNIT, Outcome::Running)],
            kept_credentials: None,
        }
        .all_up());
    }

    fn zerofs_left(outcome: Outcome) -> Vec<(&'static str, Outcome)> {
        vec![
            (ZEROFS_UNIT, outcome),
            (MOUNT_UNIT, Outcome::Running),
            (DAEMON_UNIT, Outcome::Restarted),
        ]
    }

    // A rotated key or a corrected password in host.env reaches the daemon, which is always
    // restarted, and never a ZeroFS left running — and restarting ZeroFS from here would drop
    // every device under every guest. So it is said, once, on the start that left it running.
    #[test]
    fn zerofs_left_running_on_an_environment_file_edited_since_it_came_up_is_said() {
        let came_up = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let edited_after = came_up + Duration::from_secs(60);
        let edited_before = came_up - Duration::from_secs(60);

        assert!(zerofs_kept_its_credentials(
            &zerofs_left(Outcome::Running),
            || Some((edited_after, came_up))
        ));
        assert!(!zerofs_kept_its_credentials(
            &zerofs_left(Outcome::Running),
            || Some((edited_before, came_up))
        ));
        assert!(
            !zerofs_kept_its_credentials(&zerofs_left(Outcome::Running), || Some((came_up, came_up))),
            "written in the same instant it came up is what it read"
        );
    }

    #[test]
    fn a_zerofs_this_run_brought_up_read_the_environment_file_as_it_is() {
        let came_up = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let edited_after = came_up + Duration::from_secs(60);
        for outcome in [Outcome::Started, Outcome::Restarted, Outcome::Failed] {
            assert!(
                !zerofs_kept_its_credentials(&zerofs_left(outcome), || Some((edited_after, came_up))),
                "{}",
                outcome.said()
            );
        }
        assert!(!zerofs_kept_its_credentials(
            &[(DAEMON_UNIT, Outcome::Restarted)],
            || panic!("a host without ZeroFS has nothing to ask systemd about")
        ));
        assert!(
            !zerofs_kept_its_credentials(&zerofs_left(Outcome::Running), || None),
            "a clock that cannot be read is no claim either way"
        );
    }

    #[test]
    fn what_systemd_shows_of_the_monotonic_clock_is_brought_onto_the_wall_clock() {
        assert_eq!(
            monotonic_since_boot("1500000\n"),
            Some(Duration::from_micros(1_500_000))
        );
        assert_eq!(monotonic_since_boot("0\n"), None, "never active");
        assert_eq!(monotonic_since_boot(""), None);
        assert_eq!(monotonic_since_boot("Failed to get properties"), None);

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
        assert_eq!(
            wall_clock_of(Duration::from_secs(100), Duration::from_secs(400), now),
            Some(now - Duration::from_secs(300))
        );
        assert_eq!(
            wall_clock_of(Duration::from_secs(500), Duration::from_secs(400), now),
            None,
            "active since after now is a clock that cannot be trusted"
        );
    }

    #[test]
    fn what_is_said_names_the_file_the_command_and_when_to_run_it() {
        let said = KeptCredentials {
            environment_file: PathBuf::from("/etc/nibrunner/host.env"),
        }
        .said();
        assert!(
            said.starts_with("/etc/nibrunner/host.env changed after nibrunner-zerofs.service last started"),
            "{said}"
        );
        assert!(said.contains("keeps the credentials it started with"), "{said}");
        assert!(said.contains("`systemctl restart nibrunner-zerofs`"), "{said}");
        assert!(said.contains("no guest needs its disk"), "{said}");
    }

    #[test]
    fn a_secret_still_empty_refuses_the_start_by_name() {
        let directory = tempfile::tempdir().unwrap();
        let environment_file = directory.path().join("host.env");
        std::fs::write(&environment_file, "ZEROFS_ENCRYPTION_PASSWORD=\n").unwrap();
        let mut config = zerofs_host();
        config.artifact_store_url = "s3://nibrunner-artifacts/artifacts".to_string();

        let error = refuse_unstartable(&config, &environment_file).unwrap_err();
        let said = error.to_string();
        assert!(said.contains("ZEROFS_ENCRYPTION_PASSWORD is not set"), "{said}");
        assert!(said.contains("AWS_REGION is not set"), "{said}");
        assert!(said.contains("nothing was started"), "{said}");

        assert!(
            refuse_unstartable(&HostConfig::under(Path::new("/srv/nibrunner")), &environment_file).is_ok()
        );
    }

    // The example configuration names a relay's private address for raw ports, and a host that
    // copied it as it is would bind an address it does not have — a second after `start` had
    // called it up. Said here instead, before anything is asked of systemd.
    #[test]
    fn a_listen_address_this_host_does_not_have_refuses_the_start_by_name() {
        use crate::config::RawPorts;
        let directory = tempfile::tempdir().unwrap();
        let mut config = HostConfig::under(Path::new("/srv/nibrunner"));
        config.proxy.raw = Some(RawPorts {
            // TEST-NET-1: documentation only, so no host has it.
            listen_address: "192.0.2.1".parse().unwrap(),
            max_ports_per_guest: 1,
        });
        config.metrics = Some(crate::config::MetricsConfig {
            port: 9100,
            listen_address: "127.0.0.1".parse().unwrap(),
        });

        let error = refuse_unstartable(&config, &directory.path().join("host.env")).unwrap_err();
        let said = error.to_string();
        assert!(said.contains("[proxy.raw] listen_address 192.0.2.1"), "{said}");
        assert!(!said.contains("[metrics]"), "{said}");

        assert!(this_host_has("0.0.0.0".parse().unwrap()));
        assert!(this_host_has("::".parse().unwrap()));
    }

    const A_CERTIFICATE: &str = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----";

    fn serving_tls(directory: &Path, client_ca: Option<PathBuf>) -> HostConfig {
        let certificate = directory.join("origin.crt");
        std::fs::write(&certificate, A_CERTIFICATE).unwrap();
        let mut config = HostConfig::under(Path::new("/srv/nibrunner"));
        config.proxy.http = Some(HttpListener {
            listen_address: "0.0.0.0".parse().unwrap(),
            port: 443,
            tls: Some(TlsMaterial {
                certificate,
                key: directory.join("origin.key"),
                client_ca,
            }),
            redirect_from_port: None,
        });
        config
    }

    // The daemon reads it once, at bind time, and a path that is not there is a failed unit and a
    // journal to go and read — said here instead, before anything is asked of systemd.
    #[test]
    fn tls_material_the_configuration_names_has_to_be_there() {
        let directory = tempfile::tempdir().unwrap();
        let config = serving_tls(directory.path(), None);

        let error = refuse_unstartable(&config, &directory.path().join("host.env")).unwrap_err();
        let said = error.to_string();
        assert!(said.contains("[proxy.http.tls] key"), "{said}");
        assert!(!said.contains("[proxy.http.tls] certificate"), "{said}");
    }

    // Two CA files concatenated where the first has no trailing newline glue an END marker to the
    // next BEGIN, and the daemon would load one certificate of the two without a word and refuse
    // every caller the second one signed. Said here, with the counts, before it is started.
    #[test]
    fn a_trust_pool_short_of_what_it_marks_refuses_the_start_with_the_counts() {
        let directory = tempfile::tempdir().unwrap();
        let pool = directory.path().join("origin-pull-ca.pem");
        std::fs::write(&pool, format!("{A_CERTIFICATE}{A_CERTIFICATE}\n")).unwrap();
        std::fs::write(directory.path().join("origin.key"), "").unwrap();
        let config = serving_tls(directory.path(), Some(pool.clone()));

        let error = refuse_unstartable(&config, &directory.path().join("host.env")).unwrap_err();
        let said = error.to_string();
        assert!(
            said.contains(&format!(
                "  [proxy.http.tls] client_ca.certificate {} marks 2 certificates and 1 could be read",
                pool.display()
            )),
            "{said}"
        );
        assert!(!said.contains("[proxy.http.tls] certificate"), "{said}");
        assert!(!said.contains("[proxy.http.tls] key"), "{said}");
    }
}
