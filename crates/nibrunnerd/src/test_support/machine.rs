//! A host on the machine the tests are running on: real taps, a real ruleset, real microVMs.
//!
//! [`test_host`](super::test_host) proves what the daemon decides; this proves what the machine
//! then does. It is built the way `nibrunnerd serve` builds one — [`crate::run::build_host`], then
//! the lifecycle controller — so a test drives the same daemon an operator runs, over a temporary
//! directory, with volumes as local files and the proxy on loopback.
//!
//! One at a time: the nftables ruleset and the tap names belong to the machine, not to a process,
//! so two of these at once would take each other's. The tests that use it are one binary run with
//! `--test-threads 1`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use protocol::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::controllers::lifecycle_controller::LifecycleController;
use crate::host::Host;

/// Where the tests' own tenant is built to. The justfile hands the path over, because a target
/// directory is not always where cargo's default puts it.
const TENANT_BINARY: &str = "NIBRUNNER_TEST_TENANT";
const TENANT_KEY: &str = "tenant";

/// Small, so that the ruleset a test renders is small and the ports it takes are few.
const MAX_APPS: u32 = 8;

/// How long anything a guest has to do is given before the test says it never happened. Generous:
/// a runner under load is slow, and a test that flakes is worse than a test that is slow to fail.
const PATIENCE: Duration = Duration::from_secs(90);

/// What a request gets. Longer than a wake from a snapshot and far shorter than the patience
/// above, so that a request nothing answers fails its own test rather than the whole run.
const ANSWER_DEADLINE: Duration = Duration::from_secs(30);

pub struct RunningHost {
    pub host: Arc<Host>,
    pub tenant_digest: Sha256Digest,
    lifecycle: Arc<LifecycleController>,
    controllers: Vec<tokio::task::JoinHandle<()>>,
    proxy: SocketAddr,
    stopped: bool,
    _directory: tempfile::TempDir,
}

/// A microVM outlives the process that booted it, so a test that panicked before it could stop
/// its guests has that done here. Blocking inside a drop needs a runtime with somewhere else to
/// put the work, which is why every test in this suite asks for a multi-threaded one.
impl Drop for RunningHost {
    fn drop(&mut self) {
        if self.stopped {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(self.take_down()));
        }
    }
}

/// The repository's `guest/`, where `just guest-image` leaves what a microVM boots.
fn guest_image_dir() -> Option<PathBuf> {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../guest");
    directory.join("rootfs.ext4").exists().then_some(directory)
}

fn tenant_binary() -> Option<PathBuf> {
    let named = std::env::var(TENANT_BINARY).map(PathBuf::from).ok();
    let built = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/x86_64-unknown-linux-musl/release/test-tenant");
    named.into_iter().chain([built]).find(|path| path.exists())
}

/// A port nothing is listening on. Racy in principle; in a suite that runs one test at a time
/// against a machine that is otherwise idle, it is the shortest way to a free port.
fn free_port() -> u16 {
    std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|listener| listener.local_addr())
        .map_or(0, |address| address.port())
}

/// A whole host, or nothing when this machine has no guest image and no tenant to boot on it.
pub async fn started() -> Option<RunningHost> {
    started_with(|_| {}).await
}

pub async fn started_with(edit: impl FnOnce(&mut crate::config::HostConfig)) -> Option<RunningHost> {
    let guest_image_dir = guest_image_dir()?;
    let tenant = tenant_binary()?;

    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut config = crate::config::HostConfig::under(directory.path());
    config.guest_image_dir = guest_image_dir;
    config.max_apps = MAX_APPS;
    let port = free_port();
    config.proxy = crate::config::ProxyConfig {
        http: Some(crate::config::HttpListener {
            listen_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port,
            tls: None,
        }),
        raw: Some(crate::config::RawPorts {
            listen_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            max_ports_per_guest: 2,
        }),
    };
    edit(&mut config);

    let store = PathBuf::from(&config.artifact_store_url);
    crate::json_store::make_directory(&store, 0o755).expect("an artifact store");
    let bytes = std::fs::read(&tenant).expect("the tenant this suite boots");
    std::fs::write(store.join(TENANT_KEY), &bytes).expect("the tenant in the store");
    let tenant_digest = {
        use sha2::Digest;
        Sha256Digest::parse(hex::encode(sha2::Sha256::digest(&bytes))).expect("a digest")
    };

    // What `nibrunnerd install` makes true of the running kernel — forwarding on, conntrack
    // sized — and nothing else it lays down. Without it a guest cannot reach anything outside
    // this host, so every invariant about egress would pass by never being asked.
    crate::install::kernel::apply(&config, &mut crate::install::Laid::default())
        .expect("this machine takes the settings a host needs");

    let host = crate::run::build_host(config)
        .await
        .expect("a host on this machine");
    let lifecycle = LifecycleController::new(host.clone(), crate::run::host_versions(&host));
    lifecycle.start().await;
    let controllers = lifecycle
        .controllers()
        .into_iter()
        .map(|controller| tokio::spawn(async move { controller.run().await }))
        .collect();

    Some(RunningHost {
        host,
        tenant_digest,
        lifecycle,
        controllers,
        proxy: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        stopped: false,
        _directory: directory,
    })
}

/// One app: its volume, the instance that runs the tests' tenant on it, and the hostname the
/// proxy knows it by.
#[derive(Clone)]
pub struct Tenant {
    pub app_id: AppId,
    pub volume_id: VolumeId,
    pub hostname: String,
    pub volume: DesiredVolume,
    pub instance: DesiredInstance,
}

impl Tenant {
    pub fn running(mut self) -> Self {
        self.instance.desired_state = DesiredInstanceState::Running;
        self
    }

    pub fn on_request(mut self, idle_timeout_ms: u64) -> Self {
        self.instance.desired_state = DesiredInstanceState::OnRequest;
        self.instance.idle_timeout_ms =
            Some(IdleTimeoutMs::try_from(idle_timeout_ms).expect("a timeout the protocol takes"));
        self
    }

    pub fn arguments(mut self, args: &[&str]) -> Self {
        self.instance.config.command.args = args
            .iter()
            .map(|argument| (*argument).to_string())
            .collect::<Vec<_>>()
            .try_into()
            .expect("arguments the protocol takes");
        self
    }

    pub fn edited(mut self, edit: impl FnOnce(&mut DesiredInstance)) -> Self {
        edit(&mut self.instance);
        self
    }
}

impl RunningHost {
    /// `app-<number>`, on its own volume, reachable at `app-<number>.test`, running the tenant.
    pub fn tenant(&self, number: u32) -> Tenant {
        let app_id = AppId::parse(format!("app-{number}")).expect("an app id");
        let volume_id = VolumeId::parse(format!("vol-{number}")).expect("a volume id");
        let hostname = format!("app-{number}.test");
        Tenant {
            volume: DesiredVolume {
                volume_id: volume_id.clone(),
                app_id: app_id.clone(),
                size_bytes: 64 * 1024 * 1024,
                desired_state: DesiredPresence::Present,
                initial_contents: None,
            },
            instance: DesiredInstance {
                app_id: app_id.clone(),
                deployment_id: DeploymentId::parse(format!("dep-{number}-1")).expect("a deployment id"),
                volume_id: volume_id.clone(),
                desired_state: DesiredInstanceState::Running,
                idle_timeout_ms: None,
                activation: None,
                layers: vec![DesiredLayer::Executable {
                    object: StoredObject {
                        digest: self.tenant_digest.clone(),
                        object_key: ObjectKey::parse(TENANT_KEY).expect("a key"),
                    },
                    destination_path: ExecutablePath::parse("/app/tenant").expect("a path"),
                }],
                config: AppConfig {
                    ports: vec![],
                    http_port: DEFAULT_HTTP_PORT,
                    command: Command {
                        program: GuestPath::parse("/app/tenant").expect("a path"),
                        args: TenantArguments::default(),
                        working_directory: GuestPath::parse("/app").expect("a path"),
                        environment: TenantEnvironment::default(),
                    },
                    resources: DEFAULT_INSTANCE_RESOURCES,
                    health_check: super::TCP_HEALTH_CHECK,
                    restart_policy: DEFAULT_RESTART_POLICY,
                },
                hostnames: vec![AppHostname {
                    hostname: Hostname::parse(&hostname).expect("a hostname"),
                    kind: AppHostnameKind::Platform,
                }],
            },
            app_id,
            volume_id,
            hostname,
        }
    }

    /// The document, as whoever deploys writes it. Everything this host should hold, every time:
    /// an app left out of a call is an app the document no longer names.
    pub async fn deploy(&self, apps: &[Tenant]) {
        let document = HostDesiredState {
            host_id: HostId::parse("host-1").expect("a host id"),
            volumes: apps.iter().map(|app| app.volume.clone()).collect(),
            instances: apps.iter().map(|app| app.instance.clone()).collect(),
            checkpoints: vec![],
            exports: vec![],
        };
        super::write_desired_state(&self.host.config.desired_state_file, &document);
    }

    pub async fn report(&self) -> HostReportedState {
        crate::domain::report::writer::build(&self.host, crate::run::host_versions(&self.host)).await
    }

    /// What the file the control plane reads says, rather than what a fresh build would say.
    pub fn reported_file(&self) -> Option<HostReportedState> {
        crate::json_store::read_json(&crate::domain::report::writer::reported_state_file(&self.host))
            .ok()
            .flatten()
    }

    pub async fn instance(&self, app_id: &AppId) -> Option<ReportedInstance> {
        self.report()
            .await
            .instances
            .into_iter()
            .find(|instance| &instance.app_id == app_id)
    }

    /// Waits for the report to say something, and says what it did say when it never does.
    pub async fn until(&self, expected: &str, settled: impl Fn(&HostReportedState) -> bool) -> Duration {
        let started = Instant::now();
        loop {
            let report = self.report().await;
            if settled(&report) {
                return started.elapsed();
            }
            if started.elapsed() > PATIENCE {
                let states: Vec<String> = report
                    .instances
                    .iter()
                    .map(|instance| {
                        format!(
                            "{}={:?}{}",
                            instance.app_id,
                            instance.state,
                            instance
                                .message
                                .as_ref()
                                .map(|said| format!(" ({said})"))
                                .unwrap_or_default()
                        )
                    })
                    .collect();
                let volumes: Vec<String> = report
                    .volumes
                    .iter()
                    .map(|volume| format!("{}={:?}", volume.volume_id, volume.state))
                    .collect();
                panic!(
                    "waited {PATIENCE:?} for {expected}; the report held instances [{}] and volumes [{}]",
                    states.join(", "),
                    volumes.join(", ")
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn until_state(&self, app_id: &AppId, state: InstanceState) -> Duration {
        self.until(&format!("{app_id} to be {state:?}"), |report| {
            report
                .instances
                .iter()
                .any(|instance| &instance.app_id == app_id && instance.state == state)
        })
        .await
    }

    /// One request through the proxy on a connection of its own, which is what makes a refusal
    /// visible rather than absorbed by a pool.
    pub async fn get(&self, tenant: &Tenant, path: &str) -> std::io::Result<Answer> {
        self.get_as(&tenant.hostname, path).await
    }

    /// Every request has a deadline, because a proxy holding a connection open into a guest that
    /// will never answer is one of the things being tested for, and a test that waited on it for
    /// ever would hang the whole suite instead of failing.
    pub async fn get_as(&self, hostname: &str, path: &str) -> std::io::Result<Answer> {
        tokio::time::timeout(ANSWER_DEADLINE, async {
            let request = format!("GET {path} HTTP/1.1\r\nhost: {hostname}\r\nconnection: close\r\n\r\n");
            let mut stream = tokio::net::TcpStream::connect(self.proxy).await?;
            stream.write_all(request.as_bytes()).await?;
            let mut held = Vec::new();
            stream.read_to_end(&mut held).await?;
            Answer::parse(&held)
        })
        .await
        .unwrap_or_else(|_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("nothing answered {hostname}{path} in {ANSWER_DEADLINE:?}"),
            ))
        })
    }

    pub fn proxy_address(&self) -> SocketAddr {
        self.proxy
    }

    /// Puts an app to sleep down the path the idle pass uses, by telling this host what a reading
    /// of the counters would have told it. A document cannot name an idle timeout under a minute,
    /// and a test that waited one out would be a test about `tokio::time`.
    pub async fn let_sleep(&self, tenant: &Tenant) {
        for _ in 0..50 {
            super::measured_quiet_since(
                &self.host.state,
                &tenant.app_id,
                crate::clock::now_ms() - i64::try_from(MAX_IDLE_TIMEOUT_MS).unwrap_or(i64::MAX),
            )
            .await;
            crate::domain::reconcile::idle::apply_sleep(&self.host).await;
            if self.instance(&tenant.app_id).await.map(|held| held.state) == Some(InstanceState::Idle) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{} never went to sleep", tenant.app_id);
    }

    /// Every microVM down, every tap back, and the document this host was holding written out.
    /// A test calls this at its end; one that panicked first has it done on the way out, because
    /// a guest left running holds the slot the next test is about to be handed.
    pub async fn stop(mut self) {
        self.take_down().await;
    }

    async fn take_down(&mut self) {
        if std::mem::replace(&mut self.stopped, true) {
            return;
        }
        for task in &self.controllers {
            task.abort();
        }
        let app_ids: Vec<AppId> = self
            .host
            .state
            .records()
            .await
            .into_iter()
            .map(|record| record.app_id)
            .collect();
        for app_id in app_ids {
            let _ = self.host.vms.stop(&app_id).await;
        }
        for tap_name in self.host.vms.tap_names().await {
            let _ = self.host.vms.delete_tap(&tap_name).await;
        }
        self.lifecycle.stop().await;
    }
}

#[derive(Debug, Clone)]
pub struct Answer {
    pub status: u16,
    pub body: String,
}

impl Answer {
    fn parse(bytes: &[u8]) -> std::io::Result<Self> {
        let text = String::from_utf8_lossy(bytes);
        let (head, body) = text
            .split_once("\r\n\r\n")
            .ok_or_else(|| std::io::Error::other("no answer"))?;
        let status = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .ok_or_else(|| std::io::Error::other("no status"))?;
        Ok(Self {
            status,
            body: body.to_string(),
        })
    }
}
