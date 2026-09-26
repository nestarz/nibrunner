pub mod mocks;

use std::ops::Deref;
use std::sync::Arc;

use protocol::*;
use tokio::sync::Mutex;

use crate::domain::backoff::NO_START_ATTEMPTS;
use crate::domain::health::initial_tracker;
use crate::domain::reconcile::plan::{ObservedInstance, ObservedState, ObservedVolume};
use crate::domain::report::instance_record::{InstanceRecord, RecordFields};

pub const VOLUME_SIZE_BYTES: u64 = 4_096;
pub const OBSERVED_AT: &str = "2026-08-03T10:00:00.000Z";
pub const HOST_STORAGE_PREFIX: &str = "filesystems/host-1";
pub const ARTIFACT_BYTES: &[u8] = b"#!/usr/bin/env fake-binary\n";
pub const ARTIFACT_DIGEST: &str = "8eacc8ea7f20363ff4eeb79bc80edf5926effee2e7e13207a198ce341a0326f5";
/// Enough of a squashfs to be taken for one: the magic, then nothing.
pub const BASE_LAYER_BYTES: &[u8] = b"hsqs\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
pub const BASE_LAYER_DIGEST: &str = "82672a6e9a29fa566bd340d9ef2d06e62146d64bd4dac03e92cdd162341c6d56";

/// A zerofs host laid out the way `nibrunnerd install` lays one out, so a test that is about one
/// field says only that field.
pub fn zerofs_settings(
    with: impl FnOnce(&mut crate::config::ZerofsSettings),
) -> crate::config::ZerofsSettings {
    let mut settings = crate::config::ZerofsSettings {
        binary: "/opt/nibrunner/bin/zerofs".into(),
        config_file: "/etc/zerofs/config.toml".into(),
        mount_path: "/mnt/zerofs".into(),
        nbd_socket_path: "/run/zerofs/nbd.sock".into(),
        ninep_socket_path: "/run/zerofs/9p.sock".into(),
        rpc_socket_path: "/run/zerofs/rpc.sock".into(),
        storage_url: "s3://filesystems/host-1".to_string(),
        cache_dir: "/data/zerofs".into(),
        cache_disk_mib: 70 * 1024,
        cache_memory_mib: 2 * 1024,
        checkpoint_runtime_dir: "/run/zerofs-checkpoint".into(),
        checkpoint_config_file: "/etc/zerofs/checkpoint.toml".into(),
        checkpoint_cache_dir: "/data/zerofs-checkpoint".into(),
    };
    with(&mut settings);
    settings
}

pub fn app_id() -> AppId {
    AppId::parse("app-1").unwrap()
}

pub fn volume_id() -> VolumeId {
    VolumeId::parse("vol-1").unwrap()
}

pub fn deployment_id() -> DeploymentId {
    DeploymentId::parse("dep-1").unwrap()
}

pub fn host_id() -> HostId {
    HostId::parse("host-1").unwrap()
}

pub fn checkpoint_id() -> CheckpointId {
    CheckpointId::parse("chk-1").unwrap()
}

pub fn export_id() -> ExportId {
    ExportId::parse("exp-1").unwrap()
}

pub fn observed_at() -> Timestamp {
    Timestamp::parse(OBSERVED_AT).unwrap()
}

pub fn app_hostname() -> AppHostname {
    AppHostname {
        hostname: Hostname::parse("app-1.apps.example.com").unwrap(),
        kind: AppHostnameKind::Platform,
    }
}

pub fn tenant_environment(values: &[(&str, &str)]) -> TenantEnvironment {
    values
        .iter()
        .map(|(name, value)| (name.to_string(), TenantValue::parse(*value).unwrap()))
        .collect()
}

/// The app's own layer: one program the host packs.
pub fn layer(edit: impl FnOnce(&mut StoredObject)) -> DesiredLayer {
    let mut object = StoredObject {
        digest: Sha256Digest::parse(ARTIFACT_DIGEST).unwrap(),
        object_key: ObjectKey::parse("artifacts/9f1c2f0e-0d4e-4a1b-9c3a-1f8b6d2e7a45").unwrap(),
    };
    edit(&mut object);
    DesiredLayer::Executable {
        object,
        destination_path: ExecutablePath::parse("/app/server").unwrap(),
    }
}

/// A layer uploaded whole, attached as it is.
pub fn base_layer() -> DesiredLayer {
    DesiredLayer::Filesystem {
        object: StoredObject {
            digest: Sha256Digest::parse(BASE_LAYER_DIGEST).unwrap(),
            object_key: ObjectKey::parse("layers/debian").unwrap(),
        },
    }
}

/// What every fixture checks with: a port that accepts, which is what the tests' listeners do.
pub const TCP_HEALTH_CHECK: HealthCheck = HealthCheck::Tcp {
    probe: Probe {
        interval_ms: 5_000,
        timeout_ms: 2_000,
        grace_period_ms: 30_000,
        healthy_threshold: 1,
        unhealthy_threshold: 3,
    },
};

pub fn app_config(edit: impl FnOnce(&mut AppConfig)) -> AppConfig {
    let mut value = AppConfig {
        ports: vec![],
        http_port: DEFAULT_HTTP_PORT,
        command: Command {
            program: GuestPath::parse("/app/server").unwrap(),
            args: TenantArguments::default(),
            working_directory: GuestPath::parse("/app").unwrap(),
            environment: TenantEnvironment::default(),
        },
        resources: DEFAULT_INSTANCE_RESOURCES,
        health_check: TCP_HEALTH_CHECK,
        restart_policy: DEFAULT_RESTART_POLICY,
    };
    edit(&mut value);
    value
}

pub fn desired_instance(edit: impl FnOnce(&mut DesiredInstance)) -> DesiredInstance {
    let mut value = DesiredInstance {
        app_id: app_id(),
        deployment_id: deployment_id(),
        volume_id: volume_id(),
        desired_state: DesiredInstanceState::Running,
        idle_timeout_ms: None,
        activation: None,
        layers: vec![layer(|_| {})],
        config: app_config(|_| {}),
        hostnames: vec![],
    };
    edit(&mut value);
    value
}

pub fn desired_volume(edit: impl FnOnce(&mut DesiredVolume)) -> DesiredVolume {
    let mut value = DesiredVolume {
        volume_id: volume_id(),
        app_id: app_id(),
        size_bytes: VOLUME_SIZE_BYTES,
        desired_state: DesiredPresence::Present,
        initial_contents: None,
    };
    edit(&mut value);
    value
}

/// What a volume starts with: this archive, unpacked at `destination_path`.
pub fn initial_contents(archive: &[u8], destination_path: &str) -> InitialContents {
    use sha2::Digest;
    InitialContents {
        object: StoredObject {
            digest: Sha256Digest::parse(hex::encode(sha2::Sha256::digest(archive))).unwrap(),
            object_key: ObjectKey::parse("seeds/app-1").unwrap(),
        },
        destination_path: GuestPath::parse(destination_path).unwrap(),
    }
}

pub fn desired_checkpoint(edit: impl FnOnce(&mut DesiredCheckpoint)) -> DesiredCheckpoint {
    let mut value = DesiredCheckpoint {
        checkpoint_id: checkpoint_id(),
        volume_id: volume_id(),
        desired_state: DesiredPresence::Present,
    };
    edit(&mut value);
    value
}

pub fn desired_export(edit: impl FnOnce(&mut DesiredExport)) -> DesiredExport {
    let mut value = DesiredExport {
        export_id: export_id(),
        app_id: app_id(),
        volume_id: volume_id(),
        object_key: ObjectKey::parse("exports/app-1/exp-1.tar.gz").unwrap(),
        environment: Some(TenantEnvironment::default()),
        desired_state: DesiredPresence::Present,
    };
    edit(&mut value);
    value
}

pub fn reported_instance(edit: impl FnOnce(&mut ReportedInstance)) -> ReportedInstance {
    let mut value = ReportedInstance {
        app_id: app_id(),
        deployment_id: deployment_id(),
        state: InstanceState::Running,
        host_port: None,
        guest_ipv4: None,
        layer_digests: Vec::new(),
        restart_count: 0,
        last_restart: None,
        started_at: None,
        converged_at: None,
        last_exit_code: None,
        message: None,
    };
    edit(&mut value);
    value
}

/// A tenant the kernel killed at its memory ceiling, started again by its guest.
pub fn tenant_restart(edit: impl FnOnce(&mut TenantRestart)) -> TenantRestart {
    let mut value = TenantRestart {
        attempt: 1,
        budget: 5,
        exit: TenantExit::Signal(9),
        reason: StateMessage::new(
            "the tenant exited (137): the kernel killed it for running out of memory at its ceiling of 198 MiB; restart 1 of 5 in 500ms",
        ),
        backoff_ms: 500,
    };
    edit(&mut value);
    value
}

pub fn reported_restart(edit: impl FnOnce(&mut ReportedRestart)) -> ReportedRestart {
    let mut value = ReportedRestart {
        at: observed_at(),
        restart: tenant_restart(|_| {}),
    };
    edit(&mut value);
    value
}

pub fn reported_volume(edit: impl FnOnce(&mut ReportedVolume)) -> ReportedVolume {
    let mut value = ReportedVolume {
        volume_id: volume_id(),
        app_id: app_id(),
        state: VolumeState::Ready,
        size_bytes: VOLUME_SIZE_BYTES,
        storage_prefix: None,
        device_path: None,
        message: None,
    };
    edit(&mut value);
    value
}

pub fn revision() -> protocol::Revision {
    protocol::Revision::parse("deploy-1").expect("a constant revision")
}

pub fn desired_state(edit: impl FnOnce(&mut HostDesiredState)) -> HostDesiredState {
    let mut value = HostDesiredState {
        host_id: host_id(),
        revision: revision(),
        volumes: vec![],
        instances: vec![],
        checkpoints: vec![],
        exports: vec![],
    };
    edit(&mut value);
    value
}

/// The document as whoever deploys writes it: the file the daemon watches.
pub fn write_desired_state(path: &std::path::Path, state: &HostDesiredState) {
    crate::json_store::write_json(path, state).expect("a fixture writes where the test can");
}

/// A document as a host that had read it would hold it, digest and all.
pub fn accepted_document(desired: HostDesiredState) -> crate::desired::AcceptedDocument {
    let rendered = serde_json::to_vec(&desired).expect("a fixture renders");
    crate::desired::AcceptedDocument {
        digest: crate::desired::digest_of(&rendered),
        desired,
    }
}

pub fn observed_instance(edit: impl FnOnce(&mut ObservedInstance)) -> ObservedInstance {
    let mut value = ObservedInstance {
        app_id: app_id(),
        volume_id: Some(volume_id()),
        deployment_id: Some(deployment_id()),
        present: true,
        running: true,
        exited: false,
        refused: false,
    };
    edit(&mut value);
    value
}

pub fn observed_volume(edit: impl FnOnce(&mut ObservedVolume)) -> ObservedVolume {
    let mut value = ObservedVolume {
        volume_id: volume_id(),
        app_id: app_id(),
        attached: true,
        formatted: true,
        size_bytes: VOLUME_SIZE_BYTES,
        storage_prefix: ObjectKey::parse(HOST_STORAGE_PREFIX).unwrap(),
        device_path: Some("/dev/nbd0".to_string()),
    };
    edit(&mut value);
    value
}

pub fn observed_state(edit: impl FnOnce(&mut ObservedState)) -> ObservedState {
    let mut value = ObservedState::default();
    edit(&mut value);
    value
}

pub fn record_fields() -> RecordFields {
    let slot = nft_render::describe_slot(nft_render::FIRST_SLOT, app_id());
    RecordFields {
        ports: vec![],
        app_id: app_id(),
        deployment_id: deployment_id(),
        volume_id: volume_id(),
        hostnames: vec![app_hostname()],
        host_port: slot.host_port,
        http_port: DEFAULT_HTTP_PORT,
        guest_ipv4: slot.guest_ipv4,
        layer_digests: vec![Sha256Digest::parse(ARTIFACT_DIGEST).unwrap()],
        health_check: TCP_HEALTH_CHECK,
        resources: DEFAULT_INSTANCE_RESOURCES,
        restart_policy: DEFAULT_RESTART_POLICY,
        desired_running: true,
        on_request: false,
    }
}

pub fn instance_record(edit: impl FnOnce(&mut InstanceRecord)) -> InstanceRecord {
    let mut value = InstanceRecord::new(record_fields(), InstanceState::Running, initial_tracker());
    value.start_attempts = NO_START_ATTEMPTS;
    edit(&mut value);
    value
}

/// What a reading of the counters that found the app last reached at `moment` leaves behind: the
/// moment itself, and this host having just measured the app to arrive at it.
pub async fn measured_quiet_since(state: &crate::state::SharedState, app_id: &AppId, moment: i64) {
    state
        .modify(|snapshot| {
            snapshot.last_active_at_ms.insert(app_id.clone(), moment);
            snapshot
                .last_measured_at_ms
                .insert(app_id.clone(), crate::clock::now_ms());
        })
        .await;
}

pub static ONE_HOST_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub struct TestHost {
    _directory: tempfile::TempDir,
    pub host: Arc<crate::host::Host>,
    pub vms: mocks::VmmSpy,
    pub commands: mocks::CommandLog,
    pub exports: mocks::ExportSpy,
}

impl TestHost {
    pub fn exports_written(&self) -> Vec<(std::path::PathBuf, protocol::ObjectKey)> {
        self.exports.uploads()
    }
}

impl Deref for TestHost {
    type Target = crate::host::Host;

    fn deref(&self) -> &Self::Target {
        &self.host
    }
}

impl TestHost {
    pub fn arc(&self) -> &Arc<crate::host::Host> {
        &self.host
    }
}

pub async fn test_host() -> TestHost {
    test_host_with(crate::repositories::Repositories::sqlite(
        crate::domain::store::in_memory().await,
    ))
    .await
}

pub async fn test_host_with(repositories: crate::repositories::Repositories) -> TestHost {
    test_host_over(repositories, mocks::artifacts_holding(ARTIFACT_BYTES.to_vec())).await
}

/// A host whose store holds `seed` under the key the seeded volume fixture names, and the app's
/// layer under every other.
pub async fn test_host_seeding(seed: Vec<u8>) -> TestHost {
    let store = mocks::artifacts_answering(move |key| {
        Ok(if key.as_str().starts_with("seeds/") {
            seed.clone()
        } else {
            ARTIFACT_BYTES.to_vec()
        })
    });
    test_host_over(
        crate::repositories::Repositories::sqlite(crate::domain::store::in_memory().await),
        store,
    )
    .await
}

async fn test_host_over(
    repositories: crate::repositories::Repositories,
    artifacts: Arc<crate::ports::MockArtifactStore>,
) -> TestHost {
    use crate::adapters::net::allocator::SlotAllocator;
    use crate::adapters::net::firewall::HostFirewall;
    use crate::adapters::proxy::activator::AppActivator;
    use crate::adapters::proxy::Router;
    use crate::adapters::volumes::local_file::LocalFileVolumes;
    use crate::config::HostConfig;
    use crate::desired::DesiredStateCache;
    use crate::host::Host;
    use crate::ports::{WakeRefusal, Waker};
    use crate::state::HostState;

    struct NeverWoken;

    #[async_trait::async_trait]
    impl Waker for NeverWoken {
        async fn wake(&self, _app_id: &AppId) -> Result<(), WakeRefusal> {
            Ok(())
        }
    }

    let directory = tempfile::tempdir().expect("a temporary directory");
    // Whoever runs the tests is not root, and a seed can be given to nobody but themself.
    let owner = {
        use std::os::unix::fs::MetadataExt;
        let me = std::fs::metadata(directory.path()).expect("the directory just made");
        (me.uid(), me.gid())
    };
    let mut config = HostConfig::under(directory.path());
    // A host that serves apps under hostnames runs a proxy and binds the ports beside it, and an
    // app is refused on a host that does neither.
    config.proxy = crate::config::ProxyConfig {
        http: Some(crate::config::HttpListener {
            listen_address: std::net::Ipv4Addr::LOCALHOST.into(),
            port: 8080,
            tls: None,
        }),
        raw: Some(crate::config::RawPorts {
            listen_address: std::net::Ipv4Addr::LOCALHOST.into(),
            max_ports_per_guest: 1,
        }),
    };
    let state = HostState::shared();
    let metrics = Arc::new(crate::domain::metrics::HostMetrics::new());
    let (commands, command_log) = mocks::commands_formatting();
    let (vms, vm_spy) = mocks::vmm();
    let (exports, export_spy) = mocks::exports_accepting();
    let artifacts: Arc<dyn crate::ports::ArtifactStore> = artifacts;
    let host = Arc::new(Host {
        guest_memory_mib: u64::from(DEFAULT_INSTANCE_RESOURCES.memory_mib) * 4,
        guest_image_version: "6.1.180-test".to_string(),
        state: state.clone(),
        allocator: Arc::new(Mutex::new(SlotAllocator::addressing(config.max_apps))),
        cache: Mutex::new(DesiredStateCache::new()),
        vms,
        logs: Arc::new(crate::adapters::logs::FileLogSink::new(
            config.logs_dir(),
            config.logs.keep_bytes_per_app,
        )),
        volumes: Arc::new(LocalFileVolumes::new(
            config.volumes_dir(),
            ObjectKey::parse(&config.storage_prefix).expect("a storage prefix"),
            commands.clone(),
            crate::adapters::volumes::initial_contents::ContentsStaging::new(
                artifacts.clone(),
                config.initial_contents_dir(),
            )
            .given_to(owner.0, owner.1),
        )),
        artifacts: artifacts.clone(),
        payloads: crate::adapters::vm::layers::LayerImages::new(artifacts, config.artifact_cache_dir()),
        repositories,
        exports,
        checkpoint_servers: None,
        nbd: crate::adapters::volumes::nbd::NbdDevices::new(commands.clone()),
        commands: commands.clone(),
        firewall: Arc::new(HostFirewall::new(commands.clone())),
        router: Router::new(metrics.clone(), None),
        tls: None,
        waker: Arc::new(NeverWoken),
        activator: AppActivator::new(state.clone(), Arc::new(NeverWoken), metrics.clone()),
        stream_activator: Some(crate::adapters::proxy::StreamActivator::new(
            state.clone(),
            Arc::new(NeverWoken),
            metrics.clone(),
            std::net::Ipv4Addr::LOCALHOST.into(),
        )),
        datagram_activator: Some(crate::adapters::proxy::DatagramActivator::new(
            state,
            Arc::new(NeverWoken),
            metrics.clone(),
            std::net::Ipv4Addr::LOCALHOST.into(),
        )),
        metrics,
        config,
    });
    TestHost {
        _directory: directory,
        host,
        vms: vm_spy,
        commands: command_log,
        exports: export_spy,
    }
}

/// Every line logged on the thread that made it, as its level and message, so a test can say a
/// pass said nothing, or said a thing once.
#[derive(Clone, Default)]
pub struct Said(Arc<std::sync::Mutex<Vec<String>>>);

impl Said {
    /// Hears everything logged on this thread until the guard is dropped.
    pub fn listening() -> (Self, tracing::subscriber::DefaultGuard) {
        use tracing_subscriber::layer::SubscriberExt;
        let said = Self::default();
        let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(said.clone()));
        // Interest in a callsite is cached for the whole process by whichever thread reaches it
        // first, and a thread with no subscriber of its own caches it as never. This subscriber
        // is this thread's alone, so the cache is told to ask again.
        tracing::callsite::rebuild_interest_cache();
        (said, guard)
    }

    pub fn lines(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }

    pub fn forget(&self) {
        self.0.lock().unwrap().clear();
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Said {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Message(String);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        self.0
            .lock()
            .unwrap()
            .push(format!("{} {}", event.metadata().level(), message.0));
    }
}
