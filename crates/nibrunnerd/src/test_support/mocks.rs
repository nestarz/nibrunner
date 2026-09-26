use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use protocol::{AppId, ObjectKey};

use crate::adapters::net::tap::{MockHostNetwork, Neighbour, NetworkError, TapInterface};
use crate::adapters::vm::VmStatus;
use crate::adapters::volumes::{
    AttachedVolume, CacheReservation, MockVolumeBackend, ObservedBacking, VolumeBackend, VolumeError,
};
use crate::domain::exports::store::{ExportStoreError, MockExportStore};
use crate::ports::{
    ArtifactError, CommandError, CommandRequest, CommandResult, MockArtifactStore, MockCommandRunner,
    MockLogSink, MockVmm, TenantLogEvent, VmCall, VmError, Vmm,
};

fn shared<T>(value: T) -> Arc<Mutex<T>> {
    Arc::new(Mutex::new(value))
}

fn held<T: Clone>(cell: &Arc<Mutex<T>>) -> T {
    cell.lock().expect("no panic holds this lock").clone()
}

fn push<T>(cell: &Arc<Mutex<Vec<T>>>, value: T) {
    cell.lock().expect("no panic holds this lock").push(value);
}

#[derive(Clone, Default)]
pub struct CommandLog {
    calls: Arc<Mutex<Vec<CommandRequest>>>,
}

impl CommandLog {
    pub fn calls(&self) -> Vec<CommandRequest> {
        held(&self.calls)
    }

    pub fn executables(&self) -> Vec<String> {
        self.calls()
            .iter()
            .map(|request| request.executable().to_string())
            .collect()
    }

    pub fn commands(&self) -> Vec<Vec<String>> {
        self.calls().into_iter().map(|request| request.command).collect()
    }
}

pub fn commands_succeeding() -> (Arc<MockCommandRunner>, CommandLog) {
    commands_answering(|_| Ok(CommandResult::succeeded()))
}

/// Succeeds at everything, and leaves behind the one thing the real `mke2fs` does that anything
/// here reads back: an ext superblock on the device it was pointed at.
pub fn commands_formatting() -> (Arc<MockCommandRunner>, CommandLog) {
    commands_answering(|request| {
        if request.executable() == "mke2fs" {
            lay_superblock(request.command.last().expect("a device to format"));
        }
        Ok(CommandResult::succeeded())
    })
}

/// The ext magic where a superblock keeps it, which is all "formatted" is read from.
pub fn lay_superblock(device_path: &str) {
    use std::io::{Seek, SeekFrom, Write};
    let mut device = std::fs::OpenOptions::new()
        .write(true)
        .open(device_path)
        .expect("the device to format");
    device
        .seek(SeekFrom::Start(crate::adapters::volumes::SUPERBLOCK_MAGIC_OFFSET))
        .expect("the superblock's offset");
    device
        .write_all(&0xef53u16.to_le_bytes())
        .expect("the magic to be written");
}

pub fn commands_answering(
    answer: impl Fn(&CommandRequest) -> Result<CommandResult, CommandError> + Send + Sync + 'static,
) -> (Arc<MockCommandRunner>, CommandLog) {
    let log = CommandLog::default();
    let calls = log.calls.clone();
    let mut runner = MockCommandRunner::new();
    runner.expect_run().returning(move |request| {
        push(&calls, request.clone());
        answer(&request)
    });
    (Arc::new(runner), log)
}

#[derive(Clone)]
pub struct VmmSpy {
    calls: Arc<Mutex<Vec<VmCall>>>,
    removed_taps: Arc<Mutex<Vec<String>>>,
    present_taps: Arc<Mutex<Vec<String>>>,
    status: Arc<Mutex<VmStatus>>,
    on_boot: Arc<Mutex<Option<VmError>>>,
    on_sleep: Arc<Mutex<Option<VmError>>>,
    on_wake: Arc<Mutex<Option<VmError>>>,
    verdict: Arc<Mutex<Option<String>>>,
    adopted: Arc<Mutex<Vec<AppId>>>,
}

impl Default for VmmSpy {
    fn default() -> Self {
        Self {
            calls: shared(Vec::new()),
            removed_taps: shared(Vec::new()),
            present_taps: shared(Vec::new()),
            status: shared(VmStatus::default()),
            on_boot: shared(None),
            on_sleep: shared(None),
            on_wake: shared(None),
            verdict: shared(None),
            adopted: shared(Vec::new()),
        }
    }
}

impl VmmSpy {
    pub fn boot_error(&self, error: Option<VmError>) {
        *self.on_boot.lock().expect("no panic holds this lock") = error;
    }

    pub fn calls(&self) -> Vec<VmCall> {
        held(&self.calls)
    }

    pub fn set_status(&self, status: VmStatus) {
        *self.status.lock().expect("no panic holds this lock") = status;
    }

    pub fn refuse_sleep(&self, error: VmError) {
        *self.on_sleep.lock().expect("no panic holds this lock") = Some(error);
    }

    pub fn refuse_wake(&self, error: VmError) {
        *self.on_wake.lock().expect("no panic holds this lock") = Some(error);
    }

    pub fn set_verdict(&self, verdict: impl Into<String>) {
        *self.verdict.lock().expect("no panic holds this lock") = Some(verdict.into());
    }

    pub fn removed_taps(&self) -> Vec<String> {
        held(&self.removed_taps)
    }

    pub fn set_present_taps(&self, names: Vec<String>) {
        *self.present_taps.lock().expect("no panic holds this lock") = names;
    }

    pub fn set_adopted(&self, app_ids: Vec<AppId>) {
        *self.adopted.lock().expect("no panic holds this lock") = app_ids;
    }
}

pub const NOWHERE_VM_DIR: &str = "/nowhere/vm";

pub fn vmm() -> (Arc<MockVmm>, VmmSpy) {
    let spy = VmmSpy::default();
    let mut vms = MockVmm::new();

    let (calls, on_boot) = (spy.calls.clone(), spy.on_boot.clone());
    vms.expect_boot().returning(move |_| {
        push(&calls, VmCall::Boot);
        held(&on_boot).map_or(Ok(()), Err)
    });
    let (calls, on_sleep) = (spy.calls.clone(), spy.on_sleep.clone());
    vms.expect_sleep().returning(move |_| {
        push(&calls, VmCall::Sleep);
        held(&on_sleep).map_or(Ok(()), Err)
    });
    let (calls, on_wake) = (spy.calls.clone(), spy.on_wake.clone());
    vms.expect_wake().returning(move |_| {
        push(&calls, VmCall::Wake);
        held(&on_wake).map_or(Ok(()), Err)
    });
    let calls = spy.calls.clone();
    vms.expect_stop().returning(move |_| {
        push(&calls, VmCall::Stop);
        Ok(())
    });
    let calls = spy.calls.clone();
    vms.expect_discard().returning(move |_| {
        push(&calls, VmCall::Discard);
        Ok(())
    });
    let (calls, removed_taps) = (spy.calls.clone(), spy.removed_taps.clone());
    vms.expect_delete_tap().returning(move |name: &str| {
        push(&calls, VmCall::DeleteTap);
        push(&removed_taps, name.to_string());
        Ok(())
    });
    let removed_taps = spy.removed_taps.clone();
    let present = spy.present_taps.clone();
    vms.expect_tap_names().returning(move || {
        let gone = held(&removed_taps);
        held(&present)
            .into_iter()
            .filter(|name: &String| !gone.contains(name))
            .collect()
    });
    let status = spy.status.clone();
    vms.expect_statuses().returning(move |app_ids: &[AppId]| {
        let status = held(&status);
        app_ids.iter().map(|app_id| (app_id.clone(), status)).collect()
    });
    let adopted = spy.adopted.clone();
    vms.expect_adopted_app_ids().returning(move || held(&adopted));
    vms.expect_readopt().returning(|_: &AppId| Ok(()));
    let verdict = spy.verdict.clone();
    vms.expect_guest_verdict().returning(move |_| held(&verdict));
    vms.expect_working_dir()
        .returning(|app_id: &AppId| PathBuf::from(NOWHERE_VM_DIR).join(app_id.as_str()));

    (Arc::new(vms), spy)
}

/// A gate every call of one kind waits at until the test lets it through, counting how many
/// are waiting, so that how many the host runs side by side is something a test can see.
pub struct Gate {
    permits: tokio::sync::Semaphore,
    in_flight: AtomicUsize,
    most_in_flight: AtomicUsize,
}

impl Gate {
    fn closed() -> Self {
        Self {
            permits: tokio::sync::Semaphore::new(0),
            in_flight: AtomicUsize::new(0),
            most_in_flight: AtomicUsize::new(0),
        }
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    pub fn most_in_flight(&self) -> usize {
        self.most_in_flight.load(Ordering::SeqCst)
    }

    pub fn let_through(&self, calls: usize) {
        self.permits.add_permits(calls);
    }

    /// Waits until this many calls are at the gate, or panics after long enough.
    pub async fn held_up(&self, calls: usize) {
        let waited = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while self.in_flight() < calls {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await;
        assert!(waited.is_ok(), "{calls} calls never reached the gate at once");
    }

    /// Runs `call` once the test lets it through, counting it as in flight from now until then.
    async fn through<T>(&self, call: impl std::future::Future<Output = T>) -> T {
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.most_in_flight.fetch_max(in_flight, Ordering::SeqCst);
        self.permits
            .acquire()
            .await
            .expect("the gate is never closed")
            .forget();
        let outcome = call.await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        outcome
    }
}

/// The spy's VMM with every sleep held at a gate until the test lets it through, so that how
/// many the host runs side by side is something a test can see, and a wake that lands
/// mid-snapshot something it can stage.
pub struct HeldSleeps {
    vms: Arc<MockVmm>,
    gate: Gate,
    wakes_mid_sleep: AtomicUsize,
}

impl HeldSleeps {
    pub fn in_flight(&self) -> usize {
        self.gate.in_flight()
    }

    pub fn most_in_flight(&self) -> usize {
        self.gate.most_in_flight()
    }

    /// Wakes that were asked for while a snapshot was still being written.
    pub fn wakes_mid_sleep(&self) -> usize {
        self.wakes_mid_sleep.load(Ordering::SeqCst)
    }

    pub fn let_through(&self, sleeps: usize) {
        self.gate.let_through(sleeps);
    }

    pub async fn held_up(&self, sleeps: usize) {
        self.gate.held_up(sleeps).await;
    }
}

pub fn vmm_holding_sleeps() -> (Arc<HeldSleeps>, VmmSpy) {
    let (vms, spy) = vmm();
    let held = HeldSleeps {
        vms,
        gate: Gate::closed(),
        wakes_mid_sleep: AtomicUsize::new(0),
    };
    (Arc::new(held), spy)
}

#[async_trait::async_trait]
impl Vmm for HeldSleeps {
    async fn boot(&self, request: crate::ports::BootRequest) -> Result<(), VmError> {
        self.vms.boot(request).await
    }

    async fn sleep(&self, request: crate::ports::SuspendRequest) -> Result<(), VmError> {
        self.gate.through(self.vms.sleep(request)).await
    }

    async fn wake(&self, request: crate::ports::SuspendRequest) -> Result<(), VmError> {
        if self.in_flight() > 0 {
            self.wakes_mid_sleep.fetch_add(1, Ordering::SeqCst);
        }
        self.vms.wake(request).await
    }

    async fn stop(&self, app_id: &AppId) -> Result<(), VmError> {
        self.vms.stop(app_id).await
    }

    async fn discard(&self, app_id: &AppId) -> Result<(), VmError> {
        self.vms.discard(app_id).await
    }

    async fn delete_tap(&self, tap_name: &str) -> Result<(), VmError> {
        self.vms.delete_tap(tap_name).await
    }

    async fn tap_names(&self) -> Vec<String> {
        self.vms.tap_names().await
    }

    async fn statuses(&self, app_ids: &[AppId]) -> BTreeMap<AppId, VmStatus> {
        self.vms.statuses(app_ids).await
    }

    async fn adopted_app_ids(&self) -> Vec<AppId> {
        self.vms.adopted_app_ids().await
    }

    async fn readopt(&self, app_id: &AppId) -> Result<(), VmError> {
        self.vms.readopt(app_id).await
    }

    async fn guest_verdict(&self, app_id: &AppId) -> Option<String> {
        self.vms.guest_verdict(app_id).await
    }

    fn working_dir(&self, app_id: &AppId) -> PathBuf {
        self.vms.working_dir(app_id)
    }
}

/// The given backend with every provision held at the gate until the test lets it through.
pub struct HeldProvisions {
    volumes: MockVolumeBackend,
    gate: Arc<Gate>,
}

pub fn volumes_holding_provisions(volumes: MockVolumeBackend) -> (Arc<HeldProvisions>, Arc<Gate>) {
    let gate = Arc::new(Gate::closed());
    let held = HeldProvisions {
        volumes,
        gate: gate.clone(),
    };
    (Arc::new(held), gate)
}

#[async_trait::async_trait]
impl VolumeBackend for HeldProvisions {
    async fn provision(&self, desired: &protocol::DesiredVolume) -> Result<AttachedVolume, VolumeError> {
        self.gate.through(self.volumes.provision(desired)).await
    }

    async fn attach(
        &self,
        volume_id: &protocol::VolumeId,
        app_id: &AppId,
    ) -> Result<AttachedVolume, VolumeError> {
        self.volumes.attach(volume_id, app_id).await
    }

    async fn detach(&self, volume_id: &protocol::VolumeId, app_id: &AppId) -> Result<(), VolumeError> {
        self.volumes.detach(volume_id, app_id).await
    }

    async fn teardown(&self, volume_id: &protocol::VolumeId, app_id: &AppId) -> Result<(), VolumeError> {
        self.volumes.teardown(volume_id, app_id).await
    }

    async fn flush(&self) -> Result<(), VolumeError> {
        self.volumes.flush().await
    }

    async fn create_checkpoint(&self, checkpoint_id: &protocol::CheckpointId) -> Result<(), VolumeError> {
        self.volumes.create_checkpoint(checkpoint_id).await
    }

    async fn delete_checkpoint(&self, checkpoint_id: &protocol::CheckpointId) -> Result<(), VolumeError> {
        self.volumes.delete_checkpoint(checkpoint_id).await
    }

    async fn observe_checkpoints(&self) -> Vec<protocol::CheckpointId> {
        self.volumes.observe_checkpoints().await
    }

    async fn observe(&self, owners: &BTreeMap<protocol::VolumeId, AppId>) -> Vec<ObservedBacking> {
        self.volumes.observe(owners).await
    }

    fn reserved_cache(&self) -> CacheReservation {
        self.volumes.reserved_cache()
    }
}

pub fn artifacts_holding(bytes: impl Into<Vec<u8>>) -> Arc<MockArtifactStore> {
    let bytes = bytes.into();
    artifacts_answering(move |_| Ok(bytes.clone()))
}

pub fn artifacts_answering(
    answer: impl Fn(&ObjectKey) -> Result<Vec<u8>, ArtifactError> + Send + Sync + 'static,
) -> Arc<MockArtifactStore> {
    let mut artifacts = MockArtifactStore::new();
    artifacts.expect_read().returning(move |key| answer(key));
    Arc::new(artifacts)
}

pub fn artifacts_refusing(error: ArtifactError) -> Arc<MockArtifactStore> {
    let mut artifacts = MockArtifactStore::new();
    artifacts.expect_read().returning(move |_| Err(error.clone()));
    Arc::new(artifacts)
}

#[derive(Clone, Default)]
pub struct LogSpy {
    events: Arc<Mutex<Vec<TenantLogEvent>>>,
}

impl LogSpy {
    pub fn events(&self) -> Vec<TenantLogEvent> {
        held(&self.events)
    }
}

pub fn log_sink() -> (Arc<MockLogSink>, LogSpy) {
    let spy = LogSpy::default();
    let events = spy.events.clone();
    let mut sink = MockLogSink::new();
    sink.expect_publish().returning(move |published| {
        events.lock().expect("no panic holds this lock").extend(published);
    });
    (Arc::new(sink), spy)
}

#[derive(Clone, Default)]
pub struct ExportSpy {
    uploads: Arc<Mutex<Vec<(PathBuf, ObjectKey)>>>,
}

impl ExportSpy {
    pub fn uploads(&self) -> Vec<(PathBuf, ObjectKey)> {
        held(&self.uploads)
    }
}

pub fn exports_accepting() -> (Arc<MockExportStore>, ExportSpy) {
    exports_answering(|| Ok(()))
}

pub fn exports_answering(
    answer: impl Fn() -> Result<(), ExportStoreError> + Send + Sync + 'static,
) -> (Arc<MockExportStore>, ExportSpy) {
    let spy = ExportSpy::default();
    let uploads = spy.uploads.clone();
    let mut store = MockExportStore::new();
    store
        .expect_upload()
        .returning(move |bundle_path: &Path, object_key: &ObjectKey| {
            push(&uploads, (bundle_path.to_path_buf(), object_key.clone()));
            answer()
        });
    (Arc::new(store), spy)
}

#[derive(Clone, Default)]
pub struct NetworkSpy {
    taps: Arc<Mutex<Vec<TapInterface>>>,
    neighbours: Arc<Mutex<Vec<Neighbour>>>,
    removed: Arc<Mutex<Vec<String>>>,
}

impl NetworkSpy {
    pub fn taps(&self) -> Vec<TapInterface> {
        held(&self.taps)
    }

    pub fn neighbours(&self) -> Vec<Neighbour> {
        held(&self.neighbours)
    }

    pub fn removed(&self) -> Vec<String> {
        held(&self.removed)
    }
}

pub fn network() -> (Arc<MockHostNetwork>, NetworkSpy) {
    let spy = NetworkSpy::default();
    let mut network = MockHostNetwork::new();

    let taps = spy.taps.clone();
    network.expect_ensure_tap().returning(move |tap: &TapInterface| {
        push(&taps, tap.clone());
        Ok(())
    });
    let neighbours = spy.neighbours.clone();
    network
        .expect_refresh_neighbour()
        .returning(move |neighbour: &Neighbour| {
            push(&neighbours, neighbour.clone());
            Ok(())
        });
    let (taps, removed) = (spy.taps.clone(), spy.removed.clone());
    network.expect_delete_tap().returning(move |name: &str| {
        push(&removed, name.to_string());
        if let Ok(mut held) = taps.lock() {
            held.retain(|tap| tap.tap_name != name);
        }
        Ok(())
    });
    let taps = spy.taps.clone();
    network
        .expect_tap_names()
        .returning(move || held(&taps).into_iter().map(|tap| tap.tap_name).collect());

    (Arc::new(network), spy)
}

pub fn network_refusing(error: NetworkError) -> Arc<MockHostNetwork> {
    let mut network = MockHostNetwork::new();
    let refusal = error.clone();
    network
        .expect_ensure_tap()
        .returning(move |_| Err(refusal.clone()));
    network
        .expect_refresh_neighbour()
        .returning(move |_| Err(error.clone()));
    network.expect_tap_names().returning(Vec::new);
    network.expect_delete_tap().returning(move |_| Ok(()));
    Arc::new(network)
}
