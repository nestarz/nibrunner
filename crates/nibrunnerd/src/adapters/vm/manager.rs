use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use protocol::AppId;

use crate::adapters::cron_registration::cron_registration_socket_path;
use crate::adapters::logs::receiver::{tenant_log_socket_path, TenantLogReceiver};
use crate::adapters::net::tap::{HostNetwork, Neighbour, TapInterface};
use crate::adapters::vm::firecracker_api::FirecrackerApi;
use crate::adapters::vm::process::VmProcesses;
use crate::adapters::vm::snapshot::{
    ensure_loadable, measure_snapshot_disk, refusal_to_sleep, snapshot_bytes_for, snapshot_paths, Reserved,
    SleepSubject, SnapshotHost, SnapshotStamp, SnapshotsInFlight, StagedSnapshot,
};
use crate::adapters::vm::status::VmStatus;
use crate::adapters::vm::time_sync::{self, WakeStalled};
use crate::adapters::volumes::VolumeBackend;
use crate::domain::metrics::sleep_wake::WakePhase;
use crate::json_store::{make_directory, write_json};
use crate::ports::{
    BootRequest, CommandRunner, LogSink, SuspendRequest, VmError, Vmm, WakeOutcome, Writable,
};
use crate::state::SharedState;
use guest_contract::firecracker::{render_firecracker_config, VmNetwork, VmPaths, VmVsock};
use guest_contract::instance_env::{render_instance_env, InstanceEnvContent};
use guest_contract::paths::WritableRoot;

pub const FIRECRACKER_CONFIG_FILENAME: &str = "firecracker.json";
/// What the kernel is asked to reclaim from a VMM just paused: its idle file cache, and with a
/// swap allowance the guest pages it can spare.
const FROZEN_RECLAIM_BYTES: u64 = 64 * 1024 * 1024;
pub const GUEST_KERNEL_FILENAME: &str = "vmlinux";
pub const GUEST_ROOTFS_FILENAME: &str = "rootfs.ext4";
pub const GUEST_MANIFEST_FILENAME: &str = "manifest.json";

const VM_DIR_MODE: u32 = 0o700;
const FIRST_GUEST_CID: u32 = 3;
// Firecracker answers PATCH /vm in about a millisecond; a fifth of a second is a VMM that will not.
const RESUME_BUDGET: std::time::Duration = std::time::Duration::from_millis(200);

pub struct VmManager {
    pub start_permits: tokio::sync::Semaphore,
    pub vm_dir: PathBuf,
    pub snapshot_dir: PathBuf,
    pub guest_image_dir: PathBuf,
    pub firecracker: PathBuf,
    pub(crate) jailer: super::jailer::Jailer,
    pub guest_image_version: String,
    pub processes: VmProcesses,
    pub network: Arc<dyn HostNetwork>,
    pub volumes: Arc<dyn VolumeBackend>,
    pub commands: Arc<dyn CommandRunner>,
    pub logs: Arc<TenantLogReceiver>,
    pub cron_registration: Arc<crate::adapters::cron_registration::CronRegistrationReceiver>,
    pub sink: Arc<dyn LogSink>,
    pub state: SharedState,
    pub metrics: Arc<crate::domain::metrics::HostMetrics>,
    pub in_flight: SnapshotsInFlight,
}

struct PreparedVm {
    #[cfg(test)]
    config_file: PathBuf,
    jail: super::jailer::Jail,
}

impl VmManager {
    fn admit_start(&self) -> Result<tokio::sync::SemaphorePermit<'_>, VmError> {
        self.start_permits.try_acquire().map_err(|_| VmError::StartBusy)
    }

    fn key_path(&self, app_id: &AppId) -> PathBuf {
        self.vm_dir.join("keys").join(app_id.as_str())
    }

    /// The id the microVM answering to `app_id` was booted under: its own, unless a cut-over
    /// handed the app a microVM booted beside it. That microVM keeps its jail, cgroup, sockets and
    /// snapshot exchange, all named after the id it was booted under, and the app is mapped onto
    /// it here. Two apps only ever trade ids with each other, so mapping an id twice gives it back:
    /// the app a microVM's id answers for is `key` of that id.
    pub fn key(&self, app_id: &AppId) -> AppId {
        std::fs::read_to_string(self.key_path(app_id))
            .ok()
            .and_then(|key| AppId::parse(key.trim()).ok())
            .unwrap_or_else(|| app_id.clone())
    }

    fn write_key(&self, app_id: &AppId, key: &AppId) -> std::io::Result<()> {
        let path = self.key_path(app_id);
        if key == app_id {
            return match std::fs::remove_file(&path) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
                _ => Ok(()),
            };
        }
        make_directory(path.parent().expect("a key has a directory"), VM_DIR_MODE)?;
        crate::json_store::write_text(&path, key.as_str(), 0o600)
            .map_err(|error| std::io::Error::other(error.message()))
    }

    pub fn working_dir_for(&self, app_id: &AppId) -> PathBuf {
        if let Some(record) = self.processes.read_record(app_id) {
            if let Some(root) = record.jail_root {
                return root;
            }
            return self.vm_dir.join(app_id.as_str());
        }
        super::jailer::root(&self.vm_dir.join("jailer"), app_id)
    }

    fn api(&self, app_id: &AppId) -> FirecrackerApi {
        let api = FirecrackerApi::at(self.processes.api_socket(app_id));
        match self.processes.read_record(app_id) {
            Some(record) if record.jail_root.is_some() => api.require_process(record.pid, record.jail_uid),
            _ => api,
        }
    }

    fn current_stamp(&self, request: &SuspendRequest) -> SnapshotStamp {
        SnapshotStamp {
            deployment_id: request.deployment_id.clone(),
            guest_image_version: self.guest_image_version.clone(),
            host_boot_id: self.processes.boot_id().to_string(),
            slot: request.slot.slot,
            firecracker_version: Some(super::process::FIRECRACKER_VERSION.to_string()),
            host: SnapshotHost::this_one().clone(),
        }
    }

    fn discard_snapshot(&self, app_id: &AppId) {
        let paths = snapshot_paths(&self.snapshot_dir, app_id);
        let _ = std::fs::remove_file(&paths.stamp_path);
        let _ = std::fs::remove_dir_all(&paths.directory);
    }

    // Beside the staged inputs rather than in the jail, which is rebuilt from them on every boot:
    // the jailer binds it in as the writable data drive, as it does a volume's image.
    fn scratch_disk_path(&self, app_id: &AppId) -> PathBuf {
        self.vm_dir
            .join(app_id.as_str())
            .join(super::scratch::SCRATCH_DISK_FILENAME)
    }

    async fn fresh_scratch_disk(&self, app_id: &AppId, mib: std::num::NonZeroU32) -> Result<String, VmError> {
        let path = self.scratch_disk_path(app_id);
        super::scratch::make_disk(self.commands.as_ref(), &path, mib)
            .await
            .map_err(|error| VmError::Host(error.to_string()))?;
        Ok(path.display().to_string())
    }

    async fn stage_inputs(&self, request: &BootRequest) -> Result<PathBuf, VmError> {
        let slot = &request.slot;
        let host = |error: crate::adapters::net::tap::NetworkError| VmError::Host(error.message());
        self.network
            .ensure_tap(&TapInterface {
                tap_name: slot.tap_name.clone(),
                host_ipv4: slot.host_ipv4.clone(),
                subnet_prefix_length: slot.subnet_prefix_length,
            })
            .await
            .map_err(host)?;
        self.network
            .refresh_neighbour(&Neighbour {
                guest_ipv4: slot.guest_ipv4.clone(),
                guest_mac: slot.guest_mac.clone(),
                tap_name: slot.tap_name.clone(),
            })
            .await
            .map_err(host)?;

        let working_dir = self.vm_dir.join(request.desired.app_id.as_str());
        make_directory(&working_dir, VM_DIR_MODE).map_err(|error| VmError::Host(error.to_string()))?;

        let (data_device_path, writable) = match &request.writable {
            Writable::Volume { device_path } => (Some(device_path.clone()), WritableRoot::VolumeDrive),
            Writable::Scratch(protocol::Scratch::Disk { mib }) => (
                Some(self.fresh_scratch_disk(&request.desired.app_id, *mib).await?),
                WritableRoot::VolumeDrive,
            ),
            Writable::Scratch(protocol::Scratch::Memory { mib }) => {
                (None, WritableRoot::Memory { mib: mib.get() })
            }
        };
        let rendered = render_instance_env(&InstanceEnvContent {
            http_port: request.desired.config.http_port,
            layers: request.payload.layer_image_paths.len(),
            writable,
            hostnames: &request.desired.hostnames,
            program: &request.desired.config.command.program,
            working_directory: &request.desired.config.command.working_directory,
            args: &request.desired.config.command.args,
            environment: &request.desired.config.command.environment,
            restart_policy: &request.desired.config.restart_policy,
        })
        .map_err(|error| VmError::Host(error.to_string()))?;
        let config_image = crate::adapters::vm::layers::build_instance_config_image(&working_dir, &rendered)
            .map_err(|error| VmError::Host(error.message()))?;

        let config = render_firecracker_config(
            request.desired.config.resources,
            &VmPaths {
                kernel_path: self
                    .guest_image_dir
                    .join(GUEST_KERNEL_FILENAME)
                    .display()
                    .to_string(),
                rootfs_path: self
                    .guest_image_dir
                    .join(GUEST_ROOTFS_FILENAME)
                    .display()
                    .to_string(),
                instance_config_image_path: config_image.display().to_string(),
                data_block_bytes: data_device_path
                    .as_deref()
                    .and_then(crate::adapters::volumes::filesystem_block_bytes),
                data_device_path,
                layer_image_paths: request
                    .payload
                    .layer_image_paths
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect(),
            },
            &VmNetwork {
                tap_name: slot.tap_name.clone(),
                guest_mac: slot.guest_mac.clone(),
                guest_ipv4: slot.guest_ipv4.clone(),
                host_ipv4: slot.host_ipv4.clone(),
                subnet_prefix_length: slot.subnet_prefix_length,
            },
            &VmVsock {
                guest_cid: FIRST_GUEST_CID + slot.slot,
                path: guest_contract::vsock::GUEST_VSOCK_FILENAME.to_string(),
            },
        );
        let config_file = working_dir.join(FIRECRACKER_CONFIG_FILENAME);
        write_json(&config_file, &config).map_err(|error| VmError::Host(error.message()))?;
        super::jailer_inputs::stage(&working_dir, slot.slot, config)
            .map_err(|error| VmError::Host(error.message()))?;

        Ok(config_file)
    }

    /// `app_id` is the id the microVM runs under; `source` the app its guest speaks for.
    async fn attach_channels(
        &self,
        app_id: &AppId,
        source: &AppId,
        deployment_id: &protocol::DeploymentId,
        working_dir: &Path,
    ) -> Result<(), VmError> {
        self.logs
            .attach_as(
                app_id.clone(),
                source.clone(),
                deployment_id.clone(),
                tenant_log_socket_path(working_dir),
                self.sink.clone(),
            )
            .await
            .map_err(|error| VmError::Host(error.to_string()))?;
        if let Err(error) = self
            .cron_registration
            .attach_as(
                app_id.clone(),
                source.clone(),
                deployment_id.clone(),
                cron_registration_socket_path(working_dir),
            )
            .await
        {
            self.logs.detach(app_id).await;
            return Err(VmError::Host(error.to_string()));
        }
        Ok(())
    }

    /// `request` names the microVM by the id it is booted under.
    async fn stage(&self, request: &BootRequest) -> Result<PreparedVm, VmError> {
        // What a new deployment booted beside the one serving writes is already that app's.
        let source = request
            .staged_for
            .clone()
            .unwrap_or_else(|| self.key(&request.desired.app_id));
        let config_file = self.stage_inputs(request).await?;
        #[cfg(not(test))]
        let _ = config_file;
        let jail = self.prepare_jail(&request.desired.app_id).await?;
        self.attach_channels(
            &request.desired.app_id,
            &source,
            &request.desired.deployment_id,
            &jail.root,
        )
        .await?;
        let granted = async {
            jail.grant_socket(&tenant_log_socket_path(&jail.root))
                .map_err(|error| VmError::Host(error.to_string()))?;
            jail.grant_socket(&cron_registration_socket_path(&jail.root))
                .map_err(|error| VmError::Host(error.to_string()))?;
            self.network
                .set_tap_owner(&request.slot.tap_name, jail.uid)
                .await
                .map_err(|error| VmError::Host(error.message()))
        }
        .await;
        if let Err(error) = granted {
            self.cron_registration.detach(&request.desired.app_id).await;
            self.logs.detach(&request.desired.app_id).await;
            return Err(error);
        }
        Ok(PreparedVm {
            #[cfg(test)]
            config_file,
            jail,
        })
    }

    async fn prepare_jail(&self, app_id: &AppId) -> Result<super::jailer::Jail, VmError> {
        if self.processes.status(app_id).active {
            return Err(VmError::Host(
                "a running microVM cannot have its jail rebuilt".into(),
            ));
        }
        self.logs.detach(app_id).await;
        self.cron_registration.detach(app_id).await;
        self.jailer
            .prepare(
                &self.vm_dir.join(app_id.as_str()),
                &self.vm_dir.join("jailer"),
                &self.snapshot_dir,
                app_id,
                self.processes.boot_id(),
            )
            .map_err(|error| VmError::Host(error.to_string()))
    }

    async fn create_snapshot(&self, app_id: &AppId, state: &Path, memory: &Path) -> Result<(), VmError> {
        use crate::adapters::vm::snapshot::{exchange, publish_snapshot_file, EXCHANGE_BOOT_ID_FILENAME};
        let exchange = exchange(&self.snapshot_dir, app_id);
        let parent = exchange
            .parent()
            .expect("an exchange belongs to its snapshot directory");
        make_directory(parent, VM_DIR_MODE).map_err(|error| VmError::Host(error.to_string()))?;
        crate::json_store::write_text(
            &parent.join(EXCHANGE_BOOT_ID_FILENAME),
            self.processes.boot_id(),
            0o600,
        )
        .map_err(|error| VmError::Host(error.message()))?;
        make_directory(&exchange, VM_DIR_MODE).map_err(|error| VmError::Host(error.to_string()))?;
        for name in ["state", "memory"] {
            let _ = std::fs::remove_file(exchange.join(name));
        }
        let outcome = async {
            let jailed = self
                .processes
                .read_record(app_id)
                .is_some_and(|record| record.jail_root.is_some());
            let state_source = if jailed {
                PathBuf::from("/snapshots/state")
            } else {
                exchange.join("state")
            };
            let memory_source = if jailed {
                PathBuf::from("/snapshots/memory")
            } else {
                exchange.join("memory")
            };
            self.api(app_id)
                .create_snapshot(&state_source, &memory_source)
                .await?;
            for (name, destination) in [("state", state), ("memory", memory)] {
                publish_snapshot_file(&exchange.join(name), destination)
                    .map_err(|error| VmError::Host(error.to_string()))?;
            }
            Ok(())
        }
        .await;
        for name in ["state", "memory"] {
            let _ = std::fs::remove_file(exchange.join(name));
        }
        if outcome.is_err() {
            self.discard_snapshot(app_id);
        }
        outcome
    }

    /// Resumes a paused microVM and hands its tenant back its clock, timing each stretch under
    /// `outcome`. A stretch that does not complete inside its budget is counted, and the guest's
    /// console is kept, before the caller lets go of the microVM.
    async fn resume_paused_guest(&self, app_id: &AppId, outcome: WakeOutcome) -> Result<(), WakeStalled> {
        let metrics = &self.metrics.sleep_wake;
        let resuming = std::time::Instant::now();
        let resumed = match tokio::time::timeout(RESUME_BUDGET, self.api(app_id).resume()).await {
            Err(_) => Err(WakeStalled::new(
                WakePhase::Resume,
                format!(
                    "PATCH /vm Resumed had no answer within {} ms",
                    RESUME_BUDGET.as_millis()
                ),
            )),
            Ok(answered) => answered.map_err(|error| WakeStalled::new(WakePhase::Resume, error.message())),
        };
        let synced = match resumed {
            Ok(()) => {
                metrics.phase(WakePhase::Resume, outcome, resuming.elapsed());
                let control = self
                    .working_dir_for(app_id)
                    .join(guest_contract::vsock::GUEST_VSOCK_FILENAME);
                time_sync::wake(&control).await
            }
            Err(stalled) => Err(stalled),
        };
        match synced {
            Ok(synced) => {
                metrics.phase(WakePhase::Dial, outcome, synced.dial);
                metrics.phase(WakePhase::Sync, outcome, synced.sync);
                Ok(())
            }
            Err(stalled) => {
                metrics.wake_failed(app_id, stalled.phase);
                let kept = self.processes.keep_console_of_failed_wake(app_id);
                tracing::warn!(
                    %app_id,
                    phase = stalled.phase.as_str(),
                    reason = %stalled.reason,
                    console = %kept.as_deref().map_or_else(|| "none".into(), |path| path.display().to_string()),
                    "a wake did not complete"
                );
                Err(stalled)
            }
        }
    }

    async fn resume_restored_tenant(&self, request: &SuspendRequest) -> Result<(), VmError> {
        self.resume_paused_guest(&request.app_id, WakeOutcome::Restored)
            .await
            .map_err(|stalled| VmError::WakeStalled {
                reason: stalled.to_string(),
            })?;
        self.network
            .refresh_neighbour(&Neighbour {
                guest_ipv4: request.slot.guest_ipv4.clone(),
                guest_mac: request.slot.guest_mac.clone(),
                tap_name: request.slot.tap_name.clone(),
            })
            .await
            .map_err(|error| VmError::Host(error.message()))
    }

    async fn resume_frozen_tenant(&self, app_id: &AppId, control: &Path) {
        let recovered = async {
            self.api(app_id).resume().await?;
            time_sync::wake(control)
                .await
                .map(drop)
                .map_err(|stalled| VmError::Host(stalled.to_string()))
        }
        .await;
        if let Err(error) = recovered {
            tracing::error!(%app_id, error = %error.message(), "the tenant could not resume after a refused sleep");
            self.processes.stop(app_id).await;
        }
    }

    async fn publish_sleep_snapshot(
        &self,
        staged: StagedSnapshot,
        paths: super::snapshot::SnapshotPaths,
        stamp: SnapshotStamp,
    ) -> Result<u64, VmError> {
        tokio::task::spawn_blocking(move || staged.publish(&paths, stamp))
            .await
            .map_err(|error| VmError::Host(error.to_string()))
            .and_then(std::convert::identity)
    }

    fn reserve_snapshot_memory(
        &self,
        app_id: &AppId,
    ) -> Result<Option<super::limits::SnapshotMemoryLimit>, VmError> {
        let Some(root) = self
            .processes
            .read_record(app_id)
            .and_then(|record| record.jail_root)
        else {
            return Ok(None);
        };
        let config: guest_contract::firecracker::FirecrackerConfig =
            crate::json_store::read_json(&root.join(FIRECRACKER_CONFIG_FILENAME))
                .map_err(|error| VmError::Host(error.message()))?
                .ok_or_else(|| VmError::Host("the paused VM has no staged machine configuration".into()))?;
        super::limits::VmLimits::for_machine(&config.machine_config)
            .and_then(|limits| {
                limits.reserve_snapshot_memory(
                    &self
                        .processes
                        .cgroup(app_id)
                        .unwrap_or_else(|| super::limits::cgroup_path(app_id)),
                )
            })
            .map_err(|error| VmError::Host(format!("snapshot memory could not be reserved: {error}")))
    }

    fn lift_memory_high(&self, app_id: &AppId) -> Result<Option<super::limits::MemoryHighLift>, VmError> {
        if !self
            .processes
            .read_record(app_id)
            .is_some_and(|record| record.jail_root.is_some())
        {
            return Ok(None);
        }
        let cgroup = self
            .processes
            .cgroup(app_id)
            .unwrap_or_else(|| super::limits::cgroup_path(app_id));
        super::limits::lift_memory_high(&cgroup).map_err(|error| {
            VmError::Host(format!(
                "memory.high could not be lifted for the snapshot: {error}"
            ))
        })
    }

    async fn snapshot_paused_vm(
        &self,
        request: &SuspendRequest,
        paths: super::snapshot::SnapshotPaths,
        stamp: SnapshotStamp,
    ) -> Result<u64, VmError> {
        let mut memory = self.reserve_snapshot_memory(&request.app_id)?;
        let _unthrottled = self.lift_memory_high(&request.app_id)?;
        let outcome = async {
            let staged = StagedSnapshot::create(&self.snapshot_dir)?;
            self.create_snapshot(
                &request.app_id,
                &staged.paths.state_path,
                &staged.paths.memory_path,
            )
            .await?;
            // Publication keeps the paused VMM alive so a failed seal can recover its memory.
            self.publish_sleep_snapshot(staged, paths, stamp).await
        }
        .await;
        if outcome.is_ok() {
            self.processes.stop(&request.app_id).await;
        } else {
            self.discard_snapshot(&request.app_id);
        }
        if let Some(memory) = memory.as_mut() {
            if let Err(error) = memory.restore() {
                self.processes.stop(&request.app_id).await;
                return Err(VmError::Host(format!(
                    "the VM's memory ceiling could not be restored: {error}"
                )));
            }
        }
        drop(memory);
        #[cfg(target_os = "linux")]
        if outcome.is_ok()
            && self
                .processes
                .read_record(&request.app_id)
                .is_some_and(|record| record.jail_root.is_some())
        {
            // Retained snapshot pages belong to host storage, not the next VMM's running ceiling.
            super::limits::release_stopped_cgroup(&request.app_id)
                .await
                .map_err(|error| {
                    VmError::Host(format!("the sleeping VM's cgroup could not be released: {error}"))
                })?;
        }
        outcome
    }

    /// Whether this microVM may be snapshotted now, and its share of the disk while it is.
    async fn admit_snapshot(&self, app_id: &AppId) -> Result<Reserved<'_>, String> {
        // `app_id` is the microVM's; the record is its app's.
        let record = self.state.record(&self.key(app_id)).await;
        let subject = record.as_ref().map(|record| SleepSubject {
            stop_requested: record.stop_requested,
            desired_running: record.desired_running,
            ever_healthy: record.health.ever_healthy,
        });
        if let Some(refusal) = refusal_to_sleep(subject) {
            return Err(refusal.to_string());
        }
        let wanted_bytes = record.map_or(0, |record| snapshot_bytes_for(record.resources.memory_mib));
        let disk = measure_snapshot_disk(&self.snapshot_dir, self.volumes.reserved_cache().disk_bytes)
            .map_err(|error| {
                tracing::warn!(%app_id, %error, "snapshot disk could not be measured");
                "the disk it would be written to cannot be measured".to_string()
            })?;
        tracing::info!(
            %app_id,
            total_bytes = disk.total_bytes,
            available_bytes = disk.available_bytes,
            snapshot_bytes = disk.snapshot_bytes,
            in_flight_bytes = self.in_flight.bytes(),
            "snapshot disk measured"
        );
        self.in_flight.admit(&disk, wanted_bytes)
    }
}

#[async_trait]
impl Vmm for VmManager {
    async fn boot(&self, mut request: BootRequest) -> Result<(), VmError> {
        let _starting = self.admit_start()?;
        request.desired.app_id = self.key(&request.desired.app_id);
        let app_id = request.desired.app_id.clone();
        self.discard_snapshot(&app_id);
        let staged = std::time::Instant::now();
        let mut prepared = self.stage(&request).await?;
        let staged_ms = staged.elapsed().as_millis();
        prepared
            .jail
            .limits
            .constrain(super::limits::RuntimeControls::for_instance(
                request.desired.config.resources,
                request.desired.limits.as_ref(),
            ));

        let starting = std::time::Instant::now();
        let started = self
            .processes
            .spawn(&app_id, &self.firecracker, &self.jailer, &prepared.jail, true)
            .await
            .and_then(|record| prepared.jail.apply_oom_score(record.pid));
        if let Err(error) = started {
            self.processes.stop(&app_id).await;
            self.cron_registration.detach(&app_id).await;
            let _ = self.logs.detach(&app_id).await;
            return Err(VmError::Host(error.to_string()));
        }
        let control = prepared
            .jail
            .root
            .join(guest_contract::vsock::GUEST_VSOCK_FILENAME);
        if let Err(error) =
            super::startup::initialized(&control, || self.processes.status(&app_id).active).await
        {
            self.processes.stop(&app_id).await;
            self.cron_registration.detach(&app_id).await;
            self.logs.detach(&app_id).await;
            return Err(error);
        }
        tracing::info!(
            %app_id,
            slot = request.slot.slot,
            staged_ms,
            vmm_ms = starting.elapsed().as_millis(),
            "instance booting"
        );
        Ok(())
    }

    async fn freeze(&self, app_id: &AppId) -> Result<(), VmError> {
        self.freeze_vm(&self.key(app_id)).await
    }

    async fn thaw(&self, app_id: &AppId) -> Result<(), VmError> {
        self.thaw_vm(&self.key(app_id)).await
    }

    async fn sleep(&self, mut request: SuspendRequest) -> Result<(), VmError> {
        request.app_id = self.key(&request.app_id);
        if self.processes.status(&request.app_id).frozen {
            self.thaw_vm(&request.app_id).await?;
        }
        // Held to the end, whichever way the snapshot goes: only once it is on the disk does
        // the next measurement count it, and only once it has failed is the room free again.
        let _share_of_the_disk = self
            .admit_snapshot(&request.app_id)
            .await
            .map_err(|reason| VmError::SleepRefused { reason })?;
        let paths = snapshot_paths(&self.snapshot_dir, &request.app_id);
        let stamp = self.current_stamp(&request);
        let api = self.api(&request.app_id);

        self.discard_snapshot(&request.app_id);
        if let Err(error) = self.volumes.flush().await {
            tracing::warn!(app_id = %request.app_id, error = %error.message(), "the volume backend would not flush");
        }

        let control = self
            .working_dir_for(&request.app_id)
            .join(guest_contract::vsock::GUEST_VSOCK_FILENAME);
        if let Err(error) = time_sync::freeze_tenant(&control, time_sync::Hold::Leased).await {
            if let Err(recovery) = time_sync::wake(&control).await {
                tracing::warn!(app_id = %request.app_id, error = %recovery, "guest clock recovery failed after sleep refusal");
            }
            return Err(error);
        }
        let paused = std::time::Instant::now();
        // The guest must still hold its freeze lease when the VMM is paused.
        let pause = tokio::time::timeout(
            std::time::Duration::from_millis(guest_contract::control::TENANT_CONTROL_TIMEOUT_MS),
            api.pause(),
        )
        .await
        .map_err(|_| {
            VmError::Host("the VMM did not pause before the tenant freeze lease could expire".into())
        })
        .and_then(std::convert::identity);
        if let Err(error) = pause {
            self.resume_frozen_tenant(&request.app_id, &control).await;
            return Err(error);
        }
        let memory_bytes = match self.snapshot_paused_vm(&request, paths, stamp).await {
            Ok(bytes) => bytes,
            Err(error) => {
                self.resume_frozen_tenant(&request.app_id, &control).await;
                return Err(error);
            }
        };

        self.metrics.sleep_wake.snapshotted(paused.elapsed());
        tracing::info!(
            app_id = %request.app_id,
            slot = request.slot.slot,
            snapshot_ms = paused.elapsed().as_millis(),
            memory_bytes,
            "instance asleep"
        );
        Ok(())
    }

    async fn wake(&self, mut request: SuspendRequest) -> Result<(), VmError> {
        request.app_id = self.key(&request.app_id);
        let _starting = self.admit_start()?;
        let paths = snapshot_paths(&self.snapshot_dir, &request.app_id);
        let expected = self.current_stamp(&request);
        let restoring = std::time::Instant::now();
        let checked_paths = paths.clone();
        let verified = tokio::task::spawn_blocking(move || ensure_loadable(&checked_paths, &expected))
            .await
            .map_err(|error| VmError::Host(error.to_string()))
            .and_then(std::convert::identity);
        if let Err(error) = verified {
            self.discard_snapshot(&request.app_id);
            return Err(error);
        }

        if !self
            .processes
            .read_record(&request.app_id)
            .is_some_and(|record| record.jail_root.is_some())
        {
            self.discard_snapshot(&request.app_id);
            return Err(VmError::SnapshotUnusable {
                reason: "the snapshot was created by a legacy VMM without a jail".into(),
            });
        }

        let _ = std::fs::remove_file(&paths.stamp_path);
        let mut jail = self.prepare_jail(&request.app_id).await?;
        self.readopt_vm(&request.app_id).await?;
        jail.grant_socket(&tenant_log_socket_path(&jail.root))
            .map_err(|error| VmError::Host(error.to_string()))?;
        jail.grant_socket(&cron_registration_socket_path(&jail.root))
            .map_err(|error| VmError::Host(error.to_string()))?;
        self.network
            .set_tap_owner(&request.slot.tap_name, jail.uid)
            .await
            .map_err(|error| VmError::Host(error.message()))?;
        let exchange = super::snapshot::exchange(&self.snapshot_dir, &request.app_id);
        for (source, name) in [(&paths.state_path, "state"), (&paths.memory_path, "memory")] {
            let target = exchange.join(name);
            let _ = std::fs::remove_file(&target);
            use std::os::unix::fs::PermissionsExt;
            std::os::unix::fs::chown(source, Some(0), Some(jail.gid))
                .map_err(|error| VmError::Host(error.to_string()))?;
            std::fs::set_permissions(source, std::fs::Permissions::from_mode(0o440))
                .map_err(|error| VmError::Host(error.to_string()))?;
            std::fs::hard_link(source, &target).map_err(|error| VmError::Host(error.to_string()))?;
        }
        let state_path = PathBuf::from("/snapshots/state");
        let memory_path = PathBuf::from("/snapshots/memory");
        if let Some(record) = self.state.record(&request.app_id).await {
            jail.limits
                .constrain(super::limits::RuntimeControls::for_instance(
                    record.resources,
                    record.limits.as_ref(),
                ));
        }
        let started = self
            .processes
            .spawn(&request.app_id, &self.firecracker, &self.jailer, &jail, false)
            .await
            .and_then(|record| jail.apply_oom_score(record.pid))
            .map_err(|error| VmError::Host(error.to_string()));
        let outcome = match started {
            Err(error) => Err(error),
            Ok(_) => {
                let api = self.api(&request.app_id);
                match api.load_snapshot(&state_path, &memory_path).await {
                    Err(error) => Err(error),
                    Ok(()) => self.resume_restored_tenant(&request).await,
                }
            }
        };
        if outcome.is_err() {
            self.processes.stop(&request.app_id).await;
        }
        self.discard_snapshot(&request.app_id);
        for name in ["state", "memory"] {
            let _ = std::fs::remove_file(exchange.join(name));
        }
        outcome?;
        self.metrics.sleep_wake.restored(restoring.elapsed());
        tracing::info!(
            app_id = %request.app_id,
            slot = request.slot.slot,
            restore_ms = restoring.elapsed().as_millis(),
            "instance awake"
        );
        Ok(())
    }

    async fn stop(&self, app_id: &AppId) -> Result<(), VmError> {
        let app_id = &self.key(app_id);
        self.discard_snapshot(app_id);
        self.processes.stop(app_id).await;
        if let Err(error) = super::scratch::remove_disk(&self.scratch_disk_path(app_id)) {
            tracing::warn!(%app_id, %error, "the scratch disk of a stopped microVM could not be deleted");
        }
        Ok(())
    }

    async fn discard(&self, app_id: &AppId) -> Result<(), VmError> {
        let owner = app_id;
        let app_id = &self.key(owner);
        // Once neither of two apps that traded microVMs holds one, the trade is undone: the next
        // microVM of each is booted under its own id again.
        if app_id != owner
            && self.processes.read_record(owner).is_none()
            && !snapshot_paths(&self.snapshot_dir, owner).stamp_path.exists()
        {
            let _ = self.write_key(owner, owner);
            let _ = self.write_key(app_id, app_id);
        }
        self.discard_snapshot(app_id);
        self.processes.forget(app_id);
        self.logs.detach(app_id).await;
        self.cron_registration.detach(app_id).await;
        let _ = std::fs::remove_dir_all(self.vm_dir.join(app_id.as_str()));
        let _ = std::fs::remove_dir_all(
            super::jailer::root(&self.vm_dir.join("jailer"), app_id)
                .parent()
                .expect("a jail root has an app directory"),
        );
        let _ = std::fs::remove_dir_all(super::snapshot::exchange(&self.snapshot_dir, app_id));
        Ok(())
    }

    async fn delete_tap(&self, tap_name: &str) -> Result<(), VmError> {
        self.network
            .delete_tap(tap_name)
            .await
            .map_err(|error| VmError::Host(error.message()))
    }

    async fn tap_names(&self) -> Vec<String> {
        self.network.tap_names().await
    }

    async fn statuses(&self, app_ids: &[AppId]) -> std::collections::BTreeMap<AppId, VmStatus> {
        app_ids
            .iter()
            .map(|app_id| (app_id.clone(), self.processes.status(&self.key(app_id))))
            .collect()
    }

    async fn adopted_app_ids(&self) -> Vec<AppId> {
        self.processes
            .adopted_app_ids()
            .iter()
            .map(|key| self.key(key))
            .collect()
    }

    async fn memory(&self, app_ids: &[AppId]) -> std::collections::BTreeMap<AppId, protocol::ReportedMemory> {
        app_ids
            .iter()
            .filter_map(|id| {
                self.processes
                    .memory(&self.key(id))
                    .map(|memory| (id.clone(), memory))
            })
            .collect()
    }

    async fn apply_limits(
        &self,
        app_id: &AppId,
        resources: protocol::InstanceResources,
        limits: Option<protocol::InstanceLimits>,
    ) -> Result<(), VmError> {
        let app_id = &self.key(app_id);
        let Some(record) = self
            .processes
            .read_record(app_id)
            .filter(|record| record.jail_root.is_some() && self.processes.status(app_id).active)
        else {
            return Ok(());
        };
        let Some(cgroup) = self.processes.cgroup(app_id) else {
            return Ok(());
        };
        super::limits::RuntimeControls::for_instance(resources, limits.as_ref())
            .write(&cgroup, record.pid)
            .map_err(|error| VmError::Host(format!("the instance's limits could not be applied: {error}")))
    }

    fn holds_snapshot(&self, app_id: &AppId) -> bool {
        snapshot_paths(&self.snapshot_dir, &self.key(app_id))
            .stamp_path
            .exists()
    }

    async fn readopt(&self, app_id: &AppId) -> Result<(), VmError> {
        self.readopt_vm(&self.key(app_id)).await
    }

    async fn guest_verdict(&self, app_id: &AppId) -> Option<String> {
        let console = std::fs::read_to_string(self.processes.console_path(&self.key(app_id))).ok()?;
        guest_contract::control::exit_reason(&console)
    }

    fn working_dir(&self, app_id: &AppId) -> PathBuf {
        self.working_dir_for(&self.key(app_id))
    }

    async fn expire_snapshot(&self, app_id: &AppId, kept_for: std::time::Duration) -> bool {
        let app_id = &self.key(app_id);
        let paths = snapshot_paths(&self.snapshot_dir, app_id);
        // The stamp is the last thing a sleep writes and a wake removes, so its age is how long
        // the snapshot has been waiting to be woken from.
        let Ok(written) = std::fs::metadata(&paths.stamp_path).and_then(|info| info.modified()) else {
            return false;
        };
        if written.elapsed().unwrap_or_default() < kept_for {
            return false;
        }
        let bytes = super::snapshot::read_snapshot_bytes(&paths.directory);
        self.discard_snapshot(app_id);
        tracing::info!(
            %app_id,
            bytes,
            "a snapshot kept past its snapshotTtlMs was deleted; the next request boots the app afresh"
        );
        true
    }

    async fn exchange(&self, first: &AppId, second: &AppId) -> Result<(), VmError> {
        let (first_key, second_key) = (self.key(first), self.key(second));
        self.write_key(first, &second_key)
            .and_then(|()| self.write_key(second, &first_key))
            .map_err(|error| VmError::Host(format!("the microVMs could not trade apps: {error}")))
    }
}

impl VmManager {
    /// Pauses the microVM run under `app_id`.
    async fn freeze_vm(&self, app_id: &AppId) -> Result<(), VmError> {
        if !self
            .processes
            .read_record(app_id)
            .is_some_and(|record| record.jail_root.is_some())
            || !self.processes.status(app_id).active
        {
            return Err(VmError::Host(
                "only a running jailed microVM can be frozen".into(),
            ));
        }
        // Recorded first, so a daemon that dies half way readopts a guest it must not route to.
        self.processes
            .remember_frozen(app_id, true)
            .map_err(|error| VmError::Host(error.to_string()))?;
        // The VMM alone is paused: a paused guest runs nothing, so its tenant needs no hold, and
        // kvmclock reads the host's clock again on the guest's first instruction after the
        // resume, so the thaw needs no clock step either.
        if let Err(error) = self.api(app_id).pause().await {
            let _ = self.processes.remember_frozen(app_id, false);
            return Err(error);
        }
        if let Some(cgroup) = self.processes.cgroup(app_id) {
            if let Err(error) = super::limits::reclaim(&cgroup, FROZEN_RECLAIM_BYTES).await {
                tracing::debug!(%app_id, %error, "a frozen microVM's memory reclaim was incomplete");
            }
        }
        Ok(())
    }

    /// Resumes the microVM run under `app_id`.
    async fn thaw_vm(&self, app_id: &AppId) -> Result<(), VmError> {
        self.api(app_id).resume().await?;
        self.processes
            .remember_frozen(app_id, false)
            .map_err(|error| VmError::Host(error.to_string()))
    }

    /// Takes back up a microVM by the id it runs under, `app_id`.
    async fn readopt_vm(&self, app_id: &AppId) -> Result<(), VmError> {
        // The deployment its output should be stamped with is the one the record remembers. A
        // guest this host holds no record of has nothing to attribute its lines to, so it is left
        // for the reconcile pass, which discards a microVM it was never told to keep.
        let owner = self.key(app_id);
        let Some(record) = self.state.record(&owner).await else {
            return Ok(());
        };
        let source = record.staging_for.clone().unwrap_or(owner);
        let working_dir = self.working_dir_for(app_id);
        self.attach_channels(app_id, &source, &record.deployment_id, &working_dir)
            .await?;
        if self
            .processes
            .read_record(app_id)
            .is_some_and(|record| record.jail_root.is_some())
        {
            use std::os::unix::fs::MetadataExt;
            let owner = std::fs::metadata(&working_dir).map_err(|error| VmError::Host(error.to_string()))?;
            for socket in [
                tenant_log_socket_path(&working_dir),
                cron_registration_socket_path(&working_dir),
            ] {
                crate::unix_socket::own(&socket, owner.uid(), owner.gid())
                    .map_err(|error| VmError::Host(error.to_string()))?;
            }
        }
        if self
            .processes
            .read_record(app_id)
            .is_some_and(|record| record.jail_root.is_some())
        {
            use std::os::unix::fs::MetadataExt;
            let owner = std::fs::metadata(&working_dir).map_err(|error| VmError::Host(error.to_string()))?;
            for socket in [
                tenant_log_socket_path(&working_dir),
                cron_registration_socket_path(&working_dir),
            ] {
                crate::unix_socket::own(&socket, owner.uid(), owner.gid())
                    .map_err(|error| VmError::Host(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GuestImageError {
    #[error("{path} is not there, so this host has no guest image to boot a tenant from")]
    Missing { path: String },
    #[error("{name} in {directory} is {actual}, not the {expected} its manifest describes")]
    Altered {
        name: String,
        directory: String,
        expected: String,
        actual: String,
    },
    #[error("the guest image manifest in {directory} could not be read: {reason}")]
    Unreadable { directory: String, reason: String },
}

impl GuestImageError {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

pub fn verify_guest_image(guest_image_dir: &Path) -> Result<String, GuestImageError> {
    let unreadable = |reason: &str| GuestImageError::Unreadable {
        directory: guest_image_dir.display().to_string(),
        reason: reason.to_string(),
    };
    let manifest_path = guest_image_dir.join(GUEST_MANIFEST_FILENAME);
    let manifest: serde_json::Value = crate::json_store::read_json(&manifest_path)
        .map_err(|error| unreadable(&error.message()))?
        .ok_or_else(|| GuestImageError::Missing {
            path: manifest_path.display().to_string(),
        })?;
    let version = manifest
        .get("version")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| unreadable("it names no version"))?
        .to_string();
    let described = manifest
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| unreadable("it describes no artifacts"))?;

    for name in [GUEST_KERNEL_FILENAME, GUEST_ROOTFS_FILENAME] {
        let expected = described
            .iter()
            .find(|artifact| artifact.get("name").and_then(serde_json::Value::as_str) == Some(name))
            .and_then(|artifact| artifact.get("sha256"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| unreadable(&format!("it describes no {name}")))?;
        let path = guest_image_dir.join(name);
        let bytes = std::fs::read(&path).map_err(|_| GuestImageError::Missing {
            path: path.display().to_string(),
        })?;
        let actual = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&bytes));
        if actual != expected {
            return Err(GuestImageError::Altered {
                name: name.to_string(),
                directory: guest_image_dir.display().to_string(),
                expected: expected.to_string(),
                actual,
            });
        }
    }
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::logs::FileLogSink;
    use crate::adapters::volumes::local_file::LocalFileVolumes;
    use crate::state::HostState;
    use crate::test_support::mocks;
    use crate::test_support::*;
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::TokioIo;
    use protocol::ObjectKey;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Mutex;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixListener;

    #[test]
    fn a_full_start_budget_refuses_immediately_and_recovers_when_a_start_finishes() {
        let mut host = fixture();
        host.manager.start_permits = tokio::sync::Semaphore::new(1);
        let first = host.manager.admit_start().unwrap();
        assert!(host.manager.admit_start().is_err());
        drop(first);
        assert!(host.manager.admit_start().is_ok());
    }

    fn fake_boot_binary(fixture: &mut Fixture, command: &str) {
        use std::os::unix::fs::PermissionsExt;
        let binary = fixture._directory.path().join("fake-jailer");
        std::fs::write(&binary, format!("#!/bin/sh\nexec {command}\n")).unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
        fixture.manager.jailer.binary = binary;
        fixture.manager.start_permits = tokio::sync::Semaphore::new(1);
    }

    #[tokio::test]
    async fn a_spawned_vm_holds_start_capacity_until_its_guest_control_port_answers() {
        let mut fixture = fixture();
        fake_boot_binary(&mut fixture, "sleep 30");
        let boot = fixture.manager.boot(boot_request(desired_instance(|_| {})));
        tokio::pin!(boot);
        let spawned = async {
            while !fixture.manager.processes.status(&app_id()).active {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        };
        tokio::select! {
            result = &mut boot => panic!("boot ended before guest initialization: {result:?}"),
            result = tokio::time::timeout(std::time::Duration::from_secs(5), spawned) => result.unwrap(),
        }
        let another = boot_request(desired_instance(|desired| {
            desired.app_id = AppId::parse("app-2").unwrap()
        }));
        assert_eq!(fixture.manager.boot(another).await, Err(VmError::StartBusy));
        assert_eq!(
            fixture.manager.wake(suspend_request()).await,
            Err(VmError::StartBusy)
        );

        let control = fixture
            .manager
            .working_dir_for(&app_id())
            .join(guest_contract::vsock::GUEST_VSOCK_FILENAME);
        let listener = UnixListener::bind(control).unwrap();
        let (asked, answer) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut wire = BufReader::new(stream);
            let mut line = String::new();
            wire.read_line(&mut line).await.unwrap();
            assert_eq!(line, "CONNECT 51001\n");
            wire.get_mut().write_all(b"ERR\n").await.unwrap();
            drop(wire);
            let (stream, _) = listener.accept().await.unwrap();
            let mut wire = BufReader::new(stream);
            line.clear();
            wire.read_line(&mut line).await.unwrap();
            asked.send(()).unwrap();
            released.await.unwrap();
            wire.get_mut().write_all(b"OK 1234\n").await.unwrap();
        });
        tokio::select! {
            result = &mut boot => panic!("boot ended before a successful guest handshake: {result:?}"),
            result = answer => result.unwrap(),
        }
        assert!(fixture.manager.admit_start().is_err());
        release.send(()).unwrap();
        boot.await.unwrap();
        server.await.unwrap();
        assert!(fixture.manager.admit_start().is_ok());
        assert!(fixture.manager.processes.status(&app_id()).active);
        fixture.manager.processes.stop(&app_id()).await;
    }

    #[tokio::test]
    async fn a_vm_that_exits_before_initializing_releases_capacity_and_detaches_its_channels() {
        let mut fixture = fixture();
        fake_boot_binary(&mut fixture, "false");
        let error = fixture
            .manager
            .boot(boot_request(desired_instance(|_| {})))
            .await
            .unwrap_err();
        assert!(error.message().contains("exited before"));
        assert!(fixture.manager.admit_start().is_ok());
        assert!(!fixture.manager.processes.status(&app_id()).active);
        assert!(fixture.manager.logs.attached().await.is_empty());
        assert!(fixture.manager.cron_registration.attached().await.is_empty());
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        manager: VmManager,
        network: mocks::NetworkSpy,
        commands: mocks::CommandLog,
        state: SharedState,
    }

    fn fixture() -> Fixture {
        let directory = tempfile::tempdir_in("/tmp").unwrap();
        let root = directory.path();
        let (network, network_spy) = mocks::network();
        let (commands, command_log) = mocks::commands_formatting();
        let state = HostState::shared();
        let manager = VmManager {
            start_permits: tokio::sync::Semaphore::new(2),
            metrics: Arc::new(crate::domain::metrics::HostMetrics::new()),
            vm_dir: root.join("vm"),
            snapshot_dir: root.join("snapshots"),
            guest_image_dir: root.join("guest"),
            firecracker: root.join("bin/firecracker"),
            jailer: super::super::jailer::Jailer::for_testing(root.join("bin/jailer")),
            guest_image_version: "6.1.180-test".into(),
            processes: VmProcesses::new(root.join("run")),
            network,
            volumes: Arc::new(LocalFileVolumes::new(
                root.join("volumes"),
                ObjectKey::parse("volumes").unwrap(),
                mocks::commands_succeeding().0,
                crate::adapters::volumes::initial_contents::ContentsStaging::new(
                    mocks::artifacts_holding(Vec::new()),
                    root.join("initial-contents"),
                ),
            )),
            commands,
            logs: TenantLogReceiver::new(),
            cron_registration: crate::adapters::cron_registration::CronRegistrationReceiver::new(Arc::new(
                crate::domain::cron::registry::CronRegistry::new(
                    Arc::new(crate::repositories::cron_repository::MockCronRepository::new()),
                    10,
                    chrono_tz::UTC,
                ),
            )),
            sink: Arc::new(FileLogSink::new(
                root.join("logs"),
                crate::config::LogsConfig::default().keep_bytes_per_app,
            )),
            state: state.clone(),
            in_flight: SnapshotsInFlight::default(),
        };
        Fixture {
            _directory: directory,
            manager,
            network: network_spy,
            commands: command_log,
            state,
        }
    }

    async fn fake_firecracker(
        listener: UnixListener,
        calls: Arc<Mutex<Vec<String>>>,
        refuse: Option<&'static str>,
    ) {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let calls = calls.clone();
            tokio::spawn(async move {
                let service =
                    hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                        let calls = calls.clone();
                        async move {
                            let path = request.uri().path().to_string();
                            let body = request.into_body().collect().await.unwrap().to_bytes();
                            if refuse == Some("silent-pause")
                                && String::from_utf8_lossy(&body).contains("Paused")
                            {
                                std::future::pending::<()>().await;
                            }
                            let refused = refuse.is_some_and(|target| target == path)
                                && (path != "/vm" || String::from_utf8_lossy(&body).contains("Paused"));
                            calls.lock().unwrap().push(path);
                            Ok::<_, std::convert::Infallible>(
                                hyper::Response::builder()
                                    .status(if refused {
                                        hyper::StatusCode::BAD_REQUEST
                                    } else {
                                        hyper::StatusCode::NO_CONTENT
                                    })
                                    .body(Full::new(bytes::Bytes::new()))
                                    .unwrap(),
                            )
                        }
                    });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    }

    async fn fake_guest_control(
        listener: UnixListener,
        calls: Arc<Mutex<Vec<String>>>,
        old_init: bool,
        refuse_clock: bool,
    ) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let calls = calls.clone();
            tokio::spawn(async move {
                let mut wire = BufReader::new(stream);
                let mut line = String::new();
                wire.read_line(&mut line).await.unwrap();
                assert_eq!(line, "CONNECT 51001\n");
                wire.get_mut().write_all(b"OK 1234\n").await.unwrap();
                line.clear();
                wire.read_line(&mut line).await.unwrap();
                let request = line.trim().to_string();
                calls.lock().unwrap().push(request.clone());
                if old_init {
                    return;
                }
                if request == guest_contract::control::TENANT_FREEZE_REQUEST {
                    wire.get_mut().write_all(b"READY\n").await.unwrap();
                    line.clear();
                    wire.read_line(&mut line).await.unwrap();
                    assert!(line
                        .trim()
                        .starts_with(guest_contract::control::TENANT_FREEZE_COMMIT));
                    calls.lock().unwrap().push(line.trim().into());
                    wire.get_mut().write_all(b"OK\n").await.unwrap();
                } else if request.starts_with(guest_contract::control::TENANT_CLOCK_REQUEST) {
                    wire.get_mut()
                        .write_all(if refuse_clock { b"REFUSED\n" } else { b"READY\n" })
                        .await
                        .unwrap();
                    line.clear();
                    wire.read_line(&mut line).await.unwrap();
                    if refuse_clock {
                        return;
                    }
                    calls.lock().unwrap().push(line.trim().to_string());
                    if line.starts_with(guest_contract::control::TENANT_CLOCK_RELEASE) {
                        wire.get_mut().write_all(b"OK\n").await.unwrap();
                    }
                }
            });
        }
    }

    #[cfg(unix)]
    async fn running_fake_vm(
        fixture: &mut Fixture,
        old_init: bool,
        refuse_api: Option<&'static str>,
        refuse_clock: bool,
    ) -> Arc<Mutex<Vec<String>>> {
        use std::os::unix::fs::PermissionsExt;
        let working_dir = fixture.manager.vm_dir.join(app_id().as_str());
        std::fs::create_dir_all(&working_dir).unwrap();
        let binary = fixture._directory.path().join("fake-firecracker");
        std::fs::write(&binary, b"#!/bin/sh\n: > started\nexec sleep 30\n").unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
        fixture.manager.jailer.binary = binary;
        fixture
            .manager
            .processes
            .spawn(
                &app_id(),
                &fixture.manager.firecracker,
                &fixture.manager.jailer,
                &super::super::jailer::Jail::for_testing(working_dir.clone()),
                false,
            )
            .await
            .unwrap();
        let mut record = fixture.manager.processes.read_record(&app_id()).unwrap();
        record.jail_root = None;
        record.jail_uid = None;
        fixture.manager.processes.write_record(&record).unwrap();
        std::fs::remove_file(fixture.manager.processes.api_socket(&app_id())).unwrap();
        let marker = working_dir.join("started");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !marker.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap_or_else(|error| {
            panic!(
                "fake VMM did not start: {error}; console: {:?}",
                std::fs::read_to_string(fixture.manager.processes.console_path(&app_id()))
            )
        });
        let calls = Arc::new(Mutex::new(Vec::new()));
        let api = UnixListener::bind(fixture.manager.processes.api_socket(&app_id())).unwrap();
        let control =
            UnixListener::bind(working_dir.join(guest_contract::vsock::GUEST_VSOCK_FILENAME)).unwrap();
        tokio::spawn(fake_firecracker(api, calls.clone(), refuse_api));
        tokio::spawn(fake_guest_control(control, calls.clone(), old_init, refuse_clock));
        fixture
            .state
            .put_record(instance_record(|record| record.health.ever_healthy = true))
            .await;
        calls
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_old_guest_that_does_not_answer_sleep_keeps_running() {
        let mut fixture = fixture();
        let calls = running_fake_vm(&mut fixture, true, None, false).await;
        let request = SuspendRequest {
            app_id: app_id(),
            deployment_id: deployment_id(),
            slot: nft_render::describe_slot(0, app_id()),
        };

        assert!(fixture.manager.sleep(request).await.is_err());
        assert!(fixture.manager.processes.status(&app_id()).active);
        let calls = calls.lock().unwrap().clone();
        assert_eq!(calls[0], "SLEEP");
        assert!(calls[1] == "WAKE");
        fixture.manager.processes.stop(&app_id()).await;
    }

    fn suspend_request() -> SuspendRequest {
        SuspendRequest {
            app_id: app_id(),
            deployment_id: deployment_id(),
            slot: nft_render::describe_slot(0, app_id()),
        }
    }

    #[tokio::test]
    async fn refused_pauses_and_snapshots_resume_and_release_the_tenant() {
        for refused in ["/vm", "/snapshot/create", "silent-pause"] {
            let mut fixture = fixture();
            let calls = running_fake_vm(&mut fixture, false, Some(refused), false).await;
            assert!(fixture.manager.sleep(suspend_request()).await.is_err());
            assert!(fixture.manager.processes.status(&app_id()).active);
            let calls = calls.lock().unwrap().clone();
            assert_eq!(&calls[..2], ["SLEEP", "HOLD"]);
            assert_eq!(calls[calls.len() - 3], "/vm");
            assert_eq!(calls[calls.len() - 2], "WAKE");
            assert!(calls.last().unwrap().starts_with("GO "));
            assert!(!snapshot_paths(&fixture.manager.snapshot_dir, &app_id())
                .stamp_path
                .exists());
            fixture.manager.processes.stop(&app_id()).await;
        }
    }

    #[tokio::test]
    async fn a_failed_snapshot_publication_leaves_no_loadable_snapshot() {
        let fixture = fixture();
        let staged = StagedSnapshot::create(&fixture.manager.snapshot_dir).unwrap();
        let pending = staged.paths.directory.clone();
        let paths = snapshot_paths(&fixture.manager.snapshot_dir, &app_id());
        assert!(fixture
            .manager
            .publish_sleep_snapshot(
                staged,
                paths.clone(),
                fixture.manager.current_stamp(&suspend_request())
            )
            .await
            .is_err());
        assert!(!pending.exists());
        assert!(!paths.directory.exists());
    }

    #[tokio::test]
    async fn a_tenant_that_cannot_be_released_after_a_snapshot_refusal_is_stopped() {
        let mut fixture = fixture();
        running_fake_vm(&mut fixture, false, Some("/snapshot/create"), true).await;
        assert!(fixture.manager.sleep(suspend_request()).await.is_err());
        assert!(!fixture.manager.processes.status(&app_id()).active);
    }

    #[tokio::test]
    async fn a_restored_tenant_is_released_before_its_neighbour_is_refreshed() {
        let mut fixture = fixture();
        let calls = running_fake_vm(&mut fixture, false, None, false).await;
        fixture
            .manager
            .resume_restored_tenant(&suspend_request())
            .await
            .unwrap();
        let calls = calls.lock().unwrap().clone();
        assert_eq!(&calls[..2], ["/vm", "WAKE"]);
        assert!(calls[2].starts_with("GO "));
        assert_eq!(fixture.network.neighbours().len(), 1);
        fixture.manager.processes.stop(&app_id()).await;
    }

    #[tokio::test]
    async fn a_restored_guest_that_refuses_clock_sync_requires_a_cold_boot() {
        let mut fixture = fixture();
        running_fake_vm(&mut fixture, false, None, true).await;
        let error = fixture
            .manager
            .resume_restored_tenant(&suspend_request())
            .await
            .unwrap_err();
        let VmError::WakeStalled { reason } = &error else {
            panic!("{error}");
        };
        assert!(reason.starts_with("sync did not complete: "), "{reason}");
        assert_eq!(
            fixture.manager.metrics.sleep_wake.of(&app_id()).wake_failures,
            [0, 0, 1, 0]
        );
        assert!(fixture
            .manager
            .processes
            .failed_wake_console_path(&app_id())
            .exists());
        assert!(fixture.network.neighbours().is_empty());
        fixture.manager.processes.stop(&app_id()).await;
    }

    fn config_drive(working_dir: &Path) -> String {
        use std::io::Read;
        let image = std::fs::read(working_dir.join("config.squashfs")).unwrap();
        let filesystem = backhand::FilesystemReader::from_reader(std::io::Cursor::new(image)).unwrap();
        let node = filesystem
            .files()
            .find(|node| node.fullpath.to_string_lossy() == "/instance.env")
            .expect("the config drive holds instance.env");
        let backhand::InnerNode::File(file) = &node.inner else {
            panic!("instance.env is not a file");
        };
        let mut bytes = Vec::new();
        filesystem.file(file).reader().read_to_end(&mut bytes).unwrap();
        String::from_utf8(bytes).unwrap()
    }

    fn boot_request(desired: protocol::DesiredInstance) -> BootRequest {
        BootRequest {
            staged_for: None,
            slot: nft_render::describe_slot(0, desired.app_id.clone()),
            desired,
            writable: Writable::Volume {
                device_path: "/dev/loop0".into(),
            },
            payload: crate::ports::PreparedPayload {
                layer_image_paths: vec![PathBuf::from("/cache/abc/executable-0123456789abcdef.squashfs")],
                fetched_bytes: 0,
            },
        }
    }

    #[tokio::test]
    async fn staging_writes_the_machine_description_the_boot_contract_names() {
        let fixture = fixture();
        let request = boot_request(desired_instance(|instance| {
            instance.hostnames = vec![app_hostname()]
        }));
        let config_file = fixture.manager.stage(&request).await.unwrap().config_file;

        let config: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&config_file).unwrap()).unwrap();
        let drives = config["drives"].as_array().unwrap();
        assert_eq!(drives.len(), 4);
        assert!(drives[0]["path_on_host"]
            .as_str()
            .unwrap()
            .ends_with("guest/rootfs.ext4"));
        assert!(drives[1]["path_on_host"]
            .as_str()
            .unwrap()
            .ends_with("config.squashfs"));
        assert_eq!(drives[2]["path_on_host"], "/dev/loop0");
        assert_eq!(drives[2]["cache_type"], "Writeback");
        assert!(drives[3]["path_on_host"]
            .as_str()
            .unwrap()
            .ends_with("executable-0123456789abcdef.squashfs"));
        assert!(config["boot-source"]["boot_args"]
            .as_str()
            .unwrap()
            .contains("clocksource=kvm-clock"));
        assert_eq!(config["vsock"]["uds_path"], "logs.vsock");
        assert_eq!(config["vsock"]["guest_cid"], 3);

        assert_eq!(fixture.network.taps()[0].tap_name, "nbr0");
        assert_eq!(fixture.network.neighbours()[0].guest_mac, "02:00:0a:c9:00:02");
        assert_eq!(
            fixture.network.owners(),
            vec![(
                "nbr0".into(),
                std::fs::metadata(fixture.manager.working_dir_for(&app_id()))
                    .unwrap()
                    .uid()
            )]
        );
        assert_eq!(fixture.manager.logs.attached().await, vec![app_id()]);
    }

    #[tokio::test]
    async fn the_config_drive_carries_the_port_the_tenant_was_told_to_listen_on_and_how_many_layers_follow() {
        let fixture = fixture();
        let mut request = boot_request(desired_instance(|_| {}));
        request
            .payload
            .layer_image_paths
            .push(PathBuf::from("/cache/def/layer.img"));
        fixture.manager.stage(&request).await.unwrap();
        let written = config_drive(&fixture.manager.vm_dir.join(app_id().as_str()));
        assert!(written.contains("NIBRUN_HTTP_PORT=3000"));
        assert!(written.contains("NIBRUN_LAYERS=2\n"), "{written}");
    }

    fn scratch_request(scratch: protocol::Scratch) -> BootRequest {
        BootRequest {
            writable: Writable::Scratch(scratch),
            ..boot_request(desired_instance(|instance| {
                instance.volume_id = None;
                instance.scratch = Some(scratch);
            }))
        }
    }

    fn mib(value: u32) -> std::num::NonZeroU32 {
        std::num::NonZeroU32::new(value).unwrap()
    }

    fn drive_paths(config_file: &Path) -> Vec<String> {
        let config: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(config_file).unwrap()).unwrap();
        config["drives"]
            .as_array()
            .unwrap()
            .iter()
            .map(|drive| drive["path_on_host"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn a_memory_scratch_boots_with_no_data_drive_and_tells_the_guest_how_large_its_tmpfs_is() {
        let fixture = fixture();
        let request = scratch_request(protocol::Scratch::Memory { mib: mib(512) });
        let config_file = fixture.manager.stage(&request).await.unwrap().config_file;

        let drives = drive_paths(&config_file);
        assert_eq!(drives.len(), 3, "{drives:?}");
        assert!(drives[2].ends_with("executable-0123456789abcdef.squashfs"));
        let written = config_drive(&fixture.manager.vm_dir.join(app_id().as_str()));
        assert!(written.contains("NIBRUN_SCRATCH_MIB=512\n"), "{written}");
        assert!(fixture.commands.commands().is_empty());
        assert!(!fixture.manager.scratch_disk_path(&app_id()).exists());
    }

    #[tokio::test]
    async fn a_disk_scratch_is_a_sparse_ext4_file_made_afresh_in_the_microvms_directory_at_every_boot() {
        let fixture = fixture();
        let request = scratch_request(protocol::Scratch::Disk { mib: mib(8192) });
        let path = fixture.manager.scratch_disk_path(&app_id());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"what the last boot wrote").unwrap();

        let config_file = fixture.manager.stage(&request).await.unwrap().config_file;

        let drives = drive_paths(&config_file);
        assert_eq!(drives[2], path.display().to_string());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 8192 * 1024 * 1024);
        let mut head = [0u8; 24];
        std::io::Read::read_exact(&mut std::fs::File::open(&path).unwrap(), &mut head).unwrap();
        assert_eq!(head, [0u8; 24], "the last boot's bytes are gone");
        let formats = fixture.commands.commands();
        assert_eq!(formats.len(), 1);
        assert_eq!(formats[0][0], "mke2fs");
        assert_eq!(formats[0].last(), Some(&path.display().to_string()));
        let written = config_drive(&fixture.manager.vm_dir.join(app_id().as_str()));
        assert!(!written.contains("NIBRUN_SCRATCH_MIB"), "{written}");
    }

    #[tokio::test]
    async fn a_disk_scratch_that_cannot_be_formatted_refuses_the_boot() {
        let mut fixture = fixture();
        fixture.manager.commands = mocks::commands_answering(|request| {
            Err(crate::ports::CommandError::failed(
                request,
                &crate::ports::CommandResult {
                    code: 1,
                    stdout: String::new(),
                    stderr: "no space left on device".into(),
                },
            ))
        })
        .0;
        let Err(refused) = fixture
            .manager
            .stage(&scratch_request(protocol::Scratch::Disk { mib: mib(64) }))
            .await
        else {
            panic!("a scratch disk that could not be formatted was booted");
        };
        let refused = refused.message();
        assert!(refused.contains("scratch disk"), "{refused}");
        assert!(refused.contains("no space left on device"), "{refused}");
    }

    #[tokio::test]
    async fn a_stopped_microvm_leaves_no_scratch_disk_behind() {
        let fixture = fixture();
        fixture
            .manager
            .stage(&scratch_request(protocol::Scratch::Disk { mib: mib(64) }))
            .await
            .unwrap();
        let path = fixture.manager.scratch_disk_path(&app_id());
        assert!(path.exists());
        fixture.manager.stop(&app_id()).await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn a_sleep_is_refused_for_every_reason_the_record_gives() {
        let fixture = fixture();
        let request = SuspendRequest {
            app_id: app_id(),
            deployment_id: deployment_id(),
            slot: nft_render::describe_slot(0, app_id()),
        };
        let refused = fixture.manager.sleep(request.clone()).await.unwrap_err();
        assert!(refused.message().contains("holds no record"));

        fixture
            .state
            .put_record(instance_record(|record| record.health.ever_healthy = false))
            .await;
        let never_answered = fixture.manager.sleep(request.clone()).await.unwrap_err();
        assert!(never_answered.message().contains("finished booting"));

        fixture
            .state
            .put_record(instance_record(|record| {
                record.health.ever_healthy = true;
                record.stop_requested = true;
            }))
            .await;
        let stopping = fixture.manager.sleep(request).await.unwrap_err();
        assert!(stopping.message().contains("asked to stop"));
    }

    #[tokio::test]
    async fn a_wake_with_no_loadable_snapshot_says_so_and_leaves_nothing_behind() {
        let fixture = fixture();
        let request = SuspendRequest {
            app_id: app_id(),
            deployment_id: deployment_id(),
            slot: nft_render::describe_slot(0, app_id()),
        };
        let paths = snapshot_paths(&fixture.manager.snapshot_dir, &app_id());
        std::fs::create_dir_all(&paths.directory).unwrap();
        std::fs::write(&paths.memory_path, b"pretend memory").unwrap();
        write_json(
            &paths.stamp_path,
            &SnapshotStamp {
                firecracker_version: Some(crate::adapters::vm::process::FIRECRACKER_VERSION.to_string()),
                deployment_id: deployment_id(),
                guest_image_version: "an-older-image".into(),
                host_boot_id: fixture.manager.processes.boot_id().to_string(),
                slot: 0,
                host: SnapshotHost::this_one().clone(),
            },
        )
        .unwrap();

        let error = fixture.manager.wake(request).await.unwrap_err();
        assert!(matches!(error, VmError::SnapshotUnusable { .. }));
        assert!(error.message().contains("guest image has changed"));
        assert!(
            !paths.directory.exists(),
            "an unloadable snapshot is discarded, not left"
        );
    }

    #[tokio::test]
    async fn a_failed_snapshot_publication_resumes_the_paused_guest() {
        use http_body_util::{BodyExt, Full};
        use hyper_util::rt::TokioIo;

        let fixture = fixture();
        fixture
            .state
            .put_record(instance_record(|record| record.health.ever_healthy = true))
            .await;
        let working_dir = fixture.manager.working_dir_for(&app_id());
        std::fs::create_dir_all(&working_dir).unwrap();
        let control =
            UnixListener::bind(working_dir.join(guest_contract::vsock::GUEST_VSOCK_FILENAME)).unwrap();
        let control_calls = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(fake_guest_control(control, control_calls.clone(), false, false));
        let socket = fixture.manager.processes.api_socket(&app_id());
        make_directory(socket.parent().unwrap(), 0o700).unwrap();
        let listener = tokio::net::UnixListener::bind(socket).unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls = seen.clone();
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (stream, _) = listener.accept().await.unwrap();
                let calls = calls.clone();
                tokio::spawn(async move {
                    let service =
                        hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                            let calls = calls.clone();
                            async move {
                                let path = request.uri().path().to_string();
                                let body = request.into_body().collect().await.unwrap().to_bytes();
                                let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                                if path == "/snapshot/create" {
                                    std::fs::write(body["snapshot_path"].as_str().unwrap(), b"state")
                                        .unwrap();
                                    std::fs::write(body["mem_file_path"].as_str().unwrap(), b"").unwrap();
                                }
                                calls.lock().unwrap().push((path, body));
                                Ok::<_, std::convert::Infallible>(
                                    hyper::Response::builder()
                                        .status(hyper::StatusCode::NO_CONTENT)
                                        .body(Full::new(bytes::Bytes::new()))
                                        .unwrap(),
                                )
                            }
                        });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        let error = fixture
            .manager
            .sleep(SuspendRequest {
                app_id: app_id(),
                deployment_id: deployment_id(),
                slot: nft_render::describe_slot(0, app_id()),
            })
            .await
            .unwrap_err();
        server.await.unwrap();
        let control_calls = control_calls.lock().unwrap().clone();
        assert_eq!(&control_calls[..3], ["SLEEP", "HOLD", "WAKE"]);
        assert!(control_calls[3].starts_with("GO "));
        assert!(matches!(error, VmError::Host(_)), "{error}");
        let calls = seen.lock().unwrap();
        assert_eq!(calls[0].1["state"], "Paused");
        assert_eq!(calls[1].0, "/snapshot/create");
        assert_eq!(calls[2].1["state"], "Resumed");
        assert!(!snapshot_paths(&fixture.manager.snapshot_dir, &app_id())
            .directory
            .exists());
        assert!(std::fs::read_dir(&fixture.manager.snapshot_dir)
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".pending-")));
        let exchange = super::super::snapshot::exchange(&fixture.manager.snapshot_dir, &app_id());
        assert!(!exchange.join("state").exists());
        assert!(!exchange.join("memory").exists());
        assert_eq!(fixture.manager.in_flight.bytes(), 0);
    }

    #[tokio::test]
    async fn a_corrupted_snapshot_is_discarded_before_a_hypervisor_is_started() {
        let fixture = fixture();
        let request = SuspendRequest {
            app_id: app_id(),
            deployment_id: deployment_id(),
            slot: nft_render::describe_slot(0, app_id()),
        };
        let paths = snapshot_paths(&fixture.manager.snapshot_dir, &app_id());
        let staged = StagedSnapshot::create(&fixture.manager.snapshot_dir).unwrap();
        std::fs::write(&staged.paths.state_path, b"state").unwrap();
        std::fs::write(&staged.paths.memory_path, b"memory").unwrap();
        staged
            .publish(&paths, fixture.manager.current_stamp(&request))
            .unwrap();
        std::fs::write(&paths.memory_path, b"broken").unwrap();

        let error = fixture.manager.wake(request).await.unwrap_err();
        assert!(matches!(error, VmError::SnapshotUnusable { .. }), "{error}");
        assert!(error.message().contains("SHA-256"), "{error}");
        assert!(fixture.manager.processes.read_record(&app_id()).is_none());
        assert!(!paths.directory.exists());
    }

    fn running_under(fixture: &Fixture, key: &AppId) {
        fixture
            .manager
            .processes
            .write_record(&crate::adapters::vm::process::VmRecord {
                frozen: false,
                app_id: key.clone(),
                pid: std::process::id() as i32,
                host_boot_id: fixture.manager.processes.boot_id().into(),
                started_at_ms: 0,
                exit_code: None,
                signal: None,
                stop_requested: false,
                jail_root: None,
                jail_uid: None,
            })
            .unwrap();
    }

    #[tokio::test]
    async fn an_app_handed_the_microvm_booted_beside_it_reaches_that_microvm_by_its_own_id() {
        let fixture = fixture();
        let staging = AppId::parse("app-1--next").unwrap();
        running_under(&fixture, &staging);

        fixture.manager.exchange(&app_id(), &staging).await.unwrap();

        assert_eq!(fixture.manager.key(&app_id()), staging);
        assert_eq!(fixture.manager.key(&staging), app_id());
        assert_eq!(
            fixture.manager.working_dir(&app_id()),
            fixture.manager.working_dir_for(&staging)
        );
        assert!(fixture.manager.statuses(&[app_id()]).await[&app_id()].loaded);
        assert!(!fixture.manager.statuses(std::slice::from_ref(&staging)).await[&staging].loaded);
        assert_eq!(fixture.manager.adopted_app_ids().await, vec![app_id()]);
    }

    #[tokio::test]
    async fn a_trade_is_kept_while_either_app_holds_a_microvm_and_undone_once_neither_does() {
        let fixture = fixture();
        let staging = AppId::parse("app-1--next").unwrap();
        running_under(&fixture, &staging);
        fixture.manager.exchange(&app_id(), &staging).await.unwrap();

        // The old deployment, now under the staging id, goes; the app keeps what it was handed,
        // and the next deployment booted beside it lands on the id the old one left free.
        fixture.manager.discard(&staging).await.unwrap();
        assert_eq!(fixture.manager.key(&app_id()), staging);
        assert_eq!(fixture.manager.key(&staging), app_id());

        fixture.manager.discard(&app_id()).await.unwrap();
        assert_eq!(fixture.manager.key(&app_id()), app_id());
        assert_eq!(fixture.manager.key(&staging), staging);
    }

    #[tokio::test]
    async fn a_deployment_booted_beside_the_serving_one_is_listened_for_under_its_own_id() {
        let fixture = fixture();
        let staging = AppId::parse("app-1--next").unwrap();
        fixture
            .manager
            .stage(&BootRequest {
                staged_for: Some(app_id()),
                ..boot_request(desired_instance(|instance| instance.app_id = staging.clone()))
            })
            .await
            .unwrap();
        assert_eq!(fixture.manager.logs.attached().await, vec![staging.clone()]);
        assert_eq!(fixture.manager.cron_registration.attached().await, vec![staging]);
    }

    #[tokio::test]
    async fn a_snapshot_is_deleted_once_it_has_gone_unwoken_for_its_ttl_and_kept_until_then() {
        let fixture = fixture();
        let paths = snapshot_paths(&fixture.manager.snapshot_dir, &app_id());
        std::fs::create_dir_all(&paths.directory).unwrap();
        std::fs::write(&paths.memory_path, b"memory").unwrap();
        std::fs::write(&paths.stamp_path, b"{}").unwrap();

        let an_hour = std::time::Duration::from_secs(3600);
        assert!(!fixture.manager.expire_snapshot(&app_id(), an_hour).await);
        assert!(paths.memory_path.exists());

        assert!(
            fixture
                .manager
                .expire_snapshot(&app_id(), std::time::Duration::ZERO)
                .await
        );
        assert!(!paths.directory.exists());
        assert!(
            !fixture
                .manager
                .expire_snapshot(&app_id(), std::time::Duration::ZERO)
                .await
        );
    }

    #[tokio::test]
    async fn discarding_takes_the_working_directory_the_record_and_the_log_attachment() {
        let fixture = fixture();
        fixture
            .manager
            .stage(&boot_request(desired_instance(|_| {})))
            .await
            .unwrap();
        assert!(fixture.manager.working_dir_for(&app_id()).exists());
        fixture.manager.discard(&app_id()).await.unwrap();
        assert!(!fixture.manager.working_dir_for(&app_id()).exists());
        assert!(fixture.manager.logs.attached().await.is_empty());
        assert!(fixture.manager.cron_registration.attached().await.is_empty());
        assert_eq!(
            fixture.manager.statuses(&[app_id()]).await[&app_id()],
            VmStatus::default()
        );
    }

    fn write_console(fixture: &Fixture, console: &str) {
        let path = fixture.manager.processes.console_path(&app_id());
        make_directory(path.parent().unwrap(), 0o700).unwrap();
        std::fs::write(path, console).unwrap();
    }

    #[tokio::test]
    async fn the_guest_verdict_is_what_its_init_said_before_it_shut_the_guest_down() {
        let fixture = fixture();
        write_console(
            &fixture,
            "[nibrun] starting the tenant\n[nibrun] the tenant has stopped; shutting the guest down\n[   15.7] reboot: Restarting system\n",
        );
        assert_eq!(
            fixture.manager.guest_verdict(&app_id()).await.as_deref(),
            Some("the tenant has stopped; shutting the guest down")
        );
    }

    #[tokio::test]
    async fn a_microvm_killed_from_outside_has_no_verdict_whatever_its_init_was_saying() {
        let fixture = fixture();
        write_console(
            &fixture,
            "[nibrun] guest runtime starting\n[nibrun] starting /app/probe as uid 65534 in /app, with 198 MiB to spend\n",
        );
        assert_eq!(fixture.manager.guest_verdict(&app_id()).await, None);
    }

    #[tokio::test]
    async fn a_boot_a_tap_could_not_be_made_for_starts_no_hypervisor_at_all() {
        let mut fixture = fixture();
        fixture.manager.network = mocks::network_refusing(crate::adapters::net::tap::NetworkError {
            what: "a tap device",
            device: "nbr0".into(),
            reason: "operation not permitted".into(),
        });
        let error = fixture
            .manager
            .boot(boot_request(desired_instance(|_| {})))
            .await
            .unwrap_err();
        assert!(matches!(error, VmError::Host(_)), "{error}");
        assert!(error.message().contains("operation not permitted"), "{error}");
        assert!(fixture.manager.processes.read_record(&app_id()).is_none());
        assert!(fixture.manager.logs.attached().await.is_empty());
        assert!(fixture.manager.cron_registration.attached().await.is_empty());
    }

    #[tokio::test]
    async fn a_tap_that_cannot_be_granted_to_the_jail_leaves_no_partial_channels() {
        let mut fixture = fixture();
        let mut network = crate::adapters::net::tap::MockHostNetwork::new();
        network.expect_ensure_tap().returning(|_| Ok(()));
        network.expect_refresh_neighbour().returning(|_| Ok(()));
        network.expect_set_tap_owner().returning(|name, _| {
            Err(crate::adapters::net::tap::NetworkError {
                what: "tap ownership",
                device: name.into(),
                reason: "operation not permitted".into(),
            })
        });
        fixture.manager.network = Arc::new(network);
        let error = fixture
            .manager
            .boot(boot_request(desired_instance(|_| {})))
            .await
            .unwrap_err();
        assert!(error.message().contains("tap ownership"));
        assert!(fixture.manager.logs.attached().await.is_empty());
        assert!(fixture.manager.cron_registration.attached().await.is_empty());
        assert!(fixture.manager.processes.read_record(&app_id()).is_none());
    }

    #[tokio::test]
    async fn a_hypervisor_that_would_not_start_takes_the_log_attachment_down_behind_it() {
        let fixture = fixture();
        let error = fixture
            .manager
            .boot(boot_request(desired_instance(|_| {})))
            .await
            .unwrap_err();
        assert!(matches!(error, VmError::Host(_)), "{error}");
        assert!(
            fixture.manager.logs.attached().await.is_empty(),
            "a socket nothing will write to is not left listening"
        );
        assert!(fixture.manager.cron_registration.attached().await.is_empty());
    }

    #[tokio::test]
    async fn an_adopted_microvm_is_listened_for_again_where_its_guest_reconnects() {
        let fixture = fixture();
        // A guest adopted from an earlier daemon still has a record; the deployment its lines are
        // stamped with is read from it.
        fixture.state.put_record(instance_record(|_| {})).await;

        fixture.manager.readopt(&app_id()).await.unwrap();

        assert_eq!(fixture.manager.logs.attached().await, vec![app_id()]);
        assert_eq!(fixture.manager.cron_registration.attached().await, vec![app_id()]);
        let cron_socket = crate::adapters::cron_registration::cron_registration_socket_path(
            &fixture.manager.working_dir_for(&app_id()),
        );
        assert!(tokio::net::UnixStream::connect(&cron_socket).await.is_ok());
        let socket = crate::adapters::logs::receiver::tenant_log_socket_path(
            &fixture.manager.working_dir_for(&app_id()),
        );
        assert!(
            tokio::net::UnixStream::connect(&socket).await.is_ok(),
            "the host is bound where the guest reconnects after a daemon restart"
        );
    }

    #[tokio::test]
    async fn a_running_legacy_vmm_keeps_its_existing_channels_until_it_is_redeployed() {
        let fixture = fixture();
        fixture.state.put_record(instance_record(|_| {})).await;
        fixture
            .manager
            .processes
            .write_record(&crate::adapters::vm::process::VmRecord {
                app_id: app_id(),
                pid: std::process::id() as i32,
                host_boot_id: fixture.manager.processes.boot_id().into(),
                started_at_ms: 0,
                exit_code: None,
                signal: None,
                stop_requested: false,
                jail_root: None,
                jail_uid: None,
                frozen: false,
            })
            .unwrap();
        let legacy_directory = fixture.manager.vm_dir.join(app_id().as_str());
        assert_eq!(fixture.manager.working_dir_for(&app_id()), legacy_directory);
        fixture.manager.readopt(&app_id()).await.unwrap();
        assert!(
            tokio::net::UnixStream::connect(tenant_log_socket_path(&legacy_directory))
                .await
                .is_ok()
        );
        assert!(
            tokio::net::UnixStream::connect(cron_registration_socket_path(&legacy_directory))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_recorded_jail_is_readopted_using_its_existing_identity() {
        use std::os::unix::fs::MetadataExt;
        let fixture = fixture();
        let short_directory = tempfile::tempdir_in("/tmp").unwrap();
        let root = super::super::jailer::root(short_directory.path(), &app_id());
        make_directory(&root, VM_DIR_MODE).unwrap();
        let identity = std::fs::metadata(&root).unwrap();
        fixture.state.put_record(instance_record(|_| {})).await;
        fixture
            .manager
            .processes
            .write_record(&crate::adapters::vm::process::VmRecord {
                app_id: app_id(),
                pid: std::process::id() as i32,
                host_boot_id: fixture.manager.processes.boot_id().into(),
                started_at_ms: 0,
                exit_code: None,
                signal: None,
                stop_requested: false,
                jail_root: Some(root.clone()),
                jail_uid: None,
                frozen: false,
            })
            .unwrap();
        fixture.manager.readopt(&app_id()).await.unwrap();
        assert_eq!(fixture.manager.working_dir_for(&app_id()), root);
        for socket in [
            tenant_log_socket_path(&root),
            cron_registration_socket_path(&root),
        ] {
            let owner = std::fs::metadata(&socket).unwrap();
            assert_eq!((owner.uid(), owner.gid()), (identity.uid(), identity.gid()));
            assert!(tokio::net::UnixStream::connect(socket).await.is_ok());
        }
    }

    #[tokio::test]
    async fn readopting_a_microvm_this_host_holds_no_record_of_is_a_no_op() {
        let fixture = fixture();
        fixture.manager.readopt(&app_id()).await.unwrap();
        assert!(fixture.manager.logs.attached().await.is_empty());
        assert!(fixture.manager.cron_registration.attached().await.is_empty());
    }

    #[tokio::test]
    async fn staging_binds_the_crontab_socket_before_the_guest_is_started() {
        let fixture = fixture();
        fixture
            .manager
            .stage(&boot_request(desired_instance(|_| {})))
            .await
            .unwrap();
        let socket = crate::adapters::cron_registration::cron_registration_socket_path(
            &fixture.manager.working_dir_for(&app_id()),
        );
        assert!(tokio::net::UnixStream::connect(&socket).await.is_ok());
        assert_eq!(fixture.manager.cron_registration.attached().await, vec![app_id()]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(socket).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[tokio::test]
    async fn a_crontab_socket_failure_leaves_no_partial_log_attachment() {
        let fixture = fixture();
        let socket = crate::adapters::cron_registration::cron_registration_socket_path(
            &fixture.manager.working_dir_for(&app_id()),
        );
        std::fs::create_dir_all(&socket).unwrap();
        assert!(fixture
            .manager
            .stage(&boot_request(desired_instance(|_| {})))
            .await
            .is_err());
        assert!(fixture.manager.logs.attached().await.is_empty());
        assert!(fixture.manager.cron_registration.attached().await.is_empty());
    }

    #[tokio::test]
    async fn readoption_cleans_up_the_log_attachment_when_the_crontab_socket_cannot_bind() {
        let fixture = fixture();
        fixture.state.put_record(instance_record(|_| {})).await;
        let socket = crate::adapters::cron_registration::cron_registration_socket_path(
            &fixture.manager.working_dir_for(&app_id()),
        );
        std::fs::create_dir_all(&socket).unwrap();
        assert!(fixture.manager.readopt(&app_id()).await.is_err());
        assert!(fixture.manager.logs.attached().await.is_empty());
        assert!(fixture.manager.cron_registration.attached().await.is_empty());
    }

    #[tokio::test]
    async fn a_stop_takes_the_snapshot_with_it_so_nothing_is_woken_back_into_the_old_state() {
        let fixture = fixture();
        let paths = snapshot_paths(&fixture.manager.snapshot_dir, &app_id());
        make_directory(&paths.directory, 0o700).unwrap();
        std::fs::write(&paths.memory_path, b"pretend memory").unwrap();
        write_json(
            &paths.stamp_path,
            &SnapshotStamp {
                firecracker_version: Some(crate::adapters::vm::process::FIRECRACKER_VERSION.to_string()),
                deployment_id: deployment_id(),
                guest_image_version: fixture.manager.guest_image_version.clone(),
                host_boot_id: fixture.manager.processes.boot_id().to_string(),
                slot: 0,
                host: SnapshotHost::this_one().clone(),
            },
        )
        .unwrap();

        fixture.manager.stop(&app_id()).await.unwrap();
        assert!(!paths.directory.exists());
        assert!(!paths.stamp_path.exists());
    }

    #[tokio::test]
    async fn a_microvm_nothing_was_ever_recorded_for_is_still_stopped_and_discarded_cleanly() {
        let fixture = fixture();
        fixture.manager.stop(&app_id()).await.unwrap();
        fixture.manager.discard(&app_id()).await.unwrap();
        assert!(fixture.manager.adopted_app_ids().await.is_empty());
        assert_eq!(fixture.manager.guest_verdict(&app_id()).await, None);
    }

    #[tokio::test]
    async fn a_console_that_holds_nothing_the_guest_said_gives_no_verdict() {
        let fixture = fixture();
        write_console(
            &fixture,
            "[    0.0] Linux version 6.1.180\n[   15.7] reboot: Restarting system\n",
        );
        assert_eq!(fixture.manager.guest_verdict(&app_id()).await, None);
    }

    #[tokio::test]
    async fn the_working_directory_a_boot_uses_is_the_one_the_rest_of_the_host_is_told_about() {
        let fixture = fixture();
        assert_eq!(
            fixture.manager.working_dir(&app_id()),
            fixture.manager.working_dir_for(&app_id())
        );
        assert!(fixture
            .manager
            .working_dir(&app_id())
            .starts_with(&fixture.manager.vm_dir));
        assert_ne!(
            fixture.manager.working_dir(&app_id()),
            fixture
                .manager
                .working_dir(&protocol::AppId::parse("app-2").unwrap())
        );
    }

    #[tokio::test]
    async fn the_status_of_every_app_asked_after_is_answered_for_even_when_none_is_running() {
        let fixture = fixture();
        let neighbour = protocol::AppId::parse("app-2").unwrap();
        let statuses = fixture.manager.statuses(&[app_id(), neighbour.clone()]).await;
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[&app_id()], VmStatus::default());
        assert_eq!(statuses[&neighbour], VmStatus::default());
        assert!(fixture.manager.statuses(&[]).await.is_empty());
    }

    #[tokio::test]
    async fn a_sleep_the_disk_cannot_hold_is_refused_before_the_microvm_is_paused() {
        let mut fixture = fixture();
        // A directory under a regular file is one nobody can make. A path that merely does not
        // exist is no refusal at all for the root a host runs this daemon as: the measure makes
        // the directory first, so /nowhere would simply be created, the disk would read fine, and
        // the sleep would carry on to a microVM that is not there.
        let elsewhere = tempfile::tempdir().unwrap();
        let occupied = elsewhere.path().join("occupied");
        std::fs::write(&occupied, b"").unwrap();
        fixture.manager.snapshot_dir = occupied.join("snapshots");
        fixture
            .state
            .put_record(instance_record(|record| record.health.ever_healthy = true))
            .await;
        let error = fixture
            .manager
            .sleep(SuspendRequest {
                app_id: app_id(),
                deployment_id: deployment_id(),
                slot: nft_render::describe_slot(0, app_id()),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, VmError::SleepRefused { .. }), "{error}");
        assert!(error.message().contains("cannot be measured"), "{error}");
    }

    #[tokio::test]
    async fn a_sleep_is_measured_against_the_snapshots_in_flight_as_well_as_those_on_the_disk() {
        let fixture = fixture();
        fixture
            .state
            .put_record(instance_record(|record| record.health.ever_healthy = true))
            .await;
        let request = SuspendRequest {
            app_id: app_id(),
            deployment_id: deployment_id(),
            slot: nft_render::describe_slot(0, app_id()),
        };
        // Admitted against a disk of its own so that its share, and not the test machine's disk,
        // is what leaves no room for the next.
        let bottomless = crate::adapters::vm::snapshot::SnapshotDisk {
            total_bytes: u64::MAX,
            available_bytes: u64::MAX,
            cache_bytes: 0,
            snapshot_bytes: 0,
        };
        let the_rest_of_the_disk = fixture.manager.in_flight.admit(&bottomless, 1 << 60).unwrap();

        let error = fixture.manager.sleep(request).await.unwrap_err();
        assert!(matches!(error, VmError::SleepRefused { .. }), "{error}");
        assert!(error.message().contains("already hold"), "{error}");
        assert_eq!(
            fixture.manager.in_flight.bytes(),
            1 << 60,
            "a refused sleep takes no share of the disk"
        );

        drop(the_rest_of_the_disk);
        assert_eq!(fixture.manager.in_flight.bytes(), 0);
    }

    #[tokio::test]
    async fn a_legacy_snapshot_is_refused_before_spawning_so_the_waker_can_cold_boot() {
        let fixture = fixture();
        let request = SuspendRequest {
            app_id: app_id(),
            deployment_id: deployment_id(),
            slot: nft_render::describe_slot(0, app_id()),
        };
        let paths = snapshot_paths(&fixture.manager.snapshot_dir, &app_id());
        let staged = StagedSnapshot::create(&fixture.manager.snapshot_dir).unwrap();
        std::fs::write(&staged.paths.state_path, b"state").unwrap();
        std::fs::write(&staged.paths.memory_path, b"memory").unwrap();
        staged
            .publish(&paths, fixture.manager.current_stamp(&request))
            .unwrap();
        fixture
            .manager
            .processes
            .write_record(&crate::adapters::vm::process::VmRecord {
                app_id: app_id(),
                pid: 1,
                host_boot_id: fixture.manager.processes.boot_id().into(),
                started_at_ms: 0,
                exit_code: Some(0),
                signal: None,
                stop_requested: true,
                jail_root: None,
                jail_uid: None,
                frozen: false,
            })
            .unwrap();
        let refused = fixture.manager.wake(request).await.unwrap_err();
        assert!(matches!(refused, VmError::SnapshotUnusable { .. }));
        assert!(refused.message().contains("legacy VMM without a jail"));
        assert_eq!(fixture.manager.processes.read_record(&app_id()).unwrap().pid, 1);
        assert!(!paths.directory.exists());
    }

    #[tokio::test]
    async fn a_wake_with_no_snapshot_kept_at_all_says_so_rather_than_starting_a_cold_guest() {
        let fixture = fixture();
        let error = fixture
            .manager
            .wake(SuspendRequest {
                app_id: app_id(),
                deployment_id: deployment_id(),
                slot: nft_render::describe_slot(0, app_id()),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, VmError::SnapshotUnusable { .. }), "{error}");
        assert!(error.message().contains("kept none"), "{error}");
    }

    #[tokio::test]
    async fn staging_the_same_app_twice_rebuilds_its_config_rather_than_refusing() {
        let fixture = fixture();
        fixture
            .manager
            .stage(&boot_request(desired_instance(|_| {})))
            .await
            .unwrap();
        let changed = desired_instance(|instance| {
            instance.config.command.environment = tenant_environment(&[("MODE", "production")])
        });
        fixture.manager.stage(&boot_request(changed)).await.unwrap();
        let written = config_drive(&fixture.manager.vm_dir.join(app_id().as_str()));
        assert!(written.contains("MODE"), "{written}");
        assert_eq!(fixture.manager.logs.attached().await, vec![app_id()]);
    }

    fn image(directory: &Path, kernel: &[u8], rootfs: &[u8]) -> String {
        std::fs::write(directory.join(GUEST_KERNEL_FILENAME), kernel).unwrap();
        std::fs::write(directory.join(GUEST_ROOTFS_FILENAME), rootfs).unwrap();
        let digest = |bytes: &[u8]| hex::encode(<sha2::Sha256 as sha2::Digest>::digest(bytes));
        let manifest = serde_json::json!({
            "version": "6.1.180-aaaa",
            "artifacts": [
                {"name": GUEST_KERNEL_FILENAME, "sha256": digest(kernel)},
                {"name": GUEST_ROOTFS_FILENAME, "sha256": digest(rootfs)},
            ],
        });
        std::fs::write(
            directory.join(GUEST_MANIFEST_FILENAME),
            serde_json::to_string(&manifest).unwrap(),
        )
        .unwrap();
        "6.1.180-aaaa".to_string()
    }

    #[test]
    fn an_image_that_is_what_its_manifest_describes_names_its_version() {
        let directory = tempfile::tempdir().unwrap();
        let version = image(directory.path(), b"a kernel", b"a root filesystem");
        assert_eq!(verify_guest_image(directory.path()).unwrap(), version);
    }

    #[test]
    fn a_host_with_no_guest_image_refuses_rather_than_failing_at_the_first_deploy() {
        let directory = tempfile::tempdir().unwrap();
        let Err(error) = verify_guest_image(directory.path()) else {
            panic!("a host with nothing to boot a tenant from was accepted");
        };
        assert!(matches!(error, GuestImageError::Missing { .. }), "{error}");
    }

    #[test]
    fn a_kernel_that_is_not_what_the_manifest_describes_is_never_booted() {
        let directory = tempfile::tempdir().unwrap();
        image(directory.path(), b"a kernel", b"a root filesystem");
        std::fs::write(directory.path().join(GUEST_KERNEL_FILENAME), b"something else").unwrap();

        let Err(error) = verify_guest_image(directory.path()) else {
            panic!("a kernel nothing vouches for was accepted");
        };
        assert!(matches!(error, GuestImageError::Altered { .. }), "{error}");
        assert!(error.message().contains(GUEST_KERNEL_FILENAME), "{error}");
    }

    #[test]
    fn a_truncated_root_filesystem_is_caught_before_a_tenant_boots_off_it() {
        let directory = tempfile::tempdir().unwrap();
        image(directory.path(), b"a kernel", b"a root filesystem");
        std::fs::write(directory.path().join(GUEST_ROOTFS_FILENAME), b"a root file").unwrap();
        assert!(verify_guest_image(directory.path()).is_err());
    }

    #[test]
    fn a_root_filesystem_the_manifest_never_mentions_is_refused_rather_than_trusted() {
        let directory = tempfile::tempdir().unwrap();
        image(directory.path(), b"a kernel", b"a root filesystem");
        let manifest = serde_json::json!({"version": "6.1.180", "artifacts": []});
        std::fs::write(
            directory.path().join(GUEST_MANIFEST_FILENAME),
            serde_json::to_string(&manifest).unwrap(),
        )
        .unwrap();
        assert!(verify_guest_image(directory.path()).is_err());
    }

    #[test]
    fn a_manifest_that_names_no_version_is_not_an_image_this_host_can_report() {
        let directory = tempfile::tempdir().unwrap();
        image(directory.path(), b"a kernel", b"a root filesystem");
        for manifest in ["{}", r#"{"version":7}"#, "not json"] {
            std::fs::write(directory.path().join(GUEST_MANIFEST_FILENAME), manifest).unwrap();
            assert!(verify_guest_image(directory.path()).is_err(), "{manifest}");
        }
    }
}
