#[cfg(feature = "schema")]
use std::borrow::Cow;

#[cfg(feature = "schema")]
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};

use crate::domain::*;
use crate::wire::*;

/// The whole of an instance's activation policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum DesiredInstanceState {
    /// Keeps the microVM up.
    Running,
    /// Brings the microVM up for the first deploy and for every request that finds it asleep, and
    /// lets it sleep again once it has been quiet for `idleTimeoutMs`.
    OnRequest,
    /// Takes the microVM down and leaves the app reachable enough to say so.
    Stopped,
}

impl DesiredInstanceState {
    pub fn as_str(self) -> &'static str {
        match self {
            DesiredInstanceState::Running => "running",
            DesiredInstanceState::OnRequest => "on-request",
            DesiredInstanceState::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum DesiredPresence {
    Present,
    Absent,
}

pub const MAX_LAYERS: usize = 8;

/// An object in the store the host's `artifacts.store_url` names, checked against `digest`
/// before anything is made from it. The digest is the whole of what is checked: a `sizeBytes`
/// beside it, which a document used to carry, is read past.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct StoredObject {
    pub digest: Sha256Digest,
    /// Where the object lives in the store.
    pub object_key: ObjectKey,
}

/// One read-only layer of the root filesystem an instance boots into. Layers stack in the order
/// the document lists them, first at the bottom, and the app's volume — or its scratch — is
/// stacked writable over all of them. The kind says what the object is, and so what the host does with it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(!try_from, transform = desired_layer_rules))]
#[serde(
    tag = "kind",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase",
    try_from = "DesiredLayerFields"
)]
pub enum DesiredLayer {
    /// A squashfs or ext4 image, attached as it was uploaded.
    Filesystem {
        #[serde(flatten)]
        object: StoredObject,
    },
    /// One program, packed into an image at `destinationPath` and run the way this host has
    /// always run one: by the guest's own init, with the app's arguments and environment.
    Executable {
        #[serde(flatten)]
        object: StoredObject,
        destination_path: ExecutablePath,
    },
    /// One program the host fetches for itself from `url`, so that nothing has to be put in the
    /// store first. `digest` is the program's: of the response, or of `zipEntry` inside it when the
    /// URL serves a zip, or of `tarEntry` inside it when the URL serves a `.tar.xz`. Nothing is
    /// made from the bytes before they match it. At most one of the two entries is named.
    DownloadedExecutable {
        url: DownloadUrl,
        digest: Sha256Digest,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        zip_entry: Option<ZipEntry>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tar_entry: Option<TarEntry>,
        destination_path: ExecutablePath,
    },
}

// The same layer, read before it is checked: serde reads a tagged enum whole, so a downloaded
// program that names two archives to look in is refused here rather than by whichever reader
// happened to look first.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", rename_all_fields = "camelCase")]
enum DesiredLayerFields {
    Filesystem {
        #[serde(flatten)]
        object: StoredObject,
    },
    Executable {
        #[serde(flatten)]
        object: StoredObject,
        destination_path: ExecutablePath,
    },
    DownloadedExecutable {
        url: DownloadUrl,
        digest: Sha256Digest,
        #[serde(default)]
        zip_entry: Option<ZipEntry>,
        #[serde(default)]
        tar_entry: Option<TarEntry>,
        destination_path: ExecutablePath,
    },
}

impl TryFrom<DesiredLayerFields> for DesiredLayer {
    type Error = InvalidValue;

    fn try_from(fields: DesiredLayerFields) -> Result<Self, Self::Error> {
        Ok(match fields {
            DesiredLayerFields::Filesystem { object } => DesiredLayer::Filesystem { object },
            DesiredLayerFields::Executable {
                object,
                destination_path,
            } => DesiredLayer::Executable {
                object,
                destination_path,
            },
            DesiredLayerFields::DownloadedExecutable {
                zip_entry: Some(_),
                tar_entry: Some(_),
                ..
            } => {
                return Err(InvalidValue::new_public(
                    "a downloaded program is taken from a zipEntry or a tarEntry; name one",
                ))
            }
            DesiredLayerFields::DownloadedExecutable {
                url,
                digest,
                zip_entry,
                tar_entry,
                destination_path,
            } => DesiredLayer::DownloadedExecutable {
                url,
                digest,
                zip_entry,
                tar_entry,
                destination_path,
            },
        })
    }
}

// What `TryFrom<DesiredLayerFields>` refuses, said in the schema's words.
#[cfg(feature = "schema")]
fn desired_layer_rules(schema: &mut schemars::Schema) {
    let downloaded = schema
        .get_mut("oneOf")
        .and_then(serde_json::Value::as_array_mut)
        .and_then(|kinds| {
            kinds.iter_mut().find(|kind| {
                kind.pointer("/properties/kind/const") == Some(&serde_json::json!("downloaded-executable"))
            })
        })
        .and_then(serde_json::Value::as_object_mut);
    if let Some(downloaded) = downloaded {
        downloaded.insert(
            "not".into(),
            serde_json::json!({
                "required": ["zipEntry", "tarEntry"],
                "properties": { "zipEntry": { "type": "string" }, "tarEntry": { "type": "string" } }
            }),
        );
    }
}

impl DesiredLayer {
    pub fn digest(&self) -> &Sha256Digest {
        match self {
            DesiredLayer::Filesystem { object } | DesiredLayer::Executable { object, .. } => &object.digest,
            DesiredLayer::DownloadedExecutable { digest, .. } => digest,
        }
    }

    /// The layer's object in the store; a downloaded layer has none.
    pub fn stored_object(&self) -> Option<&StoredObject> {
        match self {
            DesiredLayer::Filesystem { object } | DesiredLayer::Executable { object, .. } => Some(object),
            DesiredLayer::DownloadedExecutable { .. } => None,
        }
    }
}

/// Where the guest's init lives in a stacked root, and so the one place a program cannot be put.
pub const INIT_PATH: &str = "/sbin/init";

fn is_executable_path(path: &str) -> bool {
    is_guest_path(path) && path != "/" && path != INIT_PATH
}

validated_string!(
    /// Where an executable layer's program sits: a file, so not `/`, and not what starts it.
    ExecutablePath,
    "destinationPath",
    "an absolute path to a file other than /sbin/init",
    is_executable_path,
    {
        "pattern": GUEST_PATH_PATTERN,
        "maxLength": MAX_GUEST_PATH_LENGTH,
        "not": { "enum": ["/", INIT_PATH] }
    }
);

/// A writable root that lives and dies with the microVM, in place of a volume: it starts empty at
/// every cold boot, is kept through a sleep because the snapshot keeps what the guest held, and is
/// gone once the instance is stopped, replaced or expired. It is never a volume: never reported,
/// never checkpointed, never exported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "kebab-case", rename_all_fields = "camelCase")]
pub enum Scratch {
    /// A tmpfs in the guest's own memory, so what is written to it is paid for out of
    /// `config.resources.memoryMib`.
    Memory { mib: std::num::NonZeroU32 },
    /// A sparse ext4 file the host makes at every cold boot and deletes once the microVM is gone,
    /// so what is written to it costs the host's disk rather than the guest's memory.
    Disk { mib: std::num::NonZeroU32 },
}

impl Scratch {
    pub fn mib(self) -> std::num::NonZeroU32 {
        match self {
            Scratch::Memory { mib } | Scratch::Disk { mib } => mib,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(!try_from, transform = desired_instance_rules))]
#[serde(rename_all = "camelCase", try_from = "DesiredInstanceFields")]
pub struct DesiredInstance {
    pub app_id: AppId,
    /// A running instance is replaced when this changes, and only then: a new layer or config
    /// under the same `deploymentId` is not picked up.
    pub deployment_id: DeploymentId,
    /// One of this document's `volumes`, stacked writable over the layers. Absent when `scratch`
    /// is named instead; one of the two always is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_id: Option<VolumeId>,
    /// What the instance writes to when it has no volume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch: Option<Scratch>,
    pub desired_state: DesiredInstanceState,
    /// How long an `on-request` instance stays up after its last request before it sleeps: the
    /// older spelling of `activation.sleepWhen`, refused beside it. 300000 when neither is named.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_ms: Option<IdleTimeoutMs>,
    /// What puts this instance to sleep and what tells the host it is ready. A `sleepWhen` other
    /// than `never` is refused on anything but an `on-request` instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<ActivationPolicy>,
    /// Optional terminal retention for on-request instances: expired hostnames answer HTTP 410.
    /// Removing the policy or changing deploymentId revives the instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<ExpiryPolicy>,
    /// Overrides per-app configuration limits live; omission restores configuration defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<InstanceLimits>,
    /// The root filesystem, bottom layer first. At least one; at most `MAX_LAYERS`.
    pub layers: Vec<DesiredLayer>,
    pub config: AppConfig,
    /// What the HTTP proxy routes to this app's `httpPort`. Empty for an app nothing outside needs
    /// to reach by name.
    pub hostnames: Vec<AppHostname>,
}

impl DesiredInstance {
    /// The policy this instance runs under, whichever of the two spellings its document used.
    /// A document that used neither is one written before either existed, and means what it
    /// meant then.
    pub fn activation(&self) -> ActivationPolicy {
        self.activation.unwrap_or_else(|| ActivationPolicy {
            sleep_when: match self.desired_state {
                DesiredInstanceState::OnRequest => SleepPolicy::TrafficIdle {
                    timeout_ms: self.idle_timeout_ms.unwrap_or(DEFAULT_IDLE_TIMEOUT),
                },
                DesiredInstanceState::Running | DesiredInstanceState::Stopped => SleepPolicy::Never,
            },
        })
    }
}

// The document is read once and refused whole, so a pair of fields that disagree about when an
// instance sleeps is caught here rather than by whichever loop read one of them first.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DesiredInstanceFields {
    app_id: AppId,
    deployment_id: DeploymentId,
    #[serde(default)]
    volume_id: Option<VolumeId>,
    #[serde(default)]
    scratch: Option<Scratch>,
    desired_state: DesiredInstanceState,
    #[serde(default)]
    idle_timeout_ms: Option<IdleTimeoutMs>,
    #[serde(default)]
    activation: Option<ActivationPolicy>,
    #[serde(default)]
    expiry: Option<ExpiryPolicy>,
    #[serde(default)]
    limits: Option<InstanceLimits>,
    layers: Vec<DesiredLayer>,
    config: AppConfig,
    hostnames: Vec<AppHostname>,
}

impl TryFrom<DesiredInstanceFields> for DesiredInstance {
    type Error = InvalidValue;

    fn try_from(fields: DesiredInstanceFields) -> Result<Self, Self::Error> {
        if fields.expiry.is_some() && fields.desired_state != DesiredInstanceState::OnRequest {
            return Err(InvalidValue::new_public(
                "only an on-request instance may name expiry",
            ));
        }
        if fields.activation.is_some() && fields.idle_timeout_ms.is_some() {
            return Err(InvalidValue::new_public(
                "activation and idleTimeoutMs both say when an instance sleeps; name one",
            ));
        }
        if let Some(policy) = &fields.activation {
            if fields.desired_state != DesiredInstanceState::OnRequest
                && policy.sleep_when != SleepPolicy::Never
            {
                return Err(InvalidValue::new_public(
                    "only an on-request instance may name a sleepWhen other than never, because nothing would wake it again",
                ));
            }
        }
        match (&fields.volume_id, fields.scratch) {
            (Some(_), Some(_)) => {
                return Err(InvalidValue::new_public(
                    "volumeId and scratch both say what an instance writes to; name one",
                ))
            }
            (None, None) => {
                return Err(InvalidValue::new_public(
                    "an instance names a volumeId or a scratch, because its root has to be written somewhere",
                ))
            }
            (None, Some(Scratch::Memory { mib })) if mib.get() > fields.config.resources.memory_mib => {
                return Err(InvalidValue::new_public(&format!(
                    "a memory scratch of {mib} MiB cannot fit in the {} MiB the guest is given",
                    fields.config.resources.memory_mib
                )))
            }
            _ => {}
        }
        if fields.layers.is_empty() {
            return Err(InvalidValue::new_public(
                "an instance names at least one layer, because a microVM boots from something",
            ));
        }
        if fields.layers.len() > MAX_LAYERS {
            return Err(InvalidValue::new_public(&format!(
                "an instance names at most {MAX_LAYERS} layers, because a microVM has that many drives to give them"
            )));
        }
        Ok(Self {
            app_id: fields.app_id,
            deployment_id: fields.deployment_id,
            volume_id: fields.volume_id,
            scratch: fields.scratch,
            desired_state: fields.desired_state,
            idle_timeout_ms: fields.idle_timeout_ms,
            activation: fields.activation,
            expiry: fields.expiry,
            limits: fields.limits,
            layers: fields.layers,
            config: fields.config,
            hostnames: fields.hostnames,
        })
    }
}

// What `TryFrom<DesiredInstanceFields>` refuses, said in the schema's words so a document an editor
// passes is one this host takes.
#[cfg(feature = "schema")]
fn desired_instance_rules(schema: &mut schemars::Schema) {
    schema.insert(
        "allOf".into(),
        serde_json::json!([
            { "if": { "required": ["expiry"], "properties": {"expiry": {"type":"object"}} }, "then": { "properties": {"desiredState": {"const":"on-request"}} } },
            { "oneOf": [{ "required": ["volumeId"] }, { "required": ["scratch"] }] }
        ]),
    );
    schema.insert(
        "not".into(),
        serde_json::json!({ "required": ["activation", "idleTimeoutMs"] }),
    );
    schema.insert(
        "if".into(),
        serde_json::json!({
            "required": ["activation"],
            "properties": { "desiredState": { "not": { "const": "on-request" } } }
        }),
    );
    schema.insert(
        "then".into(),
        serde_json::json!({
            "properties": {
                "activation": { "properties": { "sleepWhen": { "properties": { "kind": { "const": "never" } } } } }
            }
        }),
    );
    if let Some(layers) = schema
        .get_mut("properties")
        .and_then(|properties| properties.get_mut("layers"))
        .and_then(serde_json::Value::as_object_mut)
    {
        layers.insert("minItems".into(), serde_json::json!(1));
        layers.insert("maxItems".into(), serde_json::json!(MAX_LAYERS));
    }
}

/// What a volume holds before its app has written a byte: an archive in the store — a tar,
/// gzipped or not, or a zip — unpacked under `destinationPath` in the app's root as the volume is
/// formatted. That happens once, so this is read once: a volume already formatted is its app's,
/// and a change here does nothing to it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct InitialContents {
    #[serde(flatten)]
    pub object: StoredObject,
    /// The directory the archive's entries land under, as the app sees it. Made if no layer holds
    /// it, and given — with everything unpacked into it — to the uid the program runs as.
    pub destination_path: GuestPath,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DesiredVolume {
    pub volume_id: VolumeId,
    pub app_id: AppId,
    pub size_bytes: u64,
    pub desired_state: DesiredPresence,
    /// Absent for a volume that starts empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_contents: Option<InitialContents>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DesiredCheckpoint {
    pub checkpoint_id: CheckpointId,
    pub volume_id: VolumeId,
    pub desired_state: DesiredPresence,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DesiredExport {
    pub export_id: ExportId,
    pub app_id: AppId,
    pub volume_id: VolumeId,
    pub object_key: ObjectKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<TenantEnvironment>,
    pub desired_state: DesiredPresence,
}

/// What one host should be running. The daemon watches this document at the path its
/// `paths.desired_state_file` names and converges on every change to it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct HostDesiredState {
    pub host_id: HostId,
    /// What whoever wrote this document calls this version of it, carried back in the report as
    /// `acceptedRevision`. Nothing on the host reads it: it is not ordered and not checked
    /// against the one before it, so a document may name any version it likes. Required, so that
    /// a control plane can always ask which of its own versions a host is on — and so a host
    /// answering with one nobody wrote is impossible.
    pub revision: Revision,
    /// How many apps this host is to hold a slot for, when that is more than `max_apps` in its
    /// config.toml: the running daemon widens to it in place, with no restart. It never narrows
    /// the host below its configuration, and absent, the configuration alone says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_apps: Option<u32>,
    pub volumes: Vec<DesiredVolume>,
    pub instances: Vec<DesiredInstance>,
    pub checkpoints: Vec<DesiredCheckpoint>,
    pub exports: Vec<DesiredExport>,
}

/// A tenant restart as the host heard of it: what the guest said, and when it said it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReportedRestart {
    pub at: Timestamp,
    #[serde(flatten)]
    pub restart: TenantRestart,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReportedInstance {
    pub app_id: AppId,
    pub deployment_id: DeploymentId,
    pub state: InstanceState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<ReportedMemory>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_port: Option<HostPort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_ipv4: Option<Ipv4Address>,
    /// The layers the running microVM was booted from, bottom first. Empty until one has been.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub layer_digests: Vec<Sha256Digest>,
    /// Times the supervisor inside the guest has restarted the tenant since this host last booted
    /// the app afresh. A restore from a snapshot keeps the count; a cold boot starts it over.
    pub restart_count: u32,
    /// The last of those restarts. Absent until there has been one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_restart: Option<ReportedRestart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    /// When the instance first became what the document asks of it, for the `deploymentId` and
    /// `desiredState` it now carries. Absent while it is still on its way there, and for a
    /// deployment this host was not there to see arrive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub converged_at: Option<Timestamp>,
    /// The latest host activity, including request admission and activation; absent before activity is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_active_at: Option<Timestamp>,
    /// The durable terminal-expiry decision; absent until this deployment expires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expired_at: Option<Timestamp>,
    /// For an `exited` instance, how its program ended: the code it exited with, or 128 plus the
    /// signal that killed it. Otherwise the microVM's own exit code, when it went down by itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<StateMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReportedMemory {
    pub measured_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cgroup: Option<String>,
    /// Resident mappings, with shared pages divided among their users. Snapshot pages may be
    /// charged to the process that wrote them rather than this VM's cgroup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proportional_set_bytes: Option<u64>,
    /// Resident anonymous mappings, excluding file cache and swapped pages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anonymous_set_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<ReportedMemoryLimits>,
    pub current_bytes: u64,
    pub peak_bytes: Option<u64>,
    pub swap_bytes: u64,
    pub high_events: u64,
    pub oom_kills: u64,
    pub pressure_some_us: u64,
    pub pressure_full_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReportedMemoryLimits {
    pub low_bytes: u64,
    /// Null means the kernel imposes no limit.
    pub high_bytes: Option<u64>,
    pub max_bytes: Option<u64>,
    pub swap_max_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReportedVolume {
    pub volume_id: VolumeId,
    pub app_id: AppId,
    pub state: VolumeState,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_prefix: Option<ObjectKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<StateMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReportedCheckpoint {
    pub checkpoint_id: CheckpointId,
    pub volume_id: VolumeId,
    pub state: CheckpointState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference: Option<StateMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<StateMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReportedExport {
    pub export_id: ExportId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_id: Option<CheckpointId>,
    pub state: ExportState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<StateMessage>,
}

/// What one host is running, as the daemon last wrote it to `reported.json` in its
/// `paths.state_dir`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct HostReportedState {
    pub host_id: HostId,
    pub reported_at: Timestamp,
    pub state: HostState,
    /// What the machine has.
    pub capacity: HostCapacity,
    /// What is left once every booted app is taken off.
    pub allocatable: HostCapacity,
    pub versions: HostVersions,
    pub volumes: Vec<ReportedVolume>,
    pub instances: Vec<ReportedInstance>,
    pub checkpoints: Vec<ReportedCheckpoint>,
    pub exports: Vec<ReportedExport>,
    /// The document this host is converging on, as `sha256sum` reads the file it came from.
    /// Absent until one has been taken up. A control plane checks it against the write it made
    /// to learn that this host is on that document rather than on the one before it — which is
    /// what `instances` alone cannot say about a document that changed nothing about an app.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_digest: Option<Sha256Digest>,
    /// The `revision` that document named, if it named one — the control plane's own word for
    /// what this host is converging on, beside the digest that is this host's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_revision: Option<Revision>,
    /// Set when the last document this host was handed was refused — malformed, or not the
    /// document this host reads — and cleared when a readable one is taken up. A control plane
    /// that only reads this file learns from it that its last write did not land, and why.
    #[serde(default)]
    pub message: Option<StateMessage>,
}
