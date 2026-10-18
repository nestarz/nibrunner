#[cfg(feature = "schema")]
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::Range;

#[cfg(feature = "schema")]
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};

use crate::wire::*;

const ENVIRONMENT_RESERVED_NAME: &str = "__proto__";
pub const ENVIRONMENT_NAME_PATTERN: &str = "^[A-Za-z_][A-Za-z0-9_]*$";

pub fn is_environment_name(name: &str) -> bool {
    let mut chars = name.chars();
    name != ENVIRONMENT_RESERVED_NAME
        && matches!(chars.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

const RUNTIME_VALUE_PREFIX: &str = "NIBRUN_";

pub const RUNTIME_VALUE_NAMES: [&str; 2] = ["NIBRUN_HOSTNAME", "NIBRUN_HTTP_PORT"];

fn is_name_character(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

pub fn runtime_references(value: &str) -> impl Iterator<Item = (Range<usize>, &str)> {
    value.match_indices("${").filter_map(|(start, opening)| {
        let rest = &value[start + opening.len()..];
        if !rest.starts_with(RUNTIME_VALUE_PREFIX) {
            return None;
        }
        let end = rest.find(|c| !is_name_character(c))?;
        if rest.as_bytes()[end] != b'}' {
            return None;
        }
        Some((start..start + opening.len() + end + 1, &rest[..end]))
    })
}

pub fn names_offered_runtime_values(value: &str) -> bool {
    runtime_references(value).all(|(_, name)| RUNTIME_VALUE_NAMES.contains(&name))
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SecretString", into = "SecretString")]
pub struct TenantValue(SecretString);

impl TenantValue {
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidValue> {
        let secret = SecretString::parse(value)?;
        if !names_offered_runtime_values(secret.expose()) {
            return Err(InvalidValue::new_public(
                "a tenant value names a runtime value the guest does not offer",
            ));
        }
        Ok(Self(secret))
    }

    pub fn expose(&self) -> &str {
        self.0.expose()
    }
}

impl TryFrom<SecretString> for TenantValue {
    type Error = InvalidValue;
    fn try_from(value: SecretString) -> Result<Self, Self::Error> {
        Self::parse(value.expose())
    }
}

impl From<TenantValue> for SecretString {
    fn from(value: TenantValue) -> SecretString {
        value.0
    }
}

impl std::fmt::Debug for TenantValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(REDACTED)
    }
}

#[cfg(feature = "schema")]
impl JsonSchema for TenantValue {
    fn schema_name() -> Cow<'static, str> {
        "TenantValue".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::TenantValue").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let mut schema = SecretString::json_schema(generator);
        schema.insert(
            "description".into(),
            format!(
                "Only complete `${{NAME}}` references are filled in for NAME in {}. \
                 Bare names, incomplete references and other dollar signs remain literal; \
                 complete references to unknown `NIBRUN_` names are refused.",
                RUNTIME_VALUE_NAMES.join(", ")
            )
            .into(),
        );
        schema
    }
}

#[derive(Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(
    try_from = "BTreeMap<String, TenantValue>",
    into = "BTreeMap<String, TenantValue>"
)]
pub struct TenantEnvironment(BTreeMap<String, TenantValue>);

impl TenantEnvironment {
    pub fn iter(&self) -> impl Iterator<Item = (&str, &TenantValue)> {
        self.0.iter().map(|(name, value)| (name.as_str(), value))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl TryFrom<BTreeMap<String, TenantValue>> for TenantEnvironment {
    type Error = InvalidValue;
    fn try_from(value: BTreeMap<String, TenantValue>) -> Result<Self, Self::Error> {
        if let Some(name) = value.keys().find(|name| !is_environment_name(name)) {
            return Err(InvalidValue::new_public(&format!(
                "{name} is not an environment variable name"
            )));
        }
        Ok(Self(value))
    }
}

impl From<TenantEnvironment> for BTreeMap<String, TenantValue> {
    fn from(value: TenantEnvironment) -> Self {
        value.0
    }
}

impl std::fmt::Debug for TenantEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.0.keys().map(|name| (name, REDACTED)))
            .finish()
    }
}

impl FromIterator<(String, TenantValue)> for TenantEnvironment {
    fn from_iter<T: IntoIterator<Item = (String, TenantValue)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

#[cfg(feature = "schema")]
impl JsonSchema for TenantEnvironment {
    fn schema_name() -> Cow<'static, str> {
        "TenantEnvironment".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::TenantEnvironment").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "object",
            "propertyNames": {
                "pattern": ENVIRONMENT_NAME_PATTERN,
                "not": { "const": ENVIRONMENT_RESERVED_NAME }
            },
            "additionalProperties": generator.subschema_for::<TenantValue>()
        })
    }
}

pub const MAX_ARGUMENTS: usize = 64;
pub const MAX_ARGUMENT_LENGTH: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>", into = "Vec<String>")]
pub struct TenantArguments(Vec<String>);

impl TenantArguments {
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl TryFrom<Vec<String>> for TenantArguments {
    type Error = InvalidValue;
    fn try_from(value: Vec<String>) -> Result<Self, Self::Error> {
        if value.len() > MAX_ARGUMENTS {
            return Err(InvalidValue::new_public("too many arguments"));
        }
        if value.iter().any(|argument| argument.len() > MAX_ARGUMENT_LENGTH) {
            return Err(InvalidValue::new_public("an argument is too long"));
        }
        Ok(Self(value))
    }
}

impl From<TenantArguments> for Vec<String> {
    fn from(value: TenantArguments) -> Self {
        value.0
    }
}

#[cfg(feature = "schema")]
impl JsonSchema for TenantArguments {
    fn schema_name() -> Cow<'static, str> {
        "TenantArguments".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::TenantArguments").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "description": "The arguments the binary is started with.",
            "type": "array",
            "maxItems": MAX_ARGUMENTS,
            "items": { "type": "string", "maxLength": MAX_ARGUMENT_LENGTH }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum AppHostnameKind {
    Platform,
    Custom,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AppHostname {
    pub hostname: Hostname,
    pub kind: AppHostnameKind,
}

/// A port a guest answers on beside its HTTP one, carried to it unread.
///
/// It carries whatever arrives, tcp or udp: a port is a port, the relay in front of a host
/// forwards both for every one in its range, and a guest that listens on only one of them
/// answers the other with a port-unreachable the way any host would.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct InstancePort {
    pub name: PortName,
    pub guest_port: GuestPort,
}

/// What the guest runs once its root is stacked, as uid 65534: `program` with `args`, in
/// `workingDirectory`, with `environment`. The working directory is made if it is not there,
/// given to that uid, and is where the program's persistent state lives, because everything
/// under the root persists on the volume and nowhere else is the program's to write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Command {
    pub program: GuestPath,
    pub args: TenantArguments,
    pub working_directory: GuestPath,
    pub environment: TenantEnvironment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct AppConfig {
    /// The guest port the HTTP proxy sends this app's hostnames to, and the one the health check
    /// probes.
    pub http_port: HttpPort,
    /// What this app answers on besides `httpPort`, if anything.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<InstancePort>,
    pub command: Command,
    pub resources: InstanceResources,
    pub health_check: HealthCheck,
    pub restart_policy: RestartPolicy,
}

/// How many ports an app may name is the host's to say — `proxy.raw.max_ports_per_app` — so what is
/// wrong here is only ever the shape of the list, never its length.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PortsInvalid {
    #[error("{name} is named twice, and each port is reached by its own name")]
    DuplicateName { name: String },
    #[error("guest port {port} is claimed twice, and one port cannot answer two ways")]
    DuplicateGuestPort { port: u16 },
}

impl AppConfig {
    /// Every port this app answers on, the HTTP one first.
    ///
    /// The order is the one the slot's host ports are handed out in, so an app that keeps its
    /// list keeps its ports across a restart.
    pub fn all_ports(&self) -> Vec<InstancePort> {
        let http = InstancePort {
            name: PortName::parse(HTTP_PORT_NAME).expect("a constant this crate wrote"),
            guest_port: GuestPort::new(self.http_port.get()).expect("a port is never zero"),
        };
        std::iter::once(http).chain(self.ports.iter().cloned()).collect()
    }

    pub fn validate_ports(&self) -> Result<(), PortsInvalid> {
        let mut names = std::collections::BTreeSet::new();
        let mut guest_ports = std::collections::BTreeSet::new();
        for port in self.all_ports() {
            if !names.insert(port.name.as_str().to_string()) {
                return Err(PortsInvalid::DuplicateName {
                    name: port.name.as_str().to_string(),
                });
            }
            if !guest_ports.insert(port.guest_port.get()) {
                return Err(PortsInvalid::DuplicateGuestPort {
                    port: port.guest_port.get(),
                });
            }
        }
        Ok(())
    }
}

/// What the HTTP port is called wherever ports are named together.
pub const HTTP_PORT_NAME: &str = "http";

pub const MIN_IDLE_TIMEOUT_MS: u64 = 60_000;
pub const MAX_IDLE_TIMEOUT_MS: u64 = 86_400_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct IdleTimeoutMs(u64);

impl IdleTimeoutMs {
    pub fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for IdleTimeoutMs {
    type Error = InvalidValue;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if (MIN_IDLE_TIMEOUT_MS..=MAX_IDLE_TIMEOUT_MS).contains(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidValue::new_public("idleTimeoutMs is out of range"))
        }
    }
}

impl From<IdleTimeoutMs> for u64 {
    fn from(value: IdleTimeoutMs) -> u64 {
        value.0
    }
}

#[cfg(feature = "schema")]
impl JsonSchema for IdleTimeoutMs {
    fn schema_name() -> Cow<'static, str> {
        "IdleTimeoutMs".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::IdleTimeoutMs").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "integer",
            "minimum": MIN_IDLE_TIMEOUT_MS,
            "maximum": MAX_IDLE_TIMEOUT_MS
        })
    }
}

/// Terminal retention permits no less than one hour of inactivity.
pub const MIN_EXPIRY_IDLE_MS: u64 = 3_600_000;
/// The longest terminal retention window is 36,500 days.
pub const MAX_EXPIRY_IDLE_MS: u64 = 3_153_600_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct ExpiryIdleMs(u64);

impl ExpiryIdleMs {
    pub fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for ExpiryIdleMs {
    type Error = InvalidValue;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if (MIN_EXPIRY_IDLE_MS..=MAX_EXPIRY_IDLE_MS).contains(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidValue::new_public("expiry.idleMs is out of range"))
        }
    }
}

impl From<ExpiryIdleMs> for u64 {
    fn from(value: ExpiryIdleMs) -> u64 {
        value.0
    }
}

#[cfg(feature = "schema")]
impl JsonSchema for ExpiryIdleMs {
    fn schema_name() -> Cow<'static, str> {
        "ExpiryIdleMs".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::ExpiryIdleMs").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "integer",
            "minimum": MIN_EXPIRY_IDLE_MS,
            "maximum": MAX_EXPIRY_IDLE_MS
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExpiryPolicy {
    pub idle_ms: ExpiryIdleMs,
    /// Stop new requests at this instant, then expire after admitted requests finish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
}

impl ExpiryPolicy {
    pub fn deadline_reached(&self, now_ms: i64) -> bool {
        self.deadline.as_ref().is_some_and(|at| now_ms >= at.epoch_ms())
    }
}

pub const MIN_MAX_LIFETIME_MS: u64 = 60_000;
pub const MAX_MAX_LIFETIME_MS: u64 = 604_800_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct MaxLifetimeMs(u64);

impl MaxLifetimeMs {
    pub fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for MaxLifetimeMs {
    type Error = InvalidValue;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if (MIN_MAX_LIFETIME_MS..=MAX_MAX_LIFETIME_MS).contains(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidValue::new_public("ttlMs is out of range"))
        }
    }
}

impl From<MaxLifetimeMs> for u64 {
    fn from(value: MaxLifetimeMs) -> u64 {
        value.0
    }
}

#[cfg(feature = "schema")]
impl JsonSchema for MaxLifetimeMs {
    fn schema_name() -> Cow<'static, str> {
        "MaxLifetimeMs".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::MaxLifetimeMs").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "integer",
            "minimum": MIN_MAX_LIFETIME_MS,
            "maximum": MAX_MAX_LIFETIME_MS
        })
    }
}

/// What puts a running microVM back to sleep. Only an `on-request` instance may carry one that
/// fires: nothing else on this host would wake it again, and the next reconcile pass would bring
/// it straight back up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "kebab-case", rename_all_fields = "camelCase")]
pub enum SleepPolicy {
    Never,
    TrafficIdle { timeout_ms: IdleTimeoutMs },
    MaxLifetime { ttl_ms: MaxLifetimeMs },
}

/// The shortest and longest a sleeping instance's memory snapshot may be kept unwoken.
pub const MIN_SNAPSHOT_TTL_MS: u64 = 60_000;
pub const MAX_SNAPSHOT_TTL_MS: u64 = 2_592_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct SnapshotTtlMs(u64);

impl SnapshotTtlMs {
    pub fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for SnapshotTtlMs {
    type Error = InvalidValue;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if (MIN_SNAPSHOT_TTL_MS..=MAX_SNAPSHOT_TTL_MS).contains(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidValue::new_public("snapshotTtlMs is out of range"))
        }
    }
}

impl From<SnapshotTtlMs> for u64 {
    fn from(value: SnapshotTtlMs) -> u64 {
        value.0
    }
}

#[cfg(feature = "schema")]
impl JsonSchema for SnapshotTtlMs {
    fn schema_name() -> Cow<'static, str> {
        "SnapshotTtlMs".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::SnapshotTtlMs").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "integer",
            "minimum": MIN_SNAPSHOT_TTL_MS,
            "maximum": MAX_SNAPSHOT_TTL_MS
        })
    }
}

/// The shortest and longest a new deployment may be given to become healthy beside the one
/// still serving.
pub const MIN_CUTOVER_TIMEOUT_MS: u64 = 1_000;
pub const MAX_CUTOVER_TIMEOUT_MS: u64 = 3_600_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct CutoverTimeoutMs(u64);

impl CutoverTimeoutMs {
    pub fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for CutoverTimeoutMs {
    type Error = InvalidValue;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if (MIN_CUTOVER_TIMEOUT_MS..=MAX_CUTOVER_TIMEOUT_MS).contains(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidValue::new_public("cutoverTimeoutMs is out of range"))
        }
    }
}

impl From<CutoverTimeoutMs> for u64 {
    fn from(value: CutoverTimeoutMs) -> u64 {
        value.0
    }
}

#[cfg(feature = "schema")]
impl JsonSchema for CutoverTimeoutMs {
    fn schema_name() -> Cow<'static, str> {
        "CutoverTimeoutMs".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::CutoverTimeoutMs").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "integer",
            "minimum": MIN_CUTOVER_TIMEOUT_MS,
            "maximum": MAX_CUTOVER_TIMEOUT_MS
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ActivationPolicy {
    pub sleep_when: SleepPolicy,
    /// How long the memory snapshot of an instance asleep is kept without a wake. Past that it is
    /// deleted, and the next request boots the instance afresh. Absent, it is kept until the next
    /// wake. Only an instance that sleeps has a snapshot for this to apply to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_ttl_ms: Option<SnapshotTtlMs>,
    /// Whether the host boots the instance once it is declared, or leaves it to its first request.
    #[serde(default, skip_serializing_if = "FirstBoot::is_eager")]
    pub first_boot: FirstBoot,
    /// How long a new `deploymentId` is given to become healthy beside the deployment still
    /// serving, before its hostnames are moved over to it and the old one is stopped. A new
    /// deployment that is not healthy by then is stopped and the old one keeps serving. Absent,
    /// the old deployment is stopped before the new one starts. An instance with a volume is
    /// always replaced that way, since two guests cannot write one disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cutover_timeout_ms: Option<CutoverTimeoutMs>,
}

/// When an `on-request` instance is first booted. `lazy` leaves it as one asleep with nothing to
/// wake from: its hostnames answer, but neither its layers nor any memory are held for it until a
/// request fetches and boots it. Refused on anything but an `on-request` instance, which nothing would wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum FirstBoot {
    #[default]
    Eager,
    Lazy,
}

impl FirstBoot {
    pub fn is_eager(&self) -> bool {
        *self == FirstBoot::Eager
    }
}

pub const DEFAULT_IDLE_TIMEOUT_MS: u64 = 300_000;
pub const DEFAULT_IDLE_TIMEOUT: IdleTimeoutMs = IdleTimeoutMs(DEFAULT_IDLE_TIMEOUT_MS);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", try_from = "RawInstanceResources")]
pub struct InstanceResources {
    pub vcpu_count: u32,
    pub memory_mib: u32,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(rename = "InstanceResources"))]
#[serde(rename_all = "camelCase")]
struct RawInstanceResources {
    vcpu_count: u32,
    /// A multiple of 2: guest memory is backed by transparent huge pages, which Firecracker
    /// requires in 2 MiB multiples.
    #[cfg_attr(feature = "schema", schemars(extend("multipleOf" = 2)))]
    memory_mib: u32,
}

impl TryFrom<RawInstanceResources> for InstanceResources {
    type Error = InvalidValue;
    fn try_from(raw: RawInstanceResources) -> Result<Self, Self::Error> {
        if !raw.memory_mib.is_multiple_of(2) {
            return Err(InvalidValue::new_public(
                "memoryMib must be a multiple of 2: guest memory is backed by 2 MiB huge pages",
            ));
        }
        Ok(Self {
            vcpu_count: raw.vcpu_count,
            memory_mib: raw.memory_mib,
        })
    }
}

pub const DEFAULT_INSTANCE_RESOURCES: InstanceResources = InstanceResources {
    vcpu_count: 1,
    memory_mib: 256,
};

/// What the host enforces on an instance beyond what `resources` gives it. Every field is optional
/// and applied live, without replacing the deployment: a running microVM's cgroup is rewritten and
/// the proxy's limit takes the next request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstanceLimits {
    /// Requests the proxy carries to this instance at once, in place of the host's
    /// `proxy.http.concurrency.app_concurrent`. Its host-wide limit still applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrent: Option<std::num::NonZeroU16>,
    /// Host CPU time in percent of one core, enforced as a `cpu.max` quota that only ever tightens
    /// the `vcpuCount` cores `resources` gives: `25` is a quarter of a core, and a value above
    /// `vcpuCount × 100` changes nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_percent: Option<std::num::NonZeroU16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryPolicy>,
}

/// How the kernel treats this instance's memory when the host runs short. None of it changes the
/// guest's RAM or the cgroup's hard ceiling, which `resources` decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemoryPolicy {
    /// Working-set target, capped by the hard memory budget. Does not resize guest RAM.
    pub target_mib: std::num::NonZeroU32,
    /// Maximum uncompressed bytes allowed in host swap, in MiB. Zero disables swap.
    pub swap_mib: u32,
    pub priority: MemoryPriority,
}

/// `production` keeps its target resident (`memory.low`) and is the last the OOM killer picks;
/// `preview` and `build` are throttled at their target (`memory.high`) and picked first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum MemoryPriority {
    Production,
    Standard,
    Preview,
    Build,
}

/// What tells this host an instance is well — and so what a wake waits for before it hands the
/// caller on, and what the instance's liveness is read from after that. Named in every document:
/// a default would be a kind nobody chose, and the one that looks obvious lies. A TCP connect is
/// answered by the guest kernel's accept queue whether or not the process behind it will ever
/// read the request, so a program that has stopped answering passes it for as long as it lives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "kebab-case", rename_all_fields = "camelCase")]
pub enum HealthCheck {
    /// `path`, requested on `httpPort`, answers 2xx.
    Http {
        path: String,
        #[serde(flatten)]
        probe: Probe,
    },
    /// A connection to `httpPort` is accepted. Only that — for a port that does not speak HTTP.
    Tcp {
        #[serde(flatten)]
        probe: Probe,
    },
    /// The microVM is up and `httpPort` has accepted a connection once: the tenant is listening.
    /// That is all it is asked, and only until it answers — a guest this host did not build
    /// answers no path it was not told about — so its liveness after that is the microVM being up.
    BootCompleted,
}

/// How a port is asked, and how many answers either way it takes to change the verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Probe {
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub grace_period_ms: u64,
    pub healthy_threshold: u32,
    pub unhealthy_threshold: u32,
}

/// A microVM is up or it is not, and one reading either way says which. What `boot-completed`
/// runs the same state machine on as a probed port: the grace is how long its tenant has to start
/// listening, and the timeout is on the one connect that finds it so.
const OF_THE_MICROVM: Probe = Probe {
    interval_ms: 5_000,
    timeout_ms: 2_000,
    grace_period_ms: 30_000,
    healthy_threshold: 1,
    unhealthy_threshold: 1,
};

impl HealthCheck {
    /// Whether liveness is read from a port inside the guest, or from the microVM being up.
    pub fn probes_a_port(&self) -> bool {
        !matches!(self, HealthCheck::BootCompleted)
    }

    pub fn probe(&self) -> &Probe {
        match self {
            HealthCheck::Http { probe, .. } | HealthCheck::Tcp { probe } => probe,
            HealthCheck::BootCompleted => &OF_THE_MICROVM,
        }
    }

    pub fn probe_mut(&mut self) -> Option<&mut Probe> {
        match self {
            HealthCheck::Http { probe, .. } | HealthCheck::Tcp { probe } => Some(probe),
            HealthCheck::BootCompleted => None,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            HealthCheck::Http { .. } => "http",
            HealthCheck::Tcp { .. } => "tcp",
            HealthCheck::BootCompleted => "boot-completed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RestartPolicy {
    pub max_restarts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub backoff_factor: f64,
    pub reset_after_ms: u64,
}

pub const DEFAULT_RESTART_POLICY: RestartPolicy = RestartPolicy {
    max_restarts: 5,
    initial_backoff_ms: 500,
    max_backoff_ms: 30_000,
    backoff_factor: 2.0,
    reset_after_ms: 60_000,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    Pending,
    Starting,
    Running,
    /// Paused in place, its memory kept: a quiet app the host stopped scheduling before its sleep
    /// deadline. A request resumes it without a restore.
    Frozen,
    Unhealthy,
    Stopping,
    Stopped,
    Idle,
    Expired,
    Failed,
    /// A program run once (`maxRestarts: 0`) that ended on its own, whatever its exit status:
    /// `lastExitCode` carries that status, and the instance is not started again until it is
    /// deployed afresh. `failed` stays for a guest that never ran it, or went down without saying
    /// how it ended.
    Exited,
}

pub const INSTANCE_STATES: [InstanceState; 11] = [
    InstanceState::Pending,
    InstanceState::Starting,
    InstanceState::Running,
    InstanceState::Frozen,
    InstanceState::Unhealthy,
    InstanceState::Stopping,
    InstanceState::Stopped,
    InstanceState::Idle,
    InstanceState::Expired,
    InstanceState::Failed,
    InstanceState::Exited,
];

impl InstanceState {
    pub fn as_str(self) -> &'static str {
        match self {
            InstanceState::Pending => "pending",
            InstanceState::Starting => "starting",
            InstanceState::Running => "running",
            InstanceState::Frozen => "frozen",
            InstanceState::Unhealthy => "unhealthy",
            InstanceState::Stopping => "stopping",
            InstanceState::Stopped => "stopped",
            InstanceState::Idle => "idle",
            InstanceState::Expired => "expired",
            InstanceState::Failed => "failed",
            InstanceState::Exited => "exited",
        }
    }
}

pub const DEFAULT_VOLUME_SIZE_BYTES: u64 = 8_589_934_592;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum VolumeState {
    Pending,
    Ready,
    Detached,
    Deleted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum CheckpointState {
    Pending,
    Ready,
    Deleted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum ExportState {
    Pending,
    Preparing,
    Ready,
    Failed,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct HostVersions {
    pub agent: String,
    pub guest_image: String,
    pub zerofs: String,
    pub firecracker: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct HostCapacity {
    pub vcpu_count: u32,
    pub memory_mib: u64,
    pub cache_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum HostState {
    Registering,
    Ready,
    Draining,
    Unreachable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ComputeUsage {
    pub memory_total_bytes: u64,
    pub memory_used_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_share: Option<f64>,
    pub measured_at: Timestamp,
}

/// What an app has used since this host first saw it. Every field only ever grows, so what it cost
/// over any stretch is the difference between two readings of it, and a reading that was missed
/// costs nothing but resolution. A field that went down is a counter this host restarted.
///
/// Time is metered in two, because a running app holds the memory it was promised and an idle one
/// holds only the disk its snapshot sits on. Which of those is worth what, this does not say.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UsageMeters {
    pub running_ms: u64,
    pub idle_ms: u64,
    /// Summed across the vCPUs the app was given, so a two-vCPU app that stayed busy for a second
    /// spent two seconds of it.
    pub cpu_ms: u64,
    /// What reached the guest, and what it put back on the wire. Only what was let out is
    /// counted as sent: a packet the ruleset rejected never left, so nobody is charged for it.
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    /// Disk is a level rather than a flow, so what is metered is the level multiplied by the time
    /// it was held: what was set aside for the app, and what its guest reported having filled.
    /// Mebibyte-seconds, because byte-milliseconds of a large volume outrun a `u64` in months.
    pub disk_provisioned_mib_seconds: u64,
    pub disk_used_mib_seconds: u64,
}

/// What one entry in a directory is. Anything that is neither a file nor a directory — a socket,
/// a device node, a symbolic link — is `other`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum FilesystemEntryKind {
    File,
    Directory,
    Other,
}

pub const MAX_GUEST_PATH_LENGTH: usize = 4096;
pub const GUEST_PATH_PATTERN: &str = r#"^/$|^(/(?!\.\.?(/|$))[^/\\"'\x00-\x1f]+)+$"#;

pub fn is_guest_path(path: &str) -> bool {
    if path.len() > MAX_GUEST_PATH_LENGTH || !path.starts_with('/') {
        return false;
    }
    if path == "/" {
        return true;
    }
    path[1..].split('/').all(|segment| {
        !segment.is_empty()
            && segment != "."
            && segment != ".."
            && segment
                .chars()
                .all(|c| !matches!(c, '/' | '\\' | '"' | '\'') && (c as u32) > 0x1f)
    })
}

validated_string_public!(
    GuestPath,
    "a guest path",
    is_guest_path,
    { "pattern": GUEST_PATH_PATTERN, "maxLength": MAX_GUEST_PATH_LENGTH }
);

impl GuestPath {
    pub fn root() -> Self {
        Self("/".to_string())
    }
}

/// One entry of a directory, as the guest measured it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FilesystemEntry {
    /// The entry alone, not the path it was reached by.
    pub name: String,
    pub kind: FilesystemEntryKind,
    /// What the entry takes, as the guest's own filesystem reports it.
    pub size_bytes: u64,
    pub modified_at: Timestamp,
}

pub const DIRECTORY_ENTRY_LIMIT: usize = 1000;

/// What one directory inside a guest holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DirectoryListing {
    /// The directory listed, as it was asked for.
    pub path: GuestPath,
    /// Sorted by `name`. `lost+found` is left out of the guest's root, because the filesystem
    /// the volume was made with put it there and no app did.
    pub entries: Vec<FilesystemEntry>,
    /// The directory held more entries than one answer carries, and `entries` is cut.
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FilesystemUsage {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub measured_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TenantLogStream {
    Stdout,
    Stderr,
}

impl TenantLogStream {
    pub fn as_str(self) -> &'static str {
        match self {
            TenantLogStream::Stdout => "stdout",
            TenantLogStream::Stderr => "stderr",
        }
    }
}

/// How a tenant process ended: the code it exited with, or the signal that killed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum TenantExit {
    Code(i32),
    Signal(i32),
}

impl TenantExit {
    /// The one number a shell reports for either: the code, or 128 plus the signal.
    pub fn status(self) -> i32 {
        match self {
            TenantExit::Code(code) => code,
            TenantExit::Signal(signal) => 128 + signal,
        }
    }
}

/// One restart of the tenant by the supervisor inside its guest, as the guest reported it. The
/// host never sees the process, so this is the whole of what it knows about why the tenant went
/// down and came back: which of the budget this restart was, how the tenant ended, the sentence
/// the guest printed for it, and how long it waited before starting the tenant again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TenantRestart {
    /// 1 for the first restart since the tenant last stayed up long enough to earn its budget
    /// back, up to `budget`.
    pub attempt: u32,
    pub budget: u32,
    pub exit: TenantExit,
    pub reason: StateMessage,
    pub backoff_ms: u64,
}
