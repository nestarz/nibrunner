use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;

pub const DEFAULT_CONFIG_FILE: &str = "/etc/nibrunner/config.toml";
pub const CONFIG_FILE_VARIABLE: &str = "NIBRUNNER_CONFIG";

const MAX_STORAGE_PREFIX_BYTES: usize = 512;
const MEBIBYTES_PER_GIBIBYTE: u64 = 1024;
const BYTES_PER_MEBIBYTE: u64 = 1024 * 1024;

/// Where ZeroFS serves its own scrape page, on loopback. It is a constant rather than a key
/// because it is nibrun's, and `nibrunnerd install` renders it into ZeroFS's config — but a host
/// that also serves `[metrics]` has to be kept off it, which is why this is here and not there.
pub const ZEROFS_PROMETHEUS_PORT: u16 = 9091;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{path} could not be read: {reason}")]
    Unreadable { path: String, reason: String },
    #[error("{path} is not a configuration this host can read: {reason}")]
    Malformed { path: String, reason: String },
    #[error("{field} is not {rule}")]
    Invalid { field: String, rule: String },
}

impl ConfigError {
    pub fn message(&self) -> String {
        self.to_string()
    }

    fn invalid(field: &str, rule: impl std::fmt::Display) -> Self {
        Self::Invalid {
            field: field.to_string(),
            rule: rule.to_string(),
        }
    }
}

/// Where a volume's blocks live. The settings travel with the backend that reads them, so a host
/// cannot name a zerofs mount it will never use, nor pick zerofs and leave it unaddressed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumeBackend {
    LocalFile,
    Zerofs(Box<ZerofsSettings>),
}

impl VolumeBackend {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LocalFile => "local-file",
            Self::Zerofs(_) => "zerofs",
        }
    }

    pub fn zerofs(&self) -> Option<&ZerofsSettings> {
        match self {
            Self::LocalFile => None,
            Self::Zerofs(settings) => Some(settings.as_ref()),
        }
    }
}

/// Everything `nibrunnerd install` needs to lay ZeroFS down, and everything the daemon needs to
/// talk to it once systemd has it running. The two config files this describes are rendered from
/// here rather than written beside here, so the cache size the daemon reserves against and the
/// cache size ZeroFS takes are one answer instead of two that drift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZerofsSettings {
    pub binary: PathBuf,
    pub config_file: PathBuf,
    pub mount_path: PathBuf,
    pub nbd_socket_path: PathBuf,
    pub ninep_socket_path: PathBuf,
    pub rpc_socket_path: PathBuf,
    pub storage_url: String,
    pub cache_dir: PathBuf,
    /// Held as mebibytes because that is what the rest of this daemon reserves and reports in,
    /// but stated in the file as whole gibibytes — which is all `cache_gigabytes` can read back
    /// out of the rendered config, so a fraction here would reserve against a number ZeroFS is
    /// not taking.
    pub cache_disk_mib: u64,
    pub cache_memory_mib: u64,
    pub checkpoint_runtime_dir: PathBuf,
    pub checkpoint_config_file: PathBuf,
    pub checkpoint_cache_dir: PathBuf,
}

/// Optional limits on requests through the hostname router. Omission preserves unrestricted ingress.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HttpAdmission {
    /// Requests across all routed apps, including those waiting for a wake or streaming a body.
    pub host_concurrent: std::num::NonZeroU16,
    /// Default per-app limit. Aliases of an app share this capacity.
    pub app_concurrent: std::num::NonZeroU16,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub apps: std::collections::BTreeMap<protocol::AppId, std::num::NonZeroU16>,
}

/// Optional host-enforced budgets for each Firecracker process, including its guest memory.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VmBudget {
    /// 100 is one host CPU. This does not change the guest's vCPU count.
    pub cpu_percent: std::num::NonZeroU16,
    /// Includes guest RAM and Firecracker overhead. Exceeding it can kill the VM.
    pub memory_mib: std::num::NonZeroU32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VmBudgets {
    pub default: VmBudget,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub apps: std::collections::BTreeMap<protocol::AppId, VmBudget>,
}

/// Where the world reaches an app on this host.
///
/// Every way in is a section under here, and each is absent or complete: there is no
/// half-configured listener to warn about at startup because there is no way to write one. Each
/// binds an address of its own, because each faces a different machine — HTTP arrives from the
/// edge that terminates TLS for it, a raw port from the relay that publishes it — and a host that
/// put both on one address would be saying they arrive from the same place.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProxyConfig {
    pub http: Option<HttpListener>,
    pub raw: Option<RawPorts>,
}

/// The one HTTP listener.
///
/// One per host and one per guest: a guest's hostname resolves to this host, and this is what
/// carries it to the one port that guest answers HTTP on. One rather than a plain port beside a
/// TLS port, because two would serve every app unencrypted and encrypted at once — the plain port
/// a TLS host may add, `redirect_from_port`, only ever moves a visitor to the encrypted one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpListener {
    pub listen_address: IpAddr,
    pub port: u16,
    /// Absent serves plain HTTP, which is what a host behind an edge that terminates TLS wants.
    pub tls: Option<TlsMaterial>,
    /// A second, plain port that answers every request with a redirect to the same host and path
    /// over TLS. It carries no app: a visitor who reaches it is moved, never served.
    pub redirect_from_port: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsMaterial {
    pub certificate: PathBuf,
    pub key: PathBuf,
    /// Naming a trust pool makes a caller's own certificate the price of the handshake.
    pub client_ca: Option<PathBuf>,
}

/// The ports a guest may answer on beside its HTTP one, carried to it unread.
///
/// A raw port carries a protocol this host does not speak — ssh, DNS, WireGuard — so nothing can
/// route it by name and it is reached at a port of its own. Which protocol carries each is the
/// document's to say, port by port. The HTTP listener is not one of these and is never counted.
///
/// The address is the one a relay reaches this host on. A raw port published to the world is an
/// address a tenant hands to its own users, and that is a machine of its own; this host binds
/// where that machine can see it and nowhere else. Absent is a host that carries nothing raw, and
/// a document asking one of those for such a port is refused rather than quietly left unreachable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPorts {
    pub listen_address: IpAddr,
    /// Bounded by what a slot reserves past the port its HTTP listener takes.
    pub max_ports_per_guest: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsConfig {
    pub port: u16,
    pub listen_address: IpAddr,
}

/// Where something on this host asks what a guest holds.
///
/// A unix socket rather than a port, because the answer is a tenant's own files and the only
/// callers are on this machine. It answers reads and takes no input: what this host runs is still
/// the document's to say and nothing else's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesystemConfig {
    pub socket: PathBuf,
}

/// How much of each app's output stays on disk. The newest `keep_bytes_per_app`, in two files:
/// what the sink writes to, and the one before it. Nothing in this daemon reads them back; they
/// are there for whoever tails them, and the cap is there because the disk they are on is the one
/// every other tenant's volume cache and snapshots are on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogsConfig {
    pub keep_bytes_per_app: u64,
}

/// 256 MiB: weeks of an app that logs a line per request, and about a minute of the one that
/// wrote 5 GB in twenty — still enough to read what it was doing when someone looked.
impl Default for LogsConfig {
    fn default() -> Self {
        Self {
            keep_bytes_per_app: 256 * BYTES_PER_MEBIBYTE,
        }
    }
}

/// Where the starting point keeps everything of this host's, which a Linux distribution would
/// put here. Named so that what `install` measures before there is a configuration is the disk
/// the configuration it then writes will name.
pub const STARTER_STATE_DIR: &str = "/var/lib/nibrunner";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostConfig {
    pub http_admission: Option<HttpAdmission>,
    pub max_concurrent_vm_starts: Option<std::num::NonZeroU16>,
    pub vm_budgets: Option<VmBudgets>,
    /// How many apps this host is laid out for. Everything that counts slots follows from it —
    /// the ring the allocator walks, the loopback ports reserved, the nbd minors the module is
    /// loaded with, the conntrack table's size, what the metrics page calls the total — and
    /// nothing holds a copy of it.
    pub max_apps: u32,
    pub state_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub snapshot_dir: PathBuf,
    pub guest_image_dir: PathBuf,
    pub firecracker_dir: PathBuf,
    pub desired_state_file: PathBuf,
    pub versions_file: PathBuf,
    pub artifact_store_url: String,
    pub storage_prefix: String,
    pub volumes: VolumeBackend,
    pub allowed_host_tcp_endpoints: Vec<SocketAddr>,
    pub denied_egress_addresses_v4: Vec<String>,
    pub denied_egress_addresses_v6: Vec<String>,
    pub proxy: ProxyConfig,
    pub metrics: Option<MetricsConfig>,
    pub filesystem: Option<FilesystemConfig>,
    pub logs: LogsConfig,
    pub export_store_url: String,
    pub export_staging_dir: PathBuf,
}

impl HostConfig {
    pub fn in_state_dir(&self, name: &str) -> PathBuf {
        self.state_dir.join(name)
    }

    pub fn state_db_file(&self) -> PathBuf {
        self.in_state_dir("state.db")
    }

    pub fn instances_file(&self) -> PathBuf {
        self.in_state_dir("instances.json")
    }

    pub fn slots_file(&self) -> PathBuf {
        self.in_state_dir("slots.json")
    }

    pub fn slot_cursor_file(&self) -> PathBuf {
        self.in_state_dir("slot-cursor.json")
    }

    pub fn activity_file(&self) -> PathBuf {
        self.in_state_dir("activity.json")
    }

    pub fn deleted_volumes_file(&self) -> PathBuf {
        self.in_state_dir("deleted-volumes.json")
    }

    pub fn artifact_cache_dir(&self) -> PathBuf {
        self.in_state_dir("artifacts")
    }

    pub fn vm_dir(&self) -> PathBuf {
        self.in_state_dir("vm")
    }

    pub fn volumes_dir(&self) -> PathBuf {
        self.in_state_dir("volumes")
    }

    /// Where a volume's initial contents are laid out for the format that copies them in.
    pub fn initial_contents_dir(&self) -> PathBuf {
        self.in_state_dir("initial-contents")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.in_state_dir("logs")
    }
}

mod file {
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    /// Every path here is one of this host's own, so each is absolute.
    const ABSOLUTE_PATH: &str = "^/";
    /// `s3://bucket`, with or without a prefix under it, or an absolute path.
    const STORE_URL: &str = "^(s3://[^/]+|/)";
    /// No leading or trailing `/`, and no empty, `.` or `..` segment.
    const STORAGE_PREFIX: &str = r"^(?!\.\.?(/|$))[^/]+(/(?!\.\.?(/|$))[^/]+)*$";
    const CIDR_V4: &str = r"^[0-9]{1,3}(\.[0-9]{1,3}){3}/[0-9]{1,2}$";
    const CIDR_V6: &str = "^[0-9A-Fa-f.]*:[0-9A-Fa-f:.]*/[0-9]{1,3}$";

    /// A host's `config.toml`. No key has a default: a key its section declares and the file
    /// omits is refused by name, and so is a key no section declares. What may be absent is a whole
    /// section — `[proxy]`, `[metrics]`, `[volumes.zerofs]` — and one that is present is filled in
    /// completely.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "HostConfig")]
    pub(super) struct ConfigFile {
        /// Absent preserves unlimited concurrent HTTP requests. Changes require `nibrunnerd start`.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) http_admission: Option<super::HttpAdmission>,
        /// Optional host-wide bound on simultaneous VM boots and snapshot restores.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) max_concurrent_vm_starts: Option<std::num::NonZeroU16>,
        /// Absent preserves VM processes without additional cgroup limits.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) vm_budgets: Option<super::VmBudgets>,
        /// How many apps this host is laid out for. Everything that counts slots follows from it:
        /// the slot ring, the loopback ports reserved from 21000, the nbd minors on a zerofs host,
        /// the kernel's conntrack table at 1024 entries an app, what the metrics page calls the
        /// total. `install` measures what this machine holds and writes the least of memory, disk
        /// and ports; `start` says the three against what is set.
        #[schemars(range(min = 1, max = nft_render::most_apps_the_ports_fit()))]
        pub(super) max_apps: Option<u32>,
        /// Where this host keeps what is its own.
        pub(super) paths: Option<Paths>,
        /// Where the layers a document names come from.
        pub(super) artifacts: Option<Artifacts>,
        /// Where volumes live, and how.
        pub(super) volumes: Option<Volumes>,
        /// Where a checkpoint goes when the document asks for it as a bundle.
        pub(super) exports: Option<Exports>,
        /// What a guest is denied, by name.
        pub(super) network: Option<Network>,
        /// Absent serves nothing: no hostname, no raw port.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) proxy: Option<Proxy>,
        /// Absent is a host that scrapes nothing.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) metrics: Option<Metrics>,
        /// Absent is a host nothing can ask about a guest's files.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) filesystem: Option<Filesystem>,
        /// Absent keeps the newest 256 MiB of each app's output.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) logs: Option<Logs>,
    }

    /// Every one an absolute path, and every one this host's alone.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "paths")]
    pub(super) struct Paths {
        /// Everything this host keeps, `state.db` included.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) state_dir: Option<String>,
        /// Sockets and pidfiles that outlive the daemon.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) runtime_dir: Option<String>,
        /// Where a sleeping app's memory goes. On a zerofs host it shares its disk with the cache.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) snapshot_dir: Option<String>,
        /// `vmlinux`, `rootfs.ext4` and `manifest.json`, put there by `install`.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) guest_image_dir: Option<String>,
        /// The document this host watches and converges on.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) desired_state_file: Option<String>,
        /// What `install` stamped what it laid down into, read back into `reported.json`.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) versions_file: Option<String>,
    }

    /// One store, in S3 or on this disk.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "artifacts")]
    pub(super) struct Artifacts {
        /// `s3://bucket[/prefix]`, or an absolute path.
        #[schemars(pattern(STORE_URL))]
        pub(super) store_url: Option<String>,
    }

    /// The store a bundle is put in, and the disk it is assembled on first.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "exports")]
    pub(super) struct Exports {
        /// `s3://bucket[/prefix]`, or an absolute path.
        #[schemars(pattern(STORE_URL))]
        pub(super) store_url: Option<String>,
        /// A bundle is assembled here and removed after.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) staging_dir: Option<String>,
    }

    /// The backend, and the prefix every volume on this host is under.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "volumes")]
    #[schemars(extend(
        "if" = { "properties": { "backend": { "const": "zerofs" } } },
        "then" = { "required": ["zerofs"] },
        "else" = { "not": { "required": ["zerofs"] } }
    ))]
    pub(super) struct Volumes {
        /// `local-file` is sparse files under `paths.state_dir`, and no export. `zerofs` is blocks
        /// in an object store, reached from the guest over NBD.
        #[schemars(extend("enum" = ["local-file", "zerofs"]))]
        pub(super) backend: Option<String>,
        /// Where this host's volumes live under the store: 1 to 512 bytes, no leading or trailing
        /// `/`, no empty, `.` or `..` segment. One host, not one app: every tenant here shares it,
        /// and deleting it destroys all of them.
        #[schemars(length(min = 1, max = super::MAX_STORAGE_PREFIX_BYTES), pattern(STORAGE_PREFIX))]
        pub(super) storage_prefix: Option<String>,
        /// Required by the `zerofs` backend, refused by `local-file`.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) zerofs: Option<Zerofs>,
    }

    /// Everything `install` needs to lay ZeroFS down and everything the daemon needs to reach it
    /// once systemd has it running. Its two configuration files are rendered from here.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "volumes.zerofs")]
    pub(super) struct Zerofs {
        /// Where `install` puts ZeroFS.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) binary: Option<String>,
        /// Rendered by `install`.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) config_file: Option<String>,
        /// This host's own view of the filesystem.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) mount_path: Option<String>,
        /// Where ZeroFS serves NBD, which is what a guest's disk is.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) nbd_socket_path: Option<String>,
        /// Where ZeroFS serves 9P, which is how the host reaches the filesystem itself.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) ninep_socket_path: Option<String>,
        /// Where ZeroFS answers RPC.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) rpc_socket_path: Option<String>,
        /// `s3://bucket/prefix`, or an absolute path.
        #[schemars(pattern(STORE_URL))]
        pub(super) storage_url: Option<String>,
        /// The disk cache of the object store.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) cache_dir: Option<String>,
        /// Whole gibibytes, more than 0. Size it against the disk it is on, which it shares with
        /// `paths.snapshot_dir`: a full one breaks the filesystem every app on the host runs from.
        #[schemars(range(min = 1))]
        pub(super) cache_disk_gib: Option<u64>,
        /// Whole gibibytes, more than 0. Held back from what any guest may be promised.
        #[schemars(range(min = 1))]
        pub(super) cache_memory_gib: Option<u64>,
        /// Where the checkpoint reader an export starts keeps its socket.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) checkpoint_runtime_dir: Option<String>,
        /// Rendered by `install`.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) checkpoint_config_file: Option<String>,
        /// The checkpoint reader's own cache.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) checkpoint_cache_dir: Option<String>,
    }

    /// The ranges a guest is denied by name, on top of the blanket rules: public addresses that are
    /// still yours, and that a tenant must not reach. Both may be empty; both must be there.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "network")]
    pub(super) struct Network {
        /// Exact host TCP endpoints guests may call. Absent permits no guest-initiated host connections.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) host_tcp: Option<HostTcp>,
        /// Each `a.b.c.d/n`, `n` at most 32.
        #[schemars(inner(pattern(CIDR_V4)))]
        pub(super) denied_egress_addresses_v4: Option<Vec<String>>,
        /// Each `addr/n`, `n` at most 128.
        #[schemars(inner(pattern(CIDR_V6)))]
        pub(super) denied_egress_addresses_v6: Option<Vec<String>>,
    }

    /// Named egress denies take precedence. Other host addresses and ports remain isolated.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "network.host_tcp")]
    pub(super) struct HostTcp {
        /// Exact IP:port pairs, such as ["203.0.113.10:443", "[2001:db8::10]:443"].
        pub(super) endpoints: Option<Vec<String>>,
    }

    /// Every way in. Each section under here is absent or complete, and each binds an address of
    /// its own, because each faces a different machine.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "proxy")]
    pub(super) struct Proxy {
        /// The one HTTP listener. Absent, a document naming a hostname is refused.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) http: Option<Http>,
        /// Ports carried to a guest unread. Absent carries nothing raw.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) raw: Option<Raw>,
    }

    /// The one HTTP listener, where the edge reaches it. One per host: a plain port beside a TLS one
    /// would serve every app both ways forever, so the only plain port allowed redirects.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "proxy.http")]
    pub(super) struct Http {
        /// An IP address to bind.
        pub(super) listen_address: Option<String>,
        /// A free port, outside the range the slots take from 21000. Not 0.
        #[schemars(range(min = 1))]
        pub(super) port: Option<u16>,
        /// Serve the port encrypted. Absent is plain HTTP, which is what a host behind an edge that
        /// terminates TLS wants.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) tls: Option<Tls>,
        /// A plain port answering every request with a 301 to the same host and path over TLS,
        /// where a browser typing a bare hostname arrives. Needs `tls`; serves no app.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schemars(range(min = 1))]
        pub(super) redirect_from_port: Option<u16>,
    }

    /// One certificate for the whole host — there is no SNI selection, so a wildcard in practice —
    /// re-read whenever either file changes, so a renewal needs no restart. Obtaining it is
    /// certbot's or the edge's.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "proxy.http.tls")]
    pub(super) struct Tls {
        /// PEM.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) certificate: Option<String>,
        /// PEM.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) key: Option<String>,
        /// Makes a caller's own certificate the price of the handshake, which on an origin whose IP
        /// is discoverable is what keeps it reachable only through the edge.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(super) client_ca: Option<ClientCa>,
    }

    /// The certificates a caller may present, as a PEM trust pool.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "proxy.http.tls.client_ca")]
    pub(super) struct ClientCa {
        /// PEM, holding every certificate the pool trusts.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) certificate: Option<String>,
    }

    /// Ports carried to a guest unread — ssh, DNS, WireGuard — reached at a port of their own,
    /// tcp or udp, where the relay that publishes them reaches this host.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "proxy.raw")]
    pub(super) struct Raw {
        /// An IP address to bind: a private one the relay can see, never the world's.
        pub(super) listen_address: Option<String>,
        /// How many raw ports an app may name: 1 to 7, what a slot reserves past its HTTP port.
        #[schemars(range(min = 1, max = super::MAX_RAW_PORTS))]
        pub(super) max_ports_per_guest: Option<usize>,
    }

    /// A Prometheus page, rendered from the same builder that writes `reported.json`. Nothing here
    /// is an input.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "metrics")]
    pub(super) struct Metrics {
        /// A free port, outside the range the slots take from 21000, other than `proxy.http.port`,
        /// and not 9091 on a zerofs host, which ZeroFS holds.
        #[schemars(range(min = 1))]
        pub(super) port: Option<u16>,
        /// An IP address to bind.
        pub(super) listen_address: Option<String>,
    }

    /// What a guest holds, listed on request over a socket on this machine. Nothing here is an
    /// input: it answers reads, and what this host runs stays the document's to say.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "filesystem")]
    pub(super) struct Filesystem {
        /// Where the socket is put. Whoever may read it may list any app's files, so it belongs
        /// somewhere only this host's own account can reach.
        #[schemars(pattern(ABSOLUTE_PATH))]
        pub(super) socket: Option<String>,
    }

    /// How much of each app's output stays on disk, under `paths.state_dir/logs`. Nothing on the
    /// host reads it back: it is there to be tailed.
    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[schemars(rename = "logs")]
    pub(super) struct Logs {
        /// Whole mebibytes, more than 0. The newest this many of an app's output, in two files:
        /// `<appId>.log` becomes `<appId>.log.1` when it passes this, over the one before it, so
        /// an app holds between one and two of these on disk however fast it writes. 256 is weeks
        /// of an app that logs a line per request, and about a minute of one that floods.
        #[schemars(range(min = 1))]
        pub(super) keep_mib_per_app: Option<u64>,
    }
}

impl HostConfig {
    pub fn load() -> Result<Self, ConfigError> {
        Self::from_file(&Self::configured_file())
    }

    /// The file `load` reads. Named separately because what `install` writes has to point back at
    /// the file the values came from, and pointing at the wrong one is worse than pointing at none.
    pub fn configured_file() -> PathBuf {
        std::env::var(CONFIG_FILE_VARIABLE)
            .ok()
            .filter(|named| !named.trim().is_empty())
            .map_or_else(|| PathBuf::from(DEFAULT_CONFIG_FILE), PathBuf::from)
    }

    pub fn from_file(path: &std::path::Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|error| ConfigError::Unreadable {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
        Self::from_toml(&text).map_err(|error| match error {
            ConfigError::Malformed { reason, .. } => ConfigError::Malformed {
                path: path.display().to_string(),
                reason,
            },
            other => other,
        })
    }

    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        let document: file::ConfigFile = toml::from_str(text).map_err(|error| ConfigError::Malformed {
            path: "the configuration".to_string(),
            reason: error.message().trim().replace('\n', "; "),
        })?;
        Self::from_document(&document)
    }

    fn from_document(document: &file::ConfigFile) -> Result<Self, ConfigError> {
        let max_apps = apps("max_apps", required("max_apps", document.max_apps)?)?;
        let paths = required("paths", document.paths.as_ref())?;
        let state_dir = absolute(
            "paths.state_dir",
            required_str("paths.state_dir", &paths.state_dir)?,
        )?;
        let runtime_dir = absolute(
            "paths.runtime_dir",
            required_str("paths.runtime_dir", &paths.runtime_dir)?,
        )?;

        let volumes = required("volumes", document.volumes.as_ref())?;
        let backend = match required_str("volumes.backend", &volumes.backend)? {
            "local-file" => {
                if volumes.zerofs.is_some() {
                    return Err(ConfigError::invalid(
                        "volumes.zerofs",
                        "read by the local-file backend, which is what volumes.backend says",
                    ));
                }
                VolumeBackend::LocalFile
            }
            "zerofs" => {
                let zerofs = required("volumes.zerofs", volumes.zerofs.as_ref())?;
                VolumeBackend::Zerofs(Box::new(ZerofsSettings {
                    binary: path_key("volumes.zerofs.binary", &zerofs.binary)?,
                    config_file: path_key("volumes.zerofs.config_file", &zerofs.config_file)?,
                    mount_path: path_key("volumes.zerofs.mount_path", &zerofs.mount_path)?,
                    nbd_socket_path: path_key("volumes.zerofs.nbd_socket_path", &zerofs.nbd_socket_path)?,
                    ninep_socket_path: path_key(
                        "volumes.zerofs.ninep_socket_path",
                        &zerofs.ninep_socket_path,
                    )?,
                    rpc_socket_path: path_key("volumes.zerofs.rpc_socket_path", &zerofs.rpc_socket_path)?,
                    storage_url: object_store_url(
                        "volumes.zerofs.storage_url",
                        required_str("volumes.zerofs.storage_url", &zerofs.storage_url)?,
                    )?,
                    cache_dir: path_key("volumes.zerofs.cache_dir", &zerofs.cache_dir)?,
                    cache_disk_mib: mebibytes(
                        "volumes.zerofs.cache_disk_gib",
                        required("volumes.zerofs.cache_disk_gib", zerofs.cache_disk_gib)?,
                    )?,
                    cache_memory_mib: mebibytes(
                        "volumes.zerofs.cache_memory_gib",
                        required("volumes.zerofs.cache_memory_gib", zerofs.cache_memory_gib)?,
                    )?,
                    checkpoint_runtime_dir: path_key(
                        "volumes.zerofs.checkpoint_runtime_dir",
                        &zerofs.checkpoint_runtime_dir,
                    )?,
                    checkpoint_config_file: path_key(
                        "volumes.zerofs.checkpoint_config_file",
                        &zerofs.checkpoint_config_file,
                    )?,
                    checkpoint_cache_dir: path_key(
                        "volumes.zerofs.checkpoint_cache_dir",
                        &zerofs.checkpoint_cache_dir,
                    )?,
                }))
            }
            named => {
                return Err(ConfigError::invalid(
                    "volumes.backend",
                    format!("a backend this host has, and there is no {named}"),
                ))
            }
        };

        let network = required("network", document.network.as_ref())?;
        let exports = required("exports", document.exports.as_ref())?;
        let artifacts = required("artifacts", document.artifacts.as_ref())?;

        let proxy = proxy(document.proxy.as_ref(), max_apps)?;
        let metrics = metrics(document.metrics.as_ref(), &proxy, &backend, max_apps)?;
        let filesystem = filesystem(document.filesystem.as_ref())?;
        let logs = logs(document.logs.as_ref())?;

        Ok(Self {
            http_admission: document.http_admission.clone(),
            max_concurrent_vm_starts: document.max_concurrent_vm_starts,
            vm_budgets: document.vm_budgets.clone(),
            max_apps,
            snapshot_dir: path_key("paths.snapshot_dir", &paths.snapshot_dir)?,
            guest_image_dir: path_key("paths.guest_image_dir", &paths.guest_image_dir)?,
            firecracker_dir: runtime_dir.join("firecracker"),
            desired_state_file: path_key("paths.desired_state_file", &paths.desired_state_file)?,
            versions_file: path_key("paths.versions_file", &paths.versions_file)?,
            artifact_store_url: object_store_url(
                "artifacts.store_url",
                required_str("artifacts.store_url", &artifacts.store_url)?,
            )?,
            storage_prefix: storage_prefix(
                "volumes.storage_prefix",
                required_str("volumes.storage_prefix", &volumes.storage_prefix)?,
            )?,
            volumes: backend,
            allowed_host_tcp_endpoints: network
                .host_tcp
                .as_ref()
                .map(|tcp| {
                    host_tcp_endpoints(required("network.host_tcp.endpoints", tcp.endpoints.as_ref())?)
                })
                .transpose()?
                .unwrap_or_default(),
            denied_egress_addresses_v4: cidrs(
                "network.denied_egress_addresses_v4",
                required(
                    "network.denied_egress_addresses_v4",
                    network.denied_egress_addresses_v4.as_ref(),
                )?,
                Family::V4,
            )?,
            denied_egress_addresses_v6: cidrs(
                "network.denied_egress_addresses_v6",
                required(
                    "network.denied_egress_addresses_v6",
                    network.denied_egress_addresses_v6.as_ref(),
                )?,
                Family::V6,
            )?,
            proxy,
            metrics,
            filesystem,
            logs,
            export_store_url: object_store_url(
                "exports.store_url",
                required_str("exports.store_url", &exports.store_url)?,
            )?,
            export_staging_dir: path_key("exports.staging_dir", &exports.staging_dir)?,
            state_dir,
            runtime_dir,
        })
    }

    /// The starting point, laid out where a Linux distribution would put it: volumes as files on
    /// this machine's own disk, stores as directories on it, plain HTTP on :80 — because a host with
    /// no listener refuses a document that names a hostname, and a host that serves nothing is not
    /// a starting point. It is what `install` writes a host that has none — from here, so there is
    /// no file in the repository to fall behind [`Self::from_document`]. `max_apps` is the one
    /// number in it that is this machine's rather than every machine's, so it is handed in:
    /// `install` measures it.
    pub fn starter(max_apps: u32) -> Self {
        let state_dir = PathBuf::from(STARTER_STATE_DIR);
        let mut starter = Self::laid_out(
            state_dir.clone(),
            PathBuf::from("/run/nibrunner"),
            state_dir.join("guest"),
        );
        starter.max_apps = max_apps;
        starter.proxy.http = Some(HttpListener {
            listen_address: IpAddr::from([0, 0, 0, 0]),
            port: 80,
            tls: None,
            redirect_from_port: None,
        });
        // What an app has used is published here and nowhere else, so a host laid out from
        // nothing still has somewhere to read it. On loopback, because it is this host's to read.
        starter.metrics = Some(MetricsConfig {
            port: 9100,
            listen_address: IpAddr::from([127, 0, 0, 1]),
        });
        starter
    }

    pub fn under(root: &std::path::Path) -> Self {
        Self::laid_out(root.join("state"), root.join("run"), root.join("guest"))
    }

    fn laid_out(state_dir: PathBuf, runtime_dir: PathBuf, guest_image_dir: PathBuf) -> Self {
        Self {
            max_apps: 1000,
            snapshot_dir: state_dir.join("snapshots"),
            guest_image_dir,
            firecracker_dir: runtime_dir.join("firecracker"),
            desired_state_file: state_dir.join("desired.json"),
            versions_file: state_dir.join("versions.json"),
            artifact_store_url: state_dir.join("artifact-store").display().to_string(),
            storage_prefix: "volumes".to_string(),
            volumes: VolumeBackend::LocalFile,
            allowed_host_tcp_endpoints: vec![],
            denied_egress_addresses_v4: vec![],
            denied_egress_addresses_v6: vec![],
            proxy: ProxyConfig::default(),
            metrics: None,
            filesystem: None,
            http_admission: None,
            max_concurrent_vm_starts: None,
            vm_budgets: None,
            logs: LogsConfig::default(),
            export_store_url: state_dir.join("export-store").display().to_string(),
            export_staging_dir: state_dir.join("exports"),
            state_dir,
            runtime_dir,
        }
    }

    /// A host with every section in it: volumes in an object store reached from the guest over
    /// NBD, artifacts and exports in S3, TLS behind an edge that presents a client certificate,
    /// raw ports for a relay, a metrics page, a socket to ask what a guest holds, what is kept of
    /// each app's output.
    /// `deploy/config.example.toml` is this, rendered.
    ///
    /// Written out field by field rather than as changes to [`Self::starter`], so that a section
    /// added to this daemon has to be decided on here — and an example that showed every section
    /// but the newest would otherwise be exactly what nobody noticed.
    pub fn example() -> Self {
        Self {
            max_apps: 1000,
            state_dir: PathBuf::from("/var/lib/nibrunner"),
            runtime_dir: PathBuf::from("/run/nibrunner"),
            snapshot_dir: PathBuf::from("/data/nibrunner-vm"),
            guest_image_dir: PathBuf::from("/var/lib/nibrunner/guest"),
            firecracker_dir: PathBuf::from("/run/nibrunner/firecracker"),
            desired_state_file: PathBuf::from("/var/lib/nibrunner/desired.json"),
            versions_file: PathBuf::from("/var/lib/nibrunner/versions.json"),
            artifact_store_url: "s3://nibrunner-artifacts-eu-west-2-123456789012/artifacts".to_string(),
            storage_prefix: "hetzner-1".to_string(),
            volumes: VolumeBackend::Zerofs(Box::new(ZerofsSettings {
                binary: PathBuf::from("/opt/nibrunner/bin/zerofs"),
                config_file: PathBuf::from("/etc/zerofs/config.toml"),
                mount_path: PathBuf::from("/mnt/zerofs"),
                nbd_socket_path: PathBuf::from("/run/zerofs/nbd.sock"),
                ninep_socket_path: PathBuf::from("/run/zerofs/9p.sock"),
                rpc_socket_path: PathBuf::from("/run/zerofs/rpc.sock"),
                storage_url: "s3://nibrunner-filesystems-eu-west-2-123456789012/hetzner-1".to_string(),
                cache_dir: PathBuf::from("/data/zerofs"),
                cache_disk_mib: 200 * MEBIBYTES_PER_GIBIBYTE,
                cache_memory_mib: 2 * MEBIBYTES_PER_GIBIBYTE,
                checkpoint_runtime_dir: PathBuf::from("/run/zerofs-checkpoint"),
                checkpoint_config_file: PathBuf::from("/etc/zerofs/checkpoint.toml"),
                checkpoint_cache_dir: PathBuf::from("/data/zerofs-checkpoint"),
            })),
            allowed_host_tcp_endpoints: vec![],
            denied_egress_addresses_v4: vec![],
            denied_egress_addresses_v6: vec![],
            proxy: ProxyConfig {
                http: Some(HttpListener {
                    listen_address: IpAddr::from([0, 0, 0, 0]),
                    port: 443,
                    tls: Some(TlsMaterial {
                        certificate: PathBuf::from("/etc/nibrunner/tls/origin.crt"),
                        key: PathBuf::from("/etc/nibrunner/tls/origin.key"),
                        client_ca: Some(PathBuf::from("/etc/nibrunner/tls/origin-pull-ca.pem")),
                    }),
                    redirect_from_port: None,
                }),
                raw: Some(RawPorts {
                    listen_address: IpAddr::from([10, 0, 5, 18]),
                    max_ports_per_guest: 1,
                }),
            },
            metrics: Some(MetricsConfig {
                port: 9100,
                listen_address: IpAddr::from([127, 0, 0, 1]),
            }),
            filesystem: Some(FilesystemConfig {
                socket: PathBuf::from("/run/nibrunner/filesystem.sock"),
            }),
            http_admission: Some(HttpAdmission {
                host_concurrent: std::num::NonZeroU16::new(128).expect("positive limit"),
                app_concurrent: std::num::NonZeroU16::new(16).expect("positive limit"),
                apps: Default::default(),
            }),
            max_concurrent_vm_starts: std::num::NonZeroU16::new(2),
            vm_budgets: Some(VmBudgets {
                default: VmBudget {
                    cpu_percent: std::num::NonZeroU16::new(100).expect("positive budget"),
                    memory_mib: std::num::NonZeroU32::new(2304).expect("positive budget"),
                },
                apps: Default::default(),
            }),
            logs: LogsConfig::default(),
            export_store_url: "s3://nibrunner-exports-eu-west-2-123456789012/exports".to_string(),
            export_staging_dir: PathBuf::from("/var/lib/nibrunner/exports"),
        }
    }

    /// This configuration as the file [`Self::from_toml`] reads. What `install` writes a host that
    /// has none, and what `deploy/config.example.toml` is written from.
    pub fn to_toml(&self) -> String {
        toml::to_string(&self.to_document())
            .expect("every key here is a string, an integer, or a table of them")
    }

    /// The file as a JSON Schema, for an editor and for the reference the docs site renders from
    /// it. It is the *serialize* contract of the document [`Self::to_document`] writes: every key
    /// written is one [`Self::from_document`] requires, and the only keys skipped are the sections
    /// that may be absent, so what is required here is what the round trip already holds to. TOML
    /// has no null, so the null every `Option` would admit is taken back out.
    pub fn schema() -> schemars::Schema {
        let mut schema = schemars::generate::SchemaSettings::draft2020_12()
            .for_serialize()
            .with_transform(schemars::transform::RecursiveTransform(without_null))
            .into_generator()
            .into_root_schema_for::<file::ConfigFile>();
        schema.insert("$id".into(), SCHEMA_ID.into());
        schema
    }

    /// [`Self::from_document`] run backwards. Every key is required on the way in and every
    /// unknown one refused, so a key written here that is not read there, or read there that is
    /// not written here, fails the round trip by name rather than drifting.
    fn to_document(&self) -> file::ConfigFile {
        let text = |path: &std::path::Path| Some(path.display().to_string());
        file::ConfigFile {
            max_apps: Some(self.max_apps),
            paths: Some(file::Paths {
                state_dir: text(&self.state_dir),
                runtime_dir: text(&self.runtime_dir),
                snapshot_dir: text(&self.snapshot_dir),
                guest_image_dir: text(&self.guest_image_dir),
                desired_state_file: text(&self.desired_state_file),
                versions_file: text(&self.versions_file),
            }),
            artifacts: Some(file::Artifacts {
                store_url: Some(self.artifact_store_url.clone()),
            }),
            volumes: Some(file::Volumes {
                backend: Some(self.volumes.as_str().to_string()),
                storage_prefix: Some(self.storage_prefix.clone()),
                zerofs: self.volumes.zerofs().map(|settings| file::Zerofs {
                    binary: text(&settings.binary),
                    config_file: text(&settings.config_file),
                    mount_path: text(&settings.mount_path),
                    nbd_socket_path: text(&settings.nbd_socket_path),
                    ninep_socket_path: text(&settings.ninep_socket_path),
                    rpc_socket_path: text(&settings.rpc_socket_path),
                    storage_url: Some(settings.storage_url.clone()),
                    cache_dir: text(&settings.cache_dir),
                    cache_disk_gib: Some(settings.cache_disk_mib / MEBIBYTES_PER_GIBIBYTE),
                    cache_memory_gib: Some(settings.cache_memory_mib / MEBIBYTES_PER_GIBIBYTE),
                    checkpoint_runtime_dir: text(&settings.checkpoint_runtime_dir),
                    checkpoint_config_file: text(&settings.checkpoint_config_file),
                    checkpoint_cache_dir: text(&settings.checkpoint_cache_dir),
                }),
            }),
            exports: Some(file::Exports {
                store_url: Some(self.export_store_url.clone()),
                staging_dir: text(&self.export_staging_dir),
            }),
            network: Some(file::Network {
                host_tcp: (!self.allowed_host_tcp_endpoints.is_empty()).then(|| file::HostTcp {
                    endpoints: Some(
                        self.allowed_host_tcp_endpoints
                            .iter()
                            .map(ToString::to_string)
                            .collect(),
                    ),
                }),
                denied_egress_addresses_v4: Some(self.denied_egress_addresses_v4.clone()),
                denied_egress_addresses_v6: Some(self.denied_egress_addresses_v6.clone()),
            }),
            // Left out rather than written as an empty `[proxy]`: both read back the same.
            proxy: (self.proxy != ProxyConfig::default()).then(|| file::Proxy {
                http: self.proxy.http.as_ref().map(|http| file::Http {
                    listen_address: Some(http.listen_address.to_string()),
                    port: Some(http.port),
                    redirect_from_port: http.redirect_from_port,
                    tls: http.tls.as_ref().map(|tls| file::Tls {
                        certificate: text(&tls.certificate),
                        key: text(&tls.key),
                        client_ca: tls.client_ca.as_deref().map(|certificate| file::ClientCa {
                            certificate: text(certificate),
                        }),
                    }),
                }),
                raw: self.proxy.raw.as_ref().map(|raw| file::Raw {
                    listen_address: Some(raw.listen_address.to_string()),
                    max_ports_per_guest: Some(raw.max_ports_per_guest),
                }),
            }),
            metrics: self.metrics.as_ref().map(|metrics| file::Metrics {
                port: Some(metrics.port),
                listen_address: Some(metrics.listen_address.to_string()),
            }),
            filesystem: self.filesystem.as_ref().map(|filesystem| file::Filesystem {
                socket: text(&filesystem.socket),
            }),
            http_admission: self.http_admission.clone(),
            max_concurrent_vm_starts: self.max_concurrent_vm_starts,
            vm_budgets: self.vm_budgets.clone(),
            logs: Some(file::Logs {
                keep_mib_per_app: Some(self.logs.keep_bytes_per_app / BYTES_PER_MEBIBYTE),
            }),
        }
    }
}

/// Where [`HostConfig::schema`] is published, which is what an editor is pointed at.
pub const SCHEMA_ID: &str =
    "https://raw.githubusercontent.com/ilbertt/nibrunner/main/deploy/config.schema.json";

fn without_null(schema: &mut schemars::Schema) {
    let Some(object) = schema.as_object_mut() else {
        return;
    };
    if let Some(serde_json::Value::Array(types)) = object.get_mut("type") {
        types.retain(|named| named != "null");
        if let [only] = types.as_slice() {
            let only = only.clone();
            object.insert("type".into(), only);
        }
    }
    // An `Option<T>` that is not a plain type is `anyOf: [T, null]`.
    let Some(serde_json::Value::Array(members)) = object.get("anyOf") else {
        return;
    };
    let [first, second] = members.as_slice() else {
        return;
    };
    let is_null = |member: &serde_json::Value| member.get("type") == Some(&"null".into());
    let only = match (is_null(first), is_null(second)) {
        (false, true) => first.clone(),
        (true, false) => second.clone(),
        _ => return,
    };
    let serde_json::Value::Object(only) = only else {
        return;
    };
    object.remove("anyOf");
    for (key, value) in only {
        object.entry(key).or_insert(value);
    }
}

fn bind_address(field: &str, value: &Option<String>) -> Result<IpAddr, ConfigError> {
    required_str(field, value)?
        .trim()
        .parse()
        .map_err(|_| ConfigError::invalid(field, "an IP address to bind"))
}

fn proxy(document: Option<&file::Proxy>, max_apps: u32) -> Result<ProxyConfig, ConfigError> {
    let Some(document) = document else {
        return Ok(ProxyConfig::default());
    };
    let http = document
        .http
        .as_ref()
        .map(|http| {
            Ok::<_, ConfigError>(HttpListener {
                listen_address: bind_address("proxy.http.listen_address", &http.listen_address)?,
                port: listener(
                    "proxy.http.port",
                    required("proxy.http.port", http.port)?,
                    max_apps,
                )?,
                tls: http
                    .tls
                    .as_ref()
                    .map(|tls| {
                        Ok::<_, ConfigError>(TlsMaterial {
                            certificate: path_key("proxy.http.tls.certificate", &tls.certificate)?,
                            key: path_key("proxy.http.tls.key", &tls.key)?,
                            client_ca: tls
                                .client_ca
                                .as_ref()
                                .map(|pool| {
                                    path_key("proxy.http.tls.client_ca.certificate", &pool.certificate)
                                })
                                .transpose()?,
                        })
                    })
                    .transpose()?,
                redirect_from_port: http
                    .redirect_from_port
                    .map(|port| redirect_port(port, http, max_apps))
                    .transpose()?,
            })
        })
        .transpose()?;
    let raw = document
        .raw
        .as_ref()
        .map(|raw| {
            let named = required("proxy.raw.max_ports_per_guest", raw.max_ports_per_guest)?;
            if named == 0 || named > MAX_RAW_PORTS {
                return Err(ConfigError::invalid(
                    "proxy.raw.max_ports_per_guest",
                    format!(
                        "between 1 and {MAX_RAW_PORTS}, which is what a slot reserves beside the HTTP port"
                    ),
                ));
            }
            Ok(RawPorts {
                listen_address: bind_address("proxy.raw.listen_address", &raw.listen_address)?,
                max_ports_per_guest: named,
            })
        })
        .transpose()?;
    Ok(ProxyConfig { http, raw })
}

/// What a slot has left over for an app once its HTTP port is taken.
pub const MAX_RAW_PORTS: usize = nft_render::PORTS_PER_SLOT as usize - 1;

/// A host laid out for no app serves nothing, and one laid out for more than the ports fit would
/// hand a slot a port that is not one. The guest network fits more slots than the ports do, so
/// the ports are the bound.
fn apps(field: &str, count: u32) -> Result<u32, ConfigError> {
    if count == 0 {
        return Err(ConfigError::invalid(field, "at least 1"));
    }
    let most = nft_render::most_apps_the_ports_fit();
    if count > most {
        return Err(ConfigError::invalid(
            field,
            format!(
                "at most {most}, which is as many slots of {} ports as fit above {}",
                nft_render::PORTS_PER_SLOT,
                nft_render::HOST_PORT_BASE
            ),
        ));
    }
    Ok(count)
}

fn redirect_port(port: u16, http: &file::Http, max_apps: u32) -> Result<u16, ConfigError> {
    const FIELD: &str = "proxy.http.redirect_from_port";
    let port = listener(FIELD, port, max_apps)?;
    if http.tls.is_none() {
        return Err(ConfigError::invalid(
            FIELD,
            "absent unless proxy.http.tls is set, because a host serving plain HTTP has nowhere to redirect to",
        ));
    }
    if http.port == Some(port) {
        return Err(ConfigError::invalid(
            FIELD,
            format!("a different port from proxy.http.port, which is also {port}"),
        ));
    }
    Ok(port)
}

fn metrics(
    document: Option<&file::Metrics>,
    proxy: &ProxyConfig,
    volumes: &VolumeBackend,
    max_apps: u32,
) -> Result<Option<MetricsConfig>, ConfigError> {
    let Some(document) = document else {
        return Ok(None);
    };
    let port = listener("metrics.port", required("metrics.port", document.port)?, max_apps)?;
    for (named, field) in [
        (proxy.http.as_ref().map(|http| http.port), "proxy.http.port"),
        (
            proxy.http.as_ref().and_then(|http| http.redirect_from_port),
            "proxy.http.redirect_from_port",
        ),
        // Rendered into ZeroFS's own config by `nibrunnerd install`, so this host does hold it
        // even though no key here names it, and a second binding is a startup failure over there.
        (
            volumes.zerofs().map(|_| ZEROFS_PROMETHEUS_PORT),
            "the port ZeroFS scrapes on",
        ),
    ] {
        if named == Some(port) {
            return Err(ConfigError::invalid(
                "metrics.port",
                format!("a different port from {field}, which is also {port}"),
            ));
        }
    }
    // A scrape surface names every app this host runs and what each is using, so where it is bound
    // is said out loud rather than guessed at.
    Ok(Some(MetricsConfig {
        port,
        listen_address: bind_address("metrics.listen_address", &document.listen_address)?,
    }))
}

fn filesystem(document: Option<&file::Filesystem>) -> Result<Option<FilesystemConfig>, ConfigError> {
    let Some(document) = document else {
        return Ok(None);
    };
    Ok(Some(FilesystemConfig {
        socket: path_key("filesystem.socket", &document.socket)?,
    }))
}

fn logs(document: Option<&file::Logs>) -> Result<LogsConfig, ConfigError> {
    let Some(document) = document else {
        return Ok(LogsConfig::default());
    };
    let field = "logs.keep_mib_per_app";
    let mebibytes = required(field, document.keep_mib_per_app)?;
    // A cap of nothing is a cut on every line, keeping none of them.
    if mebibytes == 0 {
        return Err(ConfigError::invalid(field, "more than nothing"));
    }
    Ok(LogsConfig {
        keep_bytes_per_app: mebibytes
            .checked_mul(BYTES_PER_MEBIBYTE)
            .ok_or_else(|| ConfigError::invalid(field, "a size this machine could hold"))?,
    })
}

/// Nothing this daemon reads has a value it may leave out: a key that is here is a key the
/// configuration states, so absence is never a second meaning to work out at startup.
fn required<T>(field: &str, value: Option<T>) -> Result<T, ConfigError> {
    value.ok_or_else(|| ConfigError::invalid(field, "specified, and nothing here is optional"))
}

fn required_str<'a>(field: &str, value: &'a Option<String>) -> Result<&'a str, ConfigError> {
    required(field, value.as_deref())
}

fn path_key(field: &str, value: &Option<String>) -> Result<PathBuf, ConfigError> {
    absolute(field, required_str(field, value)?)
}

fn absolute(field: &str, value: &str) -> Result<PathBuf, ConfigError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ConfigError::invalid(field, "a path"));
    }
    let path = PathBuf::from(trimmed);
    if !path.is_absolute() {
        return Err(ConfigError::invalid(
            field,
            format!("an absolute path, but {trimmed} is not"),
        ));
    }
    Ok(path)
}

/// A cache of no size is a cache ZeroFS will not start on, and it is refused here rather than
/// there — the operator is watching this file, not that one.
fn mebibytes(field: &str, gibibytes: u64) -> Result<u64, ConfigError> {
    if gibibytes == 0 {
        return Err(ConfigError::invalid(field, "more than nothing"));
    }
    gibibytes
        .checked_mul(MEBIBYTES_PER_GIBIBYTE)
        .ok_or_else(|| ConfigError::invalid(field, "a size this machine could hold"))
}

fn listener(field: &str, port: u16, max_apps: u32) -> Result<u16, ConfigError> {
    if port == 0 {
        return Err(ConfigError::invalid(
            field,
            "a port, and 0 is the kernel picking one",
        ));
    }
    let (base, end) = nft_render::reserved_port_range(max_apps);
    if (base..=end).contains(&port) {
        return Err(ConfigError::invalid(
            field,
            format!("free, because {base}-{end} is what a slot takes for an app's loopback port"),
        ));
    }
    Ok(port)
}

enum Family {
    V4,
    V6,
}

fn host_tcp_endpoints(values: &[String]) -> Result<Vec<SocketAddr>, ConfigError> {
    let invalid = || {
        ConfigError::invalid(
            "network.host_tcp.endpoints",
            "IP:port pairs with a nonzero port and a unicast address",
        )
    };
    let mut endpoints = values
        .iter()
        .map(|value| {
            let endpoint: SocketAddr = value.parse().map_err(|_| invalid())?;
            if endpoint.port() == 0
                || endpoint.ip().is_unspecified()
                || endpoint.ip().is_multicast()
                || matches!(endpoint.ip(), IpAddr::V4(address) if address.is_broadcast())
                || matches!(endpoint, SocketAddr::V6(address) if address.scope_id() != 0)
            {
                return Err(invalid());
            }
            Ok(endpoint)
        })
        .collect::<Result<Vec<_>, _>>()?;
    endpoints.sort_unstable();
    endpoints.dedup();
    Ok(endpoints)
}

fn cidrs(field: &str, values: &[String], family: Family) -> Result<Vec<String>, ConfigError> {
    values
        .iter()
        .map(|value| {
            let value = value.trim();
            let (address, length) = value.split_once('/').ok_or_else(|| {
                ConfigError::invalid(field, format!("a CIDR range, but {value} has no prefix length"))
            })?;
            let bits: u8 = length.parse().map_err(|_| {
                ConfigError::invalid(
                    field,
                    format!("a CIDR range, but {length} is not a prefix length"),
                )
            })?;
            let widest = match family {
                Family::V4 => {
                    address.parse::<Ipv4Addr>().map_err(|_| {
                        ConfigError::invalid(
                            field,
                            format!("an IPv4 range, but {address} is not an IPv4 address"),
                        )
                    })?;
                    32
                }
                Family::V6 => {
                    address.parse::<Ipv6Addr>().map_err(|_| {
                        ConfigError::invalid(
                            field,
                            format!("an IPv6 range, but {address} is not an IPv6 address"),
                        )
                    })?;
                    128
                }
            };
            if bits > widest {
                return Err(ConfigError::invalid(
                    field,
                    format!("a range, and /{bits} is wider than the {widest} bits an address has"),
                ));
            }
            Ok(value.to_string())
        })
        .collect()
}

fn object_store_url(field: &str, value: &str) -> Result<String, ConfigError> {
    let value = value.trim();
    if let Some(rest) = value.strip_prefix("s3://") {
        let bucket = rest.split('/').next().unwrap_or_default();
        if bucket.is_empty() {
            return Err(ConfigError::invalid(field, "an s3:// URL with a bucket in it"));
        }
        return Ok(value.to_string());
    }
    if value.contains("://") {
        let scheme = value.split_once("://").map_or(value, |(scheme, _)| scheme);
        return Err(ConfigError::invalid(
            field,
            format!("a store this host can reach, and there is no {scheme} backend"),
        ));
    }
    Ok(absolute(field, value)?.display().to_string())
}

fn storage_prefix(field: &str, value: &str) -> Result<String, ConfigError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ConfigError::invalid(
            field,
            "a prefix, and an empty one names the bucket root",
        ));
    }
    if value.len() > MAX_STORAGE_PREFIX_BYTES {
        return Err(ConfigError::invalid(
            field,
            format!(
                "at most {MAX_STORAGE_PREFIX_BYTES} bytes, and this is {}",
                value.len()
            ),
        ));
    }
    if value.starts_with('/') || value.ends_with('/') {
        return Err(ConfigError::invalid(
            field,
            "a prefix without a leading or trailing /",
        ));
    }
    if value
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(ConfigError::invalid(
            field,
            "a prefix whose every segment names something",
        ));
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// The smallest document this daemon accepts. Every key it reads is in here, because there is
    /// no key it will supply for itself, so a test about one key starts from the whole document.
    const WHOLE: &str = r#"max_apps = 1000

[paths]
state_dir = "/var/lib/nibrunner"
runtime_dir = "/run/nibrunner"
snapshot_dir = "/var/lib/nibrunner/snapshots"
guest_image_dir = "/var/lib/nibrunner/guest"
desired_state_file = "/var/lib/nibrunner/desired.json"
versions_file = "/var/lib/nibrunner/versions.json"

[artifacts]
store_url = "/var/lib/nibrunner/artifact-store"

[volumes]
backend = "local-file"
storage_prefix = "volumes"

[exports]
store_url = "/var/lib/nibrunner/export-store"
staging_dir = "/var/lib/nibrunner/exports"

[network]
denied_egress_addresses_v4 = []
denied_egress_addresses_v6 = []
"#;

    #[test]
    fn host_tcp_endpoints_are_exact_validated_and_optional() {
        assert!(host_tcp_endpoints(&[]).unwrap().is_empty());
        assert!(parsed(WHOLE).allowed_host_tcp_endpoints.is_empty());
        assert!(refused(&format!("{WHOLE}\n[network.host_tcp]\n")).contains("network.host_tcp.endpoints"));
        let config = parsed(&format!(
            "{WHOLE}\n[network.host_tcp]\nendpoints = [\"203.0.113.10:443\"]\n"
        ));
        assert_eq!(
            config.allowed_host_tcp_endpoints,
            vec!["203.0.113.10:443".parse::<SocketAddr>().unwrap()]
        );

        let valid = vec![
            "203.0.113.10:443".to_string(),
            "[2001:db8::10]:443".to_string(),
            "203.0.113.10:443".to_string(),
        ];
        assert_eq!(host_tcp_endpoints(&valid).unwrap().len(), 2);
        for invalid in [
            "example.com:443",
            "203.0.113.0/24:443",
            "0.0.0.0:443",
            "[::]:443",
            "203.0.113.10:0",
            "224.0.0.1:443",
            "255.255.255.255:443",
            "[fe80::1%1]:443",
            "[ff02::1]:443",
            "203.0.113.10:443; accept",
        ] {
            assert!(host_tcp_endpoints(&[invalid.to_string()]).is_err(), "{invalid}");
        }
    }

    /// A key as the refusals name it: `section.key`, or the bare key above the first section.
    fn field_of(section: &str, key: &str) -> String {
        if section.is_empty() {
            key.to_string()
        } else {
            format!("{section}.{key}")
        }
    }

    /// `WHOLE` with some keys written differently, and anything in `extra` appended. A field named
    /// here that the document does not have is a typo, not a new key, so it fails loudly.
    fn document(changes: &[(&str, &str)], extra: &str) -> String {
        let mut section = String::new();
        let mut seen: Vec<&str> = vec![];
        let mut out: Vec<String> = vec![];
        for line in WHOLE.lines() {
            if let Some(name) = line.strip_prefix('[').and_then(|rest| rest.strip_suffix(']')) {
                section = name.to_string();
                out.push(line.to_string());
                continue;
            }
            let Some((key, _)) = line.split_once(" = ") else {
                out.push(line.to_string());
                continue;
            };
            let field = field_of(&section, key);
            match changes.iter().find(|(named, _)| *named == field) {
                Some((named, value)) => {
                    seen.push(named);
                    out.push(format!("{key} = {value}"));
                }
                None => out.push(line.to_string()),
            }
        }
        for (named, _) in changes {
            assert!(seen.contains(named), "{named} is not a key this document has");
        }
        out.push(extra.to_string());
        out.join("\n")
    }

    fn whole() -> String {
        document(&[], "")
    }

    fn with(extra: &str) -> HostConfig {
        parsed(&document(&[], extra))
    }

    /// Every listener binds somewhere, so a document naming one names that too. Each on an
    /// address of its own, because each faces a different machine.
    fn bound(listeners: &str) -> String {
        listeners
            .replace("[proxy.http]\n", "[proxy.http]\nlisten_address = \"0.0.0.0\"\n")
            .replace("[proxy.raw]\n", "[proxy.raw]\nlisten_address = \"10.0.5.18\"\n")
    }

    /// `WHOLE` with one key struck out, which is the only way a key can now be absent.
    fn without(field: &str) -> String {
        let mut section = String::new();
        let mut found = false;
        let mut out: Vec<String> = vec![];
        for line in WHOLE.lines() {
            if let Some(name) = line.strip_prefix('[').and_then(|rest| rest.strip_suffix(']')) {
                section = name.to_string();
            } else if let Some((key, _)) = line.split_once(" = ") {
                if field_of(&section, key) == field {
                    found = true;
                    continue;
                }
            }
            out.push(line.to_string());
        }
        assert!(found, "{field} is not a key this document has");
        out.join("\n")
    }

    fn parsed(text: &str) -> HostConfig {
        HostConfig::from_toml(text).unwrap()
    }

    fn refused(text: &str) -> String {
        HostConfig::from_toml(text).unwrap_err().message()
    }

    #[test]
    fn http_admission_is_opt_in_and_refuses_zero_unknown_and_invalid_app_limits() {
        let original = HostConfig::starter(10).to_toml();
        assert!(HostConfig::from_toml(&original).unwrap().http_admission.is_none());
        let valid = format!("{original}\n[http_admission]\nhost_concurrent=8\napp_concurrent=2\n[http_admission.apps]\napp-one=3\n");
        let enabled = HostConfig::from_toml(&valid).unwrap();
        assert_eq!(
            enabled.http_admission.as_ref().unwrap().apps[&protocol::AppId::parse("app-one").unwrap()].get(),
            3
        );
        assert_eq!(HostConfig::from_toml(&enabled.to_toml()).unwrap(), enabled);
        for invalid in [
            valid.replace("host_concurrent=8", "host_concurrent=0"),
            valid.replace("app_concurrent=2", "app_concurrent=65536"),
            valid.replace("app-one=3", "app-one=0"),
            valid.replace("app-one=3", "'../app'=3"),
            valid.replace("app_concurrent=2", "unknown=2"),
        ] {
            assert!(HostConfig::from_toml(&invalid).is_err());
        }
    }

    #[test]
    fn a_key_the_document_leaves_out_is_refused_rather_than_filled_in() {
        for field in [
            "max_apps",
            "paths.state_dir",
            "paths.runtime_dir",
            "paths.snapshot_dir",
            "paths.guest_image_dir",
            "paths.desired_state_file",
            "paths.versions_file",
            "artifacts.store_url",
            "volumes.backend",
            "volumes.storage_prefix",
            "exports.store_url",
            "exports.staging_dir",
            "network.denied_egress_addresses_v4",
            "network.denied_egress_addresses_v6",
        ] {
            let message = refused(&without(field));
            assert!(message.contains(field), "{field}: {message}");
            assert!(message.contains("nothing here is optional"), "{field}: {message}");
        }
    }

    #[test]
    fn a_section_this_daemon_reads_is_refused_when_the_document_has_none() {
        for section in ["paths", "artifacts", "volumes", "exports", "network"] {
            let stripped: String = WHOLE
                .split("\n\n")
                .filter(|block| !block.starts_with(&format!("[{section}]")))
                .collect::<Vec<_>>()
                .join("\n\n");
            let message = refused(&stripped);
            assert!(message.contains(section), "{section}: {message}");
        }
    }

    #[test]
    fn a_host_with_no_configuration_file_does_not_start_on_guesses() {
        let error = HostConfig::from_file(Path::new("/nonexistent/nibrunner/config.toml")).unwrap_err();
        assert!(matches!(error, ConfigError::Unreadable { .. }), "{error}");
    }

    #[test]
    fn every_file_a_host_writes_is_under_the_directory_the_document_names() {
        let config = parsed(&whole());
        assert_eq!(config.state_dir, PathBuf::from("/var/lib/nibrunner"));
        assert_eq!(
            config.desired_state_file,
            PathBuf::from("/var/lib/nibrunner/desired.json")
        );
        assert_eq!(
            config.firecracker_dir,
            PathBuf::from("/run/nibrunner/firecracker")
        );
        assert_eq!(config.storage_prefix, "volumes");
        assert_eq!(config.proxy, ProxyConfig::default());
        assert_eq!(config.metrics, None);
        assert_eq!(config.logs, LogsConfig::default());
    }

    #[test]
    fn a_key_this_daemon_does_not_have_is_refused_by_name() {
        let message = refused(&document(&[], "[proxy.http]\nprot = 80\n"));
        assert!(message.contains("prot"), "{message}");
    }

    #[test]
    fn a_section_this_daemon_does_not_have_is_refused_too() {
        let message = refused(&document(&[], "[zerofs]\nbinary = \"/usr/bin/zerofs\"\n"));
        assert!(message.contains("zerofs"), "{message}");
    }

    #[test]
    fn a_relative_path_is_refused_because_it_names_a_different_place_each_time() {
        let message = refused(&document(&[("paths.state_dir", "\"var/lib/nibrunner\"")], ""));
        assert!(message.contains("paths.state_dir"), "{message}");
        assert!(message.contains("absolute"), "{message}");
    }

    #[test]
    fn a_path_that_is_only_whitespace_names_nothing_and_is_refused() {
        let message = refused(&document(&[("paths.snapshot_dir", "\"   \"")], ""));
        assert!(message.contains("paths.snapshot_dir"), "{message}");
        assert!(message.contains("is not a path"), "{message}");
        assert_eq!(
            with(&bound(
                "[proxy.http]\nport = 443\n\n[proxy.http.tls]\ncertificate = \"  /tls/origin.crt  \"\nkey = \"/tls/origin.key\"\n"
            ))
            .proxy
            .http
            .unwrap()
            .tls
            .unwrap()
            .certificate,
            PathBuf::from("/tls/origin.crt")
        );
    }

    #[test]
    fn a_tls_host_may_name_a_plain_port_that_only_redirects() {
        let tls = "[proxy.http.tls]\ncertificate = \"/tls/origin.crt\"\nkey = \"/tls/origin.key\"\n";
        let served = with(&bound(&format!(
            "[proxy.http]\nport = 443\nredirect_from_port = 80\n\n{tls}"
        )))
        .proxy
        .http
        .unwrap();
        assert_eq!((served.port, served.redirect_from_port), (443, Some(80)));
        assert_eq!(
            with(&bound(&format!("[proxy.http]\nport = 443\n\n{tls}")))
                .proxy
                .http
                .unwrap()
                .redirect_from_port,
            None
        );

        let plain = refused(&document(
            &[],
            &bound("[proxy.http]\nport = 8080\nredirect_from_port = 80\n"),
        ));
        assert!(plain.contains("proxy.http.redirect_from_port"), "{plain}");
        assert!(plain.contains("proxy.http.tls"), "{plain}");

        let same = refused(&document(
            &[],
            &bound(&format!(
                "[proxy.http]\nport = 443\nredirect_from_port = 443\n\n{tls}"
            )),
        ));
        assert!(same.contains("different port from proxy.http.port"), "{same}");

        let slot = refused(&document(
            &[],
            &bound(&format!(
                "[proxy.http]\nport = 443\nredirect_from_port = 21000\n\n{tls}"
            )),
        ));
        assert!(
            slot.contains("proxy.http.redirect_from_port") && slot.contains("21000"),
            "{slot}"
        );

        let scrape = refused(&document(
            &[],
            &bound(&format!(
                "[proxy.http]\nport = 443\nredirect_from_port = 9100\n\n{tls}\n[metrics]\nport = 9100\nlisten_address = \"127.0.0.1\"\n"
            )),
        ));
        assert!(scrape.contains("proxy.http.redirect_from_port"), "{scrape}");

        let mut config = HostConfig::example();
        config.proxy.http.as_mut().unwrap().redirect_from_port = Some(80);
        assert_eq!(
            parsed(&config.to_toml()),
            config,
            "the key is written and read back"
        );
    }

    #[test]
    fn a_proxy_port_a_slot_would_take_is_refused() {
        let message = refused(&document(&[], &bound("[proxy.http]\nport = 21000\n")));
        assert!(message.contains("proxy.http.port"), "{message}");
        assert!(message.contains("21000"), "{message}");
        assert!(message.contains("28999"), "{message}");
        // The whole stride is reserved, not just the port each slot's first app answers on.
        assert!(refused(&document(&[], &bound("[proxy.http]\nport = 23000\n"))).contains("proxy.http.port"));
        assert_eq!(
            with(&bound("[proxy.http]\nport = 29000\n"))
                .proxy
                .http
                .unwrap()
                .port,
            29000
        );
    }

    // The range a listener is kept out of is as wide as the host is laid out for, so a smaller
    // host frees ports a bigger one reserves.
    #[test]
    fn the_ports_a_listener_is_kept_out_of_follow_how_many_apps_the_host_is_laid_out_for() {
        let on_a_small_host = |port: u16| {
            document(
                &[("max_apps", "63")],
                &bound(&format!("[proxy.http]\nport = {port}\n")),
            )
        };
        let message = refused(&on_a_small_host(21503));
        assert!(message.contains("21000-21503"), "{message}");
        assert_eq!(parsed(&on_a_small_host(21504)).proxy.http.unwrap().port, 21504);
    }

    #[test]
    fn how_many_apps_a_host_is_laid_out_for_is_bounded_by_the_ports_a_slot_takes() {
        let message = refused(&document(&[("max_apps", "0")], ""));
        assert!(message.contains("max_apps"), "{message}");
        assert!(message.contains("at least 1"), "{message}");

        let message = refused(&document(&[("max_apps", "5568")], ""));
        assert!(message.contains("max_apps"), "{message}");
        assert!(message.contains("at most 5567"), "{message}");
        assert!(message.contains("21000"), "{message}");

        assert_eq!(parsed(&document(&[("max_apps", "5567")], "")).max_apps, 5567);
        assert_eq!(parsed(&whole()).max_apps, 1000);
    }

    #[test]
    fn a_port_the_kernel_would_pick_is_not_a_port_this_host_can_be_found_on() {
        let message = refused(&document(&[], &bound("[proxy.http]\nport = 0\n")));
        assert!(message.contains("proxy.http.port"), "{message}");
        assert!(message.contains("kernel picking one"), "{message}");
    }

    #[test]
    fn a_listener_that_binds_nowhere_is_refused_rather_than_bound_everywhere() {
        let message = refused(&document(&[], "[proxy.http]\nport = 8080\n"));
        assert!(message.contains("proxy.http.listen_address"), "{message}");
        let raw = refused(&document(&[], "[proxy.raw]\nmax_ports_per_guest = 1\n"));
        assert!(raw.contains("proxy.raw.listen_address"), "{raw}");
        // A host that offers no way in is not asked where it would have bound one.
        assert_eq!(parsed(&whole()).proxy, ProxyConfig::default());
    }

    #[test]
    fn each_listener_binds_where_it_says_and_not_where_the_other_does() {
        let both = with(&bound(
            "[proxy.http]\nport = 8080\n\n[proxy.raw]\nmax_ports_per_guest = 1\n",
        ));
        assert_eq!(
            both.proxy.http.unwrap().listen_address,
            IpAddr::from([0, 0, 0, 0]),
            "HTTP faces the edge"
        );
        assert_eq!(
            both.proxy.raw.unwrap().listen_address,
            IpAddr::from([10, 0, 5, 18]),
            "raw ports face the relay"
        );
    }

    #[test]
    fn tls_is_a_property_of_the_one_listener_rather_than_a_second_one() {
        let plain = with(&bound("[proxy.http]\nport = 8080\n"));
        let listener = plain.proxy.http.unwrap();
        assert_eq!(listener.port, 8080);
        assert_eq!(listener.tls, None, "no material is plain HTTP, not a refusal");

        let secure = with(&bound(
            "[proxy.http]\nport = 8443\n\n[proxy.http.tls]\ncertificate = \"/tls/c\"\nkey = \"/tls/k\"\n",
        ));
        assert_eq!(secure.proxy.http.unwrap().port, 8443);

        // The listener it would have belonged to is gone, so there is nowhere to write a second.
        let second = refused(&document(&[], &bound("[proxy.https]\nport = 8443\n")));
        assert!(second.contains("https"), "{second}");
    }

    #[test]
    fn a_listener_is_named_whole_or_not_at_all() {
        let message = refused(&document(
            &[],
            &bound("[proxy.http]\nport = 8443\n\n[proxy.http.tls]\n"),
        ));
        assert!(message.contains("proxy.http.tls.certificate"), "{message}");
        let half = refused(&document(
            &[],
            &bound("[proxy.http]\nport = 8443\n\n[proxy.http.tls]\ncertificate = \"/tls/origin.crt\"\n"),
        ));
        assert!(half.contains("proxy.http.tls.key"), "{half}");
        let pool = refused(&document(
            &[],
            &bound("[proxy.http]\nport = 8443\n\n[proxy.http.tls]\ncertificate = \"/tls/c\"\nkey = \"/tls/k\"\n\n[proxy.http.tls.client_ca]\n"),
        ));
        assert!(pool.contains("proxy.http.tls.client_ca.certificate"), "{pool}");
    }

    #[test]
    fn a_trust_pool_is_only_reachable_where_something_serves_tls_to_check_a_caller_against_it() {
        let served = with(&bound("[proxy.http]\nport = 8443\n\n[proxy.http.tls]\ncertificate = \"/tls/c\"\nkey = \"/tls/k\"\n\n[proxy.http.tls.client_ca]\ncertificate = \"/tls/ca.pem\"\n"));
        assert_eq!(
            served.proxy.http.unwrap().tls.unwrap().client_ca,
            Some(PathBuf::from("/tls/ca.pem"))
        );
        let open = with(&bound(
            "[proxy.http]\nport = 8443\n\n[proxy.http.tls]\ncertificate = \"/tls/c\"\nkey = \"/tls/k\"\n",
        ));
        assert_eq!(open.proxy.http.unwrap().tls.unwrap().client_ca, None);
    }

    #[test]
    fn how_many_ports_an_app_may_name_is_the_hosts_to_say_and_is_bounded_by_the_slot() {
        assert_eq!(
            with(&bound("[proxy.raw]\nmax_ports_per_guest = 1\n"))
                .proxy
                .raw
                .unwrap()
                .max_ports_per_guest,
            1
        );
        for refused_count in ["0", &(MAX_RAW_PORTS + 1).to_string()] {
            let message = refused(&document(
                &[],
                &bound(&format!("[proxy.raw]\nmax_ports_per_guest = {refused_count}\n")),
            ));
            assert!(message.contains("proxy.raw.max_ports_per_guest"), "{message}");
        }
        assert_eq!(
            with(&bound(&format!(
                "[proxy.raw]\nmax_ports_per_guest = {MAX_RAW_PORTS}\n"
            )))
            .proxy
            .raw
            .unwrap()
            .max_ports_per_guest,
            MAX_RAW_PORTS
        );
    }

    #[test]
    fn a_scrape_surface_is_bound_where_the_document_says_and_nowhere_otherwise() {
        assert_eq!(parsed(&whole()).metrics, None);

        let on = with("[metrics]\nport = 9100\nlisten_address = \"0.0.0.0\"\n");
        assert_eq!(
            on.metrics,
            Some(MetricsConfig {
                port: 9100,
                listen_address: IpAddr::from([0, 0, 0, 0]),
            })
        );

        assert!(refused(&document(&[], "[metrics]\nport = 9100\n")).contains("metrics.listen_address"));
        assert!(
            refused(&document(&[], "[metrics]\nlisten_address = \"127.0.0.1\"\n")).contains("metrics.port")
        );
        assert!(refused(&document(
            &[],
            "[metrics]\nport = 21000\nlisten_address = \"127.0.0.1\"\n"
        ))
        .contains("a slot takes"));
        assert!(refused(&document(
            &[],
            "[metrics]\nport = 9100\nlisten_address = \"here\"\n"
        ))
        .contains("metrics.listen_address"));
        let clash = refused(&document(
            &[],
            &bound("[proxy.http]\nport = 9100\n\n[metrics]\nport = 9100\nlisten_address = \"127.0.0.1\"\n"),
        ));
        assert!(clash.contains("proxy.http.port"), "{clash}");
    }

    #[test]
    fn a_host_keeps_256_mib_of_each_app_unless_the_document_says_how_much() {
        let silent = parsed(&whole()).logs;
        assert_eq!(silent, LogsConfig::default());
        assert_eq!(silent.keep_bytes_per_app, 256 * 1024 * 1024);

        assert_eq!(
            with("[logs]\nkeep_mib_per_app = 8\n").logs.keep_bytes_per_app,
            8 * 1024 * 1024
        );

        assert!(refused(&document(&[], "[logs]\n")).contains("logs.keep_mib_per_app"));
        let nothing = refused(&document(&[], "[logs]\nkeep_mib_per_app = 0\n"));
        assert!(nothing.contains("logs.keep_mib_per_app"), "{nothing}");
        assert!(nothing.contains("more than nothing"), "{nothing}");
        assert!(
            refused(&document(&[], "[logs]\nkeep_mib_per_app = 9223372036854775807\n"))
                .contains("logs.keep_mib_per_app")
        );
        assert!(refused(&document(&[], "[logs]\nkeep_mib_per_app = 256\nfiles = 3\n")).contains("files"));
    }

    #[test]
    fn a_range_that_nft_would_reject_is_refused_before_the_ruleset_is_rendered() {
        let bad = |value: &str| refused(&document(&[("network.denied_egress_addresses_v4", value)], ""));
        assert!(bad("[\"172.31.0.0\"]").contains("prefix length"));
        assert!(bad("[\"172.31.0.0/33\"]").contains("wider"));
        assert!(bad("[\"fd00::/8\"]").contains("IPv4"));
        assert!(bad("[\"10.0.0.0/eight\"]").contains("not a prefix length"));
        assert!(refused(&document(
            &[("network.denied_egress_addresses_v6", "[\"172.31.0.0/16\"]")],
            ""
        ))
        .contains("IPv6"));
        assert!(refused(&document(
            &[("network.denied_egress_addresses_v6", "[\"fd00::/129\"]")],
            ""
        ))
        .contains("wider"));
        assert_eq!(
            parsed(&document(
                &[("network.denied_egress_addresses_v4", "[\"172.31.0.0/16\"]")],
                ""
            ))
            .denied_egress_addresses_v4,
            vec!["172.31.0.0/16".to_string()]
        );
        assert_eq!(
            parsed(&document(
                &[("network.denied_egress_addresses_v6", "[\" fd00::/8 \"]")],
                ""
            ))
            .denied_egress_addresses_v6,
            vec!["fd00::/8".to_string()]
        );
    }

    #[test]
    fn a_store_this_host_has_no_backend_for_is_refused_at_startup() {
        let store = |value: &str| document(&[("artifacts.store_url", value)], "");
        assert!(refused(&store("\"gs://bucket\"")).contains("no gs backend"));
        assert!(refused(&store("\"s3://\"")).contains("bucket"));
        assert!(refused(&store("\"srv/artifacts\"")).contains("absolute"));
        assert_eq!(
            parsed(&store("\"s3://nibrun/artifacts\"")).artifact_store_url,
            "s3://nibrun/artifacts"
        );
        assert_eq!(
            parsed(&document(&[("exports.store_url", "\"s3://nibrun-exports\"")], "")).export_store_url,
            "s3://nibrun-exports"
        );
    }

    #[test]
    fn a_prefix_that_would_become_a_key_nobody_can_find_is_refused() {
        for bad in ["/volumes", "volumes/", "", "volumes//app", "volumes/../etc"] {
            let text = document(&[("volumes.storage_prefix", &format!("\"{bad}\""))], "");
            assert!(
                HostConfig::from_toml(&text).is_err(),
                "{bad} was accepted as a storage prefix"
            );
        }
        let too_long = "a".repeat(MAX_STORAGE_PREFIX_BYTES + 1);
        assert!(refused(&document(
            &[("volumes.storage_prefix", &format!("\"{too_long}\""))],
            ""
        ))
        .contains("at most 512 bytes"));
        let longest = "b".repeat(MAX_STORAGE_PREFIX_BYTES);
        assert_eq!(
            parsed(&document(
                &[("volumes.storage_prefix", &format!("\"{longest}\""))],
                ""
            ))
            .storage_prefix,
            longest
        );
        assert_eq!(
            parsed(&document(
                &[("volumes.storage_prefix", "\"hosts/one/volumes\"")],
                ""
            ))
            .storage_prefix,
            "hosts/one/volumes"
        );
    }

    #[test]
    fn a_backend_travels_with_the_settings_it_reads_and_no_others() {
        assert_eq!(parsed(&whole()).volumes, VolumeBackend::LocalFile);
        assert_eq!(VolumeBackend::LocalFile.as_str(), "local-file");
        assert!(refused(&document(&[("volumes.backend", "\"nfs\"")], "")).contains("no nfs"),);

        let orphaned = refused(&document(&[], ZEROFS));
        assert!(orphaned.contains("volumes.zerofs"), "{orphaned}");
        assert!(orphaned.contains("local-file"), "{orphaned}");

        let unaddressed = refused(&document(&[("volumes.backend", "\"zerofs\"")], ""));
        assert!(unaddressed.contains("volumes.zerofs"), "{unaddressed}");
    }

    const ZEROFS: &str = r#"[volumes.zerofs]
binary = "/usr/local/bin/zerofs"
config_file = "/etc/zerofs/one.toml"
mount_path = "/srv/zerofs"
nbd_socket_path = "/run/zerofs/one.sock"
ninep_socket_path = "/run/zerofs/9p.sock"
rpc_socket_path = "/run/zerofs/rpc.sock"
storage_url = "s3://filesystems-one/host-1"
cache_dir = "/data/zerofs"
cache_disk_gib = 70
cache_memory_gib = 2
checkpoint_runtime_dir = "/run/zerofs-checkpoints"
checkpoint_config_file = "/etc/zerofs/checkpoint.toml"
checkpoint_cache_dir = "/data/zerofs-checkpoint"
"#;

    #[test]
    fn where_this_hosts_zerofs_is_can_be_moved_whole() {
        let config = parsed(&document(&[("volumes.backend", "\"zerofs\"")], ZEROFS));
        assert_eq!(config.volumes.as_str(), "zerofs");
        let zerofs = config.volumes.zerofs().unwrap();
        assert_eq!(zerofs.binary, PathBuf::from("/usr/local/bin/zerofs"));
        assert_eq!(
            zerofs.checkpoint_runtime_dir,
            PathBuf::from("/run/zerofs-checkpoints")
        );
        assert!(refused(&document(
            &[("volumes.backend", "\"zerofs\"")],
            &ZEROFS.replace("/srv/zerofs", "srv/zerofs")
        ))
        .contains("volumes.zerofs.mount_path"));
        assert!(refused(&document(
            &[("volumes.backend", "\"zerofs\"")],
            "[volumes.zerofs]\nbinary = \"/usr/local/bin/zerofs\"\n"
        ))
        .contains("volumes.zerofs.config_file"));
    }

    // The cache this host reserves against is read back out of the file `install` renders, and
    // `cache_gigabytes` truncates to whole gibibytes — so a fraction here would hold back a number
    // ZeroFS is not taking. Stating it in gibibytes is what makes that unspellable.
    #[test]
    fn a_cache_is_stated_in_the_unit_it_can_be_read_back_in() {
        let config = parsed(&document(&[("volumes.backend", "\"zerofs\"")], ZEROFS));
        let zerofs = config.volumes.zerofs().unwrap();
        assert_eq!(zerofs.cache_disk_mib, 70 * 1024);
        assert_eq!(zerofs.cache_memory_mib, 2 * 1024);

        for empty in ["cache_disk_gib", "cache_memory_gib"] {
            let zeroed = ZEROFS
                .replace(&format!("{empty} = 70"), &format!("{empty} = 0"))
                .replace(&format!("{empty} = 2"), &format!("{empty} = 0"));
            let message = refused(&document(&[("volumes.backend", "\"zerofs\"")], &zeroed));
            assert!(message.contains(empty), "{message}");
        }
    }

    // ZeroFS binds this itself, and a second binding is a startup failure over there rather than
    // a refusal here — so a host that serves both is stopped while an operator is still watching.
    #[test]
    fn a_metrics_port_zerofs_already_holds_is_refused() {
        let clash =
            format!("{ZEROFS}\n[metrics]\nport = {ZEROFS_PROMETHEUS_PORT}\nlisten_address = \"127.0.0.1\"\n");
        let message = refused(&document(&[("volumes.backend", "\"zerofs\"")], &clash));
        assert!(message.contains("metrics.port"), "{message}");

        let local = format!("[metrics]\nport = {ZEROFS_PROMETHEUS_PORT}\nlisten_address = \"127.0.0.1\"\n");
        assert_eq!(
            parsed(&document(&[], &local)).metrics.unwrap().port,
            ZEROFS_PROMETHEUS_PORT,
            "a local-file host runs no zerofs, so nothing holds that port"
        );
    }

    #[test]
    fn a_finished_bundle_is_never_kept_inside_the_tree_the_reap_removes() {
        for staging in ["\"/var/lib/nibrunner/exports\"", "\"/mnt/scratch/exports\""] {
            let config = parsed(&document(&[("exports.staging_dir", staging)], ""));
            let store = PathBuf::from(&config.export_store_url);
            assert!(
                !store.starts_with(&config.export_staging_dir),
                "{} is inside {}",
                store.display(),
                config.export_staging_dir.display()
            );
        }
    }

    #[test]
    fn a_malformed_document_names_the_file_it_came_from() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        std::fs::write(&path, "[proxy\n").unwrap();
        let message = HostConfig::from_file(&path).unwrap_err().message();
        assert!(message.contains("config.toml"), "{message}");
    }

    // What `install` writes a host that has none is the smallest document every test here starts
    // from, plus the listener a starting point has to serve on, the page what an app has used is
    // read from, and the log cap written out rather than left to be found — so the two are held
    // to be one text.
    #[test]
    fn the_configuration_this_binary_carries_is_the_smallest_document_with_a_listener_rendered() {
        assert_eq!(
            HostConfig::starter(1000).to_toml(),
            document(
                &[],
                &bound(
                    "\n[proxy.http]\nport = 80\n\n[metrics]\nport = 9100\nlisten_address = \"127.0.0.1\"\n\n[logs]\nkeep_mib_per_app = 256\n"
                )
            )
        );
    }

    // Reading is writing run backwards, key for key. Every key is required on the way in and every
    // unknown one refused, so a key rendered that is not read, or read that is not rendered, fails
    // here by name — which is what lets the example this repository ships be written from code.
    #[test]
    fn a_configuration_rendered_is_the_configuration_read_back() {
        for config in [
            HostConfig::starter(320),
            HostConfig::example(),
            HostConfig::under(Path::new("/srv/one-host")),
        ] {
            let rendered = config.to_toml();
            assert_eq!(parsed(&rendered), config, "{rendered}");
        }
    }

    // Every section this daemon reads is in the example, or it is not an example of every section.
    #[test]
    fn the_example_names_every_section_there_is() {
        let config = HostConfig::example();
        assert!(config.volumes.zerofs().is_some());
        assert!(config
            .proxy
            .http
            .as_ref()
            .and_then(|http| http.tls.as_ref())
            .and_then(|tls| tls.client_ca.as_ref())
            .is_some());
        assert!(config.proxy.raw.is_some());
        assert!(config.metrics.is_some());
        assert!(config.filesystem.is_some());
    }

    #[test]
    fn a_host_nothing_asks_about_a_guests_files_serves_no_socket_to_ask_on() {
        assert_eq!(parsed(&whole()).filesystem, None);
        assert_eq!(
            with("[filesystem]\nsocket = \"/run/nibrunner/filesystem.sock\"\n").filesystem,
            Some(FilesystemConfig {
                socket: PathBuf::from("/run/nibrunner/filesystem.sock"),
            })
        );
    }

    #[test]
    fn a_socket_that_is_not_somewhere_this_host_can_put_one_is_refused_by_name() {
        for named in ["filesystem.sock", ""] {
            let message = refused(&document(&[], &format!("[filesystem]\nsocket = \"{named}\"\n")));
            assert!(message.contains("filesystem.socket"), "{named}: {message}");
        }
        assert!(refused(&document(&[], "[filesystem]\n")).contains("filesystem.socket"));
    }

    #[test]
    fn a_host_rooted_under_one_directory_keeps_every_file_it_writes_inside_it() {
        let root = Path::new("/srv/one-host");
        let config = HostConfig::under(root);
        for path in [
            config.state_db_file(),
            config.instances_file(),
            config.slots_file(),
            config.slot_cursor_file(),
            config.activity_file(),
            config.deleted_volumes_file(),
            config.artifact_cache_dir(),
            config.vm_dir(),
            config.volumes_dir(),
            config.logs_dir(),
            config.desired_state_file.clone(),
            config.versions_file.clone(),
            config.export_staging_dir.clone(),
            config.firecracker_dir.clone(),
        ] {
            assert!(
                path.starts_with(root),
                "{} escapes {}",
                path.display(),
                root.display()
            );
        }
        assert_eq!(config.in_state_dir("anything"), root.join("state/anything"));
        assert_eq!(config.volumes, VolumeBackend::LocalFile);
        assert_eq!(config.proxy, ProxyConfig::default());
    }

    #[test]
    fn every_file_this_host_keeps_has_a_name_of_its_own() {
        let config = HostConfig::under(Path::new("/srv/one-host"));
        let named = [
            config.state_db_file(),
            config.instances_file(),
            config.slots_file(),
            config.slot_cursor_file(),
            config.activity_file(),
            config.deleted_volumes_file(),
            config.artifact_cache_dir(),
            config.vm_dir(),
            config.volumes_dir(),
            config.logs_dir(),
        ];
        let distinct: std::collections::BTreeSet<_> = named.iter().collect();
        assert_eq!(distinct.len(), named.len());
    }

    #[test]
    fn a_whole_document_reads_back_as_it_was_written() {
        let config = parsed(&document(
            &[
                ("paths.state_dir", "\"/srv/nibrunner\""),
                ("paths.snapshot_dir", "\"/mnt/cache/snapshots\""),
                ("artifacts.store_url", "\"s3://nibrun-artifacts/prod\""),
                ("network.denied_egress_addresses_v4", "[\"172.31.0.0/16\"]"),
            ],
            r#"[proxy.http]
listen_address = "0.0.0.0"
port = 443

[proxy.http.tls]
certificate = "/etc/nibrunner/origin.crt"
key = "/etc/nibrunner/origin.key"

[proxy.http.tls.client_ca]
certificate = "/etc/nibrunner/origin-pull-ca.pem"

[proxy.raw]
listen_address = "10.0.5.18"
max_ports_per_guest = 1

[metrics]
port = 9100
listen_address = "127.0.0.1"

[logs]
keep_mib_per_app = 64
"#,
        ));
        assert_eq!(config.state_dir, PathBuf::from("/srv/nibrunner"));
        assert_eq!(config.snapshot_dir, PathBuf::from("/mnt/cache/snapshots"));
        assert_eq!(config.artifact_store_url, "s3://nibrun-artifacts/prod");
        assert_eq!(
            config.denied_egress_addresses_v4,
            vec!["172.31.0.0/16".to_string()]
        );
        assert_eq!(
            config.proxy.http,
            Some(HttpListener {
                listen_address: IpAddr::from([0, 0, 0, 0]),
                port: 443,
                tls: Some(TlsMaterial {
                    certificate: PathBuf::from("/etc/nibrunner/origin.crt"),
                    key: PathBuf::from("/etc/nibrunner/origin.key"),
                    client_ca: Some(PathBuf::from("/etc/nibrunner/origin-pull-ca.pem")),
                }),
                redirect_from_port: None,
            })
        );
        assert_eq!(
            config.proxy.raw,
            Some(RawPorts {
                listen_address: IpAddr::from([10, 0, 5, 18]),
                max_ports_per_guest: 1,
            })
        );
        assert_eq!(
            config.metrics,
            Some(MetricsConfig {
                port: 9100,
                listen_address: IpAddr::from([127, 0, 0, 1]),
            })
        );
        assert_eq!(config.logs.keep_bytes_per_app, 64 * 1024 * 1024);
    }

    mod schema {
        use super::*;

        fn validator() -> jsonschema::Validator {
            let schema = HostConfig::schema().to_value();
            jsonschema::meta::validate(&schema).expect("a schema the draft accepts");
            jsonschema::validator_for(&schema).expect("a schema that compiles")
        }

        /// The file as the schema sees it: TOML has no null and no other type JSON lacks, so a
        /// document is the same document either way.
        fn json(text: &str) -> serde_json::Value {
            let document: toml::Value = toml::from_str(text).expect(text);
            serde_json::to_value(document).expect("a TOML value is a JSON value")
        }

        /// `WHOLE` on the zerofs backend, with `ZEROFS` as given — or with one line of it
        /// rewritten, which a rewrite that misses leaves accepted, and so caught below.
        fn zerofs(section: &str) -> String {
            document(&[("volumes.backend", "\"zerofs\"")], section)
        }

        #[test]
        fn the_schema_accepts_what_the_parser_accepts() {
            let validator = validator();
            let accepted = [
                whole(),
                zerofs(ZEROFS),
                HostConfig::starter(320).to_toml(),
                HostConfig::example().to_toml(),
                HostConfig::under(Path::new("/srv/one-host")).to_toml(),
                document(
                    &[
                        ("artifacts.store_url", "\"s3://nibrun-artifacts\""),
                        ("network.denied_egress_addresses_v4", "[\"172.31.0.0/16\"]"),
                        ("network.denied_egress_addresses_v6", "[\"fd00::/8\"]"),
                    ],
                    &bound(
                        "[proxy.http]\nport = 443\n\n[proxy.http.tls]\ncertificate = \"/etc/nibrunner/origin.crt\"\nkey = \"/etc/nibrunner/origin.key\"\n\n[proxy.http.tls.client_ca]\ncertificate = \"/etc/nibrunner/origin-pull-ca.pem\"\n\n[proxy.raw]\nmax_ports_per_guest = 7\n\n[metrics]\nport = 9100\nlisten_address = \"127.0.0.1\"\n",
                    ),
                ),
            ];
            for text in accepted {
                parsed(&text);
                let errors: Vec<String> = validator
                    .iter_errors(&json(&text))
                    .map(|e| e.to_string())
                    .collect();
                assert!(errors.is_empty(), "{errors:#?}\n{text}");
            }
        }

        #[test]
        fn the_schema_refuses_what_the_parser_refuses() {
            let validator = validator();
            let mut broken: Vec<String> = [
                "max_apps",
                "paths.state_dir",
                "paths.versions_file",
                "artifacts.store_url",
                "volumes.backend",
                "volumes.storage_prefix",
                "exports.staging_dir",
                "network.denied_egress_addresses_v6",
            ]
            .into_iter()
            .map(without)
            .collect();
            broken.extend(
                [
                    ("max_apps", "0"),
                    ("max_apps", "5568"),
                    ("paths.state_dir", "\"var/lib/nibrunner\""),
                    ("artifacts.store_url", "\"gs://bucket\""),
                    ("artifacts.store_url", "\"s3://\""),
                    ("volumes.backend", "\"nfs\""),
                    ("volumes.storage_prefix", "\"\""),
                    ("volumes.storage_prefix", "\"/volumes\""),
                    ("volumes.storage_prefix", "\"volumes/\""),
                    ("volumes.storage_prefix", "\"a//b\""),
                    ("volumes.storage_prefix", "\"a/../b\""),
                    ("network.denied_egress_addresses_v4", "[\"172.31.0.0\"]"),
                    ("network.denied_egress_addresses_v4", "[\"fd00::/8\"]"),
                    ("network.denied_egress_addresses_v6", "[\"172.31.0.0/16\"]"),
                ]
                .into_iter()
                .map(|change| document(&[change], "")),
            );
            broken.extend(
                [
                    "[proxy.http]\nport = 0\n",
                    "[proxy.http]\nport = 80\n\n[proxy.http.tls]\ncertificate = \"/etc/nibrunner/origin.crt\"\n",
                    "[proxy.raw]\nmax_ports_per_guest = 0\n",
                    "[proxy.raw]\nmax_ports_per_guest = 8\n",
                    "[metrics]\nport = 9100\n",
                    "[metrics]\nport = 9100\nlisten_address = \"127.0.0.1\"\nroute = \"/metrics\"\n",
                    "[logs]\n",
                    "[logs]\nkeep_mib_per_app = 0\n",
                    "[logs]\nkeep_mib_per_app = 256\nfiles = 3\n",
                    "[proxy.http]\nport = 80\n\n[storage]\nbackend = \"zerofs\"\n",
                ]
                .into_iter()
                .map(|extra| document(&[], &bound(extra))),
            );
            broken.extend([
                document(&[], ZEROFS),
                zerofs(""),
                zerofs(&ZEROFS.replace("cache_disk_gib = 70", "cache_disk_gib = 0")),
                zerofs(&ZEROFS.replace("s3://filesystems-one/host-1", "filesystems/host-1")),
            ]);
            for text in broken {
                HostConfig::from_toml(&text).expect_err(&text);
                assert!(!validator.is_valid(&json(&text)), "the schema took\n{text}");
            }
        }
    }
}
