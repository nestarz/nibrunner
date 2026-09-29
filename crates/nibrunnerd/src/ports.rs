use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use guest_contract::filesystem::{MeasuredBytes, MeasuredCompute};
use protocol::{
    AppId, DeploymentId, DesiredInstance, DesiredLayer, DownloadUrl, ObjectKey, Sha256Digest, StoredObject,
};

use crate::adapters::vm::VmStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRequest {
    pub command: Vec<String>,
    pub stdin: Option<String>,
    pub timeout: Duration,
}

impl CommandRequest {
    pub fn new(command: &[&str]) -> Self {
        Self {
            command: command.iter().map(|part| part.to_string()).collect(),
            stdin: None,
            timeout: Duration::from_secs(120),
        }
    }

    pub fn with_stdin(mut self, stdin: impl Into<String>) -> Self {
        self.stdin = Some(stdin.into());
        self
    }

    pub fn executable(&self) -> &str {
        self.command.first().map_or("", String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

impl CommandResult {
    pub fn succeeded() -> Self {
        Self {
            code: 0,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    pub fn with_stdout(stdout: impl Into<String>) -> Self {
        Self {
            stdout: stdout.into(),
            ..Self::succeeded()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommandError {
    #[error("{executable} exited {code}{reason}")]
    Failed {
        executable: String,
        code: i32,
        reason: String,
    },
    #[error("{executable} did not finish in time")]
    TimedOut { executable: String },
    #[error("{executable} could not be run: {reason}")]
    Unstartable { executable: String, reason: String },
}

impl CommandError {
    pub fn message(&self) -> String {
        self.to_string()
    }

    pub fn failed(request: &CommandRequest, result: &CommandResult) -> Self {
        let tail = result.stderr.trim().lines().next_back().unwrap_or("").to_string();
        Self::Failed {
            executable: request.executable().to_string(),
            code: result.code,
            reason: if tail.is_empty() {
                String::new()
            } else {
                format!(": {tail}")
            },
        }
    }
}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait CommandRunner: Send + Sync {
    async fn run(&self, request: CommandRequest) -> Result<CommandResult, CommandError>;
}

#[async_trait]
pub trait CommandRunnerExt {
    async fn stdout_of(&self, request: CommandRequest) -> Result<String, CommandError>;
}

#[async_trait]
impl<T: CommandRunner + ?Sized> CommandRunnerExt for T {
    async fn stdout_of(&self, request: CommandRequest) -> Result<String, CommandError> {
        let result = self.run(request.clone()).await?;
        if result.code == 0 {
            Ok(result.stdout)
        } else {
            Err(CommandError::failed(&request, &result))
        }
    }
}

#[derive(Debug, Clone)]
pub struct BootRequest {
    pub desired: DesiredInstance,
    pub slot: nft_render::AppSlot,
    pub data_device_path: String,
    pub payload: PreparedPayload,
}

#[derive(Debug, Clone)]
pub struct SuspendRequest {
    pub app_id: AppId,
    pub deployment_id: DeploymentId,
    pub slot: nft_render::AppSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VmError {
    #[error("this host is busy starting other microVMs; retry later")]
    StartBusy,
    #[error("the saved microVM state cannot be restored: {reason}")]
    SnapshotUnusable { reason: String },
    #[error("this microVM must not be snapshotted: {reason}")]
    SleepRefused { reason: String },
    #[error("no microVM answered {socket_path}: {reason}")]
    Unreachable { socket_path: String, reason: String },
    #[error("the microVM refused {path} with {status}{detail}")]
    Rejected {
        path: String,
        status: u16,
        detail: String,
    },
    #[error("{0}")]
    Host(String),
}

impl VmError {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeOutcome {
    Restored,
    AlreadyRunning,
    ColdBoot,
}

impl WakeOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            WakeOutcome::Restored => "restored",
            WakeOutcome::AlreadyRunning => "already-running",
            WakeOutcome::ColdBoot => "cold-boot",
        }
    }
}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait Vmm: Send + Sync {
    async fn boot(&self, request: BootRequest) -> Result<(), VmError>;
    async fn sleep(&self, request: SuspendRequest) -> Result<(), VmError>;
    async fn wake(&self, request: SuspendRequest) -> Result<(), VmError>;
    async fn stop(&self, app_id: &AppId) -> Result<(), VmError>;
    async fn discard(&self, app_id: &AppId) -> Result<(), VmError>;
    async fn delete_tap(&self, tap_name: &str) -> Result<(), VmError>;
    async fn tap_names(&self) -> Vec<String>;
    async fn statuses(&self, app_ids: &[AppId]) -> BTreeMap<AppId, VmStatus>;
    async fn adopted_app_ids(&self) -> Vec<AppId>;
    /// Listen again for the log output of a microVM adopted from an earlier daemon run. The
    /// guest keeps its end of the socket across a daemon restart, but a fresh receiver never
    /// bound the host end, so without this its output would go nowhere for the rest of its life.
    async fn readopt(&self, app_id: &AppId) -> Result<(), VmError>;
    /// Why a microVM that exited went down, when its guest said: init's reason for shutting it
    /// down, or the kernel's panic. A guest killed from outside said nothing.
    async fn guest_verdict(&self, app_id: &AppId) -> Option<String>;
    fn working_dir(&self, app_id: &AppId) -> PathBuf;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArtifactError {
    #[error("the object could not be fetched: {0}")]
    Transfer(String),
    #[error("the object hashes to {actual}, not to the {expected} it claims")]
    DigestMismatch { expected: Sha256Digest, actual: String },
    #[error("the layer {digest} is neither a squashfs nor an ext4 image")]
    NotAnImage { digest: Sha256Digest },
    #[error("{entry} is not in the archive {url} serves")]
    NotInArchive { url: DownloadUrl, entry: String },
    #[error("the layer image could not be built: {0}")]
    Unpackable(String),
}

impl ArtifactError {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GuestReading {
    pub filesystem: Option<MeasuredBytes>,
    pub compute: Option<MeasuredCompute>,
}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait GuestMeasurements: Send + Sync {
    async fn measure(&self, app_id: &AppId) -> GuestReading;
}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn read(&self, object_key: &ObjectKey) -> Result<Vec<u8>, ArtifactError>;
    /// The body a URL answers with, for a layer the document says to fetch rather than to find here.
    async fn download(&self, url: &DownloadUrl) -> Result<Vec<u8>, ArtifactError>;
}

#[async_trait]
pub trait ArtifactStoreExt {
    async fn read_verified(&self, object: &StoredObject) -> Result<Vec<u8>, ArtifactError>;
}

#[async_trait]
impl<T: ArtifactStore + ?Sized> ArtifactStoreExt for T {
    async fn read_verified(&self, object: &StoredObject) -> Result<Vec<u8>, ArtifactError> {
        use sha2::Digest;

        let bytes = self.read(&object.object_key).await?;
        let actual = hex::encode(sha2::Sha256::digest(&bytes));
        if actual != object.digest.as_str() {
            return Err(ArtifactError::DigestMismatch {
                expected: object.digest.clone(),
                actual,
            });
        }
        Ok(bytes)
    }
}

/// The images a microVM is given for its layers, one each, in the order the document listed them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedPayload {
    pub layer_image_paths: Vec<PathBuf>,
    /// What was pulled from the store to prepare these, as the store sized it. 0 when every
    /// layer was already in the cache.
    pub fetched_bytes: u64,
}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait PayloadBuilder: Send + Sync {
    async fn prepare(&self, layers: &[DesiredLayer]) -> Result<PreparedPayload, ArtifactError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantLogEvent {
    pub app_id: AppId,
    pub deployment_id: DeploymentId,
    pub source_id: String,
    pub sequence: u64,
    pub observed_at: protocol::Timestamp,
    pub body: TenantLogBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantLogBody {
    /// One line the tenant wrote, without the newline that ended it. A line the tenant left
    /// open long enough, or was still writing when its stream closed, arrives as it stands.
    Data {
        stream: protocol::TenantLogStream,
        text: String,
    },
    Gap {
        dropped_bytes: u64,
    },
    /// The guest's supervisor restarted the tenant, in its place in the stream: after the
    /// tenant's last words, before its first ones back.
    Restart(protocol::TenantRestart),
}

/// Why a wake was refused, apart from want of memory: the word a counter uses, beside the
/// sentence a log line does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeFailure {
    HostStarting,
    NotNamed,
    NotOnRequest,
    NotIsolated,
    /// Its volume's device does not answer and could not be re-attached, so there is nothing
    /// sound to put the guest on.
    VolumeUnusable,
    WouldNotStart,
    NeverAnswered,
    Abandoned,
}

impl WakeFailure {
    pub fn as_str(self) -> &'static str {
        match self {
            WakeFailure::HostStarting => "host_starting",
            WakeFailure::NotNamed => "not_named",
            WakeFailure::NotOnRequest => "not_on_request",
            WakeFailure::NotIsolated => "not_isolated",
            WakeFailure::VolumeUnusable => "volume_unusable",
            WakeFailure::WouldNotStart => "would_not_start",
            WakeFailure::NeverAnswered => "never_answered",
            WakeFailure::Abandoned => "abandoned",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeRefusal {
    NoRoom { shortfall_mib: u64 },
    Failed { kind: WakeFailure, reason: String },
}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait Waker: Send + Sync {
    async fn wake(&self, app_id: &AppId) -> Result<(), WakeRefusal>;
}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait LogSink: Send + Sync {
    async fn publish(&self, events: Vec<TenantLogEvent>);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmCall {
    Boot,
    Sleep,
    Wake,
    Stop,
    Discard,
    DeleteTap,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::mocks;

    #[tokio::test]
    async fn a_command_that_failed_names_the_tail_of_what_it_said() {
        let (runner, log) = mocks::commands_answering(|_| {
            Ok(CommandResult {
                code: 1,
                stdout: String::new(),
                stderr: "banner\nthe actual reason\n".into(),
            })
        });
        let error = runner
            .stdout_of(CommandRequest::new(&["nft", "-f", "-"]))
            .await
            .unwrap_err();
        assert_eq!(error.message(), "nft exited 1: the actual reason");
        assert_eq!(log.executables(), vec!["nft"]);
        assert_eq!(log.calls()[0].command, vec!["nft", "-f", "-"]);
    }

    #[tokio::test]
    async fn a_command_that_succeeded_hands_back_what_it_wrote() {
        let (runner, _log) = mocks::commands_answering(|_| Ok(CommandResult::with_stdout("ok")));
        assert_eq!(
            runner.stdout_of(CommandRequest::new(&["nft"])).await.unwrap(),
            "ok"
        );
    }

    #[tokio::test]
    async fn a_command_that_could_not_be_started_is_handed_up_rather_than_swallowed() {
        let (runner, _log) = mocks::commands_answering(|request| {
            Err(CommandError::Unstartable {
                executable: request.executable().to_string(),
                reason: "no such file".into(),
            })
        });
        let error = runner
            .stdout_of(CommandRequest::new(&["mke2fs"]))
            .await
            .unwrap_err();
        assert_eq!(error.message(), "mke2fs could not be run: no such file");
    }

    #[test]
    fn a_request_with_no_words_in_it_names_no_executable() {
        assert_eq!(CommandRequest::new(&[]).executable(), "");
    }

    #[test]
    fn a_failure_with_nothing_on_stderr_still_names_the_code() {
        let request = CommandRequest::new(&["nft"]);
        let result = CommandResult {
            code: 2,
            stdout: String::new(),
            stderr: "   \n".into(),
        };
        assert_eq!(CommandError::failed(&request, &result).message(), "nft exited 2");
    }

    #[test]
    fn stdin_is_carried_on_the_request_that_will_be_fed_it() {
        let request = CommandRequest::new(&["nft", "-f", "-"]).with_stdin("table inet nibrun {}");
        assert_eq!(request.stdin.as_deref(), Some("table inet nibrun {}"));
    }

    #[test]
    fn every_way_a_wake_can_end_has_a_name_a_reader_can_tell_apart() {
        assert_eq!(WakeOutcome::Restored.as_str(), "restored");
        assert_eq!(WakeOutcome::AlreadyRunning.as_str(), "already-running");
        assert_eq!(WakeOutcome::ColdBoot.as_str(), "cold-boot");
    }
}
