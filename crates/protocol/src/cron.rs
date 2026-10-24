#[cfg(feature = "schema")]
use std::borrow::Cow;

#[cfg(feature = "schema")]
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};

use crate::{
    AppId, DeploymentId, InvalidValue, SecretString, Sha256Digest, TenantEnvironment, Timestamp, REDACTED,
};

pub const MAX_CRONTAB_BYTES: usize = 65_536;
pub const MAX_CRON_ENVIRONMENT_VARIABLES: usize = 256;
pub const MAX_CRON_SCHEDULE_LENGTH: usize = 256;
pub const MAX_CRON_COMMAND_LENGTH: usize = 4096;
pub const MAX_CRON_RETRIES: usize = 5;
pub const MAX_CRON_RETRY_DELAY_MS: u32 = 3_600_000;
const MAX_CRON_JOB_NAME_LENGTH: usize = 63;
/// `is_nonempty_line`, in the schema's words.
#[cfg(feature = "schema")]
const CRON_LINE_PATTERN: &str = r"^[^\r\n\u0000]*[^\r\n\u0000 \t][^\r\n\u0000]*$";
#[cfg(feature = "schema")]
const CRON_JOB_NAME_PATTERN: &str = "^[a-z0-9][a-z0-9-]{0,62}$";

fn is_nonempty_line(value: &str) -> bool {
    !value.trim_matches([' ', '\t']).is_empty() && !value.contains(['\0', '\r', '\n'])
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CronSchedule(String);

impl CronSchedule {
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidValue> {
        let value = value.into();
        if value.chars().count() > MAX_CRON_SCHEDULE_LENGTH || !is_nonempty_line(&value) {
            return Err(InvalidValue::new_public(
                "a cron schedule must be a nonempty line within the length limit",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for CronSchedule {
    type Error = InvalidValue;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<CronSchedule> for String {
    fn from(value: CronSchedule) -> Self {
        value.0
    }
}

#[cfg(feature = "schema")]
impl JsonSchema for CronSchedule {
    fn schema_name() -> Cow<'static, str> {
        "CronSchedule".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "a five-field cron expression or an alias such as @daily, as the crontab line gave it",
            "pattern": CRON_LINE_PATTERN,
            "maxLength": MAX_CRON_SCHEDULE_LENGTH,
        })
    }
}

/// A job a deployment registered from inside its guest with `crontab`, as this host schedules it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReportedCronJob {
    /// The `cronJobId` every line its runs write to the app's log carries.
    pub job_id: String,
    pub schedule: CronSchedule,
    /// SHA-256 of the command line, in hex: the command itself may carry secrets, and a control
    /// plane that wrote the crontab can match it without reading it back.
    pub command_digest: Sha256Digest,
    /// When the host next runs it, in the host's `[cron] time_zone`. Absent when the schedule has
    /// no occurrence this host can find.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run: Option<Timestamp>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SecretString", into = "SecretString")]
pub struct CronCommand(SecretString);

impl CronCommand {
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidValue> {
        Self::try_from(SecretString::parse(value)?)
    }

    pub fn expose(&self) -> &str {
        self.0.expose()
    }
}

impl TryFrom<SecretString> for CronCommand {
    type Error = InvalidValue;

    fn try_from(value: SecretString) -> Result<Self, Self::Error> {
        if value.expose().len() > MAX_CRON_COMMAND_LENGTH || !is_nonempty_line(value.expose()) {
            return Err(InvalidValue::new_public(
                "a cron command must be a nonempty line within the length limit",
            ));
        }
        Ok(Self(value))
    }
}

impl From<CronCommand> for SecretString {
    fn from(value: CronCommand) -> Self {
        value.0
    }
}

impl std::fmt::Debug for CronCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(REDACTED)
    }
}

#[cfg(feature = "schema")]
impl JsonSchema for CronCommand {
    fn schema_name() -> Cow<'static, str> {
        "CronCommand".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "one nonempty line, run by the guest's shell as the app's user in its working directory",
            "pattern": CRON_LINE_PATTERN,
            "maxLength": MAX_CRON_COMMAND_LENGTH,
        })
    }
}

fn is_cron_job_name(value: &str) -> bool {
    value.len() <= MAX_CRON_JOB_NAME_LENGTH
        && value
            .as_bytes()
            .first()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

validated_string!(
    /// What the document calls a job: the `job` its runs' log lines carry and its report's `jobId`.
    CronJobName,
    "cron job name",
    "a lowercase letter or digit, then at most 62 lowercase letters, digits or `-`",
    is_cron_job_name,
    { "pattern": CRON_JOB_NAME_PATTERN, "maxLength": MAX_CRON_JOB_NAME_LENGTH }
);

/// A job the document declares for its instance's deployment: run on `schedule`, or once at
/// `at`, in the instance's guest. A run that has not ended after `timeoutS` is cancelled; one that
/// fails or times out is run again after each of `backoffMs` in turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(!try_from, transform = desired_cron_job_rules))]
#[serde(rename_all = "camelCase", try_from = "DesiredCronJobFields")]
pub struct DesiredCronJob {
    pub name: CronJobName,
    /// In the host's `[cron] time_zone`. Absent when `at` is named instead; one of the two always is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<CronSchedule>,
    /// A single run, due when the host first sees it at or after this instant, and never again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<Timestamp>,
    pub command: CronCommand,
    pub timeout_s: std::num::NonZeroU32,
    /// The delay before each retry, at most five of at most an hour each. Empty: no retry.
    pub backoff_ms: Vec<u32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DesiredCronJobFields {
    name: CronJobName,
    #[serde(default)]
    schedule: Option<CronSchedule>,
    #[serde(default)]
    at: Option<Timestamp>,
    command: CronCommand,
    timeout_s: std::num::NonZeroU32,
    backoff_ms: Vec<u32>,
}

impl TryFrom<DesiredCronJobFields> for DesiredCronJob {
    type Error = InvalidValue;

    fn try_from(fields: DesiredCronJobFields) -> Result<Self, Self::Error> {
        if fields.schedule.is_some() == fields.at.is_some() {
            return Err(InvalidValue::new_public(
                "a cron job runs on a schedule or once at an instant; name one",
            ));
        }
        if fields.backoff_ms.len() > MAX_CRON_RETRIES
            || fields
                .backoff_ms
                .iter()
                .any(|delay| *delay > MAX_CRON_RETRY_DELAY_MS)
        {
            return Err(InvalidValue::new_public(&format!(
                "a cron job is retried at most {MAX_CRON_RETRIES} times, each after at most an hour"
            )));
        }
        Ok(Self {
            name: fields.name,
            schedule: fields.schedule,
            at: fields.at,
            command: fields.command,
            timeout_s: fields.timeout_s,
            backoff_ms: fields.backoff_ms,
        })
    }
}

// What `TryFrom<DesiredCronJobFields>` refuses, said in the schema's words.
#[cfg(feature = "schema")]
fn desired_cron_job_rules(schema: &mut Schema) {
    schema.insert(
        "oneOf".into(),
        serde_json::json!([{ "required": ["schedule"] }, { "required": ["at"] }]),
    );
    if let Some(backoff) = schema
        .get_mut("properties")
        .and_then(|properties| properties.get_mut("backoffMs"))
        .and_then(serde_json::Value::as_object_mut)
    {
        backoff.insert("maxItems".into(), MAX_CRON_RETRIES.into());
        backoff.insert(
            "items".into(),
            serde_json::json!({ "type": "integer", "minimum": 0, "maximum": MAX_CRON_RETRY_DELAY_MS }),
        );
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Crontab(String);

impl Crontab {
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidValue> {
        let value = value.into();
        if value.len() > MAX_CRONTAB_BYTES || value.contains('\0') {
            return Err(InvalidValue::new_public(
                "a crontab must fit within the byte limit and contain no nul bytes",
            ));
        }
        Ok(Self(value))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Crontab {
    type Error = InvalidValue;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<Crontab> for String {
    fn from(value: Crontab) -> Self {
        value.0
    }
}

impl std::fmt::Debug for Crontab {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(REDACTED)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CronJobFields", into = "CronJobFields")]
pub struct CronJobDefinition {
    pub schedule: CronSchedule,
    pub command: CronCommand,
    pub environment: Option<TenantEnvironment>,
}

#[derive(Clone, Serialize, Deserialize)]
struct CronJobFields {
    schedule: CronSchedule,
    command: CronCommand,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    environment: Option<TenantEnvironment>,
}

impl TryFrom<CronJobFields> for CronJobDefinition {
    type Error = InvalidValue;

    fn try_from(value: CronJobFields) -> Result<Self, Self::Error> {
        if value
            .environment
            .as_ref()
            .is_some_and(|environment| environment.len() > MAX_CRON_ENVIRONMENT_VARIABLES)
        {
            return Err(InvalidValue::new_public(
                "a cron job names too many environment variables",
            ));
        }
        Ok(Self {
            schedule: value.schedule,
            command: value.command,
            environment: value.environment,
        })
    }
}

impl From<CronJobDefinition> for CronJobFields {
    fn from(value: CronJobDefinition) -> Self {
        Self {
            schedule: value.schedule,
            command: value.command,
            environment: value.environment,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CronJobDefinitions(Vec<CronJobDefinition>);

impl CronJobDefinitions {
    pub fn validate_limit(&self, maximum: usize) -> Result<(), InvalidValue> {
        if self.0.len() > maximum {
            return Err(InvalidValue::new_public("an app names too many cron jobs"));
        }
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = &CronJobDefinition> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<CronJobDefinition>> for CronJobDefinitions {
    fn from(value: Vec<CronJobDefinition>) -> Self {
        Self(value)
    }
}

impl From<CronJobDefinitions> for Vec<CronJobDefinition> {
    fn from(value: CronJobDefinitions) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronTable {
    pub app_id: AppId,
    pub deployment_id: DeploymentId,
    pub jobs: CronJobDefinitions,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crontab: Option<Crontab>,
}
