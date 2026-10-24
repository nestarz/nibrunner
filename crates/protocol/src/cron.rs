//! The cron vocabulary of both documents, and the schedule parser the host runs, so that a control
//! plane refuses what the host would and computes the same next occurrence.
#[cfg(feature = "schema")]
use std::borrow::Cow;

use chrono::{DateTime, Datelike, Duration, LocalResult, NaiveDateTime, Offset, TimeZone, Utc};
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
        if value.chars().count() > MAX_CRON_SCHEDULE_LENGTH
            || !is_nonempty_line(&value)
            || ParsedSchedule::parse(&value).is_err()
        {
            return Err(InvalidValue::new_public(
                "a cron schedule must be a five-field expression or a supported alias within the length limit",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Its first occurrence strictly after `after`; [`InvalidCronSchedule::NoOccurrence`] when it
    /// has none within the parser's search.
    pub fn next_after<Z: TimeZone + Copy>(
        &self,
        after: DateTime<Utc>,
        time_zone: Z,
    ) -> Result<DateTime<Utc>, InvalidCronSchedule> {
        ParsedSchedule::parse(&self.0)?.next_after(after, time_zone)
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
            "description": "a five-field cron expression or an alias such as @daily, as the crontab line or the document gave it",
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

const SEARCH_DAYS: usize = 366 * 8;
const MAX_OFFSET_GAP_MINUTES: i64 = 24 * 60;
const MONTH_NAMES: [&str; 12] = [
    "JANUARY",
    "FEBRUARY",
    "MARCH",
    "APRIL",
    "MAY",
    "JUNE",
    "JULY",
    "AUGUST",
    "SEPTEMBER",
    "OCTOBER",
    "NOVEMBER",
    "DECEMBER",
];
const WEEKDAY_NAMES: [&str; 7] = [
    "SUNDAY",
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY",
    "SATURDAY",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidCronSchedule {
    #[error("the cron schedule must be a valid five-field expression or supported alias")]
    Expression,
    #[error("the cron schedule has no occurrence within the next eight years")]
    NoOccurrence,
}

#[derive(Debug, Clone)]
struct Field {
    values: Vec<u32>,
    wildcard: bool,
}

impl Field {
    fn parse(text: &str, minimum: u32, maximum: u32, names: &[&str]) -> Result<Self, InvalidCronSchedule> {
        let mut values = Vec::new();
        for part in text.split(',') {
            let (range, step) = match part.split_once('/') {
                Some((range, step)) => (
                    range,
                    step.parse::<i8>().map_err(|_| InvalidCronSchedule::Expression)?,
                ),
                None => (part, 1),
            };
            if step <= 0 {
                return Err(InvalidCronSchedule::Expression);
            }
            let number = |value: &str| -> Result<u32, InvalidCronSchedule> {
                let value = names
                    .iter()
                    .position(|name| {
                        value.eq_ignore_ascii_case(name) || value.eq_ignore_ascii_case(&name[..3])
                    })
                    .map_or_else(
                        || {
                            value
                                .parse::<i8>()
                                .map(|number| number as u32)
                                .map_err(|_| InvalidCronSchedule::Expression)
                        },
                        |index| Ok(index as u32 + minimum),
                    )?;
                if !(minimum..=maximum).contains(&value) {
                    return Err(InvalidCronSchedule::Expression);
                }
                Ok(value)
            };
            let (start, end) = if range == "*" {
                (minimum, maximum)
            } else if range.starts_with('-') && !range[1..].contains('-') {
                let start = number(range)?;
                (start, if part.contains('/') { maximum } else { start })
            } else if let Some((start, end)) = range.split_once('-') {
                (number(start)?, number(end)?)
            } else {
                let start = number(range)?;
                (start, if part.contains('/') { maximum } else { start })
            };
            if start > end {
                return Err(InvalidCronSchedule::Expression);
            }
            values.extend((start..=end).step_by(step as usize));
        }
        values.sort_unstable();
        values.dedup();
        Ok(Self {
            values,
            wildcard: text == "*",
        })
    }

    fn contains(&self, value: u32) -> bool {
        self.values.binary_search(&value).is_ok()
    }
}

#[derive(Debug, Clone)]
pub struct ParsedSchedule {
    minute: Field,
    hour: Field,
    day: Field,
    month: Field,
    weekday: Field,
}

impl ParsedSchedule {
    pub fn parse(expression: &str) -> Result<Self, InvalidCronSchedule> {
        if expression
            .chars()
            .any(|character| character.is_whitespace() && !matches!(character, ' ' | '\t'))
        {
            return Err(InvalidCronSchedule::Expression);
        }
        let normalized = expression.trim_matches([' ', '\t']).to_ascii_lowercase();
        let expression = match normalized.as_str() {
            "@yearly" | "@annually" => "0 0 1 1 *",
            "@monthly" => "0 0 1 * *",
            "@weekly" => "0 0 * * 0",
            "@daily" | "@midnight" => "0 0 * * *",
            "@hourly" => "0 * * * *",
            expression => expression,
        };
        let fields: Vec<_> = expression.split_whitespace().collect();
        let [minute, hour, day, month, weekday] = fields.as_slice() else {
            return Err(InvalidCronSchedule::Expression);
        };
        let mut weekday = Field::parse(weekday, 0, 7, &WEEKDAY_NAMES)?;
        for value in &mut weekday.values {
            if *value == 7 {
                *value = 0;
            }
        }
        weekday.values.sort_unstable();
        weekday.values.dedup();
        Ok(Self {
            minute: Field::parse(minute, 0, 59, &[])?,
            hour: Field::parse(hour, 0, 23, &[])?,
            day: Field::parse(day, 1, 31, &[])?,
            month: Field::parse(month, 1, 12, &MONTH_NAMES)?,
            weekday,
        })
    }

    /// The first occurrence strictly after `after`, in `time_zone`. A local time a clock change
    /// skips runs that much later; one it repeats runs at its first occurrence.
    pub fn next_after<Z: TimeZone + Copy>(
        &self,
        after: DateTime<Utc>,
        time_zone: Z,
    ) -> Result<DateTime<Utc>, InvalidCronSchedule> {
        let mut date = after.with_timezone(&time_zone).date_naive();
        for _ in 0..SEARCH_DAYS {
            let day_matches = self.day.contains(date.day());
            let weekday_matches = self.weekday.contains(date.weekday().num_days_from_sunday());
            let matches = if self.day.wildcard || self.weekday.wildcard {
                day_matches && weekday_matches
            } else {
                day_matches || weekday_matches
            };
            if self.month.contains(date.month()) && matches {
                let mut next = None;
                for hour in &self.hour.values {
                    for minute in &self.minute.values {
                        let local = date
                            .and_hms_opt(*hour, *minute, 0)
                            .ok_or(InvalidCronSchedule::Expression)?;
                        if let Some(candidate) = local_occurrence(local, time_zone) {
                            if candidate > after && next.is_none_or(|held| candidate < held) {
                                next = Some(candidate);
                            }
                        }
                    }
                }
                if let Some(next) = next {
                    return Ok(next);
                }
            }
            date = date.succ_opt().ok_or(InvalidCronSchedule::NoOccurrence)?;
        }
        Err(InvalidCronSchedule::NoOccurrence)
    }
}

fn local_occurrence<Z: TimeZone + Copy>(local: NaiveDateTime, time_zone: Z) -> Option<DateTime<Utc>> {
    match time_zone.from_local_datetime(&local) {
        LocalResult::Single(value) => Some(value.with_timezone(&Utc)),
        LocalResult::Ambiguous(first, second) => Some(first.min(second).with_timezone(&Utc)),
        LocalResult::None => {
            let mut before = None;
            let mut after = None;
            for minutes in 1..=MAX_OFFSET_GAP_MINUTES {
                let distance = Duration::minutes(minutes);
                if before.is_none() {
                    before = time_zone
                        .from_local_datetime(&local.checked_sub_signed(distance)?)
                        .earliest();
                }
                if after.is_none() {
                    after = time_zone
                        .from_local_datetime(&local.checked_add_signed(distance)?)
                        .earliest();
                }
                if let (Some(before), Some(after)) = (&before, &after) {
                    let gap =
                        after.offset().fix().local_minus_utc() - before.offset().fix().local_minus_utc();
                    let shifted = local.checked_add_signed(Duration::seconds(i64::from(gap)))?;
                    return time_zone
                        .from_local_datetime(&shifted)
                        .earliest()
                        .map(|value| value.with_timezone(&Utc));
                }
            }
            None
        }
    }
}
