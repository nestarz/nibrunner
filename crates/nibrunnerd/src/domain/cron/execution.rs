use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use protocol::{TenantLogStream, Timestamp};
use serde::{Deserialize, Serialize};

use crate::ports::{TenantLogBody, TenantLogEvent};

use super::jobs::CronWhen;
use super::scheduler::ScheduledRun;

const MAX_LINE_BYTES: usize = 16_384;
/// What `timeout(1)` exits with, so a run cut short reads as one.
const TIMED_OUT_EXIT: i32 = 124;

/// How an attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CronOutcome {
    /// The command's exit code, or 128 plus the signal that killed it.
    Exited(i32),
    TimedOut,
    /// Its deployment was stopped or replaced.
    Cancelled,
    Rejected(&'static str),
    Disconnected,
    /// The same job was still running when this one was due.
    Skipped,
}

impl CronOutcome {
    pub fn exit(self) -> Option<i32> {
        match self {
            Self::Exited(code) => Some(code),
            Self::TimedOut => Some(TIMED_OUT_EXIT),
            _ => None,
        }
    }

    pub fn completed(self) -> bool {
        self == Self::Exited(0)
    }

    fn status(self) -> CronStatus {
        if self.completed() {
            CronStatus::Completed
        } else {
            CronStatus::Failed
        }
    }

    pub fn reason(self) -> Option<String> {
        match self {
            Self::Exited(_) => None,
            Self::TimedOut => Some("timeout".into()),
            Self::Cancelled => Some("cancelled: deployment replaced".into()),
            Self::Rejected(why) => Some(format!("rejected: {why}")),
            Self::Disconnected => Some("disconnected".into()),
            Self::Skipped => Some("skipped: previous run still running".into()),
        }
    }

    /// Only a command that ran and failed is tried again.
    fn retried(self) -> bool {
        matches!(self, Self::Exited(code) if code != 0) || self == Self::TimedOut
    }
}

/// How long to wait before attempt `attempt + 1` of `run`, which ended with `outcome` at `now`:
/// never past its backoff, and never into its next occurrence, which runs instead.
pub fn retry_delay(
    run: &ScheduledRun,
    attempt: usize,
    outcome: CronOutcome,
    now: DateTime<Utc>,
    time_zone: Tz,
) -> Option<Duration> {
    let delay = *run.job.declared.as_ref()?.backoff.get(attempt - 1)?;
    let retried_at = now + chrono::Duration::from_std(delay).ok()?;
    let due_first = match &run.job.when {
        CronWhen::Schedule(schedule) => schedule
            .next_after(now, time_zone)
            .is_ok_and(|next| next <= retried_at),
        CronWhen::At(_) => false,
    };
    (outcome.retried() && !due_first).then_some(delay)
}

/// The occurrence an attempt belongs to, to the second, as its record's key and its lines say it.
pub fn instant(run: &ScheduledRun) -> String {
    run.scheduled_at.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// A key segment as the control plane spells one: `%` and `/` escaped.
fn segment(value: &str) -> String {
    value.replace('%', "%25").replace('/', "%2F")
}

/// `cron/<app>/<timeline>/<name>/<instant>/<attempt>`: the attempt's record, and its fence.
pub fn record_key(app: &str, timeline: &str, run: &ScheduledRun, attempt: usize) -> String {
    format!(
        "cron/{}/{}/{}/{}/{attempt}",
        segment(app),
        segment(timeline),
        run.job.id,
        instant(run)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CronStatus {
    Completed,
    Failed,
}

impl CronStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// One attempt as the control plane reads it under `cron/`: created when it starts, overwritten
/// once with how it ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CronRecord {
    pub host: String,
    pub rev: String,
    pub deployment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<String>,
    pub started: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub manual: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<CronStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl CronRecord {
    pub fn ended(self, outcome: CronOutcome, ended: String, duration_ms: u64) -> Self {
        Self {
            ended: Some(ended),
            exit: outcome.exit(),
            duration_ms: Some(duration_ms),
            status: Some(outcome.status()),
            reason: outcome.reason(),
            ..self
        }
    }
}

/// The trace an attempt is, as the hex its lines and its process are given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptTrace {
    pub trace: String,
    pub span: String,
}

impl AttemptTrace {
    pub fn traceparent(&self) -> String {
        format!("00-{}-{}-01", self.trace, self.span)
    }
}

pub struct CronRunOutput {
    run: ScheduledRun,
    attempt: usize,
    trace: AttemptTrace,
    source_id: String,
    run_id: String,
    sequence: u64,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl CronRunOutput {
    pub fn new(run: ScheduledRun, attempt: usize, trace: AttemptTrace) -> Self {
        let run_id = format!("{}/{attempt}", instant(&run));
        Self {
            run,
            attempt,
            trace,
            source_id: uuid::Uuid::new_v4().to_string(),
            run_id,
            sequence: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    }

    pub fn run(&self) -> &ScheduledRun {
        &self.run
    }

    pub fn trace(&self) -> &AttemptTrace {
        &self.trace
    }

    fn event(&mut self, stream: TenantLogStream, text: String) -> TenantLogEvent {
        let event = TenantLogEvent {
            app_id: self.run.app_id.clone(),
            deployment_id: self.run.deployment_id.clone(),
            source_id: self.source_id.clone(),
            sequence: self.sequence,
            observed_at: Timestamp::from_epoch_ms(crate::clock::now_ms()),
            body: TenantLogBody::CronData {
                stream,
                text,
                job_id: self.run.job.id.clone(),
                run_id: self.run_id.clone(),
                trace: self.trace.trace.clone(),
                span: self.trace.span.clone(),
            },
        };
        self.sequence += 1;
        event
    }

    pub fn started(&mut self) -> TenantLogEvent {
        let text = format!(
            "cron {} started (scheduled {}, attempt {})",
            self.run.job.id,
            instant(&self.run),
            self.attempt
        );
        self.event(TenantLogStream::Stdout, text)
    }

    pub fn ended(&mut self, outcome: CronOutcome, duration_ms: u64) -> TenantLogEvent {
        let status = outcome.status();
        let mut text = format!("cron {} {}", self.run.job.id, status.as_str());
        if let Some(exit) = outcome.exit() {
            text.push_str(&format!(" exit {exit}"));
        }
        text.push_str(&format!(" after {duration_ms} ms"));
        if let Some(reason) = outcome.reason() {
            text.push_str(&format!(": {reason}"));
        }
        let stream = match status {
            CronStatus::Completed => TenantLogStream::Stdout,
            CronStatus::Failed => TenantLogStream::Stderr,
        };
        self.event(stream, text)
    }

    pub fn receive(&mut self, stream: TenantLogStream, bytes: &[u8]) -> Vec<TenantLogEvent> {
        let held = match stream {
            TenantLogStream::Stdout => &mut self.stdout,
            TenantLogStream::Stderr => &mut self.stderr,
        };
        let mut lines = Vec::new();
        for byte in bytes {
            if *byte == b'\n' {
                lines.push(String::from_utf8_lossy(held).into_owned());
                held.clear();
            } else {
                held.push(*byte);
                if held.len() >= MAX_LINE_BYTES {
                    let cut = match std::str::from_utf8(held) {
                        Err(error) if error.error_len().is_none() => error.valid_up_to(),
                        _ => held.len(),
                    };
                    lines.push(String::from_utf8_lossy(&held[..cut]).into_owned());
                    held.drain(..cut);
                }
            }
        }
        lines.into_iter().map(|text| self.event(stream, text)).collect()
    }

    pub fn close(&mut self) -> Vec<TenantLogEvent> {
        let mut events = Vec::new();
        for stream in [TenantLogStream::Stdout, TenantLogStream::Stderr] {
            let bytes = match stream {
                TenantLogStream::Stdout => std::mem::take(&mut self.stdout),
                TenantLogStream::Stderr => std::mem::take(&mut self.stderr),
            };
            if !bytes.is_empty() {
                events.push(self.event(stream, String::from_utf8_lossy(&bytes).into_owned()));
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::cron::jobs::CronJobs;
    use crate::test_support::{app_id, cron_run, deployment_id};

    fn trace() -> AttemptTrace {
        AttemptTrace {
            trace: "019a1b2c3d4e0123456789abcdef0123".into(),
            span: "0123456789abcdef".into(),
        }
    }

    fn output() -> CronRunOutput {
        CronRunOutput::new(cron_run("echo secret"), 1, trace())
    }

    fn text(event: &TenantLogEvent) -> &str {
        let TenantLogBody::CronData { text, .. } = &event.body else {
            panic!("output is a data event");
        };
        text
    }

    fn declared(job: serde_json::Value) -> ScheduledRun {
        let jobs = CronJobs::declared(
            &app_id(),
            &deployment_id(),
            &[serde_json::from_value(job).unwrap()],
        );
        ScheduledRun {
            app_id: app_id(),
            deployment_id: deployment_id(),
            job: jobs.jobs[0].clone(),
            scheduled_at: "2026-10-24T16:15:00Z".parse().unwrap(),
        }
    }

    fn every(schedule: &str, backoff_ms: &[u32]) -> ScheduledRun {
        declared(serde_json::json!({
            "name": "tick", "schedule": schedule, "command": "echo tick; exit ${CRON_EXIT:-0}",
            "timeoutS": 300, "backoffMs": backoff_ms
        }))
    }

    #[test]
    fn split_characters_and_lines_keep_their_run_and_order() {
        let mut first = output();
        assert!(first.receive(TenantLogStream::Stdout, &[0xc3]).is_empty());
        let events = first.receive(TenantLogStream::Stdout, &[0xa9, b'\n', b'x']);
        assert_eq!(text(&events[0]), "é");
        assert_eq!(events[0].sequence, 0);
        let tail = first.close();
        assert_eq!(text(&tail[0]), "x");
        assert_eq!(tail[0].sequence, 1);
        assert_eq!(tail[0].source_id, events[0].source_id);
        assert!(!events[0].source_id.contains("secret"));
    }

    #[test]
    fn each_attempt_has_its_own_run_and_source_but_keeps_the_job_id() {
        let first = output().receive(TenantLogStream::Stdout, b"first\n").remove(0);
        let second = CronRunOutput::new(cron_run("echo secret"), 2, trace())
            .receive(TenantLogStream::Stderr, b"second\n")
            .remove(0);
        let TenantLogBody::CronData {
            job_id: first_job,
            run_id: first_run,
            ..
        } = first.body
        else {
            panic!("cron output carries its identity")
        };
        let TenantLogBody::CronData {
            job_id: second_job,
            run_id: second_run,
            ..
        } = second.body
        else {
            panic!("cron output carries its identity")
        };
        assert_eq!(first_job, second_job);
        assert_eq!(first_run, "2026-01-01T00:01:00Z/1");
        assert_eq!(second_run, "2026-01-01T00:01:00Z/2");
        assert_ne!(first.source_id, second.source_id);
    }

    #[test]
    fn the_first_and_last_lines_say_the_attempt_and_how_it_ended_under_its_trace() {
        let mut output = CronRunOutput::new(every("*/5 * * * *", &[]), 2, trace());
        let started = output.started();
        assert_eq!(
            text(&started),
            "cron tick started (scheduled 2026-10-24T16:15:00Z, attempt 2)"
        );
        let TenantLogBody::CronData {
            stream,
            job_id,
            run_id,
            trace: line_trace,
            span,
            ..
        } = &started.body
        else {
            panic!("a cron line")
        };
        assert_eq!(*stream, TenantLogStream::Stdout);
        assert_eq!(job_id, "tick");
        assert_eq!(run_id, "2026-10-24T16:15:00Z/2");
        assert_eq!(
            (line_trace.as_str(), span.as_str()),
            (trace().trace.as_str(), trace().span.as_str())
        );
        assert_eq!(
            text(&output.ended(CronOutcome::Exited(0), 344)),
            "cron tick completed exit 0 after 344 ms"
        );
        let timed_out = output.ended(CronOutcome::TimedOut, 10_002);
        assert_eq!(
            text(&timed_out),
            "cron tick failed exit 124 after 10002 ms: timeout"
        );
        assert!(matches!(
            timed_out.body,
            TenantLogBody::CronData {
                stream: TenantLogStream::Stderr,
                ..
            }
        ));
        assert_eq!(
            text(&output.ended(CronOutcome::Cancelled, 5)),
            "cron tick failed after 5 ms: cancelled: deployment replaced"
        );
        assert_eq!(output.started().sequence, 4);
    }

    #[test]
    fn a_record_is_keyed_by_its_timeline_occurrence_and_attempt_and_says_how_it_ended() {
        let run = every("*/5 * * * *", &[]);
        assert_eq!(
            record_key("a1b2c3d4", "git-branch/main", &run, 2),
            "cron/a1b2c3d4/git-branch%2Fmain/tick/2026-10-24T16:15:00Z/2"
        );
        let started = CronRecord {
            host: "ovh-1".into(),
            rev: "0123456789ab".into(),
            deployment: "dep-1".into(),
            trace: Some(trace().trace),
            started: "2026-10-24T16:15:00.120000Z".into(),
            manual: false,
            ended: None,
            exit: None,
            duration_ms: None,
            status: None,
            reason: None,
        };
        assert_eq!(
            serde_json::to_value(&started).unwrap(),
            serde_json::json!({
                "host": "ovh-1", "rev": "0123456789ab", "deployment": "dep-1",
                "trace": trace().trace, "started": "2026-10-24T16:15:00.120000Z"
            })
        );
        let ended = started.ended(
            CronOutcome::TimedOut,
            "2026-10-24T16:20:00.120000Z".into(),
            300_000,
        );
        let written = serde_json::to_value(&ended).unwrap();
        assert_eq!(written["exit"], 124);
        assert_eq!(written["status"], "failed");
        assert_eq!(written["reason"], "timeout");
        assert_eq!(written["duration_ms"], 300_000);
        assert_eq!(serde_json::from_value::<CronRecord>(written).unwrap(), ended);
    }

    #[test]
    fn a_failed_run_is_retried_after_each_delay_in_turn_and_never_past_the_last() {
        let run = every("@daily", &[1000, 60_000]);
        let now = "2026-10-24T16:15:01Z".parse().unwrap();
        let failed = CronOutcome::Exited(1);
        assert_eq!(
            retry_delay(&run, 1, failed, now, chrono_tz::UTC),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            retry_delay(&run, 2, failed, now, chrono_tz::UTC),
            Some(Duration::from_secs(60))
        );
        assert_eq!(retry_delay(&run, 3, failed, now, chrono_tz::UTC), None);
        assert_eq!(
            retry_delay(&run, 1, CronOutcome::TimedOut, now, chrono_tz::UTC),
            Some(Duration::from_secs(1))
        );
        for outcome in [
            CronOutcome::Exited(0),
            CronOutcome::Cancelled,
            CronOutcome::Rejected("busy"),
            CronOutcome::Skipped,
        ] {
            assert_eq!(
                retry_delay(&run, 1, outcome, now, chrono_tz::UTC),
                None,
                "{outcome:?}"
            );
        }
        assert_eq!(
            retry_delay(&every("@daily", &[]), 1, failed, now, chrono_tz::UTC),
            None,
            "no backoff, no retry"
        );
    }

    #[test]
    fn a_retry_that_would_meet_the_next_occurrence_gives_way_to_it() {
        let run = every("* * * * *", &[30_000]);
        let failed = CronOutcome::Exited(1);
        assert_eq!(
            retry_delay(
                &run,
                1,
                failed,
                "2026-10-24T16:15:10Z".parse().unwrap(),
                chrono_tz::UTC
            ),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            retry_delay(
                &run,
                1,
                failed,
                "2026-10-24T16:15:40Z".parse().unwrap(),
                chrono_tz::UTC
            ),
            None
        );
    }

    #[test]
    fn an_unending_line_never_grows_past_the_output_bound() {
        let mut output = output();
        for _ in 0..10 {
            let events = output.receive(TenantLogStream::Stderr, &vec![b'x'; MAX_LINE_BYTES + 3]);
            assert_eq!(events.len(), 1);
            assert_eq!(text(&events[0]).len(), MAX_LINE_BYTES);
            assert!(output.stderr.len() < MAX_LINE_BYTES);
        }
    }
}
