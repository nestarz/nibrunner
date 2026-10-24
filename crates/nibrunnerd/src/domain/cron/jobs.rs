use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use protocol::{
    AppId, CronCommand, CronJobDefinition, CronSchedule, CronTable, Crontab, DeploymentId, DesiredCronJob,
    ReportedCronJob, Sha256Digest, TenantEnvironment, Timestamp,
};
use sha2::{Digest, Sha256};

/// When a job is due: on its schedule, or once at an instant.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CronWhen {
    Schedule(CronSchedule),
    At(Timestamp),
}

/// What a document says of its job that a crontab line cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declared {
    pub timeout: Duration,
    pub backoff: Vec<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronJob {
    /// The `job` its runs' log lines carry, and its `jobId` in the report.
    pub id: String,
    pub when: CronWhen,
    pub command: CronCommand,
    pub environment: Option<TenantEnvironment>,
    /// Absent for a job a guest registered with `crontab`.
    pub declared: Option<Declared>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronOrigin {
    /// What the guest installed, whose every edit reschedules its jobs.
    Crontab(Option<Crontab>),
    Document,
}

/// One deployment's jobs, wherever they were registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronJobs {
    pub app_id: AppId,
    pub deployment_id: DeploymentId,
    pub jobs: Vec<CronJob>,
    pub origin: CronOrigin,
}

impl CronJobs {
    pub fn registered(table: &CronTable) -> Self {
        Self {
            app_id: table.app_id.clone(),
            deployment_id: table.deployment_id.clone(),
            jobs: table
                .jobs
                .iter()
                .enumerate()
                .map(|(index, job)| CronJob {
                    id: cron_job_id(&table.app_id, &table.deployment_id, index, job),
                    when: CronWhen::Schedule(job.schedule.clone()),
                    command: job.command.clone(),
                    environment: job.environment.clone(),
                    declared: None,
                })
                .collect(),
            origin: CronOrigin::Crontab(table.crontab.clone()),
        }
    }

    pub fn declared(app_id: &AppId, deployment_id: &DeploymentId, jobs: &[DesiredCronJob]) -> Self {
        Self {
            app_id: app_id.clone(),
            deployment_id: deployment_id.clone(),
            jobs: jobs
                .iter()
                .map(|job| CronJob {
                    id: job.name.to_string(),
                    when: match (&job.schedule, &job.at) {
                        (Some(schedule), _) => CronWhen::Schedule(schedule.clone()),
                        (None, at) => {
                            CronWhen::At(at.clone().expect("a declared job names a schedule or an at"))
                        }
                    },
                    command: job.command.clone(),
                    environment: None,
                    declared: Some(Declared {
                        timeout: Duration::from_secs(job.timeout_s.get().into()),
                        backoff: job
                            .backoff_ms
                            .iter()
                            .map(|delay| Duration::from_millis((*delay).into()))
                            .collect(),
                    }),
                })
                .collect(),
            origin: CronOrigin::Document,
        }
    }
}

fn cron_job_id(
    app_id: &AppId,
    deployment_id: &DeploymentId,
    index: usize,
    job: &CronJobDefinition,
) -> String {
    let definition = serde_json::to_vec(&(app_id, deployment_id, index, job))
        .expect("validated cron definitions serialize as JSON");
    format!("cron-{}", URL_SAFE_NO_PAD.encode(Sha256::digest(definition)))
}

/// A deployment's scheduled jobs as the report names them, each with its next run after `now`.
pub fn reported_jobs(jobs: &CronJobs, now: DateTime<Utc>, time_zone: Tz) -> Vec<ReportedCronJob> {
    jobs.jobs
        .iter()
        .filter_map(|job| {
            let CronWhen::Schedule(schedule) = &job.when else {
                return None;
            };
            Some(ReportedCronJob {
                job_id: job.id.clone(),
                schedule: schedule.clone(),
                command_digest: Sha256Digest::parse(hex::encode(Sha256::digest(job.command.expose())))
                    .expect("a SHA-256 in lowercase hex is a digest"),
                next_run: schedule
                    .next_after(now, time_zone)
                    .ok()
                    .map(|next| Timestamp::from_epoch_ms(next.timestamp_millis())),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{app_id, deployment_id};

    fn job() -> CronJobDefinition {
        CronJobDefinition {
            schedule: protocol::CronSchedule::parse("@daily").unwrap(),
            command: protocol::CronCommand::parse("echo tenant-secret").unwrap(),
            environment: None,
        }
    }

    fn id(index: usize, deployment: &DeploymentId, job: &CronJobDefinition) -> String {
        cron_job_id(&app_id(), deployment, index, job)
    }

    fn declared(value: serde_json::Value) -> DesiredCronJob {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn a_registered_job_keeps_its_identity_across_runs_and_restarts() {
        let id = id(0, &deployment_id(), &job());
        assert_eq!(id, "cron-mp9IEQFcHoMd3455mmb0Ns4z4GEyMthWViIRFWvri7Y");
        assert!(!id.contains("tenant-secret"));
    }

    #[test]
    fn a_reported_job_carries_the_id_its_log_lines_carry_and_never_its_command() {
        let table = CronTable {
            app_id: app_id(),
            deployment_id: deployment_id(),
            jobs: vec![job()].into(),
            crontab: None,
        };
        let now: DateTime<Utc> = "2026-10-16T10:41:07Z".parse().unwrap();
        let reported = reported_jobs(&CronJobs::registered(&table), now, chrono_tz::Europe::Paris);
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0].job_id, id(0, &deployment_id(), &job()));
        assert_eq!(reported[0].schedule.as_str(), "@daily");
        assert_eq!(
            reported[0].command_digest.as_str(),
            hex::encode(Sha256::digest("echo tenant-secret"))
        );
        assert_eq!(
            reported[0].next_run.as_ref().map(Timestamp::as_str),
            Some("2026-10-16T22:00:00.000Z"),
            "midnight in Paris, summer time"
        );
        assert!(!serde_json::to_string(&reported)
            .unwrap()
            .contains("tenant-secret"));
    }

    #[test]
    fn a_changed_job_or_deployment_gets_a_distinct_identity() {
        let before = id(0, &deployment_id(), &job());
        let mut changed = job();
        changed.command = protocol::CronCommand::parse("echo changed").unwrap();
        assert_ne!(before, id(0, &deployment_id(), &changed));
        changed = job();
        changed.environment = Some(serde_json::from_str(r#"{"TOKEN":"tenant-secret"}"#).unwrap());
        assert_ne!(before, id(0, &deployment_id(), &changed));
        assert_ne!(before, id(1, &deployment_id(), &job()));
        assert_ne!(before, id(1, &DeploymentId::parse("dep-2").unwrap(), &job()));
    }

    #[test]
    fn a_declared_job_is_known_by_its_name_and_a_single_run_is_not_reported() {
        let jobs = CronJobs::declared(
            &app_id(),
            &deployment_id(),
            &[
                declared(serde_json::json!({
                    "name": "retry", "schedule": "*/5 * * * *", "command": "echo tick",
                    "timeoutS": 30, "backoffMs": [1000, 60000]
                })),
                declared(serde_json::json!({
                    "name": "retry", "at": "2026-10-24T16:15:00Z", "command": "echo tick",
                    "timeoutS": 30, "backoffMs": []
                })),
            ],
        );
        assert_eq!(jobs.jobs[0].id, "retry");
        assert_eq!(
            jobs.jobs[0].declared,
            Some(Declared {
                timeout: Duration::from_secs(30),
                backoff: vec![Duration::from_secs(1), Duration::from_secs(60)],
            })
        );
        assert!(matches!(jobs.jobs[1].when, CronWhen::At(_)));
        let reported = reported_jobs(&jobs, "2026-10-24T16:13:00Z".parse().unwrap(), chrono_tz::UTC);
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0].job_id, "retry");
        assert_eq!(
            reported[0].next_run.as_ref().map(Timestamp::as_str),
            Some("2026-10-24T16:15:00.000Z")
        );
    }
}
