use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use protocol::{CronJobDefinition, CronTable, ReportedCronJob, Sha256Digest, Timestamp};
use sha2::{Digest, Sha256};

use super::schedule::ParsedSchedule;
use super::scheduler::JobKey;

pub fn cron_job_id(key: &JobKey, job: &CronJobDefinition) -> String {
    let definition = serde_json::to_vec(&(&key.app_id, &key.deployment_id, key.index, job))
        .expect("validated cron definitions serialize as JSON");
    format!("cron-{}", URL_SAFE_NO_PAD.encode(Sha256::digest(definition)))
}

/// A table's jobs as the report names them, each with its next run after `now`.
pub fn reported_jobs(table: &CronTable, now: DateTime<Utc>, time_zone: Tz) -> Vec<ReportedCronJob> {
    table
        .jobs
        .iter()
        .enumerate()
        .map(|(index, job)| {
            let key = JobKey {
                app_id: table.app_id.clone(),
                deployment_id: table.deployment_id.clone(),
                index,
            };
            ReportedCronJob {
                job_id: cron_job_id(&key, job),
                schedule: job.schedule.clone(),
                command_digest: Sha256Digest::parse(hex::encode(Sha256::digest(job.command.expose())))
                    .expect("a SHA-256 in lowercase hex is a digest"),
                next_run: ParsedSchedule::parse(job.schedule.as_str())
                    .and_then(|schedule| schedule.next_after(now, time_zone))
                    .ok()
                    .map(|next| Timestamp::from_epoch_ms(next.timestamp_millis())),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{app_id, deployment_id};

    fn key() -> JobKey {
        JobKey {
            app_id: app_id(),
            deployment_id: deployment_id(),
            index: 0,
        }
    }

    fn job() -> CronJobDefinition {
        CronJobDefinition {
            schedule: protocol::CronSchedule::parse("@daily").unwrap(),
            command: protocol::CronCommand::parse("echo tenant-secret").unwrap(),
            environment: None,
        }
    }

    #[test]
    fn a_registered_job_keeps_its_identity_across_runs_and_restarts() {
        let id = cron_job_id(&key(), &job());
        assert_eq!(id, "cron-mp9IEQFcHoMd3455mmb0Ns4z4GEyMthWViIRFWvri7Y");
        assert_eq!(cron_job_id(&key(), &job()), id);
        assert!(id.starts_with("cron-"));
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
        let reported = reported_jobs(&table, now, chrono_tz::Europe::Paris);
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0].job_id, cron_job_id(&key(), &job()));
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
        let before = cron_job_id(&key(), &job());
        let mut changed = job();
        changed.command = protocol::CronCommand::parse("echo changed").unwrap();
        assert_ne!(before, cron_job_id(&key(), &changed));
        changed = job();
        changed.environment = Some(serde_json::from_str(r#"{"TOKEN":"tenant-secret"}"#).unwrap());
        assert_ne!(before, cron_job_id(&key(), &changed));
        let mut next = key();
        next.index = 1;
        assert_ne!(before, cron_job_id(&next, &job()));
        next.deployment_id = protocol::DeploymentId::parse("dep-2").unwrap();
        assert_ne!(before, cron_job_id(&next, &job()));
    }
}
