use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use protocol::cron::ParsedSchedule;
use protocol::{AppId, DeploymentId};

use super::jobs::{CronJob, CronJobs, CronOrigin, CronWhen};

/// A job is its deployment, its id and when it is due: a new schedule is a new job.
type JobKey = (AppId, DeploymentId, String, CronWhen);

#[derive(Debug, Clone)]
pub struct ScheduledRun {
    pub app_id: AppId,
    pub deployment_id: DeploymentId,
    pub job: CronJob,
    pub scheduled_at: DateTime<Utc>,
}

#[derive(Clone)]
struct ScheduledJob {
    job: CronJob,
    schedule: Option<ParsedSchedule>,
    /// Absent once a single run has been made.
    next: Option<DateTime<Utc>>,
}

impl ScheduledJob {
    fn new(job: &CronJob, now: DateTime<Utc>, time_zone: Tz) -> Option<Self> {
        let (schedule, next) = match &job.when {
            CronWhen::Schedule(schedule) => {
                let schedule = ParsedSchedule::parse(schedule.as_str()).ok()?;
                let next = schedule.next_after(now, time_zone).ok()?;
                (Some(schedule), next)
            }
            CronWhen::At(at) => (None, DateTime::from_timestamp_millis(at.epoch_ms())?),
        };
        Some(Self {
            job: job.clone(),
            schedule,
            next: Some(next),
        })
    }
}

#[derive(Default)]
pub struct CronScheduler {
    jobs: BTreeMap<JobKey, ScheduledJob>,
    tables: BTreeMap<AppId, CronJobs>,
}

impl CronScheduler {
    /// A declared job keeps its due instant for as long as the document names it; a crontab's
    /// jobs keep theirs only while the crontab is unedited. A schedule with no occurrence left is
    /// never due.
    pub fn synchronize(&mut self, tables: &[CronJobs], now: DateTime<Utc>, time_zone: Tz) {
        let mut jobs = BTreeMap::new();
        let mut next_tables = BTreeMap::new();
        for table in tables {
            let keeps = table.origin == CronOrigin::Document || self.tables.get(&table.app_id) == Some(table);
            for job in &table.jobs {
                let key = (
                    table.app_id.clone(),
                    table.deployment_id.clone(),
                    job.id.clone(),
                    job.when.clone(),
                );
                let held = self
                    .jobs
                    .get(&key)
                    .filter(|_| keeps)
                    .map(|held| ScheduledJob {
                        job: job.clone(),
                        ..held.clone()
                    })
                    .or_else(|| ScheduledJob::new(job, now, time_zone));
                if let Some(held) = held {
                    jobs.insert(key, held);
                }
            }
            next_tables.insert(table.app_id.clone(), table.clone());
        }
        self.jobs = jobs;
        self.tables = next_tables;
    }

    pub fn next_run(&self) -> Option<DateTime<Utc>> {
        self.jobs.values().filter_map(|job| job.next).min()
    }

    pub fn due(&mut self, now: DateTime<Utc>, time_zone: Tz) -> Vec<ScheduledRun> {
        let mut runs = Vec::new();
        self.jobs.retain(|(app_id, deployment_id, _, _), held| {
            let Some(scheduled_at) = held.next.filter(|next| *next <= now) else {
                return true;
            };
            held.next = match &held.schedule {
                Some(schedule) => match schedule.next_after(now, time_zone) {
                    Ok(next) => Some(next),
                    Err(_) => return false,
                },
                None => None,
            };
            runs.push(ScheduledRun {
                app_id: app_id.clone(),
                deployment_id: deployment_id.clone(),
                job: held.job.clone(),
                scheduled_at,
            });
            true
        });
        runs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{app_id, deployment_id};
    use protocol::{CronJobDefinition, CronTable};

    fn at(value: &str) -> DateTime<Utc> {
        value.parse().unwrap()
    }
    fn crontab() -> CronTable {
        CronTable {
            app_id: app_id(),
            deployment_id: deployment_id(),
            jobs: vec![CronJobDefinition {
                schedule: protocol::CronSchedule::parse("* * * * *").unwrap(),
                command: protocol::CronCommand::parse("echo ok").unwrap(),
                environment: None,
            }]
            .into(),
            crontab: None,
        }
    }
    fn table() -> CronJobs {
        CronJobs::registered(&crontab())
    }
    fn declared(jobs: serde_json::Value) -> CronJobs {
        CronJobs::declared(
            &app_id(),
            &deployment_id(),
            &serde_json::from_value::<Vec<_>>(jobs).unwrap(),
        )
    }
    fn every_minute(name: &str) -> serde_json::Value {
        serde_json::json!({
            "name": name, "schedule": "* * * * *", "command": "echo tick", "timeoutS": 30, "backoffMs": []
        })
    }

    #[test]
    fn a_restart_schedules_only_occurrences_after_it_started() {
        let mut scheduler = CronScheduler::default();
        scheduler.synchronize(&[table()], at("2026-01-01T00:05:30Z"), chrono_tz::UTC);
        assert!(scheduler
            .due(at("2026-01-01T00:05:59Z"), chrono_tz::UTC)
            .is_empty());
        assert_eq!(scheduler.due(at("2026-01-01T00:06:00Z"), chrono_tz::UTC).len(), 1);
    }

    #[test]
    fn a_delayed_pass_runs_once_and_never_replays_missed_minutes() {
        let mut scheduler = CronScheduler::default();
        scheduler.synchronize(&[table()], at("2026-01-01T00:00:00Z"), chrono_tz::UTC);
        let runs = scheduler.due(at("2026-01-01T00:10:00Z"), chrono_tz::UTC);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].scheduled_at, at("2026-01-01T00:01:00Z"));
        assert!(scheduler
            .due(at("2026-01-01T00:10:00Z"), chrono_tz::UTC)
            .is_empty());
    }

    #[test]
    fn unchanged_tables_keep_the_due_instant_and_replacements_start_afresh() {
        let mut scheduler = CronScheduler::default();
        scheduler.synchronize(&[table()], at("2026-01-01T00:00:00Z"), chrono_tz::UTC);
        scheduler.synchronize(&[table()], at("2026-01-01T00:01:00Z"), chrono_tz::UTC);
        assert_eq!(scheduler.due(at("2026-01-01T00:01:00Z"), chrono_tz::UTC).len(), 1);
        let mut replacement = crontab();
        replacement.deployment_id = DeploymentId::parse("dep-2").unwrap();
        scheduler.synchronize(
            &[CronJobs::registered(&replacement)],
            at("2026-01-01T00:02:00Z"),
            chrono_tz::UTC,
        );
        assert!(scheduler
            .due(at("2026-01-01T00:02:00Z"), chrono_tz::UTC)
            .is_empty());
        scheduler.synchronize(&[], at("2026-01-01T00:03:00Z"), chrono_tz::UTC);
        assert!(scheduler
            .due(at("2026-01-01T00:10:00Z"), chrono_tz::UTC)
            .is_empty());
    }

    #[test]
    fn editing_the_table_reschedules_even_jobs_whose_definition_did_not_change() {
        let mut scheduler = CronScheduler::default();
        scheduler.synchronize(&[table()], at("2026-01-01T00:00:00Z"), chrono_tz::UTC);
        let edited = CronTable {
            crontab: Some(protocol::Crontab::parse("# edited\n* * * * * echo ok").unwrap()),
            ..crontab()
        };
        scheduler.synchronize(
            &[CronJobs::registered(&edited)],
            at("2026-01-01T00:01:00Z"),
            chrono_tz::UTC,
        );
        assert!(scheduler
            .due(at("2026-01-01T00:01:00Z"), chrono_tz::UTC)
            .is_empty());
        assert_eq!(scheduler.next_run(), Some(at("2026-01-01T00:02:00Z")));
    }

    #[test]
    fn a_declared_job_stays_due_while_the_rest_of_its_table_changes() {
        let mut scheduler = CronScheduler::default();
        scheduler.synchronize(
            &[declared(serde_json::json!([every_minute("tick")]))],
            at("2026-01-01T00:00:00Z"),
            chrono_tz::UTC,
        );
        scheduler.synchronize(
            &[declared(serde_json::json!([
                every_minute("tick"),
                every_minute("tock")
            ]))],
            at("2026-01-01T00:01:00Z"),
            chrono_tz::UTC,
        );
        let runs = scheduler.due(at("2026-01-01T00:01:00Z"), chrono_tz::UTC);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].job.id, "tick");
        assert_eq!(runs[0].scheduled_at, at("2026-01-01T00:01:00Z"));
    }

    #[test]
    fn a_table_emptied_and_declared_again_keeps_its_jobs_due_instant() {
        let mut scheduler = CronScheduler::default();
        let jobs = declared(serde_json::json!([every_minute("tick")]));
        scheduler.synchronize(
            std::slice::from_ref(&jobs),
            at("2026-01-01T00:00:10Z"),
            chrono_tz::UTC,
        );
        scheduler.synchronize(
            &[declared(serde_json::json!([]))],
            at("2026-01-01T00:00:20Z"),
            chrono_tz::UTC,
        );
        assert_eq!(scheduler.next_run(), None);
        scheduler.synchronize(&[jobs], at("2026-01-01T00:00:50Z"), chrono_tz::UTC);
        assert_eq!(scheduler.next_run(), Some(at("2026-01-01T00:01:00Z")));
    }

    #[test]
    fn a_new_schedule_under_the_same_name_is_a_new_job() {
        let mut scheduler = CronScheduler::default();
        scheduler.synchronize(
            &[declared(serde_json::json!([every_minute("tick")]))],
            at("2026-01-01T00:00:00Z"),
            chrono_tz::UTC,
        );
        let mut hourly = every_minute("tick");
        hourly["schedule"] = serde_json::json!("@hourly");
        scheduler.synchronize(
            &[declared(serde_json::json!([hourly]))],
            at("2026-01-01T00:01:00Z"),
            chrono_tz::UTC,
        );
        assert!(scheduler
            .due(at("2026-01-01T00:01:00Z"), chrono_tz::UTC)
            .is_empty());
        assert_eq!(scheduler.next_run(), Some(at("2026-01-01T01:00:00Z")));
    }

    #[test]
    fn a_single_run_is_due_at_once_when_first_seen_late_and_never_again() {
        let mut scheduler = CronScheduler::default();
        let once = declared(serde_json::json!([{
            "name": "tick", "at": "2026-01-01T00:00:00Z", "command": "echo tick", "timeoutS": 30, "backoffMs": []
        }]));
        scheduler.synchronize(
            std::slice::from_ref(&once),
            at("2026-01-01T00:05:00Z"),
            chrono_tz::UTC,
        );
        let runs = scheduler.due(at("2026-01-01T00:05:00Z"), chrono_tz::UTC);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].scheduled_at, at("2026-01-01T00:00:00Z"));
        scheduler.synchronize(&[once], at("2026-01-01T00:06:00Z"), chrono_tz::UTC);
        assert_eq!(scheduler.next_run(), None);
        assert!(scheduler
            .due(at("2026-01-02T00:00:00Z"), chrono_tz::UTC)
            .is_empty());
    }

    #[test]
    fn a_single_run_under_its_jobs_name_is_a_job_of_its_own() {
        let mut scheduler = CronScheduler::default();
        let jobs = declared(serde_json::json!([every_minute("tick"), {
            "name": "tick", "at": "2026-01-01T00:00:30Z", "command": "echo tick", "timeoutS": 30, "backoffMs": []
        }]));
        scheduler.synchronize(
            std::slice::from_ref(&jobs),
            at("2026-01-01T00:00:00Z"),
            chrono_tz::UTC,
        );
        let manual = scheduler.due(at("2026-01-01T00:00:30Z"), chrono_tz::UTC);
        assert_eq!(manual.len(), 1);
        assert!(matches!(manual[0].job.when, CronWhen::At(_)));
        scheduler.synchronize(&[jobs], at("2026-01-01T00:00:40Z"), chrono_tz::UTC);
        let scheduled = scheduler.due(at("2026-01-01T00:01:00Z"), chrono_tz::UTC);
        assert_eq!(scheduled.len(), 1);
        assert!(matches!(scheduled[0].job.when, CronWhen::Schedule(_)));
        assert_eq!(scheduled[0].job.id, manual[0].job.id);
    }

    #[test]
    fn a_schedule_with_no_occurrence_is_never_due_and_leaves_the_others_scheduled() {
        let mut scheduler = CronScheduler::default();
        let mut never = every_minute("never");
        never["schedule"] = serde_json::json!("0 0 31 2 *");
        scheduler.synchronize(
            &[declared(serde_json::json!([never, every_minute("tick")]))],
            at("2026-01-01T00:00:00Z"),
            chrono_tz::UTC,
        );
        assert_eq!(scheduler.next_run(), Some(at("2026-01-01T00:01:00Z")));
    }
}
