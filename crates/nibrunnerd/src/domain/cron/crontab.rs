use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use protocol::{
    CronCommand, CronJobDefinition, CronJobDefinitions, CronSchedule, Crontab, TenantEnvironment,
    TenantValue, MAX_CRON_ENVIRONMENT_VARIABLES,
};

fn crontab_whitespace(character: char) -> bool {
    matches!(
        character,
        '\t' | '\n' | '\u{000b}' | '\u{000c}' | '\r' | ' ' | '\u{00a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'
    )
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidCrontab {
    #[error("the crontab exceeds its byte limit or contains a nul byte")]
    Text,
    #[error("crontab line {line} needs a schedule and a command")]
    Line { line: usize },
    #[error("crontab line {line} has an unmatched environment quote")]
    Quote { line: usize },
    #[error("crontab line {line} names a time zone different from the host's cron time zone")]
    TimeZone { line: usize },
    #[error("crontab line {line} has an invalid environment assignment")]
    Environment { line: usize },
    #[error("crontab line {line} has an invalid schedule or no future occurrence")]
    Schedule { line: usize },
    #[error("crontab line {line} has an invalid command")]
    Command { line: usize },
    #[error("the crontab names more jobs than the host permits for an app")]
    JobLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCrontab {
    pub jobs: CronJobDefinitions,
    pub crontab: Crontab,
}

fn environment_assignment(line: &str) -> Option<(&str, &str)> {
    let (name, value) = line.split_once('=')?;
    let name = name.trim_end_matches([' ', '\t']);
    let mut characters = name.chars();
    if !characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        || !characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return None;
    }
    Some((name, value.trim_start_matches([' ', '\t'])))
}

fn environment_value(text: &str, line: usize) -> Result<&str, InvalidCrontab> {
    let value = text.trim_matches(crontab_whitespace);
    if let Some(quote) = value
        .chars()
        .next()
        .filter(|character| matches!(character, '\'' | '"'))
    {
        if value.len() > 1 && value.ends_with(quote) {
            return Ok(&value[1..value.len() - 1]);
        }
        return Err(InvalidCrontab::Quote { line });
    }
    Ok(value)
}

fn job_line(line: &str, number: usize) -> Result<(&str, &str), InvalidCrontab> {
    let fields = if line.starts_with('@') { 1 } else { 5 };
    let mut remaining = line;
    for _ in 0..fields {
        let Some(end) = remaining.find([' ', '\t']) else {
            return Err(InvalidCrontab::Line { line: number });
        };
        if end == 0 {
            return Err(InvalidCrontab::Line { line: number });
        }
        remaining = remaining[end..].trim_start_matches([' ', '\t']);
    }
    if remaining.is_empty() {
        return Err(InvalidCrontab::Line { line: number });
    }
    let command_offset = line.len() - remaining.len();
    Ok((line[..command_offset].trim_end_matches([' ', '\t']), remaining))
}

pub fn parse_crontab(
    text: &str,
    max_jobs: usize,
    time_zone: Tz,
    after: DateTime<Utc>,
) -> Result<ParsedCrontab, InvalidCrontab> {
    let crontab = Crontab::parse(text).map_err(|_| InvalidCrontab::Text)?;
    let mut environment = BTreeMap::new();
    let mut jobs = Vec::new();
    for (index, raw_line) in text.split_inclusive('\n').enumerate() {
        let number = index + 1;
        let raw_line = raw_line
            .strip_suffix('\n')
            .map(|line| line.strip_suffix('\r').unwrap_or(line))
            .unwrap_or(raw_line);
        let line = raw_line.trim_start_matches(crontab_whitespace);
        if line.trim_matches(crontab_whitespace).is_empty() || line.starts_with('#') {
            continue;
        }
        if line.contains(['\r', '\u{2028}', '\u{2029}']) {
            return Err(InvalidCrontab::Line { line: number });
        }
        if let Some((name, text)) = environment_assignment(line) {
            if !protocol::is_environment_name(name) {
                return Err(InvalidCrontab::Environment { line: number });
            }
            let value = environment_value(text, number)?;
            if name == "CRON_TZ" && value != time_zone.name() {
                return Err(InvalidCrontab::TimeZone { line: number });
            }
            environment.insert(
                name.to_owned(),
                TenantValue::parse(value).map_err(|_| InvalidCrontab::Environment { line: number })?,
            );
            if environment.len() > MAX_CRON_ENVIRONMENT_VARIABLES {
                return Err(InvalidCrontab::Environment { line: number });
            }
            continue;
        }
        if jobs.len() == max_jobs {
            return Err(InvalidCrontab::JobLimit);
        }
        let (schedule, command) = job_line(line, number)?;
        let schedule =
            CronSchedule::parse(schedule).map_err(|_| InvalidCrontab::Schedule { line: number })?;
        schedule
            .next_after(after, time_zone)
            .map_err(|_| InvalidCrontab::Schedule { line: number })?;
        jobs.push(CronJobDefinition {
            schedule,
            command: CronCommand::parse(command).map_err(|_| InvalidCrontab::Command { line: number })?,
            environment: Some(
                TenantEnvironment::try_from(environment.clone())
                    .map_err(|_| InvalidCrontab::Environment { line: number })?,
            ),
        });
    }
    Ok(ParsedCrontab {
        jobs: jobs.into(),
        crontab,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn after() -> DateTime<Utc> {
        "2026-10-01T00:00:00Z".parse().unwrap()
    }

    #[test]
    fn each_job_keeps_the_environment_at_its_line_and_original_text_is_preserved() {
        let text = "# tenant jobs\r\nTOKEN='first secret'\r\n  * * * * * echo first # literal comment\r\nTOKEN=second-secret\r\n@daily echo second\r\n";
        let parsed = parse_crontab(text, 2, chrono_tz::UTC, after()).unwrap();
        let jobs: Vec<_> = parsed.jobs.iter().collect();
        assert_eq!(jobs[0].command.expose(), "echo first # literal comment");
        assert_eq!(
            jobs[0]
                .environment
                .as_ref()
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .1
                .expose(),
            "first secret"
        );
        assert_eq!(
            jobs[1]
                .environment
                .as_ref()
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .1
                .expose(),
            "second-secret"
        );
        assert_eq!(parsed.crontab.expose(), text);
        assert!(!format!("{parsed:?}").contains("secret"));
    }

    #[test]
    fn the_hosts_job_limit_is_applied_and_zero_still_allows_clearing() {
        assert!(parse_crontab("", 0, chrono_tz::UTC, after())
            .unwrap()
            .jobs
            .is_empty());
        assert_eq!(
            parse_crontab("* * * * * echo ok", 0, chrono_tz::UTC, after()).unwrap_err(),
            InvalidCrontab::JobLimit
        );
        assert!(parse_crontab(&"* * * * * echo ok\n".repeat(25), 25, chrono_tz::UTC, after()).is_ok());
        assert!(parse_crontab(&"* * * * * echo ok\n".repeat(26), 25, chrono_tz::UTC, after()).is_err());
    }

    #[test]
    fn environment_time_zone_cannot_override_the_host_policy() {
        let configured = chrono_tz::Europe::Zurich;
        assert!(parse_crontab("CRON_TZ=Europe/Zurich\n@daily echo ok", 1, configured, after()).is_ok());
        assert!(matches!(
            parse_crontab("CRON_TZ=UTC\n@daily echo ok", 1, configured, after()),
            Err(InvalidCrontab::TimeZone { line: 1 })
        ));
    }

    #[test]
    fn invalid_input_errors_do_not_reveal_command_or_environment_secrets() {
        for text in [
            "TOKEN='tenant-secret",
            "@reboot tenant-secret",
            "99 * * * * tenant-secret",
            "* * * * tenant-secret",
        ] {
            let error = parse_crontab(text, 10, chrono_tz::UTC, after()).unwrap_err();
            assert!(!format!("{error:?} {error}").contains("tenant-secret"));
        }
    }

    #[test]
    fn an_environment_snapshot_cannot_grow_beyond_the_guest_limit() {
        let text: String = (0..=MAX_CRON_ENVIRONMENT_VARIABLES)
            .map(|index| format!("KEY_{index}=value\n"))
            .collect();
        assert!(matches!(
            parse_crontab(&text, 10, chrono_tz::UTC, after()),
            Err(InvalidCrontab::Environment { .. })
        ));
        assert!(matches!(
            parse_crontab("__proto__=value", 10, chrono_tz::UTC, after()),
            Err(InvalidCrontab::Environment { line: 1 })
        ));
    }

    #[test]
    fn commands_keep_spaces_quotes_and_percent_signs_for_the_guest_shell() {
        let parsed = parse_crontab(
            "\t0  2\t* * *  printf '%s' \"some value\"  ",
            1,
            chrono_tz::UTC,
            after(),
        )
        .unwrap();
        let job = parsed.jobs.iter().next().unwrap();
        assert_eq!(job.schedule.as_str(), "0  2\t* * *");
        assert_eq!(job.command.expose(), "printf '%s' \"some value\"  ");
    }

    #[test]
    fn crontab_trimming_and_line_endings_follow_the_production_parser() {
        let parsed = parse_crontab(
            "\u{feff}TOKEN=\u{feff}'secret'\u{feff}\r\n\u{feff}@DAILY echo ok\r\n",
            1,
            chrono_tz::UTC,
            after(),
        )
        .unwrap();
        assert_eq!(
            parsed
                .jobs
                .iter()
                .next()
                .unwrap()
                .environment
                .as_ref()
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .1
                .expose(),
            "secret"
        );
        for text in [
            "TOKEN=value\r",
            "@daily echo ok\r",
            "@daily echo ok\u{2028}",
            "TOKEN=value\u{2029}",
        ] {
            assert!(parse_crontab(text, 1, chrono_tz::UTC, after()).is_err());
        }
    }
}
