use protocol::{
    AppHostname, AppHostnameKind, GuestPath, Hostname, HttpPort, RestartPolicy, TenantArguments,
    TenantEnvironment,
};

use crate::paths::WritableRoot;

pub const INSTANCE_ENV_FILENAME: &str = "instance.env";
pub const INSTANCE_CONFIG_IMAGE: &str = "config.squashfs";

const RUNTIME_PREFIX: &str = "NIBRUN_";
const TENANT_PREFIX: &str = "ENV_";

const DNS_SERVERS: [&str; 2] = ["1.1.1.1", "1.0.0.1"];

#[derive(Debug, Clone)]
pub struct InstanceEnvContent<'a> {
    pub http_port: HttpPort,
    /// How many layer drives follow the ones every guest has.
    pub layers: usize,
    pub writable: WritableRoot,
    pub hostnames: &'a [AppHostname],
    pub program: &'a GuestPath,
    pub working_directory: &'a GuestPath,
    pub args: &'a TenantArguments,
    pub environment: &'a TenantEnvironment,
    pub restart_policy: &'a RestartPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{variable_name} has no representation on the config drive")]
pub struct UnrepresentableEnvironment {
    pub variable_name: String,
}

fn has_forbidden_character(value: &str) -> bool {
    value.contains(['\n', '\r', '\0'])
}

fn platform_hostname(hostnames: &[AppHostname]) -> Option<&Hostname> {
    hostnames
        .iter()
        .find(|each| each.kind == AppHostnameKind::Platform)
        .map(|each| &each.hostname)
}

fn js_number(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

pub fn render_instance_env(content: &InstanceEnvContent<'_>) -> Result<String, UnrepresentableEnvironment> {
    let mut lines = vec![
        format!("{RUNTIME_PREFIX}HTTP_PORT={}", content.http_port),
        format!("{RUNTIME_PREFIX}LAYERS={}", content.layers),
        format!("{RUNTIME_PREFIX}PROGRAM={}", content.program),
        format!("{RUNTIME_PREFIX}CWD={}", content.working_directory),
    ];
    if let WritableRoot::Memory { mib } = content.writable {
        lines.push(format!("{RUNTIME_PREFIX}SCRATCH_MIB={mib}"));
    }
    if let Some(hostname) = platform_hostname(content.hostnames) {
        lines.push(format!("{RUNTIME_PREFIX}HOSTNAME={hostname}"));
    }
    let policy = content.restart_policy;
    lines.push(format!("{RUNTIME_PREFIX}MAX_RESTARTS={}", policy.max_restarts));
    lines.push(format!(
        "{RUNTIME_PREFIX}INITIAL_BACKOFF_MS={}",
        policy.initial_backoff_ms
    ));
    lines.push(format!(
        "{RUNTIME_PREFIX}MAX_BACKOFF_MS={}",
        policy.max_backoff_ms
    ));
    lines.push(format!(
        "{RUNTIME_PREFIX}BACKOFF_FACTOR={}",
        js_number(policy.backoff_factor)
    ));
    lines.push(format!(
        "{RUNTIME_PREFIX}RESET_AFTER_MS={}",
        policy.reset_after_ms
    ));
    lines.push(format!("{RUNTIME_PREFIX}DNS={}", DNS_SERVERS.join(",")));
    for (index, argument) in content.args.iter().enumerate() {
        if has_forbidden_character(argument) {
            return Err(UnrepresentableEnvironment {
                variable_name: format!("{RUNTIME_PREFIX}ARG_{index}"),
            });
        }
        lines.push(format!("{RUNTIME_PREFIX}ARG_{index}={argument}"));
    }
    for (name, value) in content.environment.iter() {
        if has_forbidden_character(value.expose()) {
            return Err(UnrepresentableEnvironment {
                variable_name: name.to_string(),
            });
        }
        lines.push(format!("{TENANT_PREFIX}{name}={}", value.expose()));
    }
    Ok(format!("{}\n", lines.join("\n")))
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InstanceEnvError {
    #[error("{key} is missing, which is a bug in whatever wrote this file")]
    Missing { key: String },
    #[error("{key} is not {rule}")]
    Malformed { key: String, rule: &'static str },
    #[error("{key} names {reference}, which this runtime does not offer")]
    UnknownReference { key: String, reference: String },
    #[error("there are more {what} than this runtime carries")]
    TooMany { what: &'static str },
}

pub const MAX_NAMESERVERS: usize = 3;
pub const MAX_ARGUMENTS: usize = 64;
pub const MAX_TENANT_VARIABLES: usize = 256;
pub const CONFIG_MAX_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct InstanceConfig {
    pub http_port: u32,
    pub layers: usize,
    pub writable: WritableRoot,
    pub program: String,
    pub working_directory: String,
    pub hostname: Option<String>,
    pub max_restarts: u32,
    pub initial_backoff_ms: u32,
    pub max_backoff_ms: u32,
    pub backoff_factor: f64,
    pub reset_after_ms: u32,
    pub nameservers: Vec<String>,
    pub arguments: Vec<String>,
    pub environment: Vec<(String, String)>,
}

fn expand(key: &str, value: &str, runtime: &[(String, String)]) -> Result<String, InstanceEnvError> {
    if !value.contains(&format!("${RUNTIME_PREFIX}")) && !value.contains(&format!("${{{RUNTIME_PREFIX}")) {
        return Ok(value.to_string());
    }
    let mut expanded = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] != b'$' {
            expanded.push(value[at..].chars().next().unwrap_or('$'));
            at += value[at..].chars().next().map_or(1, char::len_utf8);
            continue;
        }
        let braced = value[at + 1..].starts_with('{');
        let names_at = at + 1 + usize::from(braced);
        let rest = &value[names_at..];
        if !rest.starts_with(RUNTIME_PREFIX) {
            expanded.push('$');
            at += 1;
            continue;
        }
        let end = if braced {
            match rest.find('}') {
                Some(end) => end,
                None => {
                    return Err(InstanceEnvError::Malformed {
                        key: key.to_string(),
                        rule: "a closed reference",
                    })
                }
            }
        } else {
            rest.find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .unwrap_or(rest.len())
        };
        let reference = &rest[..end];
        let Some((_, found)) = runtime.iter().find(|(name, _)| name == reference) else {
            return Err(InstanceEnvError::UnknownReference {
                key: key.to_string(),
                reference: reference.to_string(),
            });
        };
        expanded.push_str(found);
        at = names_at + end + usize::from(braced);
    }
    Ok(expanded)
}

pub fn parse_instance_env(text: &str) -> Result<InstanceConfig, InstanceEnvError> {
    let mut runtime: Vec<(String, String)> = Vec::new();
    let mut tenant: Vec<(String, String)> = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(InstanceEnvError::Malformed {
                key: line.chars().take(32).collect(),
                rule: "a KEY=VALUE line",
            });
        };
        if let Some(name) = key.strip_prefix(RUNTIME_PREFIX) {
            runtime.push((format!("{RUNTIME_PREFIX}{name}"), value.to_string()));
        } else if let Some(name) = key.strip_prefix(TENANT_PREFIX) {
            tenant.push((name.to_string(), value.to_string()));
        }
    }

    let named = |key: &str| {
        runtime
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value)
    };
    let required = |key: &str| -> Result<&String, InstanceEnvError> {
        named(key).ok_or_else(|| InstanceEnvError::Missing { key: key.to_string() })
    };
    let number = |key: &str| -> Result<u32, InstanceEnvError> {
        required(key)?.parse().map_err(|_| InstanceEnvError::Malformed {
            key: key.to_string(),
            rule: "a number",
        })
    };

    let nameservers: Vec<String> = required("NIBRUN_DNS")?
        .split(',')
        .filter(|entry| !entry.is_empty())
        .take(MAX_NAMESERVERS)
        .map(str::to_string)
        .collect();

    let mut arguments = Vec::new();
    while let Some(argument) = named(&format!("{RUNTIME_PREFIX}ARG_{}", arguments.len())) {
        if arguments.len() == MAX_ARGUMENTS {
            return Err(InstanceEnvError::TooMany { what: "arguments" });
        }
        arguments.push(argument.clone());
    }
    let numbered = runtime
        .iter()
        .filter(|(name, _)| name.starts_with(&format!("{RUNTIME_PREFIX}ARG_")))
        .count();
    if numbered != arguments.len() {
        return Err(InstanceEnvError::Malformed {
            key: format!("{RUNTIME_PREFIX}ARG_{}", arguments.len()),
            rule: "the next argument in an unbroken run from 0",
        });
    }

    if tenant.len() > MAX_TENANT_VARIABLES {
        return Err(InstanceEnvError::TooMany {
            what: "environment variables",
        });
    }
    let mut environment = Vec::with_capacity(tenant.len());
    for (name, value) in &tenant {
        environment.push((name.clone(), expand(name, value, &runtime)?));
    }

    Ok(InstanceConfig {
        http_port: number("NIBRUN_HTTP_PORT")?,
        layers: number("NIBRUN_LAYERS")? as usize,
        // Absent from what a host that predates scratch writes, which always attached a volume.
        writable: match named("NIBRUN_SCRATCH_MIB") {
            None => WritableRoot::VolumeDrive,
            Some(_) => WritableRoot::Memory {
                mib: number("NIBRUN_SCRATCH_MIB")?,
            },
        },
        program: required("NIBRUN_PROGRAM")?.clone(),
        working_directory: required("NIBRUN_CWD")?.clone(),
        hostname: named("NIBRUN_HOSTNAME").cloned(),
        max_restarts: number("NIBRUN_MAX_RESTARTS")?,
        initial_backoff_ms: number("NIBRUN_INITIAL_BACKOFF_MS")?,
        max_backoff_ms: number("NIBRUN_MAX_BACKOFF_MS")?,
        backoff_factor: required("NIBRUN_BACKOFF_FACTOR")?.parse().map_err(|_| {
            InstanceEnvError::Malformed {
                key: "NIBRUN_BACKOFF_FACTOR".to_string(),
                rule: "a number",
            }
        })?,
        reset_after_ms: number("NIBRUN_RESET_AFTER_MS")?,
        nameservers,
        arguments,
        environment,
    })
}

impl InstanceConfig {
    pub fn tenant_environment(&self) -> Vec<(String, String)> {
        let mut owned = vec![
            ("NIBRUN_HTTP_PORT".to_string(), self.http_port.to_string()),
            ("PORT".to_string(), self.http_port.to_string()),
        ];
        if let Some(hostname) = &self.hostname {
            owned.push(("NIBRUN_HOSTNAME".to_string(), hostname.clone()));
        }
        let platform: Vec<String> = owned.iter().map(|(name, _)| name.clone()).collect();
        for (name, value) in &self.environment {
            if !platform.contains(name) {
                owned.push((name.clone(), value.clone()));
            }
        }
        owned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{TenantValue, DEFAULT_HTTP_PORT, DEFAULT_RESTART_POLICY, RUNTIME_VALUE_NAMES};

    const PLATFORM_HOSTNAME: &str = "my-app.nibrun.app";

    fn hostname(name: &str, kind: AppHostnameKind) -> AppHostname {
        AppHostname {
            hostname: Hostname::parse(name).unwrap(),
            kind,
        }
    }

    fn environment(values: &[(&str, &str)]) -> TenantEnvironment {
        values
            .iter()
            .map(|(name, value)| (name.to_string(), TenantValue::parse(*value).unwrap()))
            .collect()
    }

    struct Overrides {
        http_port: HttpPort,
        writable: WritableRoot,
        hostnames: Vec<AppHostname>,
        args: Vec<String>,
        environment: TenantEnvironment,
    }

    impl Default for Overrides {
        fn default() -> Self {
            Self {
                http_port: DEFAULT_HTTP_PORT,
                writable: WritableRoot::VolumeDrive,
                hostnames: vec![hostname(PLATFORM_HOSTNAME, AppHostnameKind::Platform)],
                args: vec![],
                environment: TenantEnvironment::default(),
            }
        }
    }

    fn attempt(overrides: Overrides) -> Result<String, UnrepresentableEnvironment> {
        let args = TenantArguments::try_from(overrides.args).unwrap();
        render_instance_env(&InstanceEnvContent {
            http_port: overrides.http_port,
            layers: 1,
            writable: overrides.writable,
            hostnames: &overrides.hostnames,
            program: &GuestPath::parse("/app/server").unwrap(),
            working_directory: &GuestPath::parse("/app").unwrap(),
            args: &args,
            environment: &overrides.environment,
            restart_policy: &DEFAULT_RESTART_POLICY,
        })
    }

    fn render(overrides: Overrides) -> String {
        attempt(overrides).unwrap()
    }

    #[test]
    fn every_key_it_writes_is_one_the_runtime_knows() {
        let rendered = render(Overrides::default());
        let lines: Vec<&str> = rendered.lines().filter(|line| !line.is_empty()).collect();
        let expected = [
            "NIBRUN_HTTP_PORT=3000",
            "NIBRUN_LAYERS=1",
            "NIBRUN_PROGRAM=/app/server",
            "NIBRUN_CWD=/app",
            "NIBRUN_HOSTNAME=my-app.nibrun.app",
            "NIBRUN_MAX_RESTARTS=5",
            "NIBRUN_INITIAL_BACKOFF_MS=500",
            "NIBRUN_MAX_BACKOFF_MS=30000",
            "NIBRUN_BACKOFF_FACTOR=2",
            "NIBRUN_RESET_AFTER_MS=60000",
            "NIBRUN_DNS=1.1.1.1,1.0.0.1",
        ];
        assert_eq!(lines, expected);
    }

    #[test]
    fn a_scratch_in_memory_is_written_beside_the_layers_and_read_back_as_the_writable_root() {
        let rendered = render(Overrides {
            writable: WritableRoot::Memory { mib: 512 },
            ..Default::default()
        });
        assert!(
            rendered.contains("NIBRUN_CWD=/app\nNIBRUN_SCRATCH_MIB=512\n"),
            "{rendered}"
        );
        let config = parse_instance_env(&rendered).unwrap();
        assert_eq!(config.writable, WritableRoot::Memory { mib: 512 });
        assert!(config
            .tenant_environment()
            .iter()
            .all(|(name, _)| !name.contains("SCRATCH")));
    }

    #[test]
    fn a_config_drive_that_names_no_scratch_is_read_as_one_with_a_volume_drive() {
        let config = parse_instance_env(&render(Overrides::default())).unwrap();
        assert_eq!(config.writable, WritableRoot::VolumeDrive);
        let malformed = render(Overrides::default()) + "NIBRUN_SCRATCH_MIB=lots\n";
        assert!(matches!(
            parse_instance_env(&malformed),
            Err(InstanceEnvError::Malformed { key, .. }) if key == "NIBRUN_SCRATCH_MIB"
        ));
    }

    #[test]
    fn every_value_the_protocol_offers_is_one_a_reference_can_reach() {
        let environment: TenantEnvironment = protocol::RUNTIME_VALUE_NAMES
            .iter()
            .map(|name| {
                let value = TenantValue::parse(format!("<${{{name}}}>")).unwrap();
                (name.trim_start_matches(RUNTIME_PREFIX).to_string(), value)
            })
            .collect();
        let rendered = render(Overrides {
            environment,
            ..Default::default()
        });

        let config = parse_instance_env(&rendered).unwrap();
        assert_eq!(config.environment.len(), RUNTIME_VALUE_NAMES.len());
        for (name, value) in &config.environment {
            assert!(!value.contains('$'), "{name} was handed on unexpanded: {value}");
        }
    }

    #[test]
    fn tenant_variables_carry_the_tenant_prefix_in_a_stable_order() {
        let rendered = render(Overrides {
            environment: environment(&[("ZED", "1"), ("ALPHA", "2")]),
            ..Default::default()
        });
        assert!(rendered.contains("\nENV_ALPHA=2\nENV_ZED=1\n"));
        let shadowing = render(Overrides {
            environment: environment(&[("NIBRUN_HTTP_PORT", "9999")]),
            ..Default::default()
        });
        assert!(shadowing.contains("NIBRUN_HTTP_PORT=3000\n"));
        assert!(shadowing.contains("ENV_NIBRUN_HTTP_PORT=9999\n"));
        let raw = render(Overrides {
            environment: environment(&[("DSN", "postgres://u:p@h/db?x=1 y=2")]),
            ..Default::default()
        });
        assert!(raw.contains("ENV_DSN=postgres://u:p@h/db?x=1 y=2\n"));
        assert!(render(Overrides {
            environment: environment(&[("EMPTY", "")]),
            ..Default::default()
        })
        .contains("ENV_EMPTY=\n"));
    }

    #[test]
    fn the_hostname_written_is_the_platform_one_or_nothing() {
        let rendered = render(Overrides {
            hostnames: vec![
                hostname("www.example.com", AppHostnameKind::Custom),
                hostname(PLATFORM_HOSTNAME, AppHostnameKind::Platform),
            ],
            ..Default::default()
        });
        assert!(rendered.contains("NIBRUN_HOSTNAME=my-app.nibrun.app\n"));
        assert!(!rendered.contains("www.example.com"));
        assert!(!render(Overrides {
            hostnames: vec![],
            ..Default::default()
        })
        .contains("NIBRUN_HOSTNAME"));
        assert!(render(Overrides {
            http_port: HttpPort::new(8080).unwrap(),
            ..Default::default()
        })
        .contains("NIBRUN_HTTP_PORT=8080\n"));
    }

    #[test]
    fn what_has_no_representation_fails_the_instance_without_leaking_the_value() {
        for character in ["\n", "\r", "\0"] {
            let refused = attempt(Overrides {
                environment: environment(&[("BAD", &format!("a{character}INJECTED=1"))]),
                ..Default::default()
            })
            .unwrap_err();
            assert_eq!(refused.variable_name, "BAD");
        }
        let refused = attempt(Overrides {
            environment: environment(&[("API_KEY", "secret-value\nmore")]),
            ..Default::default()
        })
        .unwrap_err();
        assert_eq!(refused.variable_name, "API_KEY");
        assert!(!format!("{refused:?}").contains("secret-value"));
    }

    #[test]
    fn arguments_reach_the_guest_as_the_user_wrote_them() {
        let rendered = render(Overrides {
            args: vec!["serve".into(), "--http=0.0.0.0:8090".into()],
            ..Default::default()
        });
        assert!(rendered.contains("NIBRUN_ARG_0=serve"));
        assert!(rendered.contains("NIBRUN_ARG_1=--http=0.0.0.0:8090"));
        assert!(!render(Overrides::default()).contains("NIBRUN_ARG_"));
        let refused = attempt(Overrides {
            args: vec!["--flag=one\ntwo".into()],
            ..Default::default()
        })
        .unwrap_err();
        assert_eq!(refused.variable_name, "NIBRUN_ARG_0");
    }
}

#[cfg(test)]
mod both_ends {

    use super::*;
    use protocol::{TenantValue, DEFAULT_HTTP_PORT, DEFAULT_RESTART_POLICY};

    fn written(environment: &[(&str, &str)], args: &[&str]) -> String {
        let environment: TenantEnvironment = environment
            .iter()
            .map(|(name, value)| (name.to_string(), TenantValue::parse(*value).unwrap()))
            .collect();
        let args: TenantArguments = args
            .iter()
            .map(|argument| argument.to_string())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        let hostnames = [AppHostname {
            hostname: Hostname::parse("my-app.nibrun.app").unwrap(),
            kind: AppHostnameKind::Platform,
        }];
        render_instance_env(&InstanceEnvContent {
            http_port: DEFAULT_HTTP_PORT,
            layers: 2,
            writable: WritableRoot::VolumeDrive,
            hostnames: &hostnames,
            program: &GuestPath::parse("/app/server").unwrap(),
            working_directory: &GuestPath::parse("/app").unwrap(),
            args: &args,
            environment: &environment,
            restart_policy: &DEFAULT_RESTART_POLICY,
        })
        .unwrap()
    }

    #[test]
    fn what_the_host_writes_is_what_the_guest_reads() {
        let config = parse_instance_env(&written(&[("TOKEN", "hunter2")], &["--verbose", "-p"])).unwrap();
        assert_eq!(config.http_port, u32::from(DEFAULT_HTTP_PORT.get()));
        assert_eq!(config.layers, 2);
        assert_eq!(config.program, "/app/server");
        assert_eq!(config.working_directory, "/app");
        assert_eq!(config.hostname.as_deref(), Some("my-app.nibrun.app"));
        assert_eq!(config.arguments, vec!["--verbose", "-p"]);
        assert_eq!(
            config.environment,
            vec![("TOKEN".to_string(), "hunter2".to_string())]
        );
        assert_eq!(config.max_restarts, DEFAULT_RESTART_POLICY.max_restarts);
        assert_eq!(config.backoff_factor, DEFAULT_RESTART_POLICY.backoff_factor);
        assert_eq!(config.nameservers, vec!["1.1.1.1", "1.0.0.1"]);
    }

    #[test]
    fn a_tenant_variable_named_like_a_runtime_one_stays_the_tenants_and_is_then_dropped() {
        let config = parse_instance_env(&written(&[("NIBRUN_HTTP_PORT", "9999")], &[])).unwrap();
        assert_eq!(
            config.environment,
            vec![("NIBRUN_HTTP_PORT".to_string(), "9999".to_string())],
            "it is read as the tenant's"
        );
        let exported = config.tenant_environment();
        let port = exported
            .iter()
            .find(|(name, _)| name == "NIBRUN_HTTP_PORT")
            .unwrap();
        assert_eq!(
            port.1,
            DEFAULT_HTTP_PORT.to_string(),
            "and never exported over the real one"
        );
    }

    #[test]
    fn the_tenant_is_handed_the_port_under_both_names() {
        let exported = parse_instance_env(&written(&[("PORT", "1234")], &[]))
            .unwrap()
            .tenant_environment();
        for name in ["PORT", "NIBRUN_HTTP_PORT"] {
            let found = exported.iter().find(|(each, _)| each == name).unwrap();
            assert_eq!(found.1, DEFAULT_HTTP_PORT.to_string(), "{name}");
        }
        assert_eq!(exported.iter().filter(|(name, _)| name == "PORT").count(), 1);
    }

    #[test]
    fn only_a_reference_to_a_runtime_value_expands() {
        let config = parse_instance_env(&written(
            &[
                ("BARE", "port $NIBRUN_HTTP_PORT here"),
                ("BRACED", "port ${NIBRUN_HTTP_PORT} here"),
                ("SHELL_LIKE", "$HOME and $$ and a bare $"),
                ("BOTH", "$NIBRUN_HOSTNAME:$NIBRUN_HTTP_PORT"),
            ],
            &[],
        ))
        .unwrap();
        let value = |name: &str| {
            config
                .environment
                .iter()
                .find(|(each, _)| each == name)
                .map(|(_, value)| value.clone())
                .unwrap()
        };
        let port = DEFAULT_HTTP_PORT.to_string();
        assert_eq!(value("BARE"), format!("port {port} here"));
        assert_eq!(value("BRACED"), format!("port {port} here"));
        assert_eq!(value("SHELL_LIKE"), "$HOME and $$ and a bare $");
        assert_eq!(value("BOTH"), format!("my-app.nibrun.app:{port}"));
    }

    #[test]
    fn a_reference_this_runtime_does_not_offer_fails_the_boot_at_both_ends() {
        assert!(
            TenantValue::parse("$NIBRUN_NO_SUCH_THING").is_err(),
            "the host would have written it"
        );

        let handed = format!("{}ENV_BAD=$NIBRUN_NO_SUCH_THING\n", written(&[], &[]));
        let error = parse_instance_env(&handed).unwrap_err();
        assert!(
            matches!(&error, InstanceEnvError::UnknownReference { reference, .. } if reference == "NIBRUN_NO_SUCH_THING"),
            "{error}"
        );
        assert!(error.to_string().contains("BAD"), "{error}");
    }

    #[test]
    fn a_key_the_writer_left_out_is_reported_rather_than_defaulted() {
        let whole = written(&[], &[]);
        for key in [
            "NIBRUN_HTTP_PORT",
            "NIBRUN_LAYERS",
            "NIBRUN_PROGRAM",
            "NIBRUN_CWD",
            "NIBRUN_MAX_RESTARTS",
            "NIBRUN_BACKOFF_FACTOR",
            "NIBRUN_DNS",
        ] {
            let without: String = whole
                .lines()
                .filter(|line| !line.starts_with(&format!("{key}=")))
                .map(|line| format!("{line}\n"))
                .collect();
            let error = parse_instance_env(&without).unwrap_err();
            assert!(
                matches!(&error, InstanceEnvError::Missing { key: named } if named == key),
                "{key}: {error}"
            );
        }
    }

    #[test]
    fn a_hostname_is_the_one_thing_a_boot_can_do_without() {
        let without: String = written(&[], &[])
            .lines()
            .filter(|line| !line.starts_with("NIBRUN_HOSTNAME="))
            .map(|line| format!("{line}\n"))
            .collect();
        let config = parse_instance_env(&without).unwrap();
        assert_eq!(config.hostname, None);
        assert!(!config
            .tenant_environment()
            .iter()
            .any(|(name, _)| name == "NIBRUN_HOSTNAME"));
    }

    #[test]
    fn a_gap_in_the_arguments_is_refused_rather_than_shifting_the_rest_down() {
        let with_gap: String = written(&[], &["--one", "--two", "--three"])
            .lines()
            .filter(|line| !line.starts_with("NIBRUN_ARG_1="))
            .map(|line| format!("{line}\n"))
            .collect();
        let error = parse_instance_env(&with_gap).unwrap_err();
        assert!(matches!(error, InstanceEnvError::Malformed { .. }), "{error}");
    }

    #[test]
    fn a_key_from_neither_namespace_is_ignored_rather_than_refused() {
        let extended = format!("{}SOMETHING_NEW=from a later agent\n", written(&[], &[]));
        assert!(parse_instance_env(&extended).is_ok());
    }
}
