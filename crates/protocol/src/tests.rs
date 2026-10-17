use super::*;

fn cron_table_json() -> serde_json::Value {
    serde_json::json!({
        "appId": "app-1",
        "deploymentId": "dep-1",
        "jobs": [{
            "schedule": "@hourly",
            "command": "/app/server cleanup --token=tenant-secret",
            "environment": {"TOKEN": "tenant-secret"}
        }],
        "crontab": "TOKEN=tenant-secret\n@hourly /app/server cleanup --token=tenant-secret\n"
    })
}

#[test]
fn a_cron_table_round_trips_without_rewriting_tenant_input() {
    let value = cron_table_json();
    let table: CronTable = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(table.app_id.as_str(), "app-1");
    assert_eq!(table.deployment_id.as_str(), "dep-1");
    assert_eq!(serde_json::to_value(&table).unwrap(), value);
    assert!(!format!("{table:?}").contains("tenant-secret"));
}

#[test]
fn an_empty_cron_table_needs_no_original_text() {
    let value = serde_json::json!({"appId": "app-1", "deploymentId": "dep-1", "jobs": []});
    let table: CronTable = serde_json::from_value(value.clone()).unwrap();
    assert!(table.jobs.is_empty());
    assert_eq!(table.crontab, None);
    assert_eq!(serde_json::to_value(table).unwrap(), value);
}

#[test]
fn a_cron_job_may_omit_environment_overrides() {
    let value = serde_json::json!({"schedule": "* * * * *", "command": "  echo hello  "});
    let job: CronJobDefinition = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(job.environment, None);
    assert_eq!(serde_json::to_value(job).unwrap(), value);
}

#[test]
fn cron_wire_validation_leaves_expression_syntax_to_the_scheduler() {
    assert!(CronSchedule::parse("@reboot").is_ok());
    assert!(CronSchedule::parse("60 * * * *").is_ok());
}

#[test]
fn a_crontab_is_bounded_by_utf8_bytes_and_not_characters() {
    assert!(Crontab::parse("x".repeat(MAX_CRONTAB_BYTES)).is_ok());
    assert!(Crontab::parse("é".repeat(MAX_CRONTAB_BYTES / 2)).is_ok());
    assert!(Crontab::parse("x".repeat(MAX_CRONTAB_BYTES + 1)).is_err());
    assert!(Crontab::parse("é".repeat(MAX_CRONTAB_BYTES / 2 + 1)).is_err());
}

#[test]
fn cron_input_and_errors_do_not_expose_commands_or_crontabs() {
    let command = CronCommand::parse("tenant-secret").unwrap();
    let crontab = Crontab::parse("tenant-secret\n").unwrap();
    assert_eq!(format!("{command:?}"), REDACTED);
    assert_eq!(format!("{crontab:?}"), REDACTED);
    assert!(Crontab::parse("").is_ok());
    assert!(Crontab::parse("tenant-secret\0").is_err());
    let invalid = serde_json::json!({"schedule": "@hourly", "command": "tenant-secret\0"});
    let error = serde_json::from_value::<CronJobDefinition>(invalid).unwrap_err();
    assert!(!error.to_string().contains("tenant-secret"));
    assert!(!format!("{error:?}").contains("tenant-secret"));
}

#[test]
fn cron_jobs_refuse_empty_multiline_and_oversized_fields() {
    for (field, limit) in [
        ("schedule", MAX_CRON_SCHEDULE_LENGTH),
        ("command", MAX_CRON_COMMAND_LENGTH),
    ] {
        for value in ["", " \t", "x\0", "x\r", "x\n", "x\nhello"] {
            let mut table = cron_table_json();
            table["jobs"][0][field] = serde_json::json!(value);
            assert!(serde_json::from_value::<CronTable>(table).is_err(), "{field}");
        }
        for (length, accepted) in [(limit, true), (limit + 1, false)] {
            let mut table = cron_table_json();
            table["jobs"][0][field] = serde_json::json!("x".repeat(length));
            assert_eq!(
                serde_json::from_value::<CronTable>(table).is_ok(),
                accepted,
                "{field}"
            );
        }
    }
}

#[test]
fn a_cron_command_fits_the_guests_byte_limit_even_with_unicode() {
    for character in ["é", "🔑"] {
        let boundary = character.repeat(MAX_CRON_COMMAND_LENGTH / character.len());
        let command = CronCommand::parse(boundary.clone()).unwrap();
        assert_eq!(command.expose().len(), MAX_CRON_COMMAND_LENGTH);
        assert!(CronCommand::parse(format!("{boundary}x")).is_err());
        let mut table = cron_table_json();
        table["jobs"][0]["command"] = serde_json::json!(format!("{boundary}{character}"));
        assert!(serde_json::from_value::<CronTable>(table).is_err());
    }
}

#[test]
fn cron_schedule_text_has_a_character_bound_before_syntax_validation() {
    assert!(CronSchedule::parse("é".repeat(MAX_CRON_SCHEDULE_LENGTH)).is_ok());
    assert!(CronSchedule::parse("é".repeat(MAX_CRON_SCHEDULE_LENGTH + 1)).is_err());
}

#[test]
fn reading_stored_cron_jobs_does_not_impose_the_hosts_registration_limit() {
    let mut table = cron_table_json();
    table["jobs"] = serde_json::json!(vec![table["jobs"][0].clone(); 25]);
    let table: CronTable = serde_json::from_value(table).unwrap();
    assert_eq!(table.jobs.iter().count(), 25);
    assert!(table.jobs.validate_limit(25).is_ok());
    assert!(table.jobs.validate_limit(24).is_err());
    assert!(table.jobs.validate_limit(0).is_err());
    assert!(CronJobDefinitions::default().validate_limit(0).is_ok());
}

#[test]
fn a_cron_job_bounds_its_environment_overrides() {
    for (count, accepted) in [
        (MAX_CRON_ENVIRONMENT_VARIABLES, true),
        (MAX_CRON_ENVIRONMENT_VARIABLES + 1, false),
    ] {
        let mut table = cron_table_json();
        let environment: serde_json::Map<String, serde_json::Value> = (0..count)
            .map(|index| (format!("VAR_{index}"), serde_json::json!("value")))
            .collect();
        table["jobs"][0]["environment"] = serde_json::Value::Object(environment);
        assert_eq!(serde_json::from_value::<CronTable>(table).is_ok(), accepted);
    }
    for name in ["BAD=NAME", "__proto__"] {
        let mut table = cron_table_json();
        table["jobs"][0]["environment"] = serde_json::json!({name: "value"});
        assert!(serde_json::from_value::<CronTable>(table).is_err());
    }
}

#[test]
fn a_stored_cron_table_requires_its_deployment() {
    let mut table = cron_table_json();
    table.as_object_mut().unwrap().remove("deploymentId");
    assert!(serde_json::from_value::<CronTable>(table).is_err());
    let mut table = cron_table_json();
    table["deploymentId"] = serde_json::json!("");
    assert!(serde_json::from_value::<CronTable>(table).is_err());
}

fn instance_json() -> serde_json::Value {
    serde_json::json!({
        "appId": "app-1",
        "deploymentId": "dep-1",
        "volumeId": "vol-1",
        "desiredState": "on-request",
        "idleTimeoutMs": 300000,
        "layers": [
            {
                "kind": "filesystem",
                "digest": "b".repeat(64),
                "objectKey": "layers/debian-apphost"
            },
            {
                "kind": "executable",
                "destinationPath": "/app/server",
                "digest": "a".repeat(64),
                "objectKey": "artifacts/9f1c2f0e-0d4e-4a1b-9c3a-1f8b6d2e7a45"
            }
        ],
        "config": {
            "httpPort": 3000,
            "ports": [{ "name": "ssh", "guestPort": 22 }],
            "command": {
                "program": "/app/server",
                "args": ["serve"],
                "workingDirectory": "/app",
                "environment": { "DSN": "postgres://u:p@h/db", "PORT_HINT": "${NIBRUN_HTTP_PORT}" }
            },
            "resources": { "vcpuCount": 1, "memoryMib": 256 },
            "healthCheck": { "kind": "http", "path": "/healthz", "intervalMs": 5000, "timeoutMs": 2000, "gracePeriodMs": 30000, "healthyThreshold": 1, "unhealthyThreshold": 3 },
            "restartPolicy": { "maxRestarts": 5, "initialBackoffMs": 500, "maxBackoffMs": 30000, "backoffFactor": 2, "resetAfterMs": 60000 }
        },
        "hostnames": [{ "hostname": "app-1.apps.example.com", "kind": "platform" }],
        "somethingNewer": true
    })
}

fn desired_json() -> serde_json::Value {
    serde_json::json!({
        "hostId": "host-1",
        "revision": "deploy-4821",
        "volumes": [{
            "volumeId": "vol-1",
            "appId": "app-1",
            "sizeBytes": 4096,
            "desiredState": "present",
            "initialContents": {
                "digest": "c".repeat(64),
                "objectKey": "seeds/app-1",
                "destinationPath": "/app/data"
            }
        }],
        "instances": [instance_json()],
        "checkpoints": [],
        "exports": []
    })
}

#[test]
fn a_desired_state_round_trips_with_its_wire_names() {
    let parsed: HostDesiredState = serde_json::from_value(desired_json()).expect("parses");
    assert_eq!(parsed.instances[0].desired_state, DesiredInstanceState::OnRequest);
    assert_eq!(
        parsed.instances[0].idle_timeout_ms.map(|t| t.get()),
        Some(300_000)
    );
    let contents = parsed.volumes[0]
        .initial_contents
        .as_ref()
        .expect("the volume starts with something");
    assert_eq!(contents.destination_path.as_str(), "/app/data");
    assert_eq!(contents.object.object_key.as_str(), "seeds/app-1");
    let written = serde_json::to_value(&parsed).expect("serialises");
    assert_eq!(written["instances"][0]["config"]["httpPort"], 3000);
    assert_eq!(written["instances"][0]["desiredState"], "on-request");
    assert!(written["instances"][0].get("somethingNewer").is_none());
    assert_eq!(written["volumes"][0]["initialContents"]["digest"], "c".repeat(64));
}

#[test]
fn an_instance_without_labels_says_nothing_about_them_and_one_with_them_keeps_them() {
    let parsed: DesiredInstance = serde_json::from_value(instance_json()).expect("parses");
    assert!(parsed.labels.is_empty());
    assert!(serde_json::to_value(&parsed).unwrap().get("labels").is_none());
    let labelled: DesiredInstance = serde_json::from_value(instance_with(|instance| {
        instance["labels"] = serde_json::json!({ "rev": "r-12", "app": "shop" });
    }))
    .expect("parses");
    assert_eq!(
        serde_json::to_value(&labelled).unwrap()["labels"],
        serde_json::json!({ "app": "shop", "rev": "r-12" })
    );
}

#[test]
fn an_instance_carries_at_most_eight_labels() {
    let refused = read(instance_with(|instance| {
        instance["labels"] = (0..=MAX_INSTANCE_LABELS)
            .map(|index| (format!("k{index}"), serde_json::json!("v")))
            .collect::<serde_json::Map<_, _>>()
            .into();
    }))
    .expect_err("nine labels");
    assert!(refused.to_string().contains("at most 8 labels"), "{refused}");
}

#[test]
fn a_label_must_read_back_as_one_token_of_a_log_line() {
    for value in ["has space", "a=b", "quote\"", "", "tab\t"] {
        assert!(LabelValue::parse(value).is_err(), "{value:?}");
    }
    for key in ["App", "1st", "", "dotted.key", &"k".repeat(33)] {
        assert!(LabelKey::parse(key).is_err(), "{key:?}");
    }
    assert!(LabelValue::parse("ghcr.io/acme/shop:1.2+build@sha_x-y").is_ok());
    assert!(LabelKey::parse("time_line-2").is_ok());
}

#[test]
fn a_volume_that_starts_empty_says_nothing_about_it() {
    let mut document = desired_json();
    document["volumes"][0]
        .as_object_mut()
        .expect("a volume")
        .remove("initialContents");
    let parsed: HostDesiredState = serde_json::from_value(document).expect("parses");
    assert_eq!(parsed.volumes[0].initial_contents, None);
    let written = serde_json::to_value(&parsed).expect("serialises");
    assert!(written["volumes"][0].get("initialContents").is_none());
}

#[test]
fn unknown_fields_are_tolerated_and_mistyped_ones_are_not() {
    let mut document = instance_json();
    document["config"]["httpPort"] = serde_json::json!("3000");
    assert!(serde_json::from_value::<DesiredInstance>(document).is_err());
}

// A layer used to declare its size beside its digest, and every document written then still says
// so. The digest was always the whole of the check, so the number is read past rather than
// refused.
#[test]
fn a_layer_that_still_declares_its_size_is_read_as_one_that_does_not() {
    let mut document = instance_json();
    document["layers"][1]["sizeBytes"] = serde_json::json!(27);
    let parsed: DesiredInstance = serde_json::from_value(document).expect("parses");
    assert_eq!(parsed.layers[1].digest().as_str(), "a".repeat(64));
    let written = serde_json::to_value(&parsed).expect("serialises");
    assert!(written["layers"][1].get("sizeBytes").is_none());
}

#[test]
fn an_oci_layer_names_an_archive_without_changing_the_explicit_command() {
    let mut document = instance_json();
    document["layers"] = serde_json::json!([{
        "kind": "oci", "digest": "a".repeat(64), "objectKey": "images/app.tar"
    }]);
    let parsed: DesiredInstance = serde_json::from_value(document.clone()).unwrap();
    assert!(matches!(parsed.layers[0], DesiredLayer::Oci { .. }));
    assert_eq!(
        parsed.layers[0].stored_object().unwrap().object_key.as_str(),
        "images/app.tar"
    );
    assert_eq!(
        serde_json::to_value(parsed).unwrap()["config"]["command"],
        document["config"]["command"]
    );
}

#[test]
fn registry_layers_pin_content_and_keep_startup_configuration_explicit() {
    let mut document = instance_json();
    document["layers"] = serde_json::json!([{
        "kind": "oci", "repository": "docker.io/library/busybox", "digest": "a".repeat(64)
    }]);
    let parsed: DesiredInstance = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(parsed.layers[0].digest().as_str(), "a".repeat(64));
    assert!(parsed.layers[0].stored_object().is_none());
    let written = serde_json::to_value(parsed).unwrap();
    assert_eq!(written["layers"], document["layers"]);
    assert_eq!(written["config"]["command"], document["config"]["command"]);
}

#[test]
fn an_oci_layer_requires_one_unambiguous_source_in_both_the_parser_and_schema() {
    let validator =
        jsonschema::validator_for(&serde_json::to_value(schemars::schema_for!(DesiredLayer)).unwrap())
            .unwrap();
    for (source, accepted) in [
        (serde_json::json!({ "objectKey": "images/app.tar" }), true),
        (
            serde_json::json!({ "repository": "docker.io/library/busybox" }),
            true,
        ),
        (
            serde_json::json!({ "objectKey": "images/app.tar", "repository": "docker.io/library/busybox" }),
            false,
        ),
        (serde_json::json!({}), false),
        (
            serde_json::json!({ "objectKey": null, "repository": "docker.io/library/busybox" }),
            false,
        ),
        (
            serde_json::json!({ "objectKey": "images/app.tar", "repository": null }),
            false,
        ),
    ] {
        let mut layer = source;
        layer["kind"] = serde_json::json!("oci");
        layer["digest"] = serde_json::json!("a".repeat(64));
        let parsed = serde_json::from_value::<DesiredLayer>(layer.clone());
        assert_eq!(parsed.is_ok(), accepted, "{layer}");
        assert_eq!(validator.is_valid(&layer), accepted, "{layer}");
        if let Ok(parsed) = parsed {
            assert_eq!(serde_json::to_value(parsed).unwrap(), layer);
        }
    }
}

#[test]
fn registry_repository_validation_agrees_with_its_schema() {
    let validator =
        jsonschema::validator_for(&serde_json::to_value(schemars::schema_for!(OciRepository)).unwrap())
            .unwrap();
    for (repository, accepted) in [
        ("docker.io/library/busybox", true),
        ("ghcr.io/owner/my_image", true),
        ("registry.example.com:5000/team/image", true),
        ("registry.example-host/team/image", true),
        ("localhost:5000/test", true),
        ("busybox", false),
        ("library/busybox", false),
        ("https://docker.io/library/busybox", false),
        ("docker.io/library/busybox:latest", false),
        ("docker.io/library/busybox@sha256:abc", false),
        ("docker.io/../busybox", false),
        ("docker.io/library//busybox", false),
        ("docker.io/Library/Busybox", false),
        ("user:password@registry.example.com/image", false),
    ] {
        assert_eq!(OciRepository::parse(repository).is_ok(), accepted, "{repository}");
        assert_eq!(
            validator.is_valid(&serde_json::json!(repository)),
            accepted,
            "{repository}"
        );
    }
}

#[test]
fn a_secret_never_prints_itself() {
    let secret = SecretString::parse("hunter2").unwrap();
    assert_eq!(format!("{secret:?}"), REDACTED);
    let environment: TenantEnvironment = [("KEY".to_string(), TenantValue::parse("hunter2").unwrap())]
        .into_iter()
        .collect();
    assert!(!format!("{environment:?}").contains("hunter2"));
}

#[test]
fn a_tenant_value_may_name_only_offered_runtime_values() {
    assert!(TenantValue::parse("$HOME and $$ and a bcrypt $2b$10$abc").is_ok());
    assert!(TenantValue::parse("http://x:${NIBRUN_HTTP_PORT}/").is_ok());
    assert!(TenantValue::parse("${NIBRUN_HTTP_PORTS}").is_err());
    assert!(TenantValue::parse("${NIBRUN_NOPE}").is_err());
    assert!(TenantValue::parse("${NIBRUN_}").is_err());
    assert!(TenantValue::parse("${NIBRUN_BROKEN:${NIBRUN_NOPE}").is_err());
}

#[test]
fn a_tenant_value_preserves_everything_except_complete_runtime_references() {
    for literal in [
        "$",
        "{",
        "}",
        "${",
        "${}",
        "$NIBRUN_HTTP_PORT",
        "$NIBRUN_NOPE",
        "${NIBRUN_HTTP_PORT",
        "${NIBRUN_NOPE",
        "{NIBRUN_HTTP_PORT}",
        "${NIBRUN_HTTP_PORT!}",
        "${NIBRUN_HTTP_PORT with spaces}",
        "secret$NIBRUN_HTTP_PORT}suffix",
        "🔑${NIBRUN_HTTP_PORTé}尾",
    ] {
        assert_eq!(TenantValue::parse(literal).unwrap().expose(), literal);
        let decoded: TenantValue = serde_json::from_value(serde_json::json!(literal)).unwrap();
        assert_eq!(decoded.expose(), literal);
    }
    assert!(TenantValue::parse(
        "$${NIBRUN_HTTP_PORT}|{${NIBRUN_HOSTNAME}}|${NIBRUN_BROKEN:${NIBRUN_HTTP_PORT}|$"
    )
    .is_ok());
}

#[test]
fn environment_names_follow_the_shell_rule_minus_one() {
    assert!(is_environment_name("_A1"));
    assert!(!is_environment_name("1A"));
    assert!(!is_environment_name("__proto__"));
    assert!(!is_environment_name("A-B"));
}

#[test]
fn identifiers_timestamps_and_addresses_are_checked() {
    assert!(AppId::parse("0198f3aa-1c2d-7e4b-9f11-a0b1c2d3e4f5").is_ok());
    assert!(AppId::parse("has.a.dot").is_err());
    assert!(AppId::parse("x".repeat(64)).is_err());
    assert!(Timestamp::parse("2026-08-03T10:00:00.000Z").is_ok());
    assert!(Timestamp::parse("2026-08-03T10:00:00+02:00").is_ok());
    assert!(Timestamp::parse("2026-08-03T10:00:00").is_err());
    assert_eq!(
        Timestamp::from_epoch_ms(1_785_751_200_000).as_str(),
        "2026-08-03T10:00:00.000Z"
    );
    assert_eq!(
        Timestamp::parse("2026-08-03T10:00:00.000Z").unwrap().epoch_ms(),
        1_785_751_200_000
    );
    assert!(Ipv4Address::parse("10.201.0.2").is_ok());
    assert!(Ipv4Address::parse("10.201.0.256").is_err());
    assert!(Ipv4Address::parse("01.2.3.4").is_err());
    assert!(Hostname::parse("app-1.apps.example.com").is_ok());
    assert!(Hostname::parse("localhost").is_err());
    assert!(Sha256Digest::parse("A".repeat(64)).is_err());
    assert!(HttpPort::try_from(0u32).is_err());
    assert!(HttpPort::try_from(70000u32).is_err());
    assert!(GuestPath::parse("/a/b").is_ok());
    assert!(Revision::parse("deploy-4821").is_ok());
    assert!(Revision::parse("2026-09-23T10:00:00Z/7").is_ok());
    assert!(Revision::parse("").is_err());
    assert!(Revision::parse("has a space").is_err());
    assert!(Revision::parse("x".repeat(129)).is_err());
    assert!(GuestPath::parse("/a/../b").is_err());
    assert!(GuestPath::parse("/it's").is_err());
    assert!(GuestPath::parse("/a/").is_err());
}

#[test]
fn a_report_omits_what_it_does_not_know() {
    let instance = ReportedInstance {
        app_id: AppId::parse("app-1").unwrap(),
        deployment_id: DeploymentId::parse("dep-1").unwrap(),
        state: InstanceState::Running,
        host_port: HostPort::new(21000).ok(),
        guest_ipv4: None,
        layer_digests: Vec::new(),
        restart_count: 0,
        last_restart: None,
        started_at: None,
        converged_at: None,
        last_exit_code: Some(0),
        message: None,
        labels: InstanceLabels::default(),
    };
    let written = serde_json::to_value(&instance).unwrap();
    assert_eq!(written["lastExitCode"], 0);
    assert!(written.get("labels").is_none());
    assert!(written.get("startedAt").is_none());
    assert!(written.get("convergedAt").is_none());
    assert!(written.get("message").is_none());
    assert!(written.get("lastRestart").is_none());
    assert_eq!(written["hostPort"], 21000);
}

#[test]
fn a_report_written_before_a_tenant_had_ever_restarted_still_reads_back() {
    let older = serde_json::json!({
        "appId": "app-1",
        "deploymentId": "dep-1",
        "state": "running",
        "restartCount": 0
    });
    let read: ReportedInstance = serde_json::from_value(older).unwrap();
    assert_eq!(read.last_restart, None);
    assert_eq!(read.restart_count, 0);
}

#[test]
fn a_tenant_restart_is_written_flat_with_how_the_tenant_ended_named_by_kind() {
    let restart = ReportedRestart {
        at: Timestamp::parse("2026-09-14T10:00:00.000Z").unwrap(),
        restart: TenantRestart {
            attempt: 2,
            budget: 5,
            exit: TenantExit::Signal(9),
            reason: StateMessage::new("the tenant exited (137); restart 2 of 5 in 1000ms"),
            backoff_ms: 1000,
        },
    };
    let written = serde_json::to_value(&restart).unwrap();
    assert_eq!(
        written,
        serde_json::json!({
            "at": "2026-09-14T10:00:00.000Z",
            "attempt": 2,
            "budget": 5,
            "exit": { "signal": 9 },
            "reason": "the tenant exited (137); restart 2 of 5 in 1000ms",
            "backoffMs": 1000
        })
    );
    assert_eq!(
        serde_json::from_value::<ReportedRestart>(written).unwrap(),
        restart
    );
    assert_eq!(
        serde_json::to_value(TenantExit::Code(1)).unwrap(),
        serde_json::json!({ "code": 1 })
    );
    assert_eq!(TenantExit::Code(1).status(), 1);
    assert_eq!(TenantExit::Signal(9).status(), 137);
}

#[test]
fn state_messages_are_cut_to_the_wire_ceiling() {
    let message = StateMessage::new("x".repeat(600));
    assert_eq!(message.as_str().len(), MAX_STATE_MESSAGE_LENGTH);
}

fn instance_with(edit: impl FnOnce(&mut serde_json::Value)) -> serde_json::Value {
    let mut document = instance_json();
    document
        .as_object_mut()
        .expect("an object")
        .remove("idleTimeoutMs");
    edit(&mut document);
    document
}

fn read(document: serde_json::Value) -> Result<DesiredInstance, serde_json::Error> {
    serde_json::from_value(document)
}

#[test]
fn an_activation_policy_round_trips_with_its_wire_names() {
    let instance = read(instance_with(|document| {
        document["activation"] = serde_json::json!({
            "sleepWhen": { "kind": "traffic-idle", "timeoutMs": 900000 }
        });
    }))
    .expect("parses");

    let policy = instance.activation();
    assert_eq!(
        policy.sleep_when,
        SleepPolicy::TrafficIdle {
            timeout_ms: IdleTimeoutMs::try_from(900_000).unwrap()
        }
    );

    let written = serde_json::to_value(&instance).expect("serialises");
    assert_eq!(written["activation"]["sleepWhen"]["kind"], "traffic-idle");
    assert_eq!(written["activation"]["sleepWhen"]["timeoutMs"], 900_000);
    assert!(written.get("idleTimeoutMs").is_none());
}

fn health_check_of(document: serde_json::Value) -> Result<HealthCheck, serde_json::Error> {
    read(instance_with(|instance| {
        instance["config"]["healthCheck"] = document;
    }))
    .map(|instance| instance.config.health_check)
}

#[test]
fn a_health_check_is_one_of_three_kinds_each_spelled_out() {
    let probe = Probe {
        interval_ms: 5_000,
        timeout_ms: 2_000,
        grace_period_ms: 30_000,
        healthy_threshold: 1,
        unhealthy_threshold: 3,
    };
    let timing = serde_json::json!({ "intervalMs": 5000, "timeoutMs": 2000, "gracePeriodMs": 30000, "healthyThreshold": 1, "unhealthyThreshold": 3 });

    let mut http = timing.clone();
    http["kind"] = "http".into();
    http["path"] = "/api/health".into();
    assert_eq!(
        health_check_of(http).expect("parses"),
        HealthCheck::Http {
            path: "/api/health".into(),
            probe: probe.clone()
        }
    );

    let mut tcp = timing;
    tcp["kind"] = "tcp".into();
    assert_eq!(health_check_of(tcp).expect("parses"), HealthCheck::Tcp { probe });

    assert_eq!(
        health_check_of(serde_json::json!({ "kind": "boot-completed" })).expect("parses"),
        HealthCheck::BootCompleted
    );
}

// The one that looks like a sensible default is the one that lies: a kernel accepts connections
// for a process that will never answer them. So nothing is assumed, and the document says.
#[test]
fn a_health_check_that_names_no_kind_is_refused_by_that_name() {
    let error = health_check_of(serde_json::json!({
        "intervalMs": 5000, "timeoutMs": 2000, "gracePeriodMs": 30000, "healthyThreshold": 1, "unhealthyThreshold": 3
    }))
    .unwrap_err()
    .to_string();
    assert!(error.contains("kind"), "{error}");
}

#[test]
fn an_http_check_names_the_path_it_asks_for() {
    let error = health_check_of(serde_json::json!({
        "kind": "http", "intervalMs": 5000, "timeoutMs": 2000, "gracePeriodMs": 30000, "healthyThreshold": 1, "unhealthyThreshold": 3
    }))
    .unwrap_err()
    .to_string();
    assert!(error.contains("path"), "{error}");
}

#[test]
fn a_health_check_writes_back_as_it_was_read() {
    for document in [
        serde_json::json!({ "kind": "http", "path": "/healthz", "intervalMs": 1000, "timeoutMs": 500, "gracePeriodMs": 3000, "healthyThreshold": 1, "unhealthyThreshold": 2 }),
        serde_json::json!({ "kind": "tcp", "intervalMs": 1000, "timeoutMs": 500, "gracePeriodMs": 3000, "healthyThreshold": 1, "unhealthyThreshold": 2 }),
        serde_json::json!({ "kind": "boot-completed" }),
    ] {
        let parsed = health_check_of(document.clone()).expect("parses");
        assert_eq!(serde_json::to_value(&parsed).expect("serialises"), document);
    }
}

#[test]
fn only_a_check_of_a_port_probes_one_and_a_microvm_is_judged_by_one_reading() {
    assert!(!HealthCheck::BootCompleted.probes_a_port());
    assert_eq!(HealthCheck::BootCompleted.probe().unhealthy_threshold, 1);
    assert_eq!(HealthCheck::BootCompleted.probe().healthy_threshold, 1);
    // The one reading is of the port, once: a tenant has this long to start listening.
    assert_eq!(HealthCheck::BootCompleted.probe().grace_period_ms, 30_000);
    assert!(HealthCheck::BootCompleted.probe().timeout_ms > 0);
    let tcp = HealthCheck::Tcp {
        probe: Probe {
            interval_ms: 1,
            timeout_ms: 1,
            grace_period_ms: 7,
            healthy_threshold: 1,
            unhealthy_threshold: 1,
        },
    };
    assert!(tcp.probes_a_port());
    assert_eq!(tcp.probe().grace_period_ms, 7);
}

#[test]
fn a_document_that_names_no_policy_means_what_it_meant_before_there_were_any() {
    let on_request = read(instance_with(|document| {
        document["desiredState"] = serde_json::json!("on-request");
    }))
    .expect("parses");
    assert_eq!(
        on_request.activation().sleep_when,
        SleepPolicy::TrafficIdle {
            timeout_ms: DEFAULT_IDLE_TIMEOUT
        }
    );

    let timed = read(instance_with(|document| {
        document["desiredState"] = serde_json::json!("on-request");
        document["idleTimeoutMs"] = serde_json::json!(900_000);
    }))
    .expect("parses");
    assert_eq!(
        timed.activation().sleep_when,
        SleepPolicy::TrafficIdle {
            timeout_ms: IdleTimeoutMs::try_from(900_000).unwrap()
        }
    );

    for state in ["running", "stopped"] {
        let kept_up = read(instance_with(|document| {
            document["desiredState"] = serde_json::json!(state);
        }))
        .expect("parses");
        assert_eq!(kept_up.activation().sleep_when, SleepPolicy::Never, "{state}");
    }
}

#[test]
fn a_document_that_says_when_an_instance_sleeps_twice_is_refused() {
    let refused = read(instance_with(|document| {
        document["idleTimeoutMs"] = serde_json::json!(900_000);
        document["activation"] = serde_json::json!({
            "sleepWhen": { "kind": "traffic-idle", "timeoutMs": 900000 }
        });
    }))
    .expect_err("both spellings are refused");
    assert!(refused.to_string().contains("name one"), "{refused}");
}

#[test]
fn only_an_on_request_instance_may_name_a_sleep_policy_that_fires() {
    for state in ["running", "stopped"] {
        let refused = read(instance_with(|document| {
            document["desiredState"] = serde_json::json!(state);
            document["activation"] = serde_json::json!({
                "sleepWhen": { "kind": "max-lifetime", "ttlMs": 3600000 }
            });
        }))
        .expect_err("nothing would wake it again");
        assert!(refused.to_string().contains("on-request"), "{state}: {refused}");

        assert!(
            read(instance_with(|document| {
                document["desiredState"] = serde_json::json!(state);
                document["activation"] = serde_json::json!({ "sleepWhen": { "kind": "never" } });
            }))
            .is_ok(),
            "{state} may still say it never sleeps"
        );
    }
}

#[test]
fn a_policy_this_host_does_not_offer_is_refused_by_name_rather_than_ignored() {
    assert!(read(instance_with(|document| {
        document["activation"] = serde_json::json!({ "sleepWhen": { "kind": "no-sessions" } });
    }))
    .is_err());

    assert!(read(instance_with(|document| {
        document["config"]["healthCheck"] = serde_json::json!({ "kind": "agent-ready" });
    }))
    .is_err());

    assert!(
        read(instance_with(|document| {
            document["activation"] = serde_json::json!({});
        }))
        .is_err(),
        "a policy that names no sleepWhen says nothing"
    );
}

#[test]
fn a_lifetime_outside_what_a_host_will_hold_is_refused() {
    assert!(MaxLifetimeMs::try_from(MIN_MAX_LIFETIME_MS).is_ok());
    assert!(MaxLifetimeMs::try_from(MAX_MAX_LIFETIME_MS).is_ok());
    assert!(MaxLifetimeMs::try_from(MIN_MAX_LIFETIME_MS - 1).is_err());
    assert!(MaxLifetimeMs::try_from(MAX_MAX_LIFETIME_MS + 1).is_err());

    let refused = read(instance_with(|document| {
        document["desiredState"] = serde_json::json!("on-request");
        document["activation"] = serde_json::json!({
            "sleepWhen": { "kind": "max-lifetime", "ttlMs": 1000 }
        });
    }))
    .expect_err("a second is not a lifetime a host will hold");
    assert!(refused.to_string().contains("ttlMs"), "{refused}");
}

mod schema {
    use super::*;

    fn validator(schema: schemars::Schema) -> jsonschema::Validator {
        let schema = schema.to_value();
        jsonschema::meta::validate(&schema).expect("a schema the draft accepts");
        jsonschema::validator_for(&schema).expect("a schema that compiles")
    }

    fn with(mut document: serde_json::Value, pointer: &str, value: serde_json::Value) -> serde_json::Value {
        let (parent, key) = pointer.rsplit_once('/').expect(pointer);
        match document.pointer_mut(parent).expect(parent) {
            serde_json::Value::Object(object) => object.insert(key.to_string(), value),
            serde_json::Value::Array(items) => items
                .get_mut(key.parse::<usize>().expect(key))
                .map(|slot| std::mem::replace(slot, value)),
            _ => panic!("{parent} holds neither an object nor an array"),
        };
        document
    }

    fn without(mut document: serde_json::Value, pointer: &str) -> serde_json::Value {
        let (parent, key) = pointer.rsplit_once('/').expect(pointer);
        document
            .pointer_mut(parent)
            .and_then(serde_json::Value::as_object_mut)
            .expect(parent)
            .remove(key);
        document
    }

    fn reported_json() -> serde_json::Value {
        let now = Timestamp::parse("2026-08-03T10:00:00.000Z").unwrap();
        let capacity = HostCapacity {
            vcpu_count: 8,
            memory_mib: 32_768,
            cache_bytes: 1 << 40,
        };
        let state = HostReportedState {
            host_id: HostId::parse("host-1").unwrap(),
            reported_at: now.clone(),
            state: HostState::Ready,
            capacity,
            allocatable: capacity,
            versions: HostVersions {
                agent: "0.1.0".into(),
                guest_image: "2026.8.3-1".into(),
                zerofs: "0.5.0".into(),
                firecracker: "1.12.0".into(),
            },
            volumes: vec![ReportedVolume {
                volume_id: VolumeId::parse("vol-1").unwrap(),
                app_id: AppId::parse("app-1").unwrap(),
                state: VolumeState::Ready,
                size_bytes: 4096,
                storage_prefix: Some(ObjectKey::parse("volumes/vol-1").unwrap()),
                device_path: Some("/dev/nbd0".into()),
                message: Some(StateMessage::new("mounted")),
            }],
            instances: vec![ReportedInstance {
                app_id: AppId::parse("app-1").unwrap(),
                deployment_id: DeploymentId::parse("dep-1").unwrap(),
                state: InstanceState::Running,
                host_port: HostPort::new(21000).ok(),
                guest_ipv4: Some(Ipv4Address::parse("10.201.0.2").unwrap()),
                layer_digests: vec![
                    Sha256Digest::parse("b".repeat(64)).unwrap(),
                    Sha256Digest::parse("a".repeat(64)).unwrap(),
                ],
                restart_count: 1,
                last_restart: Some(ReportedRestart {
                    at: now.clone(),
                    restart: TenantRestart {
                        attempt: 1,
                        budget: 5,
                        exit: TenantExit::Signal(9),
                        reason: StateMessage::new(
                            "the tenant exited (137): the kernel killed it for running out of memory at its ceiling of 198 MiB; restart 1 of 5 in 500ms",
                        ),
                        backoff_ms: 500,
                    },
                }),
                started_at: Some(now.clone()),
                converged_at: Some(now.clone()),
                last_exit_code: Some(0),
                message: Some(StateMessage::new("healthy")),
                labels: serde_json::from_value(serde_json::json!({ "app": "shop", "rev": "r-12" })).unwrap(),
            }],
            checkpoints: vec![ReportedCheckpoint {
                checkpoint_id: CheckpointId::parse("ckpt-1").unwrap(),
                volume_id: VolumeId::parse("vol-1").unwrap(),
                state: CheckpointState::Ready,
                reference: Some(StateMessage::new("snap-1")),
                ready_at: Some(now.clone()),
                message: None,
            }],
            exports: vec![ReportedExport {
                export_id: ExportId::parse("exp-1").unwrap(),
                checkpoint_id: Some(CheckpointId::parse("ckpt-1").unwrap()),
                state: ExportState::Ready,
                size_bytes: Some(2048),
                ready_at: Some(now),
                message: None,
            }],
            accepted_digest: Some(Sha256Digest::parse("c".repeat(64)).unwrap()),
            accepted_revision: Some(Revision::parse("deploy-4821").unwrap()),
            message: Some(StateMessage::new(
                "the last document named a volume this host does not hold",
            )),
        };
        serde_json::to_value(state).unwrap()
    }

    fn activated(policy: serde_json::Value) -> serde_json::Value {
        with(
            without(desired_json(), "/instances/0/idleTimeoutMs"),
            "/instances/0/activation",
            policy,
        )
    }

    #[test]
    fn the_desired_state_schema_accepts_what_the_parser_accepts() {
        let validator = validator(crate::schema::desired_state());
        let accepted = [
            desired_json(),
            activated(serde_json::json!({
                "sleepWhen": { "kind": "max-lifetime", "ttlMs": 3600000 },
                "readyWhen": { "kind": "boot-completed" }
            })),
            activated(serde_json::json!({ "sleepWhen": { "kind": "traffic-idle", "timeoutMs": 900000 } })),
            with(
                activated(serde_json::json!({ "sleepWhen": { "kind": "never" } })),
                "/instances/0/desiredState",
                serde_json::json!("running"),
            ),
        ];
        for document in accepted {
            serde_json::from_value::<HostDesiredState>(document.clone()).unwrap();
            let errors: Vec<String> = validator.iter_errors(&document).map(|e| e.to_string()).collect();
            assert!(errors.is_empty(), "{errors:#?}");
        }
    }

    #[test]
    fn the_schema_holds_labels_to_the_parsers_bounds() {
        let validator = validator(crate::schema::desired_state());
        let labelled = |labels: serde_json::Value| with(desired_json(), "/instances/0/labels", labels);
        let eight: serde_json::Map<_, _> = (0..MAX_INSTANCE_LABELS)
            .map(|index| (format!("k{index}"), serde_json::json!("v")))
            .collect();
        let mut nine = eight.clone();
        nine.insert("k8".into(), serde_json::json!("v"));
        for document in [
            labelled(serde_json::json!({ "app": "shop", "rev": "r-12", "context": "production" })),
            labelled(serde_json::Value::Object(eight)),
        ] {
            serde_json::from_value::<HostDesiredState>(document.clone()).unwrap();
            assert!(validator.is_valid(&document), "the schema refused {document}");
        }
        for document in [
            labelled(serde_json::Value::Object(nine)),
            labelled(serde_json::json!({ "App": "shop" })),
            labelled(serde_json::json!({ "app": "has space" })),
            labelled(serde_json::json!({ "app": "" })),
            labelled(serde_json::json!({ "app": "x".repeat(129) })),
            labelled(serde_json::json!({ "app": 7 })),
        ] {
            assert!(
                serde_json::from_value::<HostDesiredState>(document.clone()).is_err(),
                "the parser took {document}"
            );
            assert!(!validator.is_valid(&document), "the schema took {document}");
        }
    }

    #[test]
    fn the_desired_state_schema_refuses_what_the_parser_refuses() {
        let validator = validator(crate::schema::desired_state());
        let sixty_five = serde_json::Value::Array(vec![serde_json::json!("x"); MAX_ARGUMENTS + 1]);
        let nine_layers =
            serde_json::Value::Array(vec![instance_json()["layers"][0].clone(); MAX_LAYERS + 1]);
        let broken = [
            with(desired_json(), "/hostId", serde_json::json!("has.a.dot")),
            without(desired_json(), "/revision"),
            with(desired_json(), "/revision", serde_json::json!("has a space")),
            with(desired_json(), "/revision", serde_json::json!("")),
            with(
                desired_json(),
                "/instances/0/appId",
                serde_json::json!("x".repeat(64)),
            ),
            with(
                desired_json(),
                "/instances/0/desiredState",
                serde_json::json!("asleep"),
            ),
            with(
                desired_json(),
                "/instances/0/idleTimeoutMs",
                serde_json::json!(10),
            ),
            with(
                desired_json(),
                "/instances/0/layers/0/digest",
                serde_json::json!("A".repeat(64)),
            ),
            with(
                desired_json(),
                "/instances/0/layers/0/objectKey",
                serde_json::json!(""),
            ),
            with(
                desired_json(),
                "/instances/0/layers/0/kind",
                serde_json::json!("binary"),
            ),
            without(desired_json(), "/instances/0/layers/0/kind"),
            without(desired_json(), "/instances/0/layers/1/digest"),
            without(desired_json(), "/instances/0/layers/1/destinationPath"),
            with(
                desired_json(),
                "/instances/0/layers/1/destinationPath",
                serde_json::json!("app/server"),
            ),
            with(
                desired_json(),
                "/instances/0/layers/1/destinationPath",
                serde_json::json!("/"),
            ),
            with(
                desired_json(),
                "/instances/0/layers/1/destinationPath",
                serde_json::json!("/sbin/init"),
            ),
            with(desired_json(), "/instances/0/layers", serde_json::json!([])),
            with(desired_json(), "/instances/0/layers", nine_layers),
            with(
                desired_json(),
                "/instances/0/config/httpPort",
                serde_json::json!(0),
            ),
            with(
                desired_json(),
                "/instances/0/config/httpPort",
                serde_json::json!(70_000),
            ),
            with(
                desired_json(),
                "/instances/0/config/httpPort",
                serde_json::json!("3000"),
            ),
            with(desired_json(), "/instances/0/config/command/args", sixty_five),
            without(desired_json(), "/instances/0/config/command/program"),
            with(
                desired_json(),
                "/instances/0/config/command/program",
                serde_json::json!("app/server"),
            ),
            without(desired_json(), "/instances/0/config/command/workingDirectory"),
            with(
                desired_json(),
                "/instances/0/config/command/workingDirectory",
                serde_json::json!("/app/"),
            ),
            with(
                desired_json(),
                "/instances/0/config/command/environment",
                serde_json::json!({ "1A": "x" }),
            ),
            with(
                desired_json(),
                "/instances/0/config/command/environment",
                serde_json::json!({ "__proto__": "x" }),
            ),
            with(
                desired_json(),
                "/instances/0/config/ports/0/name",
                serde_json::json!("SSH"),
            ),
            with(
                desired_json(),
                "/instances/0/config/ports/0/guestPort",
                serde_json::json!(0),
            ),
            with(
                desired_json(),
                "/instances/0/hostnames/0/hostname",
                serde_json::json!("localhost"),
            ),
            with(
                desired_json(),
                "/instances/0/hostnames/0/kind",
                serde_json::json!("vanity"),
            ),
            with(desired_json(), "/volumes/0/sizeBytes", serde_json::json!(-1)),
            with(
                desired_json(),
                "/volumes/0/initialContents/destinationPath",
                serde_json::json!("app/data"),
            ),
            without(desired_json(), "/volumes/0/initialContents/digest"),
            with(
                desired_json(),
                "/volumes/0/desiredState",
                serde_json::json!("gone"),
            ),
            without(desired_json(), "/instances/0/layers"),
            without(desired_json(), "/volumes/0/appId"),
            with(
                desired_json(),
                "/instances/0/activation",
                serde_json::json!({ "sleepWhen": { "kind": "traffic-idle", "timeoutMs": 900000 } }),
            ),
            with(
                activated(serde_json::json!({ "sleepWhen": { "kind": "max-lifetime", "ttlMs": 3600000 } })),
                "/instances/0/desiredState",
                serde_json::json!("running"),
            ),
            activated(serde_json::json!({ "sleepWhen": { "kind": "no-sessions" } })),
            activated(serde_json::json!({ "sleepWhen": { "kind": "max-lifetime", "ttlMs": 1000 } })),
            with(
                desired_json(),
                "/instances/0/config/healthCheck",
                serde_json::json!({ "kind": "prayer" }),
            ),
            with(
                desired_json(),
                "/instances/0/config/healthCheck",
                serde_json::json!({ "intervalMs": 5000, "timeoutMs": 2000, "gracePeriodMs": 30000, "healthyThreshold": 1, "unhealthyThreshold": 3 }),
            ),
            with(
                desired_json(),
                "/instances/0/config/healthCheck",
                serde_json::json!({ "kind": "http", "intervalMs": 5000, "timeoutMs": 2000, "gracePeriodMs": 30000, "healthyThreshold": 1, "unhealthyThreshold": 3 }),
            ),
        ];
        for document in broken {
            assert!(
                serde_json::from_value::<HostDesiredState>(document.clone()).is_err(),
                "the parser took {document}"
            );
            assert!(!validator.is_valid(&document), "the schema took {document}");
        }
    }

    // What the parser knows and JSON Schema has no words for: a value may only name the runtime
    // values the guest offers. A document the schema passes can still be refused for this.
    #[test]
    fn the_desired_state_schema_cannot_see_which_runtime_values_the_guest_offers() {
        let validator = validator(crate::schema::desired_state());
        let document = with(
            desired_json(),
            "/instances/0/config/command/environment",
            serde_json::json!({ "URL": "${NIBRUN_NOPE}" }),
        );
        assert!(serde_json::from_value::<HostDesiredState>(document.clone()).is_err());
        assert!(validator.is_valid(&document));
    }

    #[test]
    fn the_reported_state_schema_accepts_what_the_daemon_writes() {
        let validator = validator(crate::schema::reported_state());
        let document = reported_json();
        let errors: Vec<String> = validator.iter_errors(&document).map(|e| e.to_string()).collect();
        assert!(errors.is_empty(), "{errors:#?}");
    }

    #[test]
    fn the_reported_state_schema_refuses_what_the_parser_refuses() {
        let validator = validator(crate::schema::reported_state());
        let broken = [
            with(
                reported_json(),
                "/reportedAt",
                serde_json::json!("2026-08-03T10:00:00"),
            ),
            with(reported_json(), "/state", serde_json::json!("asleep")),
            with(reported_json(), "/instances/0/state", serde_json::json!("asleep")),
            with(
                reported_json(),
                "/instances/0/guestIpv4",
                serde_json::json!("01.2.3.4"),
            ),
            with(reported_json(), "/instances/0/hostPort", serde_json::json!(0)),
            with(
                reported_json(),
                "/instances/0/restartCount",
                serde_json::json!(-1),
            ),
            with(
                reported_json(),
                "/instances/0/lastRestart/exit",
                serde_json::json!({ "status": 137 }),
            ),
            without(reported_json(), "/instances/0/lastRestart/reason"),
            with(reported_json(), "/volumes/0/state", serde_json::json!("lost")),
            without(reported_json(), "/capacity"),
        ];
        for document in broken {
            assert!(
                serde_json::from_value::<HostReportedState>(document.clone()).is_err(),
                "the parser took {document}"
            );
            assert!(!validator.is_valid(&document), "the schema took {document}");
        }
    }

    #[test]
    fn every_published_schema_is_named_by_its_id() {
        for (filename, schema) in crate::schema::all() {
            let id = schema.get("$id").and_then(serde_json::Value::as_str).unwrap();
            assert_eq!(id, format!("{}{filename}", crate::schema::SCHEMA_ID_BASE));
        }
    }
}
