//! Traces and log records a guest's runtime exports over OTLP/HTTP, received on its own tap's
//! host address and filed under the unit that tap belongs to.
use protocol::Ipv4Address;

pub mod receiver;
pub(crate) mod trace;
pub(crate) mod wire;

pub use receiver::{OtlpReceiver, OtlpSender};

/// What every guest is told about the receiver on its own tap, beneath anything its images or its
/// document set: where it is, the one protocol it takes, and a one-second batch so that a trace is
/// readable while its request is still fresh. The runtime's own exporter is the app's to turn on.
pub fn exporter_defaults(host_ipv4: &Ipv4Address) -> Vec<(String, String)> {
    [
        (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            format!("http://{host_ipv4}:{}", nft_render::OTLP_RECEIVER_PORT),
        ),
        ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json".to_string()),
        ("OTEL_BSP_SCHEDULE_DELAY", "1000".to_string()),
        ("OTEL_BLRP_SCHEDULE_DELAY", "1000".to_string()),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_string(), value))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use guest_contract::instance_env::launch_environment;
    use protocol::{GuestPath, TenantEnvironment, TenantValue};

    const FOUR: [&str; 4] = [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_BSP_SCHEDULE_DELAY",
        "OTEL_BLRP_SCHEDULE_DELAY",
    ];

    fn launched(document: &[(&str, &str)]) -> Vec<(String, String)> {
        let document: TenantEnvironment = document
            .iter()
            .map(|(name, value)| (name.to_string(), TenantValue::parse(*value).unwrap()))
            .collect();
        let tap = Ipv4Address::parse("10.201.0.5").unwrap();
        launch_environment(
            &GuestPath::parse("/app").unwrap(),
            &exporter_defaults(&tap),
            &[],
            &document,
        )
        .iter()
        .map(|(name, value)| (name.to_string(), value.expose().to_string()))
        .collect()
    }

    fn value<'a>(environment: &'a [(String, String)], name: &str) -> Option<&'a str> {
        environment
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn a_unit_is_told_its_taps_receiver_in_the_four_standard_variables() {
        let environment = launched(&[("MODE", "production")]);
        assert_eq!(
            value(&environment, "OTEL_EXPORTER_OTLP_ENDPOINT"),
            Some("http://10.201.0.5:4318")
        );
        assert_eq!(
            value(&environment, "OTEL_EXPORTER_OTLP_PROTOCOL"),
            Some("http/json")
        );
        assert_eq!(value(&environment, "OTEL_BSP_SCHEDULE_DELAY"), Some("1000"));
        assert_eq!(value(&environment, "OTEL_BLRP_SCHEDULE_DELAY"), Some("1000"));
        assert_eq!(environment.len(), 2 + 4 + 1);
    }

    #[test]
    fn the_document_overrides_each_of_the_four() {
        for name in FOUR {
            let environment = launched(&[(name, "the app's")]);
            assert_eq!(value(&environment, name), Some("the app's"), "{name}");
            assert_eq!(environment.len(), 2 + 4);
        }
    }
}
