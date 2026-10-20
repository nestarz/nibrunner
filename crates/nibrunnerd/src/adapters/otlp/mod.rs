//! Traces and log records a guest's runtime exports over OTLP/HTTP, received on its own tap's
//! host address and filed under the unit that tap belongs to.
use std::borrow::Cow;

use protocol::{Ipv4Address, TenantEnvironment, TenantValue};

pub mod receiver;
pub(crate) mod trace;
pub(crate) mod wire;

pub use receiver::{OtlpReceiver, OtlpSender};

const ENDPOINT_VARIABLE: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// The environment a guest is launched with: its own, and where its tap's receiver is unless it
/// names an endpoint itself or has no room left for one.
pub fn with_endpoint<'a>(
    environment: &'a TenantEnvironment,
    host_ipv4: &Ipv4Address,
) -> Cow<'a, TenantEnvironment> {
    let named = environment.iter().any(|(name, _)| name == ENDPOINT_VARIABLE);
    let full = environment.len() >= guest_contract::instance_env::MAX_TENANT_VARIABLES;
    let endpoint = format!("http://{host_ipv4}:{}", nft_render::OTLP_RECEIVER_PORT);
    match TenantValue::parse(endpoint) {
        Ok(endpoint) if !named && !full => Cow::Owned(
            environment
                .iter()
                .map(|(name, value)| (name.to_string(), value.clone()))
                .chain([(ENDPOINT_VARIABLE.to_string(), endpoint)])
                .collect(),
        ),
        _ => Cow::Borrowed(environment),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment(pairs: &[(&str, &str)]) -> TenantEnvironment {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), TenantValue::parse(*value).unwrap()))
            .collect()
    }

    fn endpoint(environment: &TenantEnvironment) -> Option<String> {
        environment
            .iter()
            .find(|(name, _)| *name == ENDPOINT_VARIABLE)
            .map(|(_, value)| value.expose().to_string())
    }

    #[test]
    fn a_guest_is_told_its_taps_receiver_unless_its_environment_names_an_endpoint() {
        let tap = Ipv4Address::parse("10.201.0.5").unwrap();
        let given = environment(&[("MODE", "production")]);
        let launched = with_endpoint(&given, &tap);
        assert_eq!(endpoint(&launched).as_deref(), Some("http://10.201.0.5:4318"));
        assert_eq!(launched.len(), 2);

        let own = environment(&[(ENDPOINT_VARIABLE, "https://collector.example.com")]);
        assert!(matches!(with_endpoint(&own, &tap), Cow::Borrowed(_)));
        assert_eq!(
            endpoint(&with_endpoint(&own, &tap)).as_deref(),
            Some("https://collector.example.com")
        );
    }

    #[test]
    fn a_guest_whose_environment_is_full_is_launched_with_it_unchanged() {
        let names: Vec<String> = (0..guest_contract::instance_env::MAX_TENANT_VARIABLES)
            .map(|n| format!("V{n}"))
            .collect();
        let full = environment(&names.iter().map(|n| (n.as_str(), "x")).collect::<Vec<_>>());
        let tap = Ipv4Address::parse("10.201.0.1").unwrap();
        assert!(matches!(with_endpoint(&full, &tap), Cow::Borrowed(_)));
    }
}
