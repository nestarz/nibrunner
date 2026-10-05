use std::net::SocketAddr;

use protocol::{AppId, GuestPort, HostPort, Ipv4Address};

use crate::slot::{GUEST_NETWORK_CIDR, TAP_NAME_PREFIX};

pub const NFTABLES_TABLE: &str = "nibrun";

pub const NFTABLES_FAMILIES: [&str; 2] = ["ip", "ip6"];

pub const INSTANCE_METADATA_ADDRESS_V4: &str = "169.254.169.254";
pub const INSTANCE_METADATA_ADDRESS_V6: &str = "fd00:ec2::254";

const OUTPUT_NAT_PRIORITY: i32 = -100;

const TRAFFIC_CHAIN_PRIORITY: &str = "filter + 10";

const PRIVATE_DESTINATIONS_V4: [&str; 6] = [
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "169.254.0.0/16",
    "127.0.0.0/8",
    "100.64.0.0/10",
];

const PRIVATE_DESTINATIONS_V6: [&str; 3] = ["::1/128", "fe80::/10", "fc00::/7"];

const CHAIN_INDENT: &str = "  ";
const RULE_INDENT: &str = "    ";

const DENY: &str = "reject";

pub fn app_received_counter_name(app_id: &AppId) -> String {
    format!("{APP_RECEIVED_COUNTER_PREFIX}{app_id}")
}

pub fn app_sent_counter_name(app_id: &AppId) -> String {
    format!("{APP_SENT_COUNTER_PREFIX}{app_id}")
}

// Which way a counter faces is in its name rather than in where it is used, because what reads
// them back is handed a flat list of counters and their names are all it has to go on.
pub const APP_RECEIVED_COUNTER_PREFIX: &str = "rx_";
pub const APP_SENT_COUNTER_PREFIX: &str = "tx_";

/// One host port carried to one guest port. Nothing forwards a port no document named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardedPort {
    pub host_port: HostPort,
    pub guest_port: GuestPort,
    /// A raw port carries whatever arrives, tcp or udp. The HTTP port is the proxy's and carries
    /// tcp, because that is what the proxy speaks.
    pub raw: bool,
}

impl ForwardedPort {
    fn dport_match(&self) -> &'static str {
        if self.raw {
            "meta l4proto { tcp, udp } th dport"
        } else {
            "tcp dport"
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardedInstance {
    pub app_id: AppId,
    pub ports: Vec<ForwardedPort>,
    pub host_ipv4: Ipv4Address,
    pub guest_ipv4: Ipv4Address,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FirewallState {
    pub allowed_host_tcp_endpoints: Vec<SocketAddr>,
    pub instances: Vec<ForwardedInstance>,
    pub denied_egress_addresses_v4: Vec<String>,
    pub denied_egress_addresses_v6: Vec<String>,
}

fn tap_match() -> String {
    format!("\"{TAP_NAME_PREFIX}*\"")
}

fn set(values: &[&str]) -> String {
    format!("{{ {} }}", values.join(", "))
}

fn counter_objects(state: &FirewallState) -> Vec<String> {
    state
        .instances
        .iter()
        .flat_map(|instance| {
            [
                app_received_counter_name(&instance.app_id),
                app_sent_counter_name(&instance.app_id),
            ]
        })
        .flat_map(|name| {
            [
                format!("{CHAIN_INDENT}counter {name} {{"),
                format!("{CHAIN_INDENT}}}"),
                String::new(),
            ]
        })
        .collect()
}

fn chain(header: &str, rules: &[String]) -> Vec<String> {
    let mut lines = vec![format!("{CHAIN_INDENT}chain {header}")];
    lines.extend(rules.iter().map(|rule| format!("{RULE_INDENT}{rule}")));
    lines.push(format!("{CHAIN_INDENT}}}"));
    lines
}

fn table(family: &str, body: Vec<String>) -> Vec<String> {
    let mut lines = vec![
        format!("table {family} {NFTABLES_TABLE}"),
        format!("delete table {family} {NFTABLES_TABLE}"),
        String::new(),
        format!("table {family} {NFTABLES_TABLE} {{"),
    ];
    lines.extend(body);
    lines.push("}".to_string());
    lines
}

pub fn render_ruleset(state: &FirewallState) -> String {
    let mut lines = Vec::new();
    let mut v4_body = counter_objects(state);
    v4_body.extend(forward_chain_v4(state));
    v4_body.push(String::new());
    v4_body.extend(input_chain_v4(state));
    v4_body.push(String::new());
    v4_body.extend(nat_chains_v4(state));
    v4_body.push(String::new());
    v4_body.extend(traffic_chains_v4(state));
    lines.extend(table("ip", v4_body));
    lines.push(String::new());
    let mut v6_body = forward_chain_v6(state);
    v6_body.push(String::new());
    v6_body.extend(input_chain_v6(state));
    lines.extend(table("ip6", v6_body));
    format!("{}\n", lines.join("\n"))
}

fn traffic_chains_v4(state: &FirewallState) -> Vec<String> {
    if state.instances.is_empty() {
        return Vec::new();
    }
    let tap = tap_match();
    let mut output_rules = vec!["type filter hook output priority filter; policy accept;".to_string()];
    output_rules.extend(state.instances.iter().map(|instance| {
        format!(
            "ip saddr 127.0.0.0/8 ip daddr {} counter name {}",
            instance.guest_ipv4,
            app_received_counter_name(&instance.app_id)
        )
    }));

    // What a guest answers the proxy with is addressed to this host, so it is never forwarded and
    // the chain below would never see it. It arrives on a connection the input chain has already
    // accepted as established — and an accept ends a chain rather than the hook, so this one is
    // still reached.
    let mut input_rules = vec![format!(
        "type filter hook input priority {TRAFFIC_CHAIN_PRIORITY}; policy accept;"
    )];
    input_rules.extend(state.instances.iter().map(|instance| {
        format!(
            "iifname {tap} ip saddr {} counter name {}",
            instance.guest_ipv4,
            app_sent_counter_name(&instance.app_id)
        )
    }));

    let mut forward_rules = vec![format!(
        "type filter hook forward priority {TRAFFIC_CHAIN_PRIORITY}; policy accept;"
    )];
    forward_rules.extend(state.instances.iter().flat_map(|instance| {
        [
            format!(
                "iifname != {tap} oifname {tap} ip daddr {} counter name {}",
                instance.guest_ipv4,
                app_received_counter_name(&instance.app_id)
            ),
            // Only what was let out. The forward chain at the priority above this one rejects a
            // guest reaching another guest, a private destination, or an address the host denies
            // by name, and a packet it rejected never arrives here to be counted against anybody.
            format!(
                "iifname {tap} oifname != {tap} ip saddr {} counter name {}",
                instance.guest_ipv4,
                app_sent_counter_name(&instance.app_id)
            ),
        ]
    }));

    let mut lines = chain("traffic_output {", &output_rules);
    lines.push(String::new());
    lines.extend(chain("traffic_input {", &input_rules));
    lines.push(String::new());
    lines.extend(chain("traffic_forward {", &forward_rules));
    lines
}

// Ahead of the established accept, so that a flow a guest opened before an address was denied is
// cut on its next packet rather than carried by conntrack for as long as it stays warm. A tcp
// packet is answered with a reset, which ends a connected socket at once; the ICMP unreachable a
// plain reject sends is only a soft error on one, and would leave the guest retransmitting into
// the deny until it timed out.
fn denied_egress_rules(tap: &str, daddr: &str, cidrs: &[String]) -> Vec<String> {
    cidrs
        .iter()
        .flat_map(|cidr| {
            [
                format!(
                    "iifname {tap} {daddr} {cidr} meta l4proto tcp {DENY} with tcp reset comment \"denied egress\""
                ),
                format!("iifname {tap} {daddr} {cidr} {DENY} comment \"denied egress\""),
            ]
        })
        .collect()
}

fn forward_chain_v4(state: &FirewallState) -> Vec<String> {
    let tap = tap_match();
    let mut rules = vec!["type filter hook forward priority filter; policy accept;".to_string()];
    rules.extend(denied_egress_rules(
        &tap,
        "ip daddr",
        &state.denied_egress_addresses_v4,
    ));
    rules.extend([
        "ct state established,related accept".to_string(),
        format!("iifname {tap} ip daddr {INSTANCE_METADATA_ADDRESS_V4} {DENY} comment \"instance metadata endpoint\""),
        format!("iifname {tap} oifname {tap} {DENY} comment \"guest to guest\""),
        format!("iifname {tap} ip daddr {GUEST_NETWORK_CIDR} {DENY} comment \"guest to guest\""),
    ]);
    rules.push(format!(
        "iifname {tap} ip daddr {} {DENY} comment \"private destinations\"",
        set(&PRIVATE_DESTINATIONS_V4)
    ));
    chain("forward {", &rules)
}

fn input_chain_v4(state: &FirewallState) -> Vec<String> {
    input_chain(state, true)
}

fn input_chain(state: &FirewallState, ipv4: bool) -> Vec<String> {
    let tap = tap_match();
    let (daddr, denied) = if ipv4 {
        ("ip daddr", &state.denied_egress_addresses_v4)
    } else {
        ("ip6 daddr", &state.denied_egress_addresses_v6)
    };
    let mut rules = vec!["type filter hook input priority filter; policy accept;".to_string()];
    // Only replies to host-initiated flows bypass the endpoint list, so removing an endpoint
    // also closes guest-initiated connections that were already established.
    rules.push(format!(
        "iifname {tap} ct direction reply ct state established,related accept"
    ));
    rules.extend(denied_egress_rules(&tap, daddr, denied));
    for endpoint in state
        .allowed_host_tcp_endpoints
        .iter()
        .filter(|e| e.is_ipv4() == ipv4)
    {
        rules.push(format!(
            "iifname {tap} {daddr} {} tcp dport {} accept comment \"allowed host endpoint\"",
            endpoint.ip(),
            endpoint.port()
        ));
    }
    rules.push(format!("iifname {tap} {DENY} comment \"guest to host\""));
    chain("input {", &rules)
}

fn forward_chain_v6(state: &FirewallState) -> Vec<String> {
    let tap = tap_match();
    let mut rules = vec!["type filter hook forward priority filter; policy accept;".to_string()];
    rules.extend(denied_egress_rules(
        &tap,
        "ip6 daddr",
        &state.denied_egress_addresses_v6,
    ));
    rules.extend([
        "ct state established,related accept".to_string(),
        format!("iifname {tap} ip6 daddr {INSTANCE_METADATA_ADDRESS_V6} {DENY} comment \"instance metadata endpoint\""),
        format!("iifname {tap} oifname {tap} {DENY} comment \"guest to guest\""),
    ]);
    rules.push(format!(
        "iifname {tap} ip6 daddr {} {DENY} comment \"private destinations\"",
        set(&PRIVATE_DESTINATIONS_V6)
    ));
    chain("forward {", &rules)
}

fn input_chain_v6(state: &FirewallState) -> Vec<String> {
    input_chain(state, false)
}

fn nat_chains_v4(state: &FirewallState) -> Vec<String> {
    let tap = tap_match();
    let mut prerouting = vec!["type nat hook prerouting priority dstnat; policy accept;".to_string()];
    prerouting.extend(state.instances.iter().flat_map(|instance| {
        let tap = tap.clone();
        instance.ports.iter().map(move |port| {
            format!(
                "iifname != {tap} {} {} dnat to {}:{}",
                port.dport_match(),
                port.host_port,
                instance.guest_ipv4,
                port.guest_port
            )
        })
    }));
    let mut output = vec![format!(
        "type nat hook output priority {OUTPUT_NAT_PRIORITY}; policy accept;"
    )];
    output.extend(state.instances.iter().flat_map(|instance| {
        instance.ports.iter().map(move |port| {
            format!(
                "ip daddr 127.0.0.1 {} {} dnat to {}:{}",
                port.dport_match(),
                port.host_port,
                instance.guest_ipv4,
                port.guest_port
            )
        })
    }));

    let mut postrouting = vec!["type nat hook postrouting priority srcnat; policy accept;".to_string()];
    postrouting.extend(state.instances.iter().map(|instance| {
        format!(
            "oifname {tap} ip saddr 127.0.0.0/8 ip daddr {} snat to {}",
            instance.guest_ipv4, instance.host_ipv4
        )
    }));
    postrouting.push(format!(
        "ip saddr {GUEST_NETWORK_CIDR} oifname != {tap} masquerade"
    ));

    let mut lines = chain("prerouting {", &prerouting);
    lines.push(String::new());
    lines.extend(chain("output {", &output));
    lines.push(String::new());
    lines.extend(chain("postrouting {", &postrouting));
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn forwarded(host_port: u16, guest_port: u16) -> ForwardedPort {
        ForwardedPort {
            host_port: HostPort::new(host_port).unwrap(),
            guest_port: GuestPort::new(guest_port).unwrap(),
            raw: false,
        }
    }

    fn raw(host_port: u16, guest_port: u16) -> ForwardedPort {
        ForwardedPort {
            raw: true,
            ..forwarded(host_port, guest_port)
        }
    }

    fn instance() -> ForwardedInstance {
        ForwardedInstance {
            app_id: AppId::parse("0198f3aa-1c2d-7e4b-9f11-a0b1c2d3e4f5").unwrap(),
            ports: vec![forwarded(21_000, 3000)],
            host_ipv4: Ipv4Address::parse("10.201.0.1").unwrap(),
            guest_ipv4: Ipv4Address::parse("10.201.0.2").unwrap(),
        }
    }

    fn with_ssh() -> ForwardedInstance {
        ForwardedInstance {
            ports: vec![forwarded(21_000, 3000), raw(21_001, 22)],
            ..instance()
        }
    }

    fn state(instances: Vec<ForwardedInstance>, v4: &[&str], v6: &[&str]) -> FirewallState {
        FirewallState {
            instances,
            allowed_host_tcp_endpoints: vec![],
            denied_egress_addresses_v4: v4.iter().map(|s| s.to_string()).collect(),
            denied_egress_addresses_v6: v6.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn only_named_host_tcp_endpoints_are_allowed_and_denies_take_precedence() {
        let mut configured = state(vec![], &["203.0.113.0/24"], &["2001:db8::/32"]);
        configured.allowed_host_tcp_endpoints = vec![
            "203.0.113.10:443".parse().unwrap(),
            "[2001:db8::10]:443".parse().unwrap(),
        ];
        for ipv4 in [true, false] {
            let rules = input_chain(&configured, ipv4).join("\n");
            let address = if ipv4 {
                "ip daddr 203.0.113.10"
            } else {
                "ip6 daddr 2001:db8::10"
            };
            let allowed = format!("iifname \"nbr*\" {address} tcp dport 443 accept");
            let allow_at = rules.find(&allowed).unwrap();
            assert!(rules.find("denied egress").unwrap() < allow_at);
            assert!(allow_at < rules.find("guest to host").unwrap());
            assert!(rules.contains("ct direction reply ct state established,related accept"));
            assert!(!rules.contains("tcp dport 22"));
            assert!(!rules.contains("udp dport"));
            assert_eq!(rules.matches("allowed host endpoint").count(), 1);
        }
    }

    #[test]
    fn removing_host_endpoints_revokes_guest_initiated_connections() {
        for ipv4 in [true, false] {
            let rules = input_chain(&FirewallState::default(), ipv4).join("\n");
            assert!(!rules.contains("allowed host endpoint"));
            assert_eq!(
                rules
                    .lines()
                    .filter(|line| line.trim_start().starts_with("iifname") && line.ends_with(" accept"))
                    .count(),
                1
            );
            assert!(rules.contains("ct direction reply ct state established,related accept"));
            assert!(rules.contains("reject comment \"guest to host\""));
        }
    }

    fn refusals(ruleset: &str) -> Vec<String> {
        ruleset
            .lines()
            .map(str::trim)
            .filter(|line| line.contains(" reject"))
            .map(str::to_string)
            .collect()
    }

    fn v6_half(ruleset: &str) -> String {
        ruleset
            .split(&format!("table ip6 {NFTABLES_TABLE} {{"))
            .nth(1)
            .unwrap_or("")
            .to_string()
    }

    #[test]
    fn the_isolation_rules_are_never_optional() {
        let empty = render_ruleset(&state(vec![], &[], &[]));
        assert!(refusals(&empty)
            .iter()
            .any(|l| l.contains(INSTANCE_METADATA_ADDRESS_V4)));
        assert!(refusals(&empty).iter().any(|l| l.contains("guest to host")));
        let joined = refusals(&empty).join("\n");
        for cidr in ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16"] {
            assert!(joined.contains(cidr));
        }
        let with_instance = render_ruleset(&state(vec![instance()], &[], &[]));
        assert!(refusals(&with_instance)
            .iter()
            .any(|l| l.contains("guest to guest")));
        assert!(refusals(&with_instance)
            .iter()
            .any(|l| l.contains("10.201.0.0/16")));
        let denied = render_ruleset(&state(vec![], &["203.0.113.10/32"], &[]));
        assert!(refusals(&denied).iter().any(|l| l.contains("203.0.113.10/32")));
    }

    #[test]
    fn a_denied_address_is_refused_before_the_guest_is_allowed_out() {
        let vpc = "10.43.0.0/16";
        let ruleset = render_ruleset(&state(vec![], &[vpc], &[]));
        let lines: Vec<&str> = ruleset.lines().collect();
        let denied = lines
            .iter()
            .position(|l| l.contains(vpc) && l.contains("reject"))
            .unwrap();
        let allowed_out = lines.iter().position(|l| l.contains("masquerade")).unwrap();
        assert!(allowed_out > denied);
    }

    #[test]
    fn nothing_is_denied_silently_in_either_family() {
        let ruleset = render_ruleset(&state(
            vec![instance()],
            &["10.43.0.0/16"],
            &["2600:1f18:abcd::/56"],
        ));
        assert!(!ruleset.contains("drop"));
    }

    #[test]
    fn the_same_isolation_holds_over_ipv6() {
        let ruleset = render_ruleset(&state(vec![], &[], &[]));
        let v6 = v6_half(&ruleset);
        assert!(v6.contains("guest to host"));
        assert!(v6.contains("fe80::/10"));
        assert!(v6.contains("guest to guest"));
        assert!(v6.contains(INSTANCE_METADATA_ADDRESS_V6));
        assert!(v6.contains("::1/128"));
        assert!(v6.contains("fc00::/7"));
        assert!(!v6.contains("::/0"));
        assert!(ruleset.contains(&format!(
            "table ip6 {NFTABLES_TABLE}\ndelete table ip6 {NFTABLES_TABLE}\n"
        )));
    }

    #[test]
    fn the_vpc_v6_range_is_denied_by_name_before_anything_lets_a_guest_out() {
        let vpc = "2600:1f18:abcd::/56";
        let v6 = v6_half(&render_ruleset(&state(vec![], &[], &[vpc])));
        assert!(v6.contains(&format!("ip6 daddr {vpc} reject")));
        let lines: Vec<&str> = v6.lines().map(str::trim).collect();
        let denied = lines.iter().position(|l| l.contains(vpc)).unwrap();
        let blanket = lines
            .iter()
            .position(|l| l.contains("private destinations"))
            .unwrap();
        assert!(blanket > denied);
        assert!(!["::1", "fe80:", "fc", "fd"]
            .iter()
            .any(|range| vpc.starts_with(range)));
    }

    fn forward_chain(half: &str) -> Vec<&str> {
        half.lines()
            .map(str::trim)
            .skip_while(|l| !l.starts_with("chain forward {"))
            .skip(1)
            .take_while(|l| *l != "}")
            .collect()
    }

    // A guest that fetched an address two seconds before it was denied got a 200 through the deny
    // on its keep-alive connection: conntrack carried the flow past a rule that only saw new ones.
    #[test]
    fn a_flow_a_guest_opened_before_its_address_was_denied_is_cut_on_the_next_packet() {
        let v4 = "172.66.147.243/32";
        let v6 = "2606:4700::6810:1a9a/128";
        let ruleset = render_ruleset(&state(vec![instance()], &[v4], &[v6]));
        for (chain, daddr, cidr) in [
            (forward_chain(&ruleset), "ip daddr", v4),
            (forward_chain(&v6_half(&ruleset)), "ip6 daddr", v6),
        ] {
            let established = chain
                .iter()
                .position(|l| l.starts_with("ct state established"))
                .unwrap();
            let denies: Vec<usize> = chain
                .iter()
                .enumerate()
                .filter(|(_, l)| l.contains("denied egress"))
                .map(|(at, _)| at)
                .collect();
            assert!(
                denies.iter().all(|at| *at < established),
                "a deny behind the established accept only ever sees a new flow: {chain:#?}"
            );
            assert_eq!(denies.len(), 2);
            assert_eq!(
                chain[denies[0]],
                format!("iifname \"nbr*\" {daddr} {cidr} meta l4proto tcp reject with tcp reset comment \"denied egress\""),
                "a reset is what ends a connected tcp socket at once; an ICMP unreachable is only a soft error on one"
            );
            assert_eq!(
                chain[denies[1]],
                format!("iifname \"nbr*\" {daddr} {cidr} reject comment \"denied egress\""),
                "whatever is not tcp is still refused"
            );
        }
    }

    #[test]
    fn with_nothing_denied_by_name_the_established_accept_is_still_the_first_rule() {
        let ruleset = render_ruleset(&state(vec![instance()], &[], &[]));
        for chain in [forward_chain(&ruleset), forward_chain(&v6_half(&ruleset))] {
            assert!(chain[0].starts_with("type filter hook forward"));
            assert_eq!(chain[1], "ct state established,related accept");
            assert!(!ruleset.contains("denied egress"));
        }
    }

    #[test]
    fn forwarding_reaches_the_http_port_and_only_when_something_runs() {
        let ruleset = render_ruleset(&state(vec![instance()], &[], &[]));
        assert!(ruleset
            .lines()
            .any(|l| l.contains("tcp dport 21000") && l.contains("dnat to 10.201.0.2:3000")));
        assert!(!render_ruleset(&state(vec![], &[], &[])).contains("dnat to"));
        assert!(ruleset.contains("ip saddr 10.201.0.0/16 oifname != \"nbr*\" masquerade"));
        let output = ruleset.lines().find(|l| l.contains("hook output")).unwrap();
        assert!(output.contains("priority -100"));
        assert!(!ruleset.contains("hook output priority dstnat"));
        assert!(ruleset.contains("ip saddr 127.0.0.0/8 ip daddr 10.201.0.2 snat to 10.201.0.1"));
    }

    #[test]
    fn nothing_but_the_forwarded_ports_are_dnatted_to_a_guest() {
        let ruleset = render_ruleset(&state(vec![instance()], &[], &[]));
        assert_eq!(ruleset.lines().filter(|l| l.contains("dnat to")).count(), 2);
        assert!(
            !ruleset.contains("udp"),
            "the HTTP port is the proxy's, and the proxy speaks tcp"
        );
    }

    #[test]
    fn a_raw_port_is_carried_whatever_arrives_on_it_and_the_http_port_is_not() {
        let ruleset = render_ruleset(&state(vec![with_ssh()], &[], &[]));
        // Two chains dnat, prerouting for the world and output for the proxy's own loopback.
        assert_eq!(ruleset.lines().filter(|l| l.contains("dnat to")).count(), 4);
        for chain in ["iifname != \"nbr*\"", "ip daddr 127.0.0.1"] {
            assert!(
                ruleset.lines().map(str::trim).any(|l| l.starts_with(chain)
                    && l.contains("meta l4proto { tcp, udp } th dport 21001")
                    && l.ends_with("dnat to 10.201.0.2:22")),
                "{chain} does not carry both transports to ssh:\n{ruleset}"
            );
            assert!(
                ruleset.lines().map(str::trim).any(|l| l.starts_with(chain)
                    && l.contains("tcp dport 21000")
                    && !l.contains("udp")
                    && l.ends_with("dnat to 10.201.0.2:3000")),
                "{chain} carries the HTTP port on something other than tcp alone:\n{ruleset}"
            );
        }
        assert!(
            !ruleset.contains("21002"),
            "a slot reserves eight ports and forwards only what was named"
        );
    }

    #[test]
    fn an_app_is_counted_on_every_port_rather_than_only_its_http_one() {
        let ruleset = render_ruleset(&state(vec![with_ssh()], &[], &[]));
        let loopback: Vec<&str> = ruleset
            .lines()
            .map(str::trim)
            .filter(|l| l.contains("ip saddr 127.0.0.0/8") && l.contains("counter name"))
            .collect();
        assert_eq!(loopback.len(), 1, "one counter for the app, not one per port");
        assert!(
            !loopback[0].contains("dport"),
            "a counter that named a port would read an ssh session as an app gone quiet: {}",
            loopback[0]
        );
    }

    #[test]
    fn the_ruleset_is_a_function_of_state_not_a_history_of_edits() {
        let input = state(vec![instance()], &["203.0.113.0/24"], &[]);
        let ruleset = render_ruleset(&input);
        assert!(ruleset.starts_with(&format!(
            "table ip {NFTABLES_TABLE}\ndelete table ip {NFTABLES_TABLE}\n"
        )));
        assert_eq!(ruleset, render_ruleset(&input));
    }

    #[test]
    fn an_app_is_counted_wherever_it_is_reached_and_nowhere_it_is_forwarded() {
        let ruleset = render_ruleset(&state(vec![instance()], &[], &[]));
        let received = app_received_counter_name(&instance().app_id);
        for name in [&received, &app_sent_counter_name(&instance().app_id)] {
            assert!(ruleset.contains(&format!("counter {name} {{")));
            assert!(!ruleset.contains(&format!("counter \"{name}\"")));
        }
        for rule in ruleset.lines().filter(|l| l.contains("dnat to")) {
            assert!(!rule.contains("counter"));
        }
        let loopback: Vec<&str> = ruleset
            .lines()
            .map(str::trim)
            .filter(|l| l.contains("ip saddr 127.0.0.0/8") && l.contains("counter name"))
            .collect();
        assert_eq!(loopback.len(), 1);
        assert!(loopback[0].contains("ip daddr 10.201.0.2"));
        assert!(loopback[0].contains(&received));
        let forwarded: Vec<&str> = ruleset
            .lines()
            .map(str::trim)
            .filter(|l| l.contains("oifname \"nbr*\" ip daddr") && l.contains("counter name"))
            .collect();
        assert_eq!(forwarded.len(), 1);
        assert!(ruleset.contains("type filter hook forward priority filter + 10;"));
        let empty = render_ruleset(&state(vec![], &[], &[]));
        assert!(!empty.contains("counter "));
        assert!(!empty.contains("traffic_"));
    }

    #[test]
    fn what_a_guest_sends_is_counted_both_where_it_is_forwarded_and_where_it_answers_this_host() {
        let ruleset = render_ruleset(&state(vec![instance()], &[], &[]));
        let sent = app_sent_counter_name(&instance().app_id);
        let by_source: Vec<&str> = ruleset
            .lines()
            .map(str::trim)
            .filter(|l| l.contains("ip saddr 10.201.0.2") && l.contains(&sent))
            .collect();
        assert_eq!(
            by_source.len(),
            2,
            "one for what it forwarded, one for what it answered this host with: {by_source:?}"
        );
        assert!(by_source
            .iter()
            .any(|rule| rule.starts_with("iifname \"nbr*\" oifname != \"nbr*\"")));
        assert!(by_source
            .iter()
            .any(|rule| rule.starts_with("iifname \"nbr*\" ip saddr")));
        assert!(ruleset.contains("type filter hook input priority filter + 10;"));

        // What the guest is sent and what it sends are never added into one figure.
        let counted: Vec<&str> = ruleset.lines().filter(|l| l.contains("counter name")).collect();
        assert_eq!(counted.len(), 4);
        let received = app_received_counter_name(&instance().app_id);
        assert_eq!(
            counted.iter().filter(|l| l.contains(&received)).count(),
            2,
            "the proxy dialling it, and what is forwarded to it"
        );
        assert_eq!(counted.iter().filter(|l| l.contains(&sent)).count(), 2);
    }

    #[test]
    fn a_counter_names_which_way_it_faces_so_a_flat_listing_can_be_read_back() {
        let app = instance().app_id;
        assert!(app_received_counter_name(&app).starts_with(APP_RECEIVED_COUNTER_PREFIX));
        assert!(app_sent_counter_name(&app).starts_with(APP_SENT_COUNTER_PREFIX));
        assert_ne!(app_received_counter_name(&app), app_sent_counter_name(&app));
    }
}
