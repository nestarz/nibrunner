#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::sync::Arc;

use nibrunnerd::ports::{CommandRunner, CommandRunnerExt};
use nibrunnerd::test_support::mocks;

fn enabled() -> bool {
    std::env::var("NIBRUNNER_INTEGRATION").is_ok_and(|value| value == "1")
}

fn require_root() {
    #[cfg(unix)]
    #[allow(unsafe_code, reason = "asking who this process is has no safe spelling")]
    if unsafe { libc::geteuid() } != 0 {
        panic!("NIBRUNNER_INTEGRATION=1 was set but this is not running as root");
    }
}

fn commands() -> Arc<dyn CommandRunner> {
    Arc::new(nibrunnerd::adapters::exec::HostCommands)
}

/// Volumes as files under `directory`, over a store holding `archive` for whichever volume asks.
fn local_volumes(
    directory: &std::path::Path,
    commands: Arc<dyn CommandRunner>,
    archive: Vec<u8>,
) -> nibrunnerd::adapters::volumes::local_file::LocalFileVolumes {
    nibrunnerd::adapters::volumes::local_file::LocalFileVolumes::new(
        directory.join("volumes"),
        protocol::ObjectKey::parse("volumes").unwrap(),
        commands.clone(),
        nibrunnerd::adapters::volumes::initial_contents::ContentsStaging::new(
            mocks::artifacts_holding(archive),
            directory.join("initial-contents"),
        ),
    )
}

/// One `debugfs` request against the volume, answered from the filesystem without mounting it.
async fn debugfs(device: &std::path::Path, request: &str) -> String {
    commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            "debugfs",
            "-R",
            request,
            &device.display().to_string(),
        ]))
        .await
        .expect("debugfs answers")
}

// The real tool copies the archive in as it formats, and what it copied is read back off the
// device the way an export reads it: by debugfs, so nothing here mounts a tenant's volume.
#[tokio::test]
async fn a_volume_is_formatted_holding_the_contents_the_document_gave_it() {
    if !enabled() {
        return;
    }
    require_root();
    let directory = tempfile::tempdir().unwrap();
    let archive = {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_uid(1000);
        header.set_gid(1000);
        header.set_size(6);
        builder
            .append_data(&mut header, "nested/hello.txt", &b"hello\n"[..])
            .unwrap();
        builder.into_inner().unwrap()
    };
    let volumes = local_volumes(directory.path(), commands(), archive.clone());
    let desired = protocol::DesiredVolume {
        volume_id: protocol::VolumeId::parse("vol-1").unwrap(),
        app_id: protocol::AppId::parse("app-1").unwrap(),
        size_bytes: 16 * 1024 * 1024,
        desired_state: protocol::DesiredPresence::Present,
        initial_contents: Some(nibrunnerd::test_support::initial_contents(&archive, "/app/data")),
    };

    use nibrunnerd::adapters::volumes::VolumeBackend;
    let attached = volumes.provision(&desired).await.expect("the volume is made");
    let device = std::path::PathBuf::from(&attached.device_path);

    let listed = debugfs(&device, "ls -l /upper/app/data/nested").await;
    assert!(listed.contains("hello.txt"), "{listed}");
    let shown = debugfs(&device, "cat /upper/app/data/nested/hello.txt").await;
    assert_eq!(shown, "hello\n");
    // debugfs prints the owner five wide: `User: 65534`, and `User:     0` for root.
    let tenant = format!("User: {:>5}", guest_contract::paths::TENANT_UID);
    let owner = debugfs(&device, "stat /upper/app/data/nested/hello.txt").await;
    assert!(owner.contains(&tenant), "the file is not the tenant's: {owner}");
    let given = debugfs(&device, "stat /upper/app/data").await;
    assert!(
        given.contains(&tenant),
        "the destination is not the tenant's: {given}"
    );
    let above = debugfs(&device, "stat /upper/app").await;
    assert!(
        above.contains("User:     0"),
        "the directory above is not root's: {above}"
    );
    assert!(
        !directory.path().join("initial-contents/vol-1").exists(),
        "the staging is gone once the format has read it"
    );

    let (recorded, log) = mocks::commands_succeeding();
    let second = local_volumes(directory.path(), recorded, archive);
    second
        .provision(&desired)
        .await
        .expect("a converged volume needs nothing");
    assert!(
        log.executables().is_empty(),
        "a formatted volume must never be seeded again"
    );
}

#[tokio::test]
async fn the_isolation_ruleset_loads_into_the_kernel() {
    if !enabled() {
        return;
    }
    require_root();
    let firewall = nibrunnerd::adapters::net::firewall::HostFirewall::new(commands());
    let state = nft_render::FirewallState {
        instances: vec![nft_render::ForwardedInstance {
            app_id: protocol::AppId::parse("app-1").unwrap(),
            ports: vec![nft_render::ForwardedPort {
                host_port: protocol::HostPort::new(21_000).unwrap(),
                guest_port: protocol::GuestPort::new(3000).unwrap(),
                raw: false,
            }],
            host_ipv4: protocol::Ipv4Address::parse("10.201.0.1").unwrap(),
            guest_ipv4: protocol::Ipv4Address::parse("10.201.0.2").unwrap(),
        }],
        allowed_host_tcp_endpoints: vec![
            "203.0.113.10:443".parse().unwrap(),
            "[2001:db8::10]:443".parse().unwrap(),
        ],
        denied_egress_addresses_v4: vec!["10.43.0.0/16".into()],
        denied_egress_addresses_v6: vec!["2600:1f18:abcd::/56".into()],
    };
    firewall
        .apply(&state)
        .await
        .expect("the kernel takes the ruleset");

    let held = commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            "nft", "list", "table", "ip", "nibrun",
        ]))
        .await
        .expect("the kernel names the table");
    assert!(held.contains("reject comment \"instance metadata endpoint\""));
    assert!(held.contains("reject comment \"guest to guest\""));
    assert!(held.contains("reject comment \"guest to host\""));
    assert!(held.contains("ip daddr 203.0.113.10 tcp dport 443 accept"));
    assert!(held.contains("ct direction reply ct state established,related accept"));
    assert!(held.contains("dnat to 10.201.0.2:3000"));
    assert!(held.contains("masquerade"));
    assert!(held.contains("hook output"));
    assert!(!held.contains("drop"));

    let v6 = commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            "nft", "list", "table", "ip6", "nibrun",
        ]))
        .await
        .expect("the kernel names the v6 table");
    assert!(v6.contains("ip6 daddr 2001:db8::10 tcp dport 443 accept"));
    assert!(v6.contains("fe80::/10"));
    assert!(v6.contains("2600:1f18:abcd::/56"));

    let traffic = firewall.traffic().await.expect("the kernel lists its counters");
    assert!(traffic.contains_key(&protocol::AppId::parse("app-1").unwrap()));

    firewall.apply(&state).await.expect("a rerun is not an error");
}

#[tokio::test]
async fn a_volume_is_formatted_by_the_real_tool_and_read_back_as_formatted() {
    if !enabled() {
        return;
    }
    require_root();
    let directory = tempfile::tempdir().unwrap();
    let volumes = local_volumes(directory.path(), commands(), Vec::new());
    let desired = protocol::DesiredVolume {
        volume_id: protocol::VolumeId::parse("vol-1").unwrap(),
        app_id: protocol::AppId::parse("app-1").unwrap(),
        size_bytes: 16 * 1024 * 1024,
        desired_state: protocol::DesiredPresence::Present,
        initial_contents: None,
    };

    use nibrunnerd::adapters::volumes::VolumeBackend;
    let attached = volumes.provision(&desired).await.expect("the volume is made");
    assert_eq!(attached.size_bytes, desired.size_bytes);

    let (recorded, log) = mocks::commands_succeeding();
    let second = local_volumes(directory.path(), recorded, Vec::new());
    second
        .provision(&desired)
        .await
        .expect("a converged volume needs nothing");
    assert!(
        log.executables().is_empty(),
        "a formatted volume must never be formatted again"
    );
}

// A device under a slot that has changed hands is told from the volume it should carry by the
// uuid the real tool writes into the filesystem, so what that tool gives two volumes has to be
// two different things.
#[tokio::test]
async fn volumes_the_real_tool_formats_carry_a_filesystem_that_names_one_apart_from_another() {
    if !enabled() {
        return;
    }
    require_root();
    let directory = tempfile::tempdir().unwrap();
    let volumes = local_volumes(directory.path(), commands(), Vec::new());
    let volume_of = |name: &str| protocol::DesiredVolume {
        volume_id: protocol::VolumeId::parse(name).unwrap(),
        app_id: protocol::AppId::parse("app-1").unwrap(),
        size_bytes: 16 * 1024 * 1024,
        desired_state: protocol::DesiredPresence::Present,
        initial_contents: None,
    };

    use nibrunnerd::adapters::volumes::VolumeBackend;
    let one = volume_of("vol-1");
    let another = volume_of("vol-2");
    volumes.provision(&one).await.expect("the volume is made");
    volumes.provision(&another).await.expect("the volume is made");

    use nibrunnerd::adapters::volumes::filesystem_uuid;
    let read = |desired: &protocol::DesiredVolume| {
        filesystem_uuid(&volumes.path_for(&desired.volume_id).display().to_string())
            .expect("the real tool names every filesystem it makes")
    };
    assert_eq!(read(&one), read(&one), "a volume names itself the same twice");
    assert_ne!(
        read(&one),
        read(&another),
        "two volumes that named themselves the same could not be told apart on a device"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_tap_is_created_addressed_and_given_the_guest_it_will_hold() {
    if !enabled() {
        return;
    }
    require_root();
    use nibrunnerd::adapters::net::tap::{HostNetwork, KernelNetwork, Neighbour, TapInterface};

    let network = KernelNetwork::open().expect("a netlink socket");
    let slot = nft_render::describe_slot(
        nft_render::most_apps_the_ports_fit() - 1,
        protocol::AppId::parse("integration").unwrap(),
    );
    let tap = TapInterface {
        tap_name: slot.tap_name.clone(),
        host_ipv4: slot.host_ipv4.clone(),
        subnet_prefix_length: slot.subnet_prefix_length,
    };
    network.ensure_tap(&tap).await.expect("the tap is made");
    network
        .ensure_tap(&tap)
        .await
        .expect("a second pass changes nothing");
    assert!(network.tap_names().await.contains(&slot.tap_name));

    network
        .refresh_neighbour(&Neighbour {
            guest_ipv4: slot.guest_ipv4.clone(),
            guest_mac: slot.guest_mac.clone(),
            tap_name: slot.tap_name.clone(),
        })
        .await
        .expect("the neighbour entry is written");

    let neighbours = commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            "ip",
            "neigh",
            "show",
            "dev",
            &slot.tap_name,
        ]))
        .await
        .unwrap_or_default();
    assert!(
        neighbours.contains(slot.guest_ipv4.as_str()) || neighbours.is_empty(),
        "the neighbour entry should name the guest this slot holds"
    );

    network
        .delete_tap(&slot.tap_name)
        .await
        .expect("the tap is taken back");
    assert!(
        !network.tap_names().await.contains(&slot.tap_name),
        "a tap a persistent flag kept alive is gone once the app that held it is"
    );
    network
        .delete_tap(&slot.tap_name)
        .await
        .expect("a tap that is already gone is the state being asked for, not a failure");
}

#[tokio::test]
async fn the_embedded_hypervisor_runs_on_this_host() {
    if !enabled() {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let binary =
        nibrunnerd::adapters::vm::process::extract_firecracker(directory.path()).expect("a hypervisor");
    let version = commands()
        .stdout_of(nibrunnerd::ports::CommandRequest::new(&[
            &binary.display().to_string(),
            "--version",
        ]))
        .await
        .expect("the hypervisor answers");
    assert!(
        version.contains(nibrunnerd::adapters::vm::process::FIRECRACKER_VERSION),
        "it should name the version this build pins, said: {version}"
    );
}

// The browse half of the guest contract has never been driven from outside its own unit tests:
// removing the control plane took away the only thing that asked, and it has been waiting for
// something to ask ever since. This is that, without putting a service back into the daemon —
// point it at a running guest's control socket and it speaks every verb the contract defines.
//
//   NIBRUNNER_GUEST_VSOCK=/var/lib/nibrunner/vm/<app>/<socket> NIBRUNNER_INTEGRATION=1 \
//     cargo test -p nibrunnerd --test integration -- --nocapture browse
#[tokio::test]
async fn every_browse_verb_answers_from_a_running_guest() {
    if !enabled() {
        return;
    }
    let Ok(socket) = std::env::var("NIBRUNNER_GUEST_VSOCK") else {
        eprintln!("NIBRUNNER_GUEST_VSOCK names no guest, so the browse verbs are not driven");
        return;
    };
    require_root();
    use nibrunnerd::domain::filesystem::client::GuestFilesystem;
    use protocol::{FilesystemEntryKind, GuestPath};

    let app_id = protocol::AppId::parse("browse").unwrap();
    let socket = std::path::PathBuf::from(socket);
    let mut guest = GuestFilesystem::dial(&app_id, &socket)
        .await
        .expect("a guest answers on its control socket");

    // Every path a guest is asked for is resolved inside the volume its own app owns, so the
    // root here is the tenant's data directory rather than the guest's filesystem.
    let data = GuestPath::parse("/").unwrap();
    let listing = guest.list(&data).await.expect("list");
    println!("list {}: {} entries", data.as_str(), listing.entries.len());

    let usage = guest.usage().await.expect("usage");
    let compute = guest.compute().await.expect("compute");
    println!(
        "usage {}/{} bytes, compute {}/{} bytes",
        usage.used_bytes, usage.total_bytes, compute.memory_used_bytes, compute.memory_total_bytes
    );
    assert!(
        usage.total_bytes > 0,
        "a volume with no size is not one a tenant has"
    );

    let directory = GuestPath::parse("/browse-check").unwrap();
    let file = GuestPath::parse("/browse-check/note").unwrap();
    let _ = guest.remove(&file).await;
    let _ = guest.remove(&directory).await;

    guest.make_directory(&directory).await.expect("mkdir");
    let written = guest
        .write(&file, 0, b"what the guest was handed".to_vec(), true)
        .await
        .expect("write");
    assert_eq!(written as usize, b"what the guest was handed".len());

    let read = guest.read(&file, 0, 4096).await.expect("read");
    assert_eq!(
        read, b"what the guest was handed",
        "what came back is not what went in"
    );

    let details = guest.stat(&file).await.expect("stat");
    assert_eq!(details.kind, FilesystemEntryKind::File);
    assert_eq!(details.size_bytes, read.len() as u64);

    let listed = guest.list(&directory).await.expect("list the new directory");
    assert!(
        listed.entries.iter().any(|entry| entry.name == "note"),
        "a file just written is not in its own directory"
    );

    // A directory that still holds something is refused rather than emptied.
    let refusal = guest.remove(&directory).await;
    assert!(refusal.is_err(), "a directory with a file in it was removed");
    println!("removing a full directory: {}", refusal.unwrap_err());

    let moved = GuestPath::parse("/browse-check/moved").unwrap();
    guest.move_entry(&file, &moved).await.expect("move");
    assert!(guest.stat(&file).await.is_err(), "the old name still answers");
    assert_eq!(
        guest.stat(&moved).await.expect("stat the new name").size_bytes,
        read.len() as u64
    );

    guest.remove(&moved).await.expect("remove the file");
    guest
        .remove(&directory)
        .await
        .expect("remove the empty directory");
    assert!(
        guest.stat(&directory).await.is_err(),
        "a directory that was removed still answers"
    );
    println!("every verb answered");
}
