mod channels;
mod control;
mod filesystem;
mod logs;
mod memory;
mod mounts;
mod supervisor;
mod transport;

use std::process::ExitCode;

use crate::boot::Boot;

use guest_contract::instance_env::{parse_instance_env, InstanceConfig, CONFIG_MAX_BYTES};
use guest_contract::paths;

const TENANT_UMASK: libc::mode_t = 0o022;

pub(crate) fn run(boot_mode: Boot) -> ExitCode {
    if boot_mode == Boot::VirtualMachine {
        if mounts::dev().is_err() {
            return shutdown(boot_mode, None, ExitCode::FAILURE);
        }
        adopt_console();
    }
    log("runtime starting");

    supervisor::block_signals();
    if boot_mode == Boot::VirtualMachine {
        route_ctrl_alt_del_here();
    }

    let (config, ceiling) = match boot(boot_mode) {
        Ok(booted) => booted,
        Err(reason) => {
            log(&reason);
            return shutdown(boot_mode, None, ExitCode::FAILURE);
        }
    };

    let transport = boot_mode.transport();
    let channels = channels::start(transport);

    log(&format!(
        "starting {} as uid {} in {}, with {} MiB to spend",
        config.program,
        paths::TENANT_UID,
        config.working_directory,
        crate::ceiling::mib(ceiling.limit_bytes)
    ));
    let result = match supervisor::supervise(&config, &ceiling, transport) {
        supervisor::Ended::ShutdownRequested => {
            log("the tenant has stopped; shutting the runtime down");
            ExitCode::SUCCESS
        }
        supervisor::Ended::RestartBudgetExhausted => {
            log(&format!(
                "the tenant used its {} restarts without staying up; shutting the runtime down",
                config.max_restarts
            ));
            ExitCode::FAILURE
        }
        supervisor::Ended::SpawnFailed => {
            log("the tenant could not be started at all; shutting the runtime down");
            ExitCode::FAILURE
        }
    };
    shutdown(boot_mode, Some(&channels), result)
}

fn boot(boot_mode: Boot) -> Result<(InstanceConfig, memory::Ceiling), String> {
    unsafe { libc::umask(TENANT_UMASK) };
    match boot_mode {
        Boot::VirtualMachine => {
            mounts::pseudo_filesystems().map_err(|error| error.to_string())?;
            memory::mount().map_err(|error| error.to_string())?;
            let ceiling = memory::prepare(guest_memory_bytes())?;
            mounts::config(paths::CONFIG_DEVICE, paths::CONFIG_MOUNT).map_err(|error| error.to_string())?;
            let config = read_instance_config()?;
            nix::mount::umount(paths::CONFIG_MOUNT)
                .map_err(|error| format!("{} could not be unmounted: {error}", paths::CONFIG_MOUNT))?;
            stack_root(&config)?;
            write_resolv_conf(&config)?;
            Ok((config, ceiling))
        }
        Boot::PreparedRoot => {
            let ceiling = memory::adopt()?;
            let config = read_instance_config()?;
            validate_prepared_mounts()?;
            Ok((config, ceiling))
        }
    }
}

fn validate_prepared_mounts() -> Result<(), String> {
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").map_err(|error| error.to_string())?;
    for required in [
        paths::ROOT_MOUNT,
        paths::VOLUME_MOUNT,
        guest_contract::channels::PROCESS_CHANNEL_DIRECTORY,
    ] {
        if !mounts
            .lines()
            .any(|line| line.split_whitespace().nth(4) == Some(required))
        {
            return Err(format!("the runtime did not prepare the mount at {required}"));
        }
    }
    Ok(())
}

fn guest_memory_bytes() -> u64 {
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    filesystem::meminfo_kb(&meminfo, "MemTotal:") * 1024
}

fn read_instance_config() -> Result<InstanceConfig, String> {
    let text = std::fs::read_to_string(paths::CONFIG_FILE)
        .map_err(|error| format!("{} could not be read: {error}", paths::CONFIG_FILE))?;
    if text.len() > CONFIG_MAX_BYTES {
        return Err(format!(
            "{} is larger than this runtime reads",
            paths::CONFIG_FILE
        ));
    }
    let config = parse_instance_env(&text).map_err(|error| error.to_string())?;
    log(&format!(
        "instance configured: port {}, {} environment variables, {} nameservers",
        config.http_port,
        config.environment.len(),
        config.nameservers.len()
    ));
    Ok(config)
}

fn write_resolv_conf(config: &InstanceConfig) -> Result<(), String> {
    if config.nameservers.is_empty() {
        log("instance.env names no DNS server, so the tenant will not resolve hostnames");
    }
    let rendered: String = config
        .nameservers
        .iter()
        .map(|address| format!("nameserver {address}\n"))
        .collect();
    // A layer's own /etc/resolv.conf is as likely a dangling symlink as a file, so what is there
    // is replaced rather than written through.
    let _ = std::fs::remove_file(paths::RESOLV_CONF);
    std::fs::write(paths::RESOLV_CONF, rendered)
        .map_err(|error| format!("{} could not be written: {error}", paths::RESOLV_CONF))
}

/// The root the program runs in: this image at the bottom, the document's layers over it in
/// order, the volume writable over all of them.
fn stack_root(config: &InstanceConfig) -> Result<(), String> {
    let failed = |error: mounts::MountFailed| error.to_string();
    mounts::base(paths::BASE_MOUNT).map_err(failed)?;
    let mut lowers = vec![paths::BASE_MOUNT.to_string()];
    for index in 0..config.layers {
        let target = paths::layer_mount(index);
        mounts::layer(&paths::layer_device(index), &target).map_err(failed)?;
        lowers.push(target);
    }
    mounts::volume(paths::VOLUME_DEVICE, paths::VOLUME_MOUNT).map_err(failed)?;
    mounts::overlay(
        &lowers,
        paths::VOLUME_UPPER_DIR,
        paths::VOLUME_WORK_DIR,
        paths::ROOT_MOUNT,
    )
    .map_err(failed)?;
    mounts::into_root(paths::ROOT_MOUNT).map_err(failed)?;
    mounts::working_directory(
        &in_root(&config.working_directory),
        paths::TENANT_UID,
        paths::TENANT_GID,
    )
    .map_err(failed)?;

    let program = in_root(&config.program);
    let details =
        std::fs::metadata(&program).map_err(|_| format!("no layer holds a program at {}", config.program))?;
    if !details.is_file() {
        return Err(format!("{} is not a file", config.program));
    }
    if std::os::unix::fs::PermissionsExt::mode(&details.permissions()) & 0o001 == 0 {
        return Err(format!(
            "{} is not executable by the uid it runs as",
            config.program
        ));
    }
    log(&format!(
        "root stacked: this image, {} layer(s), the volume on top",
        config.layers
    ));
    Ok(())
}

fn in_root(path: &str) -> String {
    format!("{}{path}", paths::ROOT_MOUNT)
}

fn shutdown(boot_mode: Boot, channels: Option<&channels::Channels>, result: ExitCode) -> ExitCode {
    if let Some(channels) = channels {
        channels.stop();
    }
    if boot_mode == Boot::PreparedRoot {
        return result;
    }
    // The stacked root holds the volume busy, so it goes first; both lazily, because a program
    // that has just been killed may still be letting go of what it had open.
    for mount in [paths::ROOT_MOUNT, paths::VOLUME_MOUNT] {
        if let Err(error) = nix::mount::umount2(mount, nix::mount::MntFlags::MNT_DETACH) {
            if !matches!(error, nix::errno::Errno::EINVAL | nix::errno::Errno::ENOENT) {
                log(&format!("could not unmount {mount}: {error}"));
            }
        }
    }
    nix::unistd::sync();
    unsafe { libc::reboot(libc::RB_AUTOBOOT) };
    log("the guest could not be shut down");
    loop {
        unsafe { libc::pause() };
    }
}

fn adopt_console() {
    use std::os::fd::AsRawFd;
    let Ok(console) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/console")
    else {
        return;
    };
    let raw = console.as_raw_fd();
    for target in 0..=2 {
        if raw != target {
            unsafe { libc::dup2(raw, target) };
        }
    }
}

fn route_ctrl_alt_del_here() {
    if unsafe { libc::reboot(libc::RB_DISABLE_CAD) } < 0 {
        log("could not take over ctrl-alt-del");
    }
}

pub(crate) fn log(message: &str) {
    use std::io::Write;
    let mut stderr = std::io::stderr();
    let _ = writeln!(stderr, "[nibrun] {message}");
    let _ = stderr.flush();
}
