use std::io::{BufRead, BufReader, Write};
use std::os::fd::{AsRawFd, OwnedFd};
use std::time::{Duration, Instant};

use guest_contract::paths;

use crate::guest::{log, vsock};

const FREEZE_REQUEST: &str = "FREEZE";
const FREEZE_HELD: &str = "OK";
const TENANT_FREEZE: &str = "/sys/fs/cgroup/tenant/cgroup.freeze";
const TENANT_EVENTS: &str = "/sys/fs/cgroup/tenant/cgroup.events";

const MAX_HOLD: Duration = Duration::from_secs(900);

const POLL_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) fn serve() -> ! {
    let Ok(listener) = vsock::listener(guest_contract::vsock::GUEST_CONTROL_VSOCK_PORT) else {
        log("the control port could not be opened; no export can freeze this tenant");
        loop {
            std::thread::sleep(Duration::from_secs(3600));
        }
    };
    loop {
        match vsock::accept_one(&listener) {
            Ok(connection) => answer(connection),
            Err(_) => std::thread::sleep(POLL_INTERVAL),
        }
    }
}

fn answer(connection: OwnedFd) {
    let mut wire = BufReader::new(std::fs::File::from(connection));
    let mut request = String::new();
    if wire.read_line(&mut request).is_err() {
        return;
    }
    if request.trim() == guest_contract::control::TENANT_FREEZE_REQUEST {
        let result = freeze_tenant();
        if wire
            .get_mut()
            .write_all(if result.is_ok() { b"OK\n" } else { b"REFUSED\n" })
            .is_err()
            && result.is_ok()
        {
            let _ = std::fs::write(TENANT_FREEZE, "0");
        }
        return;
    }
    if let Some(nanos) = request
        .trim()
        .strip_prefix(guest_contract::control::TENANT_CLOCK_REQUEST)
    {
        let result = synchronize_clock(nanos, &mut wire);
        if let Err(error) = result {
            log(&format!("the tenant's clock could not be synchronized: {error}"));
            let _ = wire.get_mut().write_all(b"REFUSED\n");
        }
        return;
    }
    if request.trim() != FREEZE_REQUEST {
        return;
    }
    if let Err(error) = freeze(paths::VOLUME_MOUNT) {
        log(&format!("the filesystem would not freeze: {error}"));
        let _ = wire.get_mut().write_all(b"REFUSED\n");
        return;
    }
    if wire
        .get_mut()
        .write_all(format!("{FREEZE_HELD}\n").as_bytes())
        .is_err()
    {
        thaw_quietly();
        return;
    }
    let deadline = Instant::now() + MAX_HOLD;
    let mut byte = [0u8; 1];
    loop {
        match read_would_block(&mut wire, &mut byte) {
            Held::Gone => break,
            Held::Still if Instant::now() >= deadline => {
                log("a freeze was held past its ceiling and has been taken back");
                break;
            }
            Held::Still => std::thread::sleep(POLL_INTERVAL),
        }
    }
    thaw_quietly();
}

fn freeze_tenant() -> std::io::Result<()> {
    std::fs::write(TENANT_FREEZE, "1")?;
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let events = match std::fs::read_to_string(TENANT_EVENTS) {
            Ok(events) => events,
            Err(error) => {
                let _ = std::fs::write(TENANT_FREEZE, "0");
                return Err(error);
            }
        };
        if events.lines().any(|line| line == "frozen 1") {
            return Ok(());
        }
        if Instant::now() >= deadline {
            std::fs::write(TENANT_FREEZE, "0")?;
            return Err(std::io::Error::other("the tenant cgroup did not freeze"));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn synchronize_clock(nanos: &str, wire: &mut BufReader<std::fs::File>) -> std::io::Result<()> {
    let nanos = nanos
        .parse()
        .map_err(|_| std::io::Error::other("invalid host time"))?;
    set_clock(nanos)?;
    wire.get_mut()
        .write_all(format!("{}\n", guest_contract::control::TENANT_CLOCK_READY).as_bytes())?;
    let mut release = String::new();
    wire.read_line(&mut release)?;
    if release.trim() != guest_contract::control::TENANT_CLOCK_RELEASE {
        return Err(std::io::Error::other("the host did not approve the guest clock"));
    }
    std::fs::write(TENANT_FREEZE, "0")?;
    wire.get_mut()
        .write_all(format!("{}\n", guest_contract::control::TENANT_CLOCK_RELEASED).as_bytes())?;
    Ok(())
}

#[allow(unsafe_code)]
fn set_clock(nanos: u128) -> std::io::Result<()> {
    let seconds = i64::try_from(nanos / 1_000_000_000)
        .map_err(|_| std::io::Error::other("host time is out of range"))?;
    let time = libc::timespec {
        tv_sec: seconds,
        tv_nsec: (nanos % 1_000_000_000) as libc::c_long,
    };
    if unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &raw const time) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

enum Held {
    Still,
    Gone,
}

fn read_would_block(wire: &mut BufReader<std::fs::File>, byte: &mut [u8; 1]) -> Held {
    use std::os::fd::AsRawFd;
    let mut poll = libc::pollfd {
        fd: wire.get_ref().as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let ready = unsafe { libc::poll(&raw mut poll, 1, 0) };
    if ready <= 0 {
        return Held::Still;
    }
    match std::io::Read::read(wire.get_mut(), byte) {
        Ok(0) | Err(_) => Held::Gone,
        Ok(_) => Held::Gone,
    }
}

pub(crate) fn freeze(mount_point: &str) -> std::io::Result<()> {
    ioctl(mount_point, FIFREEZE)
}

pub(crate) fn thaw_quietly() {
    let _ = ioctl(paths::VOLUME_MOUNT, FITHAW);
}

const FIFREEZE: libc::c_ulong = 0xC0045877;
const FITHAW: libc::c_ulong = 0xC0045878;

#[allow(unsafe_code)]
fn ioctl(mount_point: &str, request: libc::c_ulong) -> std::io::Result<()> {
    let directory = std::fs::File::open(mount_point)?;
    let mut level: libc::c_int = 0;
    let answered = unsafe { libc::ioctl(directory.as_raw_fd(), request as libc::Ioctl, &raw mut level) };
    if answered < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
