use super::InstallError;

const ACCOUNT: &str = "nibrunner-jailer";
const RESERVATION_SIZE: u32 = 65_536;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Identities {
    pub(crate) uid_base: u32,
    pub(crate) gid_base: u32,
    /// How many slots the narrower of the two reservations covers. A desired document's `maxApps`
    /// can widen the slot ring past the `max_apps` this was checked against at startup, and a slot
    /// past the reservation would borrow an identity reserved for someone else.
    pub(crate) slots: u32,
}

struct Reservation {
    owner: String,
    start: u32,
    end: u32,
}

fn ask(arguments: &[&str]) -> Result<String, String> {
    let output = std::process::Command::new("getent")
        .args(arguments)
        .output()
        .map_err(|error| format!("getent could not read the jailer identities: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "the jailer {} could not be read; run nibrunnerd install",
            arguments[0]
        ));
    }
    String::from_utf8(output.stdout).map_err(|_| "the account database is not UTF-8".into())
}

fn reservations(text: &str) -> Result<Vec<Reservation>, String> {
    text.lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
        .map(|line| {
            let fields: Vec<_> = line.split(':').collect();
            let invalid = || "a subordinate identity reservation is malformed".to_string();
            if fields.len() != 3 || fields[0].is_empty() {
                return Err(invalid());
            }
            let start: u32 = fields[1].parse().map_err(|_| invalid())?;
            let count: u32 = fields[2].parse().map_err(|_| invalid())?;
            let end = start.checked_add(count).ok_or_else(invalid)?;
            if start == 0 || count == 0 {
                return Err(invalid());
            }
            Ok(Reservation {
                owner: fields[0].to_owned(),
                start,
                end,
            })
        })
        .collect()
}

#[cfg(test)]
fn reserved_base(text: &str, accounts: &str, uid: u32, max_apps: u32) -> Result<u32, String> {
    reserved_range(text, accounts, uid, max_apps).map(|(start, _)| start)
}

fn reserved_range(text: &str, accounts: &str, uid: u32, max_apps: u32) -> Result<(u32, u32), String> {
    let ranges = reservations(text)?;
    let numeric_owner = uid.to_string();
    let mut own = ranges
        .iter()
        .filter(|range| range.owner == ACCOUNT || range.owner == numeric_owner);
    let selected = own.next().ok_or_else(|| {
        format!("{ACCOUNT} has no subordinate identity reservation; run nibrunnerd install")
    })?;
    if own.next().is_some() {
        return Err(format!(
            "{ACCOUNT} must have exactly one subordinate identity reservation"
        ));
    }
    if max_apps == 0 || selected.end - selected.start < max_apps {
        return Err(format!(
            "the jailer identity reservation does not fit {max_apps} app slots"
        ));
    }
    for range in &ranges {
        if !std::ptr::eq(range, selected) && range.start < selected.end && selected.start < range.end {
            return Err(format!(
                "the jailer identity reservation overlaps {}",
                range.owner
            ));
        }
    }
    for line in accounts.lines().filter(|line| !line.is_empty()) {
        let fields: Vec<_> = line.split(':').collect();
        let id: u32 = fields
            .get(2)
            .and_then(|field| field.parse().ok())
            .ok_or_else(|| "an account database entry is malformed".to_string())?;
        if (selected.start..selected.end).contains(&id) {
            return Err(format!(
                "the jailer identity reservation contains the {} account",
                fields[0]
            ));
        }
    }
    Ok((selected.start, selected.end - selected.start))
}

fn account_uid(passwd: &str, shadow: &str) -> Result<u32, String> {
    let fields: Vec<_> = passwd.trim_end().split(':').collect();
    let password: Vec<_> = shadow.trim_end().split(':').collect();
    if fields.len() != 7
        || fields[0] != ACCOUNT
        || !matches!(fields[6], "/usr/sbin/nologin" | "/sbin/nologin")
        || password.len() != 9
        || password[0] != ACCOUNT
        || !password[1].starts_with(['!', '*'])
    {
        return Err(format!("{ACCOUNT} must be a locked account with a nologin shell"));
    }
    let uid: u32 = fields[2]
        .parse()
        .map_err(|_| format!("{ACCOUNT} has an invalid UID"))?;
    let gid: u32 = fields[3]
        .parse()
        .map_err(|_| format!("{ACCOUNT} has an invalid GID"))?;
    if uid == 0 || gid == 0 || uid == u32::MAX || gid == u32::MAX {
        return Err(format!("{ACCOUNT} must have a non-root UID and GID"));
    }
    Ok(uid)
}

pub(crate) fn read(max_apps: u32) -> Result<Identities, InstallError> {
    read_identities(max_apps).map_err(InstallError::Refused)
}

fn read_identities(max_apps: u32) -> Result<Identities, String> {
    let uid = account_uid(&ask(&["passwd", ACCOUNT])?, &ask(&["shadow", ACCOUNT])?)?;
    let base = |file: &str, database: &str| {
        let text = std::fs::read_to_string(file)
            .map_err(|error| format!("{file} could not be read: {error}; run nibrunnerd install"))?;
        reserved_range(&text, &ask(&[database])?, uid, max_apps).map_err(|error| format!("{file}: {error}"))
    };
    let (uid_base, uid_slots) = base("/etc/subuid", "passwd")?;
    let (gid_base, gid_slots) = base("/etc/subgid", "group")?;
    Ok(Identities {
        uid_base,
        gid_base,
        slots: uid_slots.min(gid_slots),
    })
}

pub(crate) fn ensure(max_apps: u32) -> Result<bool, InstallError> {
    reserve_identities(max_apps).map_err(InstallError::Refused)
}

fn reserve_identities(max_apps: u32) -> Result<bool, String> {
    if super::service_user::ids(ACCOUNT).is_some() {
        read_identities(max_apps)?;
        return Ok(false);
    }
    if max_apps == 0 || max_apps > RESERVATION_SIZE {
        return Err("the requested app slots do not fit the jailer identity reservation".into());
    }
    // useradd holds the subordinate-ID database locks and avoids other users' reservations.
    let output = std::process::Command::new("useradd")
        .args([
            "--system",
            "--user-group",
            "--no-create-home",
            "--shell",
            "/usr/sbin/nologin",
            "--home-dir",
            "/nonexistent",
            "--add-subids-for-system",
            "--key",
            &format!("SUB_UID_COUNT={RESERVATION_SIZE}"),
            "--key",
            &format!("SUB_GID_COUNT={RESERVATION_SIZE}"),
            ACCOUNT,
        ])
        .output()
        .map_err(|error| format!("the jailer identity reservation could not be created: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "the jailer identity reservation could not be created: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    read_identities(max_apps)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reserved_range_is_stable_and_fits_every_slot() {
        let text = "container:100000:65536\nnibrunner-jailer:165536:65536\n";
        let accounts = "root:x:0:0:root:/root:/bin/sh\n";
        assert_eq!(reserved_base(text, accounts, 999, 1000).unwrap(), 165536);
        assert_eq!(reserved_base(text, accounts, 999, 65536).unwrap(), 165536);
        assert!(reserved_base(text, accounts, 999, 65537).is_err());
    }

    #[test]
    fn the_reservation_says_how_many_slots_it_covers() {
        let text = "container:100000:65536\nnibrunner-jailer:165536:4096\n";
        assert_eq!(reserved_range(text, "", 999, 1000).unwrap(), (165536, 4096));
    }

    #[test]
    fn a_reservation_may_name_its_owner_by_numeric_uid() {
        assert_eq!(reserved_base("999:200000:8", "", 999, 8).unwrap(), 200000);
    }

    #[test]
    fn overlapping_container_ranges_and_host_accounts_are_refused() {
        assert!(reserved_base("nibrunner-jailer:200000:8\ncontainer:200004:8", "", 999, 8).is_err());
        for accounts in ["user:x:200004:1000:User:/home/user:/bin/sh", "group:x:200004:"] {
            assert!(reserved_base("nibrunner-jailer:200000:8", accounts, 999, 8).is_err());
        }
        assert!(reserved_base("nibrunner-jailer:200000:8\ncontainer:199999:9", "", 999, 8).is_err());
        assert!(reserved_base("nibrunner-jailer:200000:8\ncontainer:200008:8", "", 999, 8).is_ok());
    }

    #[test]
    fn missing_ambiguous_and_invalid_reservations_are_refused() {
        for text in [
            "",
            "nibrunner-jailer:1:8\n999:20:8",
            "nibrunner-jailer:0:8",
            "nibrunner-jailer:4294967290:8",
            "nibrunner-jailer:200000:0",
            "nibrunner-jailer:bad:8",
        ] {
            assert!(reserved_base(text, "", 999, 8).is_err(), "{text}");
        }
    }

    #[test]
    fn the_reservation_account_is_non_root_locked_and_cannot_log_in() {
        let passwd = "nibrunner-jailer:x:999:999::/nonexistent:/usr/sbin/nologin\n";
        let shadow = "nibrunner-jailer:!:20000::::::\n";
        assert_eq!(account_uid(passwd, shadow).unwrap(), 999);
        assert!(account_uid(&passwd.replace(":999:", ":0:"), shadow).is_err());
        assert!(account_uid(&passwd.replace("/usr/sbin/nologin", "/bin/sh"), shadow).is_err());
        assert!(account_uid(passwd, &shadow.replace(":!:", ":password:")).is_err());
    }
}
