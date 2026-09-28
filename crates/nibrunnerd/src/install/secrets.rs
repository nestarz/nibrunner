//! `host.env`: every secret this host could read, named whether or not the configuration it was
//! written for needs it. The ones it needs are left uncommented and empty, the rest commented —
//! so a configuration that changes to need one finds the line already there, and `nibrunnerd
//! start` can refuse by name while a needed one is still empty.

use std::collections::BTreeSet;
use std::path::Path;

use super::render::{PASSWORD_VARIABLE, REGION_VARIABLE};
use super::InstallError;
use crate::config::{HostConfig, VolumeBackend};
use crate::json_store::write_text;

pub const ACCESS_KEY_VARIABLE: &str = "AWS_ACCESS_KEY_ID";
pub const SECRET_KEY_VARIABLE: &str = "AWS_SECRET_ACCESS_KEY";
pub const ENDPOINT_VARIABLE: &str = "AWS_ENDPOINT";
pub const ALLOW_HTTP_VARIABLE: &str = "AWS_ALLOW_HTTP";
pub const VIRTUAL_HOST_VARIABLE: &str = "AWS_VIRTUAL_HOSTED_STYLE_REQUEST";

const SECRET_FILE_MODE: u32 = 0o600;

pub struct Secret {
    pub name: &'static str,
    pub needed: bool,
}

/// Every one, in the order the file lists them.
pub fn of(config: &HostConfig) -> Vec<Secret> {
    let remote = reaches_an_object_store(config);
    vec![
        Secret {
            name: PASSWORD_VARIABLE,
            needed: matches!(config.volumes, VolumeBackend::Zerofs(_)),
        },
        Secret {
            name: REGION_VARIABLE,
            needed: remote,
        },
        Secret {
            name: ACCESS_KEY_VARIABLE,
            needed: remote,
        },
        Secret {
            name: SECRET_KEY_VARIABLE,
            needed: remote,
        },
        Secret {
            name: ENDPOINT_VARIABLE,
            needed: false,
        },
        Secret {
            name: ALLOW_HTTP_VARIABLE,
            needed: false,
        },
        Secret {
            name: VIRTUAL_HOST_VARIABLE,
            needed: false,
        },
    ]
}

/// The needed ones the file gives no value. A file that is not there gives none of them one.
pub fn missing(config: &HostConfig, file: &Path) -> Vec<&'static str> {
    let set = std::fs::read_to_string(file)
        .map(|text| set_in(&text))
        .unwrap_or_default();
    of(config)
        .into_iter()
        .filter(|secret| secret.needed && !set.contains(secret.name))
        .map(|secret| secret.name)
        .collect()
}

/// Read the way systemd reads an `EnvironmentFile=`: one `KEY=value` a line, `#` a comment, a
/// value that may be quoted. A key whose value is empty is not set.
fn set_in(text: &str) -> BTreeSet<String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.split_once('='))
        .filter(|(_, value)| !unquoted(value).is_empty())
        .map(|(key, _)| key.trim().to_string())
        .collect()
}

fn unquoted(value: &str) -> &str {
    let value = value.trim();
    ["\"", "'"]
        .iter()
        .find_map(|quote| value.strip_prefix(quote)?.strip_suffix(quote))
        .unwrap_or(value)
}

/// Made, never filled. What belongs in it is a password whose loss destroys every tenant disk on
/// this host and a credential that reaches an account this daemon has no business creating things
/// in — so it is written once by whoever holds them, and a re-run never touches it again.
///
/// It is made even when this host needs nothing in it: the drop-in names it with a bare
/// `EnvironmentFile=`, and systemd fails a unit whose environment file is not there.
pub fn ensure_file(path: &Path, config: &HostConfig) -> Result<bool, InstallError> {
    if path.exists() {
        return Ok(false);
    }
    write_text(path, &render(config), SECRET_FILE_MODE)
        .map_err(|error| InstallError::Refused(error.message()))?;
    Ok(true)
}

fn render(config: &HostConfig) -> String {
    let secrets = of(config);
    let line = |name: &str| {
        let secret = secrets
            .iter()
            .find(|secret| secret.name == name)
            .expect("every variable is listed");
        if secret.needed {
            format!("{name}=\n")
        } else {
            format!("# {name}=\n")
        }
    };
    format!(
        "# Read by nibrunnerd and, on a zerofs host, by ZeroFS's own units. Made by `nibrunnerd\n\
         # install` and never written by it again: everything here is a secret it has no way to know.\n\
         #\n\
         # A line that is not a comment is one this host's configuration needs, and `nibrunnerd start`\n\
         # refuses to start while it is empty. A commented one it does not need — uncomment it if the\n\
         # configuration changes to.\n\
         \n\
         # Every tenant disk on this host is encrypted under this. Losing it loses every one of them,\n\
         # so it is permanent for the life of the bucket. volumes.backend = \"zerofs\" needs it.\n\
         {}\n\
         # A store_url or storage_url that is s3:// needs these.\n\
         {}{}{}\n\
         # Optional S3 endpoint and addressing policy. HTTP defaults to false.\n\
         {}{}{}",
        line(PASSWORD_VARIABLE),
        line(REGION_VARIABLE),
        line(ACCESS_KEY_VARIABLE),
        line(SECRET_KEY_VARIABLE),
        line(ENDPOINT_VARIABLE),
        line(ALLOW_HTTP_VARIABLE),
        line(VIRTUAL_HOST_VARIABLE),
    )
}

/// Whether anything this host is configured to reach is an object store rather than a directory.
/// A host whose artifacts, exports and volumes are all local needs no credential, and naming one
/// in its environment file would be telling it to go and find something it does not have.
fn reaches_an_object_store(config: &HostConfig) -> bool {
    let remote = |url: &str| url.starts_with("s3://");
    remote(&config.artifact_store_url)
        || remote(&config.export_store_url)
        || config
            .volumes
            .zerofs()
            .is_some_and(|settings| remote(&settings.storage_url))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local() -> HostConfig {
        HostConfig::under(Path::new("/srv/nibrunner"))
    }

    fn zerofs_in_a_bucket() -> HostConfig {
        let mut config = local();
        config.volumes = VolumeBackend::Zerofs(Box::new(crate::test_support::zerofs_settings(|settings| {
            settings.storage_url = "s3://nibrunner-filesystems/hetzner-1".to_string();
        })));
        config
    }

    fn names(secrets: &[Secret], needed: bool) -> Vec<&'static str> {
        secrets
            .iter()
            .filter(|secret| secret.needed == needed)
            .map(|secret| secret.name)
            .collect()
    }

    #[test]
    fn a_secret_file_is_made_once_and_never_written_over() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("host.env");

        assert!(ensure_file(&path, &local()).unwrap());
        std::fs::write(&path, "AWS_ACCESS_KEY_ID=real\n").unwrap();
        assert!(!ensure_file(&path, &local()).unwrap());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "AWS_ACCESS_KEY_ID=real\n"
        );
    }

    #[test]
    fn the_secret_file_is_readable_only_by_the_host_that_runs_on_it() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("host.env");
        ensure_file(&path, &local()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, SECRET_FILE_MODE);
        }
    }

    // Its volumes are files on its own disk and its stores are directories on it, so there is no
    // account for it to reach and nothing to encrypt. Every variable is still named, commented,
    // because the configuration may change to need one and the file is never written again.
    #[test]
    fn a_host_that_reaches_nothing_remote_needs_no_secret_but_is_told_every_name() {
        let secrets = of(&local());
        assert!(names(&secrets, true).is_empty());
        let written = render(&local());
        for name in names(&secrets, false) {
            assert!(written.contains(&format!("\n# {name}=\n")), "{written}");
        }
    }

    #[test]
    fn a_host_that_needs_no_secret_still_gets_the_file_its_unit_names() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("host.env");
        assert!(ensure_file(&path, &local()).unwrap());
        assert!(path.exists());
    }

    #[test]
    fn a_local_file_host_whose_artifacts_are_in_a_bucket_needs_the_credential_and_no_password() {
        let mut config = local();
        config.artifact_store_url = "s3://nibrunner-artifacts/artifacts".to_string();
        let secrets = of(&config);
        assert_eq!(
            names(&secrets, true),
            [REGION_VARIABLE, ACCESS_KEY_VARIABLE, SECRET_KEY_VARIABLE]
        );
        assert_eq!(
            names(&secrets, false),
            [
                PASSWORD_VARIABLE,
                ENDPOINT_VARIABLE,
                ALLOW_HTTP_VARIABLE,
                VIRTUAL_HOST_VARIABLE
            ]
        );
        let written = render(&config);
        assert!(written.contains("\nAWS_ACCESS_KEY_ID=\n"), "{written}");
        assert!(written.contains("\n# ZEROFS_ENCRYPTION_PASSWORD=\n"), "{written}");
    }

    #[test]
    fn a_zerofs_host_in_a_bucket_needs_all_four() {
        assert_eq!(
            names(&of(&zerofs_in_a_bucket()), true),
            [
                PASSWORD_VARIABLE,
                REGION_VARIABLE,
                ACCESS_KEY_VARIABLE,
                SECRET_KEY_VARIABLE
            ]
        );
    }

    #[test]
    fn every_store_a_host_could_reach_remotely_is_looked_at() {
        assert!(!reaches_an_object_store(&local()));

        let mut exports = local();
        exports.export_store_url = "s3://nibrunner-exports/exports".to_string();
        assert!(reaches_an_object_store(&exports));

        assert!(reaches_an_object_store(&zerofs_in_a_bucket()));
    }

    // A commented line and an empty value are both "not set": the file is written with both, and
    // a start that took either for a secret would fail later with a message naming ZeroFS.
    #[test]
    fn the_file_is_read_the_way_systemd_reads_it() {
        let set = set_in(
            "# a comment\n\
             ZEROFS_ENCRYPTION_PASSWORD=\n\
             AWS_REGION=\"eu-west-2\"\n\
             AWS_ACCESS_KEY_ID='AKIA'\n\
             # AWS_SECRET_ACCESS_KEY=commented\n\
             \n\
             not a pair\n",
        );
        assert_eq!(
            set.into_iter().collect::<Vec<_>>(),
            ["AWS_ACCESS_KEY_ID", "AWS_REGION"]
        );
    }

    #[test]
    fn what_is_missing_is_what_is_needed_and_not_set() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("host.env");
        let config = zerofs_in_a_bucket();

        assert_eq!(
            missing(&config, &path),
            [
                PASSWORD_VARIABLE,
                REGION_VARIABLE,
                ACCESS_KEY_VARIABLE,
                SECRET_KEY_VARIABLE
            ],
            "a file that is not there sets nothing"
        );

        std::fs::write(
            &path,
            "ZEROFS_ENCRYPTION_PASSWORD=hunter2\nAWS_REGION=eu-west-2\nAWS_ACCESS_KEY_ID=\n",
        )
        .unwrap();
        assert_eq!(
            missing(&config, &path),
            [ACCESS_KEY_VARIABLE, SECRET_KEY_VARIABLE]
        );
        assert!(missing(&local(), &path).is_empty());
    }
}
