//! The certificate the proxy presents, re-read whenever its files change, so a renewal is two
//! file replacements and no restart. A pair that does not load — the key replaced a moment before
//! its certificate, a truncated file — is refused and the previous one keeps serving: a handshake
//! is never the place a renewal in progress becomes an outage.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use rustls::crypto::CryptoProvider;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

use super::pem;
use crate::reload::FileStamp;

type Stamps = (Option<FileStamp>, Option<FileStamp>);

#[derive(Debug)]
struct Loaded {
    stamps: Stamps,
    refused: Option<Stamps>,
    certified: Arc<CertifiedKey>,
}

#[derive(Debug)]
pub struct ReloadingCertificate {
    certificate: PathBuf,
    key: PathBuf,
    loaded: Mutex<Loaded>,
}

impl ReloadingCertificate {
    /// Reads the pair once, and refuses what would serve nothing: a proxy that starts on an unusable
    /// certificate has none to fall back to.
    pub fn open(certificate: &Path, key: &Path) -> io::Result<Self> {
        let stamps = Self::stamps(certificate, key)?;
        let certified = Self::load(certificate, key)?;
        Ok(Self {
            certificate: certificate.to_path_buf(),
            key: key.to_path_buf(),
            loaded: Mutex::new(Loaded {
                stamps,
                refused: None,
                certified,
            }),
        })
    }

    fn stamps(certificate: &Path, key: &Path) -> io::Result<Stamps> {
        Ok((
            crate::reload::Inputs::stamp(certificate)?,
            crate::reload::Inputs::stamp(key)?,
        ))
    }

    fn load(certificate: &Path, key: &Path) -> io::Result<Arc<CertifiedKey>> {
        let chain = pem::read_certificates(certificate)?;
        tracing::info!(
            certificates = chain.len(),
            chain = %certificate.display(),
            "origin certificate chain loaded"
        );
        let private_key =
            rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(key)?))?
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "the key file holds no private key")
                })?;
        let provider = CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
        CertifiedKey::from_der(chain, private_key, &provider)
            .map(Arc::new)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    /// The pair to present now, reloaded first if either file changed since it was read.
    pub(crate) fn current(&self) -> Arc<CertifiedKey> {
        let mut loaded = self.loaded.lock().unwrap_or_else(PoisonError::into_inner);
        let Ok(stamps) = Self::stamps(&self.certificate, &self.key) else {
            return loaded.certified.clone();
        };
        if stamps == loaded.stamps || loaded.refused.as_ref() == Some(&stamps) {
            return loaded.certified.clone();
        }
        match Self::load(&self.certificate, &self.key) {
            Ok(certified) => {
                loaded.certified = certified;
                loaded.stamps = stamps;
                loaded.refused = None;
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    chain = %self.certificate.display(),
                    "the certificate files changed and do not load; still serving the previous pair"
                );
                loaded.refused = Some(stamps);
            }
        }
        loaded.certified.clone()
    }
}

impl ResolvesServerCert for ReloadingCertificate {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Pair {
        certificate: String,
        key: String,
    }

    fn pair() -> Pair {
        let made = rcgen::generate_simple_self_signed(vec!["apps.example.com".into()]).unwrap();
        Pair {
            certificate: made.cert.pem(),
            key: made.key_pair.serialize_pem(),
        }
    }

    // A rename gives the file a new inode, which is how a renewal replaces it and what the stamp sees
    // even when two writes land inside one clock tick.
    fn install(directory: &Path, name: &str, contents: &str) -> PathBuf {
        let path = directory.join(name);
        let staged = directory.join(format!("{name}.staged"));
        std::fs::write(&staged, contents).unwrap();
        std::fs::rename(staged, &path).unwrap();
        path
    }

    fn leaf(certified: &CertifiedKey) -> Vec<u8> {
        certified.cert[0].as_ref().to_vec()
    }

    #[test]
    fn a_renewed_certificate_is_served_without_a_restart() {
        let directory = tempfile::tempdir().unwrap();
        let (first, second) = (pair(), pair());
        let certificate = install(directory.path(), "fullchain.pem", &first.certificate);
        let key = install(directory.path(), "key.pem", &first.key);
        let resolver = ReloadingCertificate::open(&certificate, &key).unwrap();
        let before = leaf(&resolver.current());
        assert_eq!(
            leaf(&resolver.current()),
            before,
            "an unchanged pair is not read again"
        );

        install(directory.path(), "fullchain.pem", &second.certificate);
        install(directory.path(), "key.pem", &second.key);
        assert_ne!(leaf(&resolver.current()), before);
    }

    #[test]
    fn a_pair_caught_halfway_through_a_renewal_keeps_the_previous_certificate_serving() {
        let directory = tempfile::tempdir().unwrap();
        let (first, second) = (pair(), pair());
        let certificate = install(directory.path(), "fullchain.pem", &first.certificate);
        let key = install(directory.path(), "key.pem", &first.key);
        let resolver = ReloadingCertificate::open(&certificate, &key).unwrap();
        let before = leaf(&resolver.current());

        install(directory.path(), "fullchain.pem", &second.certificate);
        assert_eq!(
            leaf(&resolver.current()),
            before,
            "the new certificate does not match the old key"
        );
        assert_eq!(
            leaf(&resolver.current()),
            before,
            "and the refusal is remembered rather than repeated"
        );

        install(directory.path(), "key.pem", &second.key);
        assert_ne!(
            leaf(&resolver.current()),
            before,
            "the second file completes the pair"
        );
    }

    #[test]
    fn a_replacement_that_is_not_a_certificate_is_refused_and_the_old_one_kept() {
        let directory = tempfile::tempdir().unwrap();
        let first = pair();
        let certificate = install(directory.path(), "fullchain.pem", &first.certificate);
        let key = install(directory.path(), "key.pem", &first.key);
        let resolver = ReloadingCertificate::open(&certificate, &key).unwrap();
        let before = leaf(&resolver.current());

        install(directory.path(), "fullchain.pem", "");
        assert_eq!(leaf(&resolver.current()), before);
        install(directory.path(), "fullchain.pem", &first.certificate);
        assert_eq!(leaf(&resolver.current()), before);
    }

    #[test]
    fn one_file_holding_a_comment_the_chain_and_the_key_serves_as_both_paths() {
        let directory = tempfile::tempdir().unwrap();
        let (first, second) = (pair(), pair());
        let both = |p: &Pair, seq: u32| {
            format!(
                "# mf force_cert_seq={seq}\n{}\n{}",
                p.certificate.trim_end(),
                p.key
            )
        };
        let file = install(directory.path(), "pair.pem", &both(&first, 1));
        let resolver = ReloadingCertificate::open(&file, &file).unwrap();
        let before = leaf(&resolver.current());
        assert_eq!(before, pem::read_certificates(&file).unwrap()[0].as_ref());
        install(directory.path(), "pair.pem", &both(&second, 2));
        assert_ne!(
            leaf(&resolver.current()),
            before,
            "one atomic rename renews certificate and key together"
        );
    }

    #[test]
    fn a_host_that_starts_without_a_usable_certificate_does_not_start_serving() {
        let directory = tempfile::tempdir().unwrap();
        let absent = directory.path().join("absent.pem");
        assert_eq!(
            ReloadingCertificate::open(&absent, &absent).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }
}
