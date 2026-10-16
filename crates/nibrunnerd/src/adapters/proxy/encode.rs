use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_compression::tokio::bufread::{BrotliDecoder, BrotliEncoder, GzipDecoder, GzipEncoder};
use async_compression::Level;
use bytes::Bytes;
use futures::{future, StreamExt, TryStreamExt};
use http_body_util::{BodyExt, BodyStream, StreamBody};
use hyper::body::Frame;
use hyper::header::{
    HeaderMap, HeaderValue, ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE,
    CONTENT_TYPE, ETAG, VARY,
};
use hyper::{Response, StatusCode};
use tokio::io::AsyncRead;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::adapters::proxy::forward::ProxyBody;
use crate::config::{Compression, HttpEncode};

const BROTLI_QUALITY: i32 = 5;
const GZIP_LEVEL: i32 = 6;
const BYTES_PER_KIB: u64 = 1024;

/// The representation a body travels in, as `Content-Encoding` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Encoding {
    Br,
    Gzip,
    Identity,
}

impl Encoding {
    pub(crate) const ALL: [Encoding; 3] = [Encoding::Br, Encoding::Gzip, Encoding::Identity];

    pub(crate) const fn token(self) -> &'static str {
        match self {
            Encoding::Br => "br",
            Encoding::Gzip => "gzip",
            Encoding::Identity => "identity",
        }
    }

    /// What `Content-Encoding` says a body is in, or nothing when it names a coding this proxy
    /// neither produces nor reads.
    pub(crate) fn of(headers: &HeaderMap) -> Option<Self> {
        match headers.get(CONTENT_ENCODING).map(HeaderValue::to_str) {
            None => Some(Encoding::Identity),
            Some(Ok(named)) => match named.trim().to_ascii_lowercase().as_str() {
                "" | "identity" => Some(Encoding::Identity),
                "br" => Some(Encoding::Br),
                "gzip" | "x-gzip" => Some(Encoding::Gzip),
                _ => None,
            },
            Some(Err(_)) => None,
        }
    }
}

impl From<Compression> for Encoding {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::Br => Encoding::Br,
            Compression::Gzip => Encoding::Gzip,
        }
    }
}

fn quality(parameters: &str) -> f32 {
    parameters
        .split(';')
        .filter_map(|parameter| parameter.trim().strip_prefix("q="))
        .find_map(|q| q.trim().parse::<f32>().ok())
        .unwrap_or(1.0)
}

/// The coding this response will be in for this visitor: the one it accepts with the highest
/// `q`, ties broken by `offered`'s order, `*` standing for gzip, identity when it accepts none.
pub(crate) fn negotiate(headers: &HeaderMap, offered: &[Compression]) -> Encoding {
    let accepted: Vec<(String, f32)> = headers
        .get_all(ACCEPT_ENCODING)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|item| {
            let (token, parameters) = item.split_once(';').unwrap_or((item, ""));
            let token = token.trim().to_ascii_lowercase();
            (!token.is_empty()).then(|| (token, quality(parameters)))
        })
        .collect();
    let q_of = |token: &str| accepted.iter().find(|(named, _)| named == token).map(|(_, q)| *q);
    offered
        .iter()
        .map(|compression| Encoding::from(*compression))
        .filter_map(|encoding| {
            let q = q_of(encoding.token())
                .or_else(|| (encoding == Encoding::Gzip).then(|| q_of("*")).flatten())?;
            (q > 0.0).then_some((encoding, q))
        })
        .fold(None, |best: Option<(Encoding, f32)>, (encoding, q)| match best {
            Some((_, best_q)) if best_q >= q => best,
            _ => Some((encoding, q)),
        })
        .map_or(Encoding::Identity, |(encoding, _)| encoding)
}

/// What the visitor asked for, said the one way the cache keys and the app hears it.
pub(crate) fn as_negotiated(headers: &mut HeaderMap, encoding: Encoding) {
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static(encoding.token()));
}

fn compressible(media_type: &str) -> bool {
    let media_type = media_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    media_type.starts_with("text/")
        || matches!(
            media_type.as_str(),
            "application/json"
                | "application/javascript"
                | "application/ecmascript"
                | "application/xml"
                | "application/xhtml+xml"
                | "application/rss+xml"
                | "application/atom+xml"
                | "application/ld+json"
                | "application/manifest+json"
                | "application/wasm"
                | "image/svg+xml"
                | "font/ttf"
                | "font/otf"
        )
}

fn cache_control_says(headers: &HeaderMap, directive: &str) -> bool {
    headers
        .get_all(CACHE_CONTROL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|named| named.trim().eq_ignore_ascii_case(directive))
}

/// Whether a response, as the app sent it, is one this proxy compresses at all.
pub(crate) fn should_encode(status: StatusCode, headers: &HeaderMap, settings: &HttpEncode) -> bool {
    matches!(status.as_u16(), 200 | 203 | 404 | 410)
        && !headers.contains_key(CONTENT_ENCODING)
        && !headers.contains_key(CONTENT_RANGE)
        && !cache_control_says(headers, "no-transform")
        && headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(compressible)
        && headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()?.trim().parse::<u64>().ok())
            .is_none_or(|length| length >= u64::from(settings.min_kib) * BYTES_PER_KIB)
}

/// `Vary` naming `header` beside whatever it already named.
pub(crate) fn vary_on(headers: &mut HeaderMap, header: &str) {
    let named: Vec<String> = headers
        .get_all(VARY)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect();
    if named
        .iter()
        .any(|name| name == "*" || name.eq_ignore_ascii_case(header))
    {
        return;
    }
    let merged = named
        .into_iter()
        .chain(std::iter::once(header.to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    if let Ok(merged) = HeaderValue::from_str(&merged) {
        headers.insert(VARY, merged);
    }
}

// A strong validator promises the bytes are the same, and they no longer are; nginx does the same.
pub(crate) fn weaken_etag(headers: &mut HeaderMap) {
    let Some(strong) = headers
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .filter(|etag| !etag.trim_start().starts_with("W/"))
        .map(|etag| format!("W/{}", etag.trim()))
    else {
        return;
    };
    if let Ok(weak) = HeaderValue::from_str(&strong) {
        headers.insert(ETAG, weak);
    }
}

/// The headers of a response whose body is about to change representation into `encoding`.
pub(crate) fn as_encoded(headers: &mut HeaderMap, encoding: Encoding) {
    headers.remove(CONTENT_LENGTH);
    match encoding {
        Encoding::Identity => {
            headers.remove(CONTENT_ENCODING);
        }
        coded => {
            headers.insert(CONTENT_ENCODING, HeaderValue::from_static(coded.token()));
        }
    }
    weaken_etag(headers);
    vary_on(headers, "accept-encoding");
}

/// On a response this proxy compressed: the `ETag` the app sent, before it was weakened, which is
/// the one the app recognises when asked to revalidate.
#[derive(Debug, Clone)]
pub(crate) struct Recoded {
    pub(crate) origin_etag: Option<HeaderValue>,
}

/// The app's response as it leaves for a visitor that was given `encoding`: compressed when this
/// host compresses and the response is one worth compressing, as it was otherwise.
pub(crate) fn recoded(
    response: Response<ProxyBody>,
    encoding: Encoding,
    settings: Option<&HttpEncode>,
    meter: Arc<Metered>,
) -> Response<ProxyBody> {
    let Some(settings) = settings else {
        return response;
    };
    if !should_encode(response.status(), response.headers(), settings) {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    vary_on(&mut parts.headers, "accept-encoding");
    if encoding == Encoding::Identity {
        return Response::from_parts(parts, body);
    }
    let origin_etag = parts.headers.get(ETAG).cloned();
    parts.extensions.insert(Recoded { origin_etag });
    as_encoded(&mut parts.headers, encoding);
    Response::from_parts(parts, encoded(body, encoding, meter))
}

type Report = Box<dyn FnOnce(u64, u64) + Send>;

/// What went through a coder and what came out, said once when the body is done with.
pub(crate) struct Metered {
    taken: AtomicU64,
    given: AtomicU64,
    report: Mutex<Option<Report>>,
}

impl Metered {
    pub(crate) fn reporting(report: impl FnOnce(u64, u64) + Send + 'static) -> Arc<Self> {
        Arc::new(Self {
            taken: AtomicU64::new(0),
            given: AtomicU64::new(0),
            report: Mutex::new(Some(Box::new(report))),
        })
    }

    fn unmetered() -> Arc<Self> {
        Self::reporting(|_, _| {})
    }
}

impl Drop for Metered {
    fn drop(&mut self) {
        let report = self
            .report
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(report) = report {
            report(*self.taken.get_mut(), *self.given.get_mut());
        }
    }
}

fn len(bytes: &Bytes) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

// A coder reads and writes `io::Error`, and the body it hands on has to fail with the app's own
// `hyper::Error`, which only hyper can make: the app's error is held aside on the way in and handed
// back in place of the one the coder saw it as.
fn through(
    coded: impl AsyncRead + Send + Sync + 'static,
    app_failure: Arc<Mutex<Option<hyper::Error>>>,
    meter: Arc<Metered>,
) -> ProxyBody {
    let frames = ReaderStream::new(coded)
        .map(move |chunk| match chunk {
            Ok(bytes) => {
                meter.given.fetch_add(len(&bytes), Ordering::Relaxed);
                Some(Ok(Frame::data(bytes)))
            }
            Err(error) => match app_failure
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
            {
                Some(failure) => Some(Err(failure)),
                None => {
                    tracing::warn!(%error, "a body could not be recoded, and was cut short");
                    None
                }
            },
        })
        .take_while(|frame| future::ready(frame.is_some()))
        .filter_map(future::ready);
    BodyExt::boxed(StreamBody::new(frames))
}

/// `body` as it flows, in `encoding`. Trailers do not survive a change of representation.
pub(crate) fn encoded(body: ProxyBody, encoding: Encoding, meter: Arc<Metered>) -> ProxyBody {
    let app_failure = Arc::new(Mutex::new(None));
    let held = app_failure.clone();
    let taken = meter.clone();
    let plain = StreamReader::new(
        BodyStream::new(body)
            .try_filter_map(|frame| future::ready(Ok(frame.into_data().ok())))
            .inspect_ok(move |bytes| {
                taken.taken.fetch_add(len(bytes), Ordering::Relaxed);
            })
            .map_err(move |error| {
                *held.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error);
                std::io::Error::other("the app's body ended in an error")
            }),
    );
    match encoding {
        Encoding::Br => through(
            BrotliEncoder::with_quality(plain, Level::Precise(BROTLI_QUALITY)),
            app_failure,
            meter,
        ),
        Encoding::Gzip => through(
            GzipEncoder::with_quality(plain, Level::Precise(GZIP_LEVEL)),
            app_failure,
            meter,
        ),
        Encoding::Identity => through(plain, app_failure, meter),
    }
}

/// A stored body in `from`, decoded as it is sent, for a visitor that accepts no coding.
pub(crate) fn decoded(stored: Bytes, from: Encoding) -> ProxyBody {
    let coded = std::io::Cursor::new(stored);
    let nothing_from_the_app = Arc::new(Mutex::new(None));
    match from {
        Encoding::Br => through(
            BrotliDecoder::new(coded),
            nothing_from_the_app,
            Metered::unmetered(),
        ),
        Encoding::Gzip => through(
            GzipDecoder::new(coded),
            nothing_from_the_app,
            Metered::unmetered(),
        ),
        Encoding::Identity => through(coded, nothing_from_the_app, Metered::unmetered()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;

    fn accepting(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_str(value).unwrap());
        headers
    }

    const BOTH: [Compression; 2] = [Compression::Br, Compression::Gzip];

    #[test]
    fn a_visitor_is_given_the_best_coding_it_accepts_and_identity_when_it_accepts_none() {
        assert_eq!(negotiate(&accepting("gzip, deflate, br"), &BOTH), Encoding::Br);
        assert_eq!(
            negotiate(&accepting("gzip, deflate, br, zstd"), &BOTH),
            Encoding::Br
        );
        assert_eq!(negotiate(&accepting("gzip"), &BOTH), Encoding::Gzip);
        assert_eq!(negotiate(&accepting("br;q=0, gzip"), &BOTH), Encoding::Gzip);
        assert_eq!(
            negotiate(&accepting("br;q=0.5, gzip;q=0.8"), &BOTH),
            Encoding::Gzip
        );
        assert_eq!(negotiate(&accepting("*"), &BOTH), Encoding::Gzip);
        assert_eq!(negotiate(&accepting("*, gzip;q=0"), &BOTH), Encoding::Identity);
        assert_eq!(negotiate(&accepting("deflate, zstd"), &BOTH), Encoding::Identity);
        assert_eq!(negotiate(&HeaderMap::new(), &BOTH), Encoding::Identity);
        assert_eq!(
            negotiate(&accepting("gzip, br"), &[Compression::Gzip, Compression::Br]),
            Encoding::Gzip,
            "a tie goes to this host's order"
        );
        assert_eq!(negotiate(&accepting("gzip, br"), &[]), Encoding::Identity);
    }

    fn response(content_type: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_str(content_type).unwrap());
        headers
    }

    fn settings() -> HttpEncode {
        HttpEncode {
            encodings: BOTH.to_vec(),
            min_kib: 1,
        }
    }

    #[test]
    fn text_is_compressed_and_images_archives_ranges_and_no_transform_are_passed_as_they_are() {
        let ok = StatusCode::OK;
        for media_type in [
            "text/html; charset=utf-8",
            "application/json",
            "application/javascript",
            "image/svg+xml",
            "font/ttf",
            "application/wasm",
        ] {
            assert!(
                should_encode(ok, &response(media_type), &settings()),
                "{media_type}"
            );
        }
        for media_type in ["image/png", "video/mp4", "application/zip", "font/woff2"] {
            assert!(
                !should_encode(ok, &response(media_type), &settings()),
                "{media_type}"
            );
        }
        assert!(should_encode(
            StatusCode::NOT_FOUND,
            &response("text/html"),
            &settings()
        ));
        for status in [
            StatusCode::PARTIAL_CONTENT,
            StatusCode::NOT_MODIFIED,
            StatusCode::FOUND,
        ] {
            assert!(
                !should_encode(status, &response("text/html"), &settings()),
                "{status}"
            );
        }
        let with = |name: hyper::header::HeaderName, value: &'static str| {
            let mut headers = response("text/html");
            headers.insert(name, HeaderValue::from_static(value));
            headers
        };
        assert!(!should_encode(
            ok,
            &with(CACHE_CONTROL, "public, no-transform"),
            &settings()
        ));
        assert!(!should_encode(ok, &with(CONTENT_ENCODING, "gzip"), &settings()));
        assert!(!should_encode(
            ok,
            &with(CONTENT_RANGE, "bytes 0-1/2"),
            &settings()
        ));
        assert!(!should_encode(ok, &with(CONTENT_LENGTH, "1023"), &settings()));
        assert!(should_encode(ok, &with(CONTENT_LENGTH, "1024"), &settings()));
        assert!(!should_encode(ok, &HeaderMap::new(), &settings()));
    }

    #[test]
    fn a_strong_etag_is_weakened_and_vary_gains_accept_encoding_once() {
        let mut headers = response("text/html");
        headers.insert(ETAG, HeaderValue::from_static("\"v1\""));
        headers.insert(VARY, HeaderValue::from_static("Accept-Language"));
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("4096"));
        as_encoded(&mut headers, Encoding::Br);
        assert_eq!(headers[ETAG], "W/\"v1\"");
        assert_eq!(headers[VARY], "Accept-Language, accept-encoding");
        assert_eq!(headers[CONTENT_ENCODING], "br");
        assert!(!headers.contains_key(CONTENT_LENGTH));

        as_encoded(&mut headers, Encoding::Gzip);
        assert_eq!(headers[ETAG], "W/\"v1\"", "an etag already weak stays as it was");
        assert_eq!(headers[VARY], "Accept-Language, accept-encoding");

        let mut anything = HeaderMap::new();
        anything.insert(VARY, HeaderValue::from_static("*"));
        vary_on(&mut anything, "accept-encoding");
        assert_eq!(anything[VARY], "*");
    }

    async fn collected(body: ProxyBody) -> Bytes {
        body.collect().await.unwrap().to_bytes()
    }

    fn page() -> Bytes {
        Bytes::from("<p>the same paragraph, over and over</p>\n".repeat(2_000))
    }

    fn plain(bytes: Bytes) -> ProxyBody {
        Full::new(bytes).map_err(|never| match never {}).boxed()
    }

    #[tokio::test]
    async fn what_is_encoded_on_the_way_out_decodes_back_to_what_the_app_sent() {
        for encoding in [Encoding::Br, Encoding::Gzip] {
            let counted = Arc::new(Mutex::new(None));
            let reported = counted.clone();
            let meter =
                Metered::reporting(move |taken, given| *reported.lock().unwrap() = Some((taken, given)));
            let coded = collected(encoded(plain(page()), encoding, meter)).await;
            assert!(
                coded.len() * 10 < page().len(),
                "{encoding:?} made {} bytes",
                coded.len()
            );
            assert_eq!(collected(decoded(coded.clone(), encoding)).await, page());
            assert_eq!(
                *counted.lock().unwrap(),
                Some((len(&page()), len(&coded))),
                "what went in and what came out of {encoding:?}"
            );
        }
        assert_eq!(
            collected(encoded(plain(page()), Encoding::Identity, Metered::unmetered())).await,
            page()
        );
    }
}
