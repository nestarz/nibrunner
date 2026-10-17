use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use http_body_util::{BodyExt, Empty, Full};
use http_cache_semantics::{BeforeRequest, CacheOptions, CachePolicy};
use hyper::body::{Body, Frame, SizeHint};
use hyper::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT_ENCODING, AGE, AUTHORIZATION, CACHE_CONTROL, CONTENT_LENGTH,
    CONTENT_RANGE, DATE, EXPIRES, HOST, IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_RANGE,
    IF_UNMODIFIED_SINCE, PRAGMA, RANGE, SET_COOKIE, TRANSFER_ENCODING, UPGRADE, VARY,
};
use hyper::http::{request, response};
use hyper::{Method, Request, Response, StatusCode};
use protocol::DeploymentId;

use crate::adapters::proxy::encode::{self, Encoding};
use crate::adapters::proxy::forward::ProxyBody;
use crate::config::HttpCache;
use crate::domain::metrics::{CacheOutcome, HostMetrics};

pub(crate) type Clock = Arc<dyn Fn() -> SystemTime + Send + Sync>;

const BYTES_PER_MIB: u64 = 1024 * 1024;
const BYTES_PER_KIB: usize = 1024;
// What a header costs beside its name and value, in the map that holds it; the weight is a bound
// on memory, so it errs high.
const HEADER_OVERHEAD: usize = 32;
// A `Vary` on anything but the coding gives one key more than one stored answer; past a few, a
// resource that varies on everything is one nobody shares anyway.
const MOST_VARIANTS: usize = 4;

/// Where the app is, for the cache: asking it, and handing its answer through the encoder.
pub(crate) trait Origin: Send + Sync + 'static {
    fn ask(&self, request: Request<ProxyBody>) -> BoxFuture<'static, Response<ProxyBody>>;
    fn recode(&self, response: Response<ProxyBody>, encoding: Encoding) -> Response<ProxyBody>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    deployment_id: DeploymentId,
    authority: String,
    target: String,
    encoding: Encoding,
}

fn directives(headers: &HeaderMap) -> Vec<(String, Option<String>)> {
    headers
        .get_all(CACHE_CONTROL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|directive| {
            let (name, value) = directive.split_once('=').unwrap_or((directive, ""));
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim().trim_matches('"');
            (!name.is_empty()).then(|| (name, (!value.is_empty()).then(|| value.to_string())))
        })
        .collect()
}

fn says(directives: &[(String, Option<String>)], name: &str) -> bool {
    directives.iter().any(|(named, _)| named == name)
}

fn options() -> CacheOptions {
    CacheOptions {
        shared: true,
        cache_heuristic: 0.0,
        immutable_min_time_to_live: Duration::ZERO,
        ..CacheOptions::default()
    }
}

/// Whether the cache is the place for this request at all. One with a body, a range, or an
/// upgrade goes to the app as it came.
pub(crate) fn answerable<B>(request: &Request<B>) -> bool {
    let headers = request.headers();
    let has_body = headers.contains_key(TRANSFER_ENCODING)
        || headers
            .get(CONTENT_LENGTH)
            .is_some_and(|length| length.as_bytes() != b"0");
    *request.method() == Method::GET
        && !headers.contains_key(RANGE)
        && !headers.contains_key(UPGRADE)
        && !has_body
}

const CONDITIONALS: [HeaderName; 5] = [
    IF_NONE_MATCH,
    IF_MODIFIED_SINCE,
    IF_MATCH,
    IF_UNMODIFIED_SINCE,
    IF_RANGE,
];

/// A request the cache may answer, read the way it keys and matches.
pub(crate) struct Asked {
    key: Key,
    visitor: request::Parts,
    canonical: request::Parts,
}

impl Asked {
    /// `visitor`'s `Accept-Encoding` already says the one coding it was given.
    pub(crate) fn new(
        visitor: request::Parts,
        deployment_id: DeploymentId,
        authority: &str,
        encoding: Encoding,
    ) -> Self {
        let target = visitor
            .uri
            .path_and_query()
            .map_or("/", |target| target.as_str())
            .to_string();
        let mut canonical = Request::builder()
            .method(Method::GET)
            .uri(target.as_str())
            .body(())
            .expect("a path and query taken from a parsed URI is a URI")
            .into_parts()
            .0;
        canonical.headers = visitor.headers.clone();
        for conditional in &CONDITIONALS {
            canonical.headers.remove(conditional);
        }
        // A visitor's reload sends `no-cache` or `max-age=0`, and honouring it would let any
        // visitor wake a sleeping app on every request; RFC 9111 §5.2.1 leaves that to the cache.
        canonical.headers.remove(CACHE_CONTROL);
        canonical.headers.remove(PRAGMA);
        if let Ok(host) = HeaderValue::from_str(authority) {
            canonical.headers.insert(HOST, host);
        }
        encode::as_negotiated(&mut canonical.headers, encoding);
        Self {
            key: Key {
                deployment_id,
                authority: authority.to_string(),
                target,
                encoding,
            },
            visitor,
            canonical,
        }
    }

    fn forwarded(&self) -> Request<ProxyBody> {
        let mut parts = Request::builder()
            .method(self.visitor.method.clone())
            .uri(self.visitor.uri.clone())
            .version(self.visitor.version)
            .body(())
            .expect("a request rebuilt from its own parts")
            .into_parts()
            .0;
        parts.headers = self.visitor.headers.clone();
        Request::from_parts(parts, empty())
    }
}

fn empty() -> ProxyBody {
    Empty::new().map_err(|never| match never {}).boxed()
}

fn whole(bytes: Bytes) -> ProxyBody {
    Full::new(bytes).map_err(|never| match never {}).boxed()
}

/// One stored answer: the policy the crate reads it by, the headers it is served with, the body
/// as stored.
pub(crate) struct Stored {
    policy: CachePolicy,
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    weight: u32,
}

fn weight_of(headers: &HeaderMap) -> usize {
    headers
        .iter()
        .map(|(name, value)| name.as_str().len() + value.len() + HEADER_OVERHEAD)
        .sum()
}

impl Stored {
    fn lifetime(&self, now: SystemTime) -> Duration {
        self.policy.time_to_live(now)
    }

    fn headers_as_served(&self, now: SystemTime) -> HeaderMap {
        let mut headers = self.headers.clone();
        headers.insert(AGE, HeaderValue::from(self.policy.age(now).as_secs()));
        if let Ok(date) = HeaderValue::from_str(&httpdate::fmt_http_date(now)) {
            headers.insert(DATE, date);
        }
        headers
    }

    fn serve(&self, now: SystemTime) -> Response<ProxyBody> {
        let headers = self.headers_as_served(now);
        let mut served = Response::new(whole(self.body.clone()));
        *served.status_mut() = self.status;
        *served.headers_mut() = headers;
        served
    }
}

/// Every answer stored under one key, one per `Vary` variant, newest first.
pub(crate) struct Variants {
    stored: Vec<Arc<Stored>>,
    lifetime: Duration,
    weight: u32,
}

struct UntilTheLastVariantExpires;

impl moka::Expiry<Key, Arc<Variants>> for UntilTheLastVariantExpires {
    fn expire_after_create(&self, _: &Key, variants: &Arc<Variants>, _: Instant) -> Option<Duration> {
        Some(variants.lifetime)
    }

    fn expire_after_update(
        &self,
        _: &Key,
        variants: &Arc<Variants>,
        _: Instant,
        _: Option<Duration>,
    ) -> Option<Duration> {
        Some(variants.lifetime)
    }
}

/// What the cache answered, and how.
pub(crate) struct Answered {
    pub(crate) response: Response<ProxyBody>,
    pub(crate) outcome: CacheOutcome,
}

pub(crate) struct ResponseCache {
    entries: moka::future::Cache<Key, Arc<Variants>>,
    max_entry_bytes: usize,
    clock: Clock,
    metrics: Arc<HostMetrics>,
}

fn explicit_lifetime(headers: &HeaderMap) -> bool {
    let directives = directives(headers);
    says(&directives, "s-maxage") || says(&directives, "max-age") || headers.contains_key(EXPIRES)
}

impl ResponseCache {
    pub(crate) fn new(settings: &HttpCache, clock: Clock, metrics: Arc<HostMetrics>) -> Arc<Self> {
        Arc::new(Self {
            entries: moka::future::Cache::builder()
                .max_capacity(u64::from(settings.memory_mib.get()) * BYTES_PER_MIB)
                .weigher(|_, variants: &Arc<Variants>| variants.weight)
                .expire_after(UntilTheLastVariantExpires)
                .build(),
            max_entry_bytes: usize::try_from(settings.max_entry_kib.get())
                .unwrap_or(usize::MAX)
                .saturating_mul(BYTES_PER_KIB),
            clock,
            metrics,
        })
    }

    pub(crate) fn system_clock() -> Clock {
        Arc::new(SystemTime::now)
    }

    async fn weighed(&self) {
        self.entries.run_pending_tasks().await;
        self.metrics.proxy.cache_weighs(self.entries.weighted_size());
    }

    /// A request the cache may answer: a fresh stored answer when there is one, the app's
    /// otherwise, kept on its way to the visitor when the app made it storable. A stale entry is
    /// asked again and replaced.
    pub(crate) async fn answer(self: &Arc<Self>, asked: Asked, origin: Arc<dyn Origin>) -> Answered {
        let now = (self.clock)();
        match self.find(&asked.key, &asked.canonical, now).await {
            Some(stored) => self.hit(&stored, now),
            None => self.miss(asked, origin).await,
        }
    }

    async fn find(&self, key: &Key, canonical: &request::Parts, now: SystemTime) -> Option<Arc<Stored>> {
        let variants = self.entries.get(key).await?;
        variants
            .stored
            .iter()
            .find(|stored| {
                matches!(
                    stored.policy.before_request(canonical, now),
                    BeforeRequest::Fresh(_)
                )
            })
            .cloned()
    }

    /// Flow 1: fresh, answered from memory without the app.
    fn hit(&self, stored: &Stored, now: SystemTime) -> Answered {
        Answered {
            response: stored.serve(now),
            outcome: CacheOutcome::Hit,
        }
    }

    /// Flow 2: asked of the app, and kept once its body has gone to the visitor whole.
    async fn miss(self: &Arc<Self>, asked: Asked, origin: Arc<dyn Origin>) -> Answered {
        let requested_at = (self.clock)();
        let response = origin.ask(asked.forwarded()).await;
        let response = self.keeping(&asked, origin.recode(response, asked.key.encoding), requested_at);
        Answered {
            response,
            outcome: CacheOutcome::Miss,
        }
    }

    fn stored(
        &self,
        canonical: &request::Parts,
        mut response: response::Parts,
        body: Bytes,
        requested_at: SystemTime,
        now: SystemTime,
    ) -> Option<Stored> {
        // A coding this proxy neither produces nor reads is passed on and never stored.
        Encoding::of(&response.headers)?;
        // Given the time the request left rather than the time the answer came, the crate's
        // `age_value + resident_time` is RFC 9111 §4.2.3's `age_value + response_delay +
        // resident_time`, without the apparent age a skewed `Date` would add.
        let policy = CachePolicy::new_options(canonical, &response, requested_at, options());
        let shared_with_cookies =
            response.headers.contains_key(SET_COOKIE) && !says(&directives(&response.headers), "public");
        let storable = policy.is_storable()
            && explicit_lifetime(&response.headers)
            && !shared_with_cookies
            && !response.headers.contains_key(CONTENT_RANGE)
            && response.status != StatusCode::PARTIAL_CONTENT
            && body.len() <= self.max_entry_bytes
            && policy.time_to_live(now) > Duration::ZERO;
        if !storable {
            return None;
        }
        response.headers.remove(hyper::header::CONNECTION);
        let kept = matched_by(canonical, &response.headers);
        let policy = CachePolicy::new_options(&kept, &response, requested_at, options());
        let weight = weight_of(&response.headers) * 2 + weight_of(&kept.headers) + body.len();
        Some(Stored {
            policy,
            status: response.status,
            headers: response.headers,
            body,
            weight: u32::try_from(weight).unwrap_or(u32::MAX),
        })
    }

    async fn keep(&self, key: Key, canonical: &request::Parts, stored: Arc<Stored>, now: SystemTime) {
        let others = self
            .entries
            .get(&key)
            .await
            .map(|variants| variants.stored.clone())
            .unwrap_or_default()
            .into_iter()
            .filter(|other| {
                !matches!(
                    other.policy.before_request(canonical, now),
                    BeforeRequest::Fresh(_) | BeforeRequest::Stale { matches: true, .. }
                ) && other.lifetime(now) > Duration::ZERO
            });
        let stored: Vec<Arc<Stored>> = std::iter::once(stored)
            .chain(others)
            .take(MOST_VARIANTS)
            .collect();
        let variants = Variants {
            lifetime: stored
                .iter()
                .map(|stored| stored.lifetime(now))
                .max()
                .unwrap_or_default(),
            weight: stored
                .iter()
                .fold(0u32, |weight, stored| weight.saturating_add(stored.weight)),
            stored,
        };
        self.entries.insert(key, Arc::new(variants)).await;
        self.weighed().await;
    }

    /// `response` on its way to the visitor, its body copied aside as it passes and stored once it
    /// has passed whole, if the app made it storable and it fits.
    fn keeping(
        self: &Arc<Self>,
        asked: &Asked,
        response: Response<ProxyBody>,
        requested_at: SystemTime,
    ) -> Response<ProxyBody> {
        // A body the app compressed in a coding other than the one negotiated is passed on as it
        // is and never decoded to be stored; one sent as identity is what this key's visitors get.
        if !matches!(Encoding::of(response.headers()), Some(sent) if sent == asked.key.encoding || sent == Encoding::Identity)
        {
            return response;
        }
        let probe = CachePolicy::new_options(&asked.canonical, &response, requested_at, options());
        if !probe.is_storable() || !explicit_lifetime(response.headers()) {
            return response;
        }
        let (parts, body) = response.into_parts();
        let headers = parts.headers.clone();
        let status = parts.status;
        let cache = self.clone();
        let key = asked.key.clone();
        let canonical = asked.canonical.clone();
        let kept = Tee::new(body, self.max_entry_bytes, move |body: Bytes| {
            tokio::spawn(async move {
                let mut response = Response::new(());
                *response.status_mut() = status;
                *response.headers_mut() = headers;
                response
                    .headers_mut()
                    .insert(CONTENT_LENGTH, HeaderValue::from(body.len()));
                let now = (cache.clock)();
                let (response, ()) = response.into_parts();
                if let Some(stored) = cache.stored(&canonical, response, body, requested_at, now) {
                    cache.keep(key, &canonical, Arc::new(stored), now).await;
                }
            });
        });
        Response::from_parts(parts, kept.boxed())
    }
}

/// The request a stored answer is matched by: the host, the coding, and what its `Vary` names,
/// rather than every header the first visitor happened to send.
fn matched_by(canonical: &request::Parts, response: &HeaderMap) -> request::Parts {
    let mut kept = Request::builder()
        .method(canonical.method.clone())
        .uri(canonical.uri.clone())
        .body(())
        .expect("a request rebuilt from its own parts")
        .into_parts()
        .0;
    let varied: Vec<String> = response
        .get_all(VARY)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    for (name, value) in &canonical.headers {
        let named = name == HOST
            || name == ACCEPT_ENCODING
            || name == AUTHORIZATION
            || varied.iter().any(|varied| varied == name.as_str());
        if named {
            kept.headers.append(name.clone(), value.clone());
        }
    }
    kept
}

/// A body passed on as it flows, with a copy kept up to `limit` bytes and handed to `done` once
/// the last of it has passed. A body that fails, carries trailers, or outgrows the limit is
/// passed on and not kept.
struct Tee {
    body: ProxyBody,
    kept: Option<BytesMut>,
    limit: usize,
    done: Option<Box<dyn FnOnce(Bytes) + Send + Sync>>,
}

impl Tee {
    fn new(body: ProxyBody, limit: usize, done: impl FnOnce(Bytes) + Send + Sync + 'static) -> Self {
        let mut tee = Self {
            body,
            kept: Some(BytesMut::new()),
            limit,
            done: Some(Box::new(done)),
        };
        // hyper does not poll a body that says it has ended, so an empty one is kept now.
        if tee.body.is_end_stream() {
            tee.finish();
        }
        tee
    }

    fn finish(&mut self) {
        if let (Some(kept), Some(done)) = (self.kept.take(), self.done.take()) {
            done(kept.freeze());
        }
    }
}

impl Body for Tee {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let tee = self.get_mut();
        let polled = Pin::new(&mut tee.body).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => match (frame.data_ref(), tee.kept.as_mut()) {
                (Some(data), Some(kept)) if kept.len() + data.len() <= tee.limit => {
                    kept.extend_from_slice(data);
                }
                _ => tee.kept = None,
            },
            Poll::Ready(Some(Err(_))) => tee.kept = None,
            Poll::Ready(None) => tee.finish(),
            Poll::Pending => {}
        }
        if matches!(polled, Poll::Ready(Some(Ok(_)))) && tee.body.is_end_stream() {
            tee.finish();
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Compression, HttpEncode};
    use hyper::header::ETAG;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// A clock the test moves by hand.
    #[derive(Clone)]
    struct Hands(Arc<Mutex<SystemTime>>);

    impl Hands {
        fn at_noon() -> Self {
            Self(Arc::new(Mutex::new(
                SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
            )))
        }

        fn clock(&self) -> Clock {
            let hands = self.0.clone();
            Arc::new(move || *hands.lock().unwrap())
        }

        fn now(&self) -> SystemTime {
            *self.0.lock().unwrap()
        }

        fn forward(&self, by: Duration) {
            *self.0.lock().unwrap() += by;
        }
    }

    type Answering = dyn Fn(&request::Parts) -> Response<ProxyBody> + Send + Sync;

    /// An app behind the cache, answering as `answer` says and counting every time it is asked,
    /// which is every time it would have been woken.
    struct App {
        asked: AtomicUsize,
        heard: Mutex<Vec<request::Parts>>,
        answer: Box<Answering>,
        encode: Option<HttpEncode>,
    }

    impl App {
        fn answering(
            answer: impl Fn(&request::Parts) -> Response<ProxyBody> + Send + Sync + 'static,
        ) -> Arc<Self> {
            Self::compressing(Some(both()), answer)
        }

        fn compressing(
            encode: Option<HttpEncode>,
            answer: impl Fn(&request::Parts) -> Response<ProxyBody> + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                asked: AtomicUsize::new(0),
                heard: Mutex::new(Vec::new()),
                answer: Box::new(answer),
                encode,
            })
        }

        fn woken(&self) -> usize {
            self.asked.load(Ordering::SeqCst)
        }

        fn last_heard(&self) -> request::Parts {
            self.heard.lock().unwrap().last().cloned().unwrap()
        }
    }

    impl Origin for App {
        fn ask(&self, request: Request<ProxyBody>) -> BoxFuture<'static, Response<ProxyBody>> {
            self.asked.fetch_add(1, Ordering::SeqCst);
            let (parts, _) = request.into_parts();
            let response = (self.answer)(&parts);
            self.heard.lock().unwrap().push(parts);
            Box::pin(async move { response })
        }

        fn recode(&self, response: Response<ProxyBody>, encoding: Encoding) -> Response<ProxyBody> {
            encode::recoded(
                response,
                encoding,
                self.encode.as_ref(),
                encode::Metered::reporting(|_, _| {}),
            )
        }
    }

    fn both() -> HttpEncode {
        HttpEncode {
            encodings: vec![Compression::Br, Compression::Gzip],
            min_kib: 1,
        }
    }

    fn page() -> Bytes {
        Bytes::from("<p>the same paragraph, over and over</p>\n".repeat(1_000))
    }

    fn answer(headers: &[(&str, &str)], body: Bytes) -> Response<ProxyBody> {
        let mut response = Response::new(whole(body));
        for (name, value) in headers {
            response.headers_mut().append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        response
    }

    fn html(cache_control: &'static str) -> impl Fn(&request::Parts) -> Response<ProxyBody> + Send + Sync {
        move |_| {
            answer(
                &[
                    ("content-type", "text/html"),
                    ("cache-control", cache_control),
                    ("etag", "\"v1\""),
                ],
                page(),
            )
        }
    }

    fn cache_of(hands: &Hands, memory_mib: u32, max_entry_kib: u32) -> Arc<ResponseCache> {
        ResponseCache::new(
            &HttpCache {
                memory_mib: memory_mib.try_into().unwrap(),
                max_entry_kib: max_entry_kib.try_into().unwrap(),
            },
            hands.clock(),
            Arc::new(HostMetrics::new()),
        )
    }

    fn asked_with(method: Method, path: &str, deployment: &str, headers: &[(&str, &str)]) -> Asked {
        let mut builder = Request::builder().method(method).uri(path);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let (mut parts, ()) = builder.body(()).unwrap().into_parts();
        let encoding = encode::negotiate(&parts.headers, &both().encodings);
        encode::as_negotiated(&mut parts.headers, encoding);
        Asked::new(
            parts,
            DeploymentId::parse(deployment).unwrap(),
            "app-1.apps.example.com",
            encoding,
        )
    }

    fn asked(accept_encoding: &str) -> Asked {
        asked_with(Method::GET, "/", "dep-1", &[("accept-encoding", accept_encoding)])
    }

    async fn sent(
        cache: &Arc<ResponseCache>,
        asked: Asked,
        app: &Arc<App>,
    ) -> (Response<ProxyBody>, CacheOutcome, Bytes) {
        let answered = cache.answer(asked, app.clone()).await;
        let (parts, body) = answered.response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        settled(cache).await;
        (
            Response::from_parts(parts, whole(Bytes::new())),
            answered.outcome,
            body,
        )
    }

    /// The store happens on a task of its own once the body has passed.
    async fn settled(cache: &Arc<ResponseCache>) {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        cache.entries.run_pending_tasks().await;
    }

    async fn inflated(body: Bytes, from: Encoding) -> Bytes {
        encode::decoded(body, from).collect().await.unwrap().to_bytes()
    }

    #[test]
    fn only_what_the_app_made_storable_for_an_explicit_lifetime_is_stored() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let canonical = asked("identity").canonical;
        let storable = |headers: &[(&str, &str)]| {
            let (parts, _) = answer(headers, Bytes::from_static(b"x")).into_parts();
            cache
                .stored(
                    &canonical,
                    parts,
                    Bytes::from_static(b"x"),
                    hands.now(),
                    hands.now(),
                )
                .is_some()
        };
        let mut authorized =
            asked_with(Method::GET, "/", "dep-1", &[("authorization", "Bearer secret")]).canonical;
        let storable_for = |canonical: &mut request::Parts, cache_control: &str| {
            let (parts, _) =
                answer(&[("cache-control", cache_control)], Bytes::from_static(b"x")).into_parts();
            cache
                .stored(
                    canonical,
                    parts,
                    Bytes::from_static(b"x"),
                    hands.now(),
                    hands.now(),
                )
                .is_some()
        };
        let table: [(&[(&str, &str)], bool); 11] = [
            (&[("cache-control", "public, max-age=60")], true),
            (&[("cache-control", "s-maxage=60")], true),
            (&[("expires", "Thu, 01 Jan 2099 00:00:00 GMT")], true),
            (&[], false),
            (&[("last-modified", "Thu, 01 Jan 1970 00:00:00 GMT")], false),
            (&[("cache-control", "public")], false),
            (&[("cache-control", "no-store, max-age=60")], false),
            (&[("cache-control", "private, max-age=60")], false),
            (
                &[("cache-control", "max-age=60"), ("set-cookie", "session=1")],
                false,
            ),
            (
                &[
                    ("cache-control", "public, max-age=60"),
                    ("set-cookie", "theme=dark"),
                ],
                true,
            ),
            (&[("cache-control", "max-age=0")], false),
        ];
        for (headers, expected) in table {
            assert_eq!(storable(headers), expected, "{headers:?}");
        }
        assert!(!storable_for(&mut authorized, "max-age=60"));
        assert!(storable_for(&mut authorized, "public, max-age=60"));
        assert!(storable_for(&mut authorized, "s-maxage=60"));
        let (partial, _) = answer(&[("cache-control", "max-age=60")], Bytes::new()).into_parts();
        let mut partial = partial;
        partial.status = StatusCode::PARTIAL_CONTENT;
        assert!(cache
            .stored(&canonical, partial, Bytes::new(), hands.now(), hands.now())
            .is_none());
    }

    #[test]
    fn an_answer_is_fresh_until_its_age_and_the_time_it_took_reach_its_lifetime() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let canonical = asked("identity").canonical;
        let requested_at = hands.now();
        hands.forward(Duration::from_secs(2));
        let (parts, _) = answer(&[("cache-control", "max-age=60"), ("age", "10")], Bytes::new()).into_parts();
        let stored = cache
            .stored(&canonical, parts, Bytes::new(), requested_at, hands.now())
            .unwrap();
        // current_age = age_value (10) + response_delay (2) + resident_time; fresh while below 60.
        assert_eq!(stored.policy.age(hands.now()), Duration::from_secs(12));
        assert_eq!(stored.policy.time_to_live(hands.now()), Duration::from_secs(48));
        hands.forward(Duration::from_secs(47));
        assert!(matches!(
            stored.policy.before_request(&canonical, hands.now()),
            BeforeRequest::Fresh(_)
        ));
        hands.forward(Duration::from_secs(1));
        assert!(stored.policy.is_stale(hands.now()));
        hands.forward(Duration::from_secs(5));
        assert_eq!(stored.headers_as_served(hands.now())[AGE], "65");
    }

    #[tokio::test]
    async fn a_fresh_answer_is_served_without_asking_the_app_and_with_its_age() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(html("public, max-age=60"));
        let (first, outcome, _) = sent(&cache, asked("gzip, deflate, br"), &app).await;
        assert_eq!((outcome, app.woken()), (CacheOutcome::Miss, 1));
        assert_eq!(first.headers()["content-encoding"], "br");
        assert_eq!(
            app.last_heard().headers[ACCEPT_ENCODING],
            "br",
            "the app hears the negotiated coding"
        );

        hands.forward(Duration::from_secs(5));
        let (again, outcome, body) = sent(&cache, asked("br, gzip"), &app).await;
        assert_eq!(outcome, CacheOutcome::Hit);
        assert_eq!(app.woken(), 1, "a hit never reaches the app, so never wakes it");
        assert_eq!(again.status(), StatusCode::OK);
        assert_eq!(again.headers()[AGE], "5");
        assert_eq!(again.headers()["content-encoding"], "br");
        assert_eq!(again.headers()[CONTENT_LENGTH], body.len().to_string().as_str());
        assert_eq!(inflated(body, Encoding::Br).await, page());
    }

    #[tokio::test]
    async fn every_spelling_of_accept_encoding_shares_the_one_variant_it_negotiates_to() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(html("public, max-age=60"));
        sent(&cache, asked("gzip, deflate, br"), &app).await;
        for spelling in ["gzip, deflate, br, zstd", "br", "br;q=1, gzip;q=0.5"] {
            assert_eq!(
                sent(&cache, asked(spelling), &app).await.1,
                CacheOutcome::Hit,
                "{spelling}"
            );
        }
        let (gzip, outcome, _) = sent(&cache, asked("gzip"), &app).await;
        assert_eq!(
            (outcome, app.woken()),
            (CacheOutcome::Miss, 2),
            "gzip is a variant of its own"
        );
        assert_eq!(gzip.headers()["content-encoding"], "gzip");
        assert_eq!(gzip.headers()[VARY], "accept-encoding");
        assert_eq!(
            sent(&cache, asked("deflate, gzip"), &app).await.1,
            CacheOutcome::Hit
        );
    }

    #[tokio::test]
    async fn the_weight_of_what_is_stored_holds_its_cap_under_ten_times_as_much() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 1, 1024);
        let app = App::compressing(None, |_| {
            answer(
                &[
                    ("content-type", "image/png"),
                    ("cache-control", "public, max-age=600"),
                ],
                Bytes::from(vec![7u8; 100 * 1024]),
            )
        });
        for index in 0..100 {
            sent(
                &cache,
                asked_with(Method::GET, &format!("/image-{index}.png"), "dep-1", &[]),
                &app,
            )
            .await;
        }
        cache.entries.run_pending_tasks().await;
        assert!(
            cache.entries.weighted_size() <= BYTES_PER_MIB,
            "{}",
            cache.entries.weighted_size()
        );
        assert!(
            cache.entries.entry_count() >= 5,
            "{}",
            cache.entries.entry_count()
        );
    }

    #[tokio::test]
    async fn a_body_larger_than_max_entry_kib_as_stored_is_streamed_through_unstored() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 4);
        let big = Bytes::from(
            (0..64 * 1024)
                .map(|index: u32| (index * 7919 % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let served = big.clone();
        let app = App::answering(move |_| {
            answer(
                &[
                    ("content-type", "application/octet-stream"),
                    ("cache-control", "max-age=60"),
                ],
                served.clone(),
            )
        });
        let (_, _, body) = sent(&cache, asked("br"), &app).await;
        assert_eq!(body, big);
        assert_eq!(sent(&cache, asked("br"), &app).await.1, CacheOutcome::Miss);

        let compressible = App::answering(html("public, max-age=60"));
        let small_once_compressed = cache_of(&hands, 8, 4);
        assert!(page().len() > 4 * BYTES_PER_KIB);
        sent(&small_once_compressed, asked("br"), &compressible).await;
        assert_eq!(
            sent(&small_once_compressed, asked("br"), &compressible).await.1,
            CacheOutcome::Hit,
            "the cap is on the body as stored, after compression"
        );
    }

    #[tokio::test]
    async fn no_store_is_never_stored_and_a_new_deployment_never_sees_the_old_one() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(html("no-store"));
        sent(&cache, asked("br"), &app).await;
        assert_eq!(sent(&cache, asked("br"), &app).await.1, CacheOutcome::Miss);

        let app = App::answering(html("public, max-age=60"));
        sent(&cache, asked("br"), &app).await;
        assert_eq!(sent(&cache, asked("br"), &app).await.1, CacheOutcome::Hit);
        let (_, outcome, _) = sent(
            &cache,
            asked_with(Method::GET, "/", "dep-2", &[("accept-encoding", "br")]),
            &app,
        )
        .await;
        assert_eq!(outcome, CacheOutcome::Miss);
    }

    #[tokio::test]
    async fn an_authorized_request_is_not_cached_unless_the_app_says_public() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let authorized = || asked_with(Method::GET, "/", "dep-1", &[("authorization", "Bearer secret")]);
        let private = App::answering(html("max-age=60"));
        sent(&cache, authorized(), &private).await;
        assert_eq!(sent(&cache, authorized(), &private).await.1, CacheOutcome::Miss);

        let public = App::answering(html("public, max-age=60"));
        sent(&cache, authorized(), &public).await;
        assert_eq!(sent(&cache, authorized(), &public).await.1, CacheOutcome::Hit);
    }

    #[tokio::test]
    async fn no_transform_and_images_are_stored_as_the_app_sent_them() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        for (content_type, cache_control) in [
            ("text/html", "public, max-age=60, no-transform"),
            ("image/png", "public, max-age=60"),
        ] {
            let app = App::answering(move |_| {
                answer(
                    &[("content-type", content_type), ("cache-control", cache_control)],
                    page(),
                )
            });
            let path = format!("/{}", content_type.replace('/', "-"));
            let ask = || asked_with(Method::GET, &path, "dep-1", &[("accept-encoding", "br")]);
            let (passed, _, body) = sent(&cache, ask(), &app).await;
            assert!(
                !passed.headers().contains_key("content-encoding"),
                "{content_type}"
            );
            assert_eq!(body, page());
            assert_eq!(
                sent(&cache, ask(), &app).await.1,
                CacheOutcome::Hit,
                "{content_type}"
            );
        }
    }

    #[tokio::test]
    async fn without_compression_the_cache_stores_identity_and_still_hits() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::compressing(None, html("public, max-age=60"));
        let identity = || {
            let (mut parts, ()) = Request::builder()
                .uri("/")
                .header("accept-encoding", "gzip, br")
                .body(())
                .unwrap()
                .into_parts();
            encode::as_negotiated(&mut parts.headers, Encoding::Identity);
            Asked::new(
                parts,
                DeploymentId::parse("dep-1").unwrap(),
                "app-1.apps.example.com",
                Encoding::Identity,
            )
        };
        let (first, _, body) = sent(&cache, identity(), &app).await;
        assert!(!first.headers().contains_key("content-encoding"));
        assert_eq!(body, page());
        assert_eq!(
            first.headers()[ETAG],
            "\"v1\"",
            "an etag on a body sent as it was stays strong"
        );
        let (_, outcome, body) = sent(&cache, identity(), &app).await;
        assert_eq!((outcome, body, app.woken()), (CacheOutcome::Hit, page(), 1));
    }

    #[tokio::test]
    async fn a_body_that_says_it_has_ended_before_it_is_polled_is_still_kept() {
        let kept = Arc::new(Mutex::new(None));
        let into = kept.clone();
        let _tee = Tee::new(empty(), 16, move |body| *into.lock().unwrap() = Some(body));
        assert_eq!(*kept.lock().unwrap(), Some(Bytes::new()));
    }
}
