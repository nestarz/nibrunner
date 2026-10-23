use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use http_body_util::{BodyExt, Empty, Full};
use http_cache_semantics::{AfterResponse, BeforeRequest, CacheOptions, CachePolicy};
use hyper::body::{Body, Frame, SizeHint};
use hyper::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT_ENCODING, AGE, AUTHORIZATION, CACHE_CONTROL, CONTENT_ENCODING,
    CONTENT_LENGTH, CONTENT_LOCATION, CONTENT_RANGE, DATE, ETAG, EXPIRES, HOST, IF_MATCH, IF_MODIFIED_SINCE,
    IF_NONE_MATCH, IF_RANGE, IF_UNMODIFIED_SINCE, LAST_MODIFIED, LOCATION, PRAGMA, RANGE, SET_COOKIE,
    TRANSFER_ENCODING, UPGRADE, VARY,
};
use hyper::http::{request, response};
use hyper::{Method, Request, Response, StatusCode};
use moka::ops::compute::{CompResult, Op};
use protocol::{AppId, DeploymentId, InstanceLabels};

use crate::adapters::proxy::encode::{self, Encoding};
use crate::adapters::proxy::forward::{ProxyBody, Unreachable};
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

impl Key {
    fn coded(&self, encoding: Encoding) -> Self {
        Self {
            encoding,
            ..self.clone()
        }
    }
}

/// When a stale answer may still be given, past its lifetime: RFC 5861's two extensions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Windows {
    while_revalidating: Duration,
    if_error: Duration,
}

// RFC 9213's field for every CDN, which Deno Deploy's cache reads too: Autokit sends its pages
// `cache-control: public, max-age=0, must-revalidate` for the browser beside
// `cdn-cache-control: public, s-maxage=86400, stale-while-revalidate=60, stale-if-error=86400`.
const CDN_CACHE_CONTROL: HeaderName = HeaderName::from_static("cdn-cache-control");

fn targeted(headers: &HeaderMap) -> bool {
    headers
        .get_all(CDN_CACHE_CONTROL)
        .iter()
        .any(|value| !value.as_bytes().trim_ascii().is_empty())
}

/// The fields a shared cache reads its policy from. A response with `CDN-Cache-Control` is read by
/// it alone, `Cache-Control` and `Expires` left to the browser (RFC 9213 §2.1); one without is
/// read as it came.
fn governing(headers: &HeaderMap) -> HeaderMap {
    let mut governing = headers.clone();
    if targeted(headers) {
        governing.remove(CACHE_CONTROL);
        governing.remove(EXPIRES);
        for value in headers.get_all(CDN_CACHE_CONTROL) {
            governing.append(CACHE_CONTROL, value.clone());
        }
    }
    governing
}

/// A response with `headers` as the policy reads it.
fn governed(status: StatusCode, headers: &HeaderMap) -> Response<()> {
    let mut governed = Response::new(());
    *governed.status_mut() = status;
    *governed.headers_mut() = governing(headers);
    governed
}

fn directives(headers: &HeaderMap) -> Vec<(String, Option<String>)> {
    let field = if targeted(headers) {
        CDN_CACHE_CONTROL
    } else {
        CACHE_CONTROL
    };
    headers
        .get_all(field)
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

fn seconds(directives: &[(String, Option<String>)], name: &str) -> Duration {
    directives
        .iter()
        .find(|(named, _)| named == name)
        .and_then(|(_, value)| value.as_deref()?.parse().ok())
        .map_or(Duration::ZERO, Duration::from_secs)
}

impl Windows {
    // RFC 9111 §4.2.4: these four forbid a shared cache from ever giving a stale answer. The
    // `s-maxage` among them is `Cache-Control`'s: in `CDN-Cache-Control` it is only the lifetime.
    fn of(headers: &HeaderMap) -> Self {
        let directives = directives(headers);
        let forbidding: &[&str] = if targeted(headers) {
            &["no-cache", "must-revalidate", "proxy-revalidate"]
        } else {
            &["no-cache", "must-revalidate", "proxy-revalidate", "s-maxage"]
        };
        if forbidding.iter().any(|forbidding| says(&directives, forbidding)) {
            return Self::default();
        }
        Self {
            while_revalidating: seconds(&directives, "stale-while-revalidate"),
            if_error: seconds(&directives, "stale-if-error"),
        }
    }

    fn widest(self) -> Duration {
        self.while_revalidating.max(self.if_error)
    }
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
    matches!(*request.method(), Method::GET | Method::HEAD)
        && !headers.contains_key(RANGE)
        && !headers.contains_key(UPGRADE)
        && !has_body
}

/// A method RFC 9111 §4.4 says invalidates what is stored for its target once it succeeds.
pub(crate) fn invalidating(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

const CONDITIONALS: [HeaderName; 5] = [
    IF_NONE_MATCH,
    IF_MODIFIED_SINCE,
    IF_MATCH,
    IF_UNMODIFIED_SINCE,
    IF_RANGE,
];

/// What the visitor already holds, which the cache answers with a 304 when it is still current.
#[derive(Debug, Clone, Default)]
struct Held {
    if_none_match: Option<String>,
    if_modified_since: Option<SystemTime>,
}

impl Held {
    fn of(headers: &HeaderMap) -> Self {
        Self {
            if_none_match: headers
                .get(IF_NONE_MATCH)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            if_modified_since: headers
                .get(IF_MODIFIED_SINCE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| httpdate::parse_http_date(value).ok()),
        }
    }

    // `If-None-Match` compares weakly (RFC 9110 §13.1.2), which is what lets a 304 answer an etag
    // this proxy weakened when it compressed the body.
    fn is_current(&self, status: StatusCode, headers: &HeaderMap) -> bool {
        if status != StatusCode::OK {
            return false;
        }
        let weak = |etag: &str| etag.trim().trim_start_matches("W/").to_string();
        if let Some(listed) = &self.if_none_match {
            let Some(etag) = headers.get(ETAG).and_then(|value| value.to_str().ok()) else {
                return false;
            };
            return listed
                .split(',')
                .any(|held| held.trim() == "*" || weak(held) == weak(etag));
        }
        match (
            self.if_modified_since,
            headers
                .get(LAST_MODIFIED)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| httpdate::parse_http_date(value).ok()),
        ) {
            (Some(since), Some(modified)) => modified <= since,
            _ => false,
        }
    }
}

/// Whose answers an entry holds: the app a request was routed to, whose counters it moves, and
/// the owner a purge names. Every timeline of an app shares its tags, so the owner is the `app`
/// label a control plane gives each of them, and the app's own id when it gives none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Tenant {
    app_id: AppId,
    deployment_id: DeploymentId,
    owner: Arc<str>,
}

impl Tenant {
    pub(crate) fn of(app_id: &AppId, deployment_id: &DeploymentId, labels: &InstanceLabels) -> Self {
        let owner = labels
            .iter()
            .find(|(key, _)| key.as_str() == "app")
            .map_or(app_id.as_str(), |(_, app)| app.as_str());
        Self {
            app_id: app_id.clone(),
            deployment_id: deployment_id.clone(),
            owner: Arc::from(owner),
        }
    }

    pub(crate) fn owner(&self) -> &str {
        &self.owner
    }
}

/// A request the cache may answer, read the way it keys and matches.
pub(crate) struct Asked {
    key: Key,
    tenant: Tenant,
    head: bool,
    visitor: request::Parts,
    canonical: request::Parts,
    held: Held,
}

impl Asked {
    /// `visitor`'s `Accept-Encoding` already says the one coding it was given.
    pub(crate) fn new(visitor: request::Parts, tenant: Tenant, authority: &str, encoding: Encoding) -> Self {
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
                deployment_id: tenant.deployment_id.clone(),
                authority: authority.to_string(),
                target,
                encoding,
            },
            tenant,
            head: visitor.method == Method::HEAD,
            held: Held::of(&visitor.headers),
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

/// How a stored body came to be the one stored.
#[derive(Debug, Clone, Default)]
struct Provenance {
    /// The app's own `ETag`, which revalidation sends back even when the served one was weakened.
    origin_etag: Option<HeaderValue>,
    /// Whether this proxy compressed it, so that headers refreshed by a 304 are made to say so again.
    recoded: bool,
}

impl Provenance {
    fn of(parts: &response::Parts) -> Self {
        match parts.extensions.get::<encode::Recoded>() {
            Some(recoded) => Self {
                origin_etag: recoded.origin_etag.clone(),
                recoded: true,
            },
            None => Self {
                origin_etag: parts.headers.get(ETAG).cloned(),
                recoded: false,
            },
        }
    }
}

/// One stored answer: the policy the crate reads it by, the headers it is served with, the body
/// as stored.
pub(crate) struct Stored {
    policy: CachePolicy,
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    encoding: Encoding,
    provenance: Provenance,
    windows: Windows,
    fresh_until: SystemTime,
    revalidating: AtomicBool,
    tags: Vec<Box<str>>,
    weight: u32,
}

fn weight_of(headers: &HeaderMap) -> usize {
    headers
        .iter()
        .map(|(name, value)| name.as_str().len() + value.len() + HEADER_OVERHEAD)
        .sum()
}

impl Stored {
    fn staleness(&self, now: SystemTime) -> Duration {
        now.duration_since(self.fresh_until).unwrap_or_default()
    }

    fn lifetime(&self, now: SystemTime) -> Duration {
        self.fresh_until
            .duration_since(now)
            .unwrap_or_default()
            .saturating_add(self.windows.widest())
    }

    fn claim_revalidation(&self) -> bool {
        !self.revalidating.swap(true, Ordering::AcqRel)
    }

    fn headers_as_served(&self, now: SystemTime) -> HeaderMap {
        let mut headers = self.headers.clone();
        headers.insert(AGE, HeaderValue::from(self.policy.age(now).as_secs()));
        if let Ok(date) = HeaderValue::from_str(&httpdate::fmt_http_date(now)) {
            headers.insert(DATE, date);
        }
        headers
    }

    /// The answer to `asked`, decoded from `self.encoding` when `decode` says so, or a 304 when
    /// what the visitor holds is still this.
    fn serve(&self, asked: &Asked, now: SystemTime, decode: bool) -> Response<ProxyBody> {
        let mut headers = self.headers_as_served(now);
        if asked.held.is_current(self.status, &headers) {
            let mut not_modified = Response::new(empty());
            *not_modified.status_mut() = StatusCode::NOT_MODIFIED;
            for name in [
                CACHE_CONTROL,
                CONTENT_LOCATION,
                DATE,
                ETAG,
                EXPIRES,
                VARY,
                AGE,
                LAST_MODIFIED,
            ] {
                if let Some(value) = headers.get(&name) {
                    not_modified.headers_mut().insert(name, value.clone());
                }
            }
            return not_modified;
        }
        let body = match (asked.head, decode) {
            (true, _) => empty(),
            (false, true) => encode::decoded(self.body.clone(), self.encoding),
            (false, false) => whole(self.body.clone()),
        };
        if decode {
            encode::as_encoded(&mut headers, Encoding::Identity);
        }
        let mut served = Response::new(body);
        *served.status_mut() = self.status;
        *served.headers_mut() = headers;
        served
    }
}

const CACHE_TAG: HeaderName = HeaderName::from_static("cache-tag");
const SURROGATE_KEY: HeaderName = HeaderName::from_static("surrogate-key");
const MOST_TAGS: usize = 128;
const LONGEST_TAG: usize = 1024;
// What one tag of one key costs in the index beside the tag and the key themselves.
const INDEX_ENTRY_OVERHEAD: usize = 64;

/// What a purge may name a stored answer by: `Cache-Tag`, comma separated as Deno Deploy and
/// Cloudflare read it, and `Surrogate-Key`, space separated as Fastly does. None for an answer
/// with more tags, or longer ones, than the index holds, or one it cannot read: no purge could be
/// trusted to reach it.
fn tags_of(headers: &HeaderMap) -> Option<Vec<Box<str>>> {
    let mut tags: Vec<Box<str>> = Vec::new();
    for (name, separator) in [(CACHE_TAG, ','), (SURROGATE_KEY, ' ')] {
        for value in headers.get_all(name) {
            let listed = value.to_str().ok()?.split(separator).map(str::trim);
            tags.extend(listed.filter(|tag| !tag.is_empty()).map(Box::from));
        }
    }
    tags.sort_unstable();
    tags.dedup();
    (tags.len() <= MOST_TAGS && tags.iter().all(|tag| tag.len() <= LONGEST_TAG)).then_some(tags)
}

type Generation = u64;

/// The keys each tag of each owner is stored under, so a purge finds them without a scan. A key is
/// held with the generation of the variants that put it there: the eviction of those variants, or
/// their replacement by newer ones, then takes away only what they put.
#[derive(Default)]
struct TagIndex(HashMap<Arc<str>, HashMap<Box<str>, HashMap<Key, Generation>>>);

impl TagIndex {
    fn add(&mut self, key: &Key, variants: &Variants) {
        if variants.tags.is_empty() {
            return;
        }
        let owned = self.0.entry(variants.tenant.owner.clone()).or_default();
        for tag in &variants.tags {
            owned
                .entry(tag.clone())
                .or_default()
                .insert(key.clone(), variants.generation);
        }
    }

    fn forget(&mut self, key: &Key, variants: &Variants) {
        let Some(owned) = self.0.get_mut(&variants.tenant.owner) else {
            return;
        };
        for tag in &variants.tags {
            if let Some(keys) = owned.get_mut(tag) {
                if keys.get(key) == Some(&variants.generation) {
                    keys.remove(key);
                }
                if keys.is_empty() {
                    owned.remove(tag);
                }
            }
        }
        if owned.is_empty() {
            self.0.remove(&variants.tenant.owner);
        }
    }

    /// Every key `owner` stored under any of `tags`, each with the newest generation that put it
    /// there, taken out of the index.
    fn take(&mut self, owner: &str, tags: &[String]) -> HashMap<Key, Generation> {
        let Some(owned) = self.0.get_mut(owner) else {
            return HashMap::new();
        };
        let mut taken: HashMap<Key, Generation> = HashMap::new();
        for (key, generation) in tags.iter().filter_map(|tag| owned.remove(tag.as_str())).flatten() {
            let newest = taken.entry(key).or_insert(generation);
            *newest = (*newest).max(generation);
        }
        if owned.is_empty() {
            self.0.remove(owner);
        }
        taken
    }
}

fn index_weight(key: &Key, tags: &[Box<str>]) -> usize {
    let key_weight = key.deployment_id.as_str().len() + key.authority.len() + key.target.len();
    tags.iter()
        .map(|tag| tag.len() + key_weight + INDEX_ENTRY_OVERHEAD)
        .sum()
}

/// Every answer stored under one key, one per `Vary` variant, newest first, and the tags any of
/// them carries.
pub(crate) struct Variants {
    stored: Vec<Arc<Stored>>,
    tenant: Tenant,
    tags: Vec<Box<str>>,
    generation: Generation,
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

enum Found {
    Fresh(Arc<Stored>),
    Stale(Arc<Stored>, Box<request::Parts>),
}

enum Revalidated {
    Refreshed(Arc<Stored>),
    Replaced(Response<ProxyBody>),
    Failed(Response<ProxyBody>),
    /// A 304 that does not answer for what is stored, or one that made it unstorable.
    Unmatched,
}

/// On an answer the cache is keeping as it passes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Kept;

const CACHE_STATUS: HeaderName = HeaderName::from_static("cache-status");

/// RFC 9211's account of what the cache called `name` did, added after any an app behind it gave.
/// Spelled as Deno Deploy's cache spells the same outcomes for the same Autokit pages:
/// `deno; hit`, `deno; fwd=miss; stored`, `deno; fwd=bypass; detail=not-cacheable`.
pub(crate) fn report(response: &mut Response<ProxyBody>, name: &str, outcome: CacheOutcome) {
    let status = match outcome {
        CacheOutcome::Hit => format!("{name}; hit"),
        CacheOutcome::Stale => format!("{name}; hit; detail=stale"),
        CacheOutcome::Revalidated => format!("{name}; fwd=stale; fwd-status=304"),
        CacheOutcome::Miss if response.extensions().get::<Kept>().is_some() => {
            format!("{name}; fwd=miss; stored")
        }
        CacheOutcome::Miss => format!("{name}; fwd=miss"),
        CacheOutcome::Bypass => format!("{name}; fwd=bypass"),
    };
    let status = match response
        .headers()
        .get(CACHE_STATUS)
        .and_then(|value| value.to_str().ok())
    {
        Some(behind) => format!("{behind}, {status}"),
        None => status,
    };
    if let Ok(status) = HeaderValue::from_str(&status) {
        response.headers_mut().insert(CACHE_STATUS, status);
    }
}

/// What the cache answered, and how.
pub(crate) struct Answered {
    pub(crate) response: Response<ProxyBody>,
    pub(crate) outcome: CacheOutcome,
}

pub(crate) struct ResponseCache {
    entries: moka::future::Cache<Key, Arc<Variants>>,
    tags: Arc<Mutex<TagIndex>>,
    generations: AtomicU64,
    max_entry_bytes: usize,
    clock: Clock,
    metrics: Arc<HostMetrics>,
}

fn locked(tags: &Mutex<TagIndex>) -> std::sync::MutexGuard<'_, TagIndex> {
    tags.lock().unwrap_or_else(PoisonError::into_inner)
}

fn failed(response: &Response<ProxyBody>) -> bool {
    response.extensions().get::<Unreachable>().is_some() || response.status().is_server_error()
}

fn explicit_lifetime(headers: &HeaderMap) -> bool {
    let directives = directives(headers);
    says(&directives, "s-maxage")
        || says(&directives, "max-age")
        || (!targeted(headers) && headers.contains_key(EXPIRES))
}

impl ResponseCache {
    pub(crate) fn new(settings: &HttpCache, clock: Clock, metrics: Arc<HostMetrics>) -> Arc<Self> {
        let tags = Arc::new(Mutex::new(TagIndex::default()));
        let forgetting = tags.clone();
        Arc::new(Self {
            entries: moka::future::Cache::builder()
                .max_capacity(u64::from(settings.memory_mib.get()) * BYTES_PER_MIB)
                .weigher(|_, variants: &Arc<Variants>| variants.weight)
                .expire_after(UntilTheLastVariantExpires)
                .eviction_listener(move |key, variants: Arc<Variants>, _| {
                    locked(&forgetting).forget(&key, &variants);
                })
                .build(),
            tags,
            generations: AtomicU64::new(0),
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

    /// A request the cache may answer: a stored answer when there is one it may give, the app's
    /// otherwise, kept on its way to the visitor when the app made it storable.
    pub(crate) async fn answer(self: &Arc<Self>, asked: Asked, origin: Arc<dyn Origin>) -> Answered {
        let now = (self.clock)();
        match self.find(&asked.key, &asked.canonical, now).await {
            Some(Found::Fresh(stored)) => self.hit(&asked, &stored, now, false),
            Some(Found::Stale(_, _)) if asked.head => self.miss(asked, origin).await,
            Some(Found::Stale(stored, revalidation)) => {
                if stored.staleness(now) <= stored.windows.while_revalidating {
                    self.stale_while_revalidating(asked, stored, *revalidation, origin, now)
                } else {
                    self.revalidate_inline(asked, stored, *revalidation, origin).await
                }
            }
            None => match self.coded_for_identity(&asked, now).await {
                Some(stored) => {
                    let decode = stored.encoding != Encoding::Identity;
                    self.hit(&asked, &stored, now, decode)
                }
                None => self.miss(asked, origin).await,
            },
        }
    }

    async fn find(&self, key: &Key, canonical: &request::Parts, now: SystemTime) -> Option<Found> {
        let variants = self.entries.get(key).await?;
        let mut stale = None;
        for stored in &variants.stored {
            match stored.policy.before_request(canonical, now) {
                BeforeRequest::Fresh(_) => return Some(Found::Fresh(stored.clone())),
                BeforeRequest::Stale {
                    request,
                    matches: true,
                } if stale.is_none() => stale = Some(Found::Stale(stored.clone(), Box::new(request))),
                BeforeRequest::Stale { .. } => {}
            }
        }
        stale
    }

    // An identity visitor is served from a stored gzip, then a stored br, by decoding it as it is
    // sent: one inflate pass instead of a wake.
    async fn coded_for_identity(&self, asked: &Asked, now: SystemTime) -> Option<Arc<Stored>> {
        if asked.key.encoding != Encoding::Identity {
            return None;
        }
        for coded in [Encoding::Gzip, Encoding::Br] {
            let mut canonical = asked.canonical.clone();
            encode::as_negotiated(&mut canonical.headers, coded);
            // What the app compressed itself is the app's to decode, and is never decoded here.
            if let Some(Found::Fresh(stored)) = self.find(&asked.key.coded(coded), &canonical, now).await {
                if stored.provenance.recoded || stored.encoding == Encoding::Identity {
                    return Some(stored);
                }
            }
        }
        None
    }

    /// Flow 1: fresh, answered from memory without the app.
    fn hit(&self, asked: &Asked, stored: &Stored, now: SystemTime, decode: bool) -> Answered {
        Answered {
            response: stored.serve(asked, now, decode),
            outcome: CacheOutcome::Hit,
        }
    }

    /// Flow 2: asked of the app, and kept once its body has gone to the visitor whole.
    async fn miss(self: &Arc<Self>, asked: Asked, origin: Arc<dyn Origin>) -> Answered {
        let requested_at = (self.clock)();
        let response = origin.ask(asked.forwarded()).await;
        let response = if asked.head {
            response
        } else {
            self.keeping(&asked, origin.recode(response, asked.key.encoding), requested_at)
        };
        Answered {
            response,
            outcome: CacheOutcome::Miss,
        }
    }

    /// Flow 3: stale within `stale-while-revalidate`, given at once, revalidated behind it by one
    /// request to the app however many visitors arrive meanwhile.
    fn stale_while_revalidating(
        self: &Arc<Self>,
        asked: Asked,
        stored: Arc<Stored>,
        revalidation: request::Parts,
        origin: Arc<dyn Origin>,
        now: SystemTime,
    ) -> Answered {
        let response = stored.serve(&asked, now, false);
        if stored.claim_revalidation() {
            let cache = self.clone();
            tokio::spawn(async move {
                if let Revalidated::Replaced(response) =
                    cache.revalidated(&asked, &stored, revalidation, origin).await
                {
                    let _ = response.into_body().collect().await;
                }
                stored.revalidating.store(false, Ordering::Release);
            });
        }
        Answered {
            response,
            outcome: CacheOutcome::Stale,
        }
    }

    /// Flows 4 and 5: stale past `stale-while-revalidate`, revalidated before answering; an app
    /// that fails or cannot be woken is covered for by the stale answer within `stale-if-error`.
    async fn revalidate_inline(
        self: &Arc<Self>,
        asked: Asked,
        stored: Arc<Stored>,
        revalidation: request::Parts,
        origin: Arc<dyn Origin>,
    ) -> Answered {
        let miss = |response| Answered {
            response,
            outcome: CacheOutcome::Miss,
        };
        let origin_again = origin.clone();
        match self.revalidated(&asked, &stored, revalidation, origin).await {
            Revalidated::Refreshed(refreshed) => Answered {
                response: refreshed.serve(&asked, (self.clock)(), false),
                outcome: CacheOutcome::Revalidated,
            },
            Revalidated::Replaced(response) => miss(response),
            Revalidated::Unmatched => self.miss(asked, origin_again).await,
            Revalidated::Failed(response) => {
                let now = (self.clock)();
                if stored.staleness(now) <= stored.windows.if_error {
                    Answered {
                        response: stored.serve(&asked, now, false),
                        outcome: CacheOutcome::Stale,
                    }
                } else {
                    miss(response)
                }
            }
        }
    }

    async fn revalidated(
        self: &Arc<Self>,
        asked: &Asked,
        stored: &Stored,
        mut revalidation: request::Parts,
        origin: Arc<dyn Origin>,
    ) -> Revalidated {
        if let Some(etag) = &stored.provenance.origin_etag {
            revalidation.headers.insert(IF_NONE_MATCH, etag.clone());
        }
        let requested_at = (self.clock)();
        let response = origin
            .ask(Request::from_parts(revalidation.clone(), empty()))
            .await;
        if failed(&response) {
            return Revalidated::Failed(response);
        }
        if response.status() == StatusCode::NOT_MODIFIED {
            if let AfterResponse::NotModified(_, mut refreshed) =
                stored
                    .policy
                    .after_response(&revalidation, &response, requested_at)
            {
                // The policy's own refresh is of the fields it reads, which for a response with
                // `CDN-Cache-Control` are not the ones the browser is given.
                refreshed.headers = refreshed_as_served(&stored.headers, response.headers());
                if stored.provenance.recoded {
                    encode::as_encoded(&mut refreshed.headers, stored.encoding);
                }
                refreshed
                    .headers
                    .insert(CONTENT_LENGTH, HeaderValue::from(stored.body.len()));
                refreshed.headers.remove(AGE);
                let now = (self.clock)();
                if let Some(refreshed) = self.stored(
                    &asked.canonical,
                    refreshed,
                    stored.body.clone(),
                    stored.provenance.clone(),
                    requested_at,
                    now,
                ) {
                    let refreshed = Arc::new(refreshed);
                    self.keep(
                        asked.key.clone(),
                        &asked.tenant,
                        &asked.canonical,
                        refreshed.clone(),
                        now,
                    )
                    .await;
                    return Revalidated::Refreshed(refreshed);
                }
            }
            return Revalidated::Unmatched;
        }
        Revalidated::Replaced(self.keeping(asked, origin.recode(response, asked.key.encoding), requested_at))
    }

    /// Flow 6: a `POST`, `PUT`, `PATCH` or `DELETE` that succeeded makes what is stored for its
    /// target, and for the `Location` and `Content-Location` it names on this authority, unusable.
    pub(crate) async fn invalidate(
        &self,
        deployment_id: &DeploymentId,
        authority: &str,
        target: &str,
        response: &Response<ProxyBody>,
    ) {
        if !(response.status().is_success() || response.status().is_redirection()) {
            return;
        }
        let named = [LOCATION, CONTENT_LOCATION].into_iter().filter_map(|header| {
            let uri: hyper::Uri = response.headers().get(header)?.to_str().ok()?.parse().ok()?;
            let same_authority = uri.host().is_none_or(|host| host.eq_ignore_ascii_case(authority));
            let path = uri.path_and_query()?.as_str();
            (same_authority && path.starts_with('/')).then(|| path.to_string())
        });
        for target in std::iter::once(target.to_string()).chain(named) {
            for encoding in Encoding::ALL {
                self.entries
                    .invalidate(&Key {
                        deployment_id: deployment_id.clone(),
                        authority: authority.to_string(),
                        target: target.clone(),
                        encoding,
                    })
                    .await;
            }
        }
        self.weighed().await;
    }

    fn stored(
        &self,
        canonical: &request::Parts,
        mut response: response::Parts,
        body: Bytes,
        provenance: Provenance,
        requested_at: SystemTime,
        now: SystemTime,
    ) -> Option<Stored> {
        let encoding = Encoding::of(&response.headers)?;
        let tags = tags_of(&response.headers)?;
        // Given the time the request left rather than the time the answer came, the crate's
        // `age_value + resident_time` is RFC 9111 §4.2.3's `age_value + response_delay +
        // resident_time`, without the apparent age a skewed `Date` would add.
        let policy = CachePolicy::new_options(
            canonical,
            &governed(response.status, &response.headers),
            requested_at,
            options(),
        );
        let windows = Windows::of(&response.headers);
        let fresh_until = now + policy.time_to_live(now);
        let storable = policy.is_storable()
            && explicit_lifetime(&response.headers)
            && !response.headers.contains_key(SET_COOKIE)
            && !response.headers.contains_key(CONTENT_RANGE)
            && response.status != StatusCode::PARTIAL_CONTENT
            && body.len() <= self.max_entry_bytes
            && policy.time_to_live(now) + windows.widest() > Duration::ZERO;
        if !storable {
            return None;
        }
        response.headers.remove(hyper::header::CONNECTION);
        let kept = matched_by(canonical, &response.headers);
        let policy = CachePolicy::new_options(
            &kept,
            &governed(response.status, &response.headers),
            requested_at,
            options(),
        );
        let weight = weight_of(&response.headers) * 2
            + weight_of(&kept.headers)
            + body.len()
            + tags.iter().map(|tag| tag.len()).sum::<usize>();
        Some(Stored {
            policy,
            status: response.status,
            headers: response.headers,
            body,
            encoding,
            provenance,
            windows,
            fresh_until,
            revalidating: AtomicBool::new(false),
            tags,
            weight: u32::try_from(weight).unwrap_or(u32::MAX),
        })
    }

    async fn keep(
        &self,
        key: Key,
        tenant: &Tenant,
        canonical: &request::Parts,
        stored: Arc<Stored>,
        now: SystemTime,
    ) {
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
        let mut tags: Vec<Box<str>> = stored.iter().flat_map(|stored| stored.tags.clone()).collect();
        tags.sort_unstable();
        tags.dedup();
        let indexed = u32::try_from(index_weight(&key, &tags)).unwrap_or(u32::MAX);
        let variants = Arc::new(Variants {
            lifetime: stored
                .iter()
                .map(|stored| stored.lifetime(now))
                .max()
                .unwrap_or_default(),
            weight: stored
                .iter()
                .fold(indexed, |weight, stored| weight.saturating_add(stored.weight)),
            stored,
            tenant: tenant.clone(),
            tags,
            generation: self.generations.fetch_add(1, Ordering::Relaxed),
        });
        // Indexed under the key's own lock, so a purge that finds it in the index waits for it to
        // be there to remove.
        let index = self.tags.clone();
        self.entries
            .entry(key.clone())
            .and_compute_with(|_| {
                locked(&index).add(&key, &variants);
                std::future::ready(Op::Put(variants))
            })
            .await;
        self.weighed().await;
    }

    /// A purge: every entry `owner` stored under one of `tags` removed, and how many there were.
    pub(crate) async fn purge(&self, owner: &str, tags: &[String]) -> usize {
        let taken = locked(&self.tags).take(owner, tags);
        let mut purged = 0;
        for (key, generation) in taken {
            let removed = self
                .entries
                .entry(key)
                .and_compute_with(|held| {
                    std::future::ready(match held {
                        Some(held) if held.value().generation == generation => Op::Remove,
                        _ => Op::Nop,
                    })
                })
                .await;
            if let CompResult::Removed(entry) = removed {
                self.metrics.proxy.purged(&entry.value().tenant.app_id);
                purged += 1;
            }
        }
        self.weighed().await;
        purged
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
        let probe = CachePolicy::new_options(
            &asked.canonical,
            &governed(response.status(), response.headers()),
            requested_at,
            options(),
        );
        if !probe.is_storable()
            || !explicit_lifetime(response.headers())
            || response.headers().contains_key(SET_COOKIE)
            || tags_of(response.headers()).is_none()
        {
            return response;
        }
        let (mut parts, body) = response.into_parts();
        parts.extensions.insert(Kept);
        let headers = parts.headers.clone();
        let status = parts.status;
        let provenance = Provenance::of(&parts);
        let cache = self.clone();
        let key = asked.key.clone();
        let tenant = asked.tenant.clone();
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
                if let Some(stored) = cache.stored(&canonical, response, body, provenance, requested_at, now)
                {
                    cache.keep(key, &tenant, &canonical, Arc::new(stored), now).await;
                }
            });
        });
        Response::from_parts(parts, kept.boxed())
    }
}

/// What a 304 makes of the headers a stored answer is served with: each field it carries replaces
/// the stored one, but for those that describe a body it did not send (RFC 9111 §3.2).
fn refreshed_as_served(served: &HeaderMap, not_modified: &HeaderMap) -> HeaderMap {
    let mut refreshed = served.clone();
    for name in not_modified.keys() {
        if [CONTENT_LENGTH, CONTENT_ENCODING, TRANSFER_ENCODING, CONTENT_RANGE].contains(name) {
            continue;
        }
        refreshed.remove(name);
        for value in not_modified.get_all(name) {
            refreshed.append(name.clone(), value.clone());
        }
    }
    refreshed
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
    use std::sync::atomic::AtomicUsize;
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

    fn tenant(deployment: &str) -> Tenant {
        Tenant::of(
            &AppId::parse("app-1").unwrap(),
            &DeploymentId::parse(deployment).unwrap(),
            &InstanceLabels::default(),
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
        Asked::new(parts, tenant(deployment), "app-1.apps.example.com", encoding)
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
                    Provenance::default(),
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
                    Provenance::default(),
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
                false,
            ),
            (&[("cache-control", "max-age=0")], false),
        ];
        for (headers, expected) in table {
            assert_eq!(storable(headers), expected, "{headers:?}");
        }
        assert!(storable(&[(
            "cache-control",
            "max-age=0, stale-while-revalidate=30"
        )]));
        assert!(!storable_for(&mut authorized, "max-age=60"));
        assert!(storable_for(&mut authorized, "public, max-age=60"));
        assert!(storable_for(&mut authorized, "s-maxage=60"));
        let (partial, _) = answer(&[("cache-control", "max-age=60")], Bytes::new()).into_parts();
        let mut partial = partial;
        partial.status = StatusCode::PARTIAL_CONTENT;
        assert!(cache
            .stored(
                &canonical,
                partial,
                Bytes::new(),
                Provenance::default(),
                hands.now(),
                hands.now()
            )
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
            .stored(
                &canonical,
                parts,
                Bytes::new(),
                Provenance::default(),
                requested_at,
                hands.now(),
            )
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
        assert_eq!(stored.staleness(hands.now()), Duration::ZERO);
        hands.forward(Duration::from_secs(5));
        assert_eq!(stored.staleness(hands.now()), Duration::from_secs(5));
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
    async fn a_visitor_that_accepts_no_coding_is_served_the_stored_gzip_decoded_without_a_wake() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(html("public, max-age=60"));
        sent(&cache, asked("gzip"), &app).await;
        let (plain, outcome, body) = sent(&cache, asked_with(Method::GET, "/", "dep-1", &[]), &app).await;
        assert_eq!((outcome, app.woken()), (CacheOutcome::Hit, 1));
        assert!(!plain.headers().contains_key("content-encoding"));
        assert_eq!(body, page());
    }

    #[tokio::test]
    async fn a_strong_etag_comes_back_weak_and_a_visitor_holding_it_is_answered_304() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(html("public, max-age=60"));
        let (first, _, _) = sent(&cache, asked("br"), &app).await;
        assert_eq!(first.headers()[ETAG], "W/\"v1\"");
        for held in ["W/\"v1\"", "\"v1\"", "\"v0\", W/\"v1\""] {
            let (not_modified, outcome, body) = sent(
                &cache,
                asked_with(
                    Method::GET,
                    "/",
                    "dep-1",
                    &[("accept-encoding", "br"), ("if-none-match", held)],
                ),
                &app,
            )
            .await;
            assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED, "{held}");
            assert_eq!(outcome, CacheOutcome::Hit);
            assert!(body.is_empty());
            assert_eq!(not_modified.headers()[ETAG], "W/\"v1\"");
        }
        let (changed, _, _) = sent(
            &cache,
            asked_with(
                Method::GET,
                "/",
                "dep-1",
                &[("accept-encoding", "br"), ("if-none-match", "\"v0\"")],
            ),
            &app,
        )
        .await;
        assert_eq!(changed.status(), StatusCode::OK);
        assert_eq!(app.woken(), 1);
    }

    #[tokio::test]
    async fn a_visitor_holding_the_last_modified_date_is_answered_304() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(|_| {
            answer(
                &[
                    ("content-type", "text/plain"),
                    ("cache-control", "max-age=60"),
                    ("last-modified", "Wed, 21 Oct 2026 07:28:00 GMT"),
                ],
                Bytes::from_static(b"short"),
            )
        });
        sent(&cache, asked("identity"), &app).await;
        let since = |date| asked_with(Method::GET, "/", "dep-1", &[("if-modified-since", date)]);
        assert_eq!(
            sent(&cache, since("Wed, 21 Oct 2026 07:28:00 GMT"), &app)
                .await
                .0
                .status(),
            StatusCode::NOT_MODIFIED
        );
        assert_eq!(
            sent(&cache, since("Tue, 20 Oct 2026 07:28:00 GMT"), &app)
                .await
                .0
                .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn a_head_is_answered_from_the_get_it_shares_a_key_with() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(html("public, max-age=60"));
        sent(&cache, asked("br"), &app).await;
        let (head, outcome, body) = sent(
            &cache,
            asked_with(Method::HEAD, "/", "dep-1", &[("accept-encoding", "br")]),
            &app,
        )
        .await;
        assert_eq!((outcome, app.woken()), (CacheOutcome::Hit, 1));
        assert!(body.is_empty());
        assert_eq!(head.headers()["content-encoding"], "br");
    }

    #[tokio::test]
    async fn a_stale_answer_within_stale_while_revalidate_is_served_and_revalidated_once_behind_it() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(|parts| {
            if parts
                .headers
                .get(IF_NONE_MATCH)
                .is_some_and(|held| held == "\"v1\"")
            {
                let mut not_modified = answer(
                    &[
                        ("cache-control", "public, max-age=60, stale-while-revalidate=30"),
                        ("etag", "\"v1\""),
                    ],
                    Bytes::new(),
                );
                *not_modified.status_mut() = StatusCode::NOT_MODIFIED;
                return not_modified;
            }
            html("public, max-age=60, stale-while-revalidate=30")(parts)
        });
        sent(&cache, asked("br"), &app).await;
        hands.forward(Duration::from_secs(70));
        // Answered back to back, before the revalidation behind the first has had a turn to run.
        let mut stale = Vec::new();
        for _ in 0..5 {
            stale.push(cache.answer(asked("br"), app.clone()).await);
        }
        for answered in stale {
            assert_eq!(answered.outcome, CacheOutcome::Stale);
            assert_eq!(answered.response.headers()[AGE], "70");
            let body = answered.response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(inflated(body, Encoding::Br).await, page());
        }
        settled(&cache).await;
        assert_eq!(app.woken(), 2, "five stale answers, one revalidation");
        assert_eq!(
            app.last_heard().headers[IF_NONE_MATCH],
            "\"v1\"",
            "revalidated with the app's own etag, not the weakened one"
        );
        let (refreshed, outcome, _) = sent(&cache, asked("br"), &app).await;
        assert_eq!(outcome, CacheOutcome::Hit, "the 304 made it fresh again");
        assert_eq!(refreshed.headers()[ETAG], "W/\"v1\"");
        assert_eq!(app.woken(), 2);
    }

    #[tokio::test]
    async fn a_stale_answer_past_its_windows_is_revalidated_before_it_is_served() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(|parts| {
            if parts.headers.contains_key(IF_NONE_MATCH) {
                let mut not_modified = answer(&[("etag", "\"v1\"")], Bytes::new());
                *not_modified.status_mut() = StatusCode::NOT_MODIFIED;
                return not_modified;
            }
            html("public, max-age=60, stale-if-error=600")(parts)
        });
        sent(&cache, asked("br"), &app).await;
        hands.forward(Duration::from_secs(61));
        let (revalidated, outcome, body) = sent(&cache, asked("br"), &app).await;
        assert_eq!((outcome, app.woken()), (CacheOutcome::Revalidated, 2));
        assert_eq!(revalidated.status(), StatusCode::OK);
        assert_eq!(inflated(body, Encoding::Br).await, page());
        assert_eq!(sent(&cache, asked("br"), &app).await.1, CacheOutcome::Hit);
    }

    #[tokio::test]
    async fn an_app_that_fails_its_revalidation_is_covered_for_within_stale_if_error_and_not_after() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let failing = Arc::new(AtomicBool::new(false));
        let fails = failing.clone();
        let app = App::answering(move |parts| {
            if fails.load(Ordering::SeqCst) {
                let mut refused = answer(&[], Bytes::from_static(b"This app could not be started.\n"));
                *refused.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                return refused;
            }
            html("public, max-age=60, stale-if-error=300")(parts)
        });
        sent(&cache, asked("br"), &app).await;
        failing.store(true, Ordering::SeqCst);
        hands.forward(Duration::from_secs(120));
        let (covered, outcome, _) = sent(&cache, asked("br"), &app).await;
        assert_eq!((covered.status(), outcome), (StatusCode::OK, CacheOutcome::Stale));
        hands.forward(Duration::from_secs(300));
        let (refused, outcome, _) = sent(&cache, asked("br"), &app).await;
        assert_eq!(
            (refused.status(), outcome),
            (StatusCode::SERVICE_UNAVAILABLE, CacheOutcome::Miss)
        );
    }

    #[tokio::test]
    async fn a_successful_unsafe_request_invalidates_its_target_and_the_locations_it_names() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(html("public, max-age=600"));
        for path in ["/cart", "/cart/1", "/other"] {
            sent(
                &cache,
                asked_with(Method::GET, path, "dep-1", &[("accept-encoding", "br")]),
                &app,
            )
            .await;
            sent(
                &cache,
                asked_with(Method::GET, path, "dep-1", &[("accept-encoding", "gzip")]),
                &app,
            )
            .await;
        }
        let deployment = DeploymentId::parse("dep-1").unwrap();
        let mut refused = answer(&[], Bytes::new());
        *refused.status_mut() = StatusCode::FORBIDDEN;
        cache
            .invalidate(&deployment, "app-1.apps.example.com", "/cart", &refused)
            .await;
        assert_eq!(cache.entries.entry_count(), 6, "a refused POST changed nothing");
        let created = answer(
            &[("location", "https://app-1.apps.example.com/cart/1")],
            Bytes::new(),
        );
        cache
            .invalidate(&deployment, "app-1.apps.example.com", "/cart", &created)
            .await;
        let woken = app.woken();
        for path in ["/cart", "/cart/1"] {
            let (_, outcome, _) = sent(
                &cache,
                asked_with(Method::GET, path, "dep-1", &[("accept-encoding", "br")]),
                &app,
            )
            .await;
            assert_eq!(outcome, CacheOutcome::Miss, "{path}");
        }
        let (_, outcome, _) = sent(
            &cache,
            asked_with(Method::GET, "/other", "dep-1", &[("accept-encoding", "br")]),
            &app,
        )
        .await;
        assert_eq!(outcome, CacheOutcome::Hit);
        assert_eq!(app.woken(), woken + 2);
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
            let (_, outcome, body) = sent(&cache, asked_with(Method::GET, &path, "dep-1", &[]), &app).await;
            assert_eq!((outcome, body), (CacheOutcome::Hit, page()), "{content_type}");
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
                tenant("dep-1"),
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
    async fn an_answer_the_app_compressed_itself_is_stored_under_that_coding_and_never_decoded_for_others() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(|parts| {
            let gzip = parts.headers[ACCEPT_ENCODING] == "gzip";
            let mut headers = vec![
                ("content-type", "text/html"),
                ("cache-control", "public, max-age=60"),
            ];
            if gzip {
                headers.push(("content-encoding", "gzip"));
            }
            answer(&headers, Bytes::from_static(b"as the app sent it"))
        });
        sent(&cache, asked("gzip"), &app).await;
        let (_, outcome, body) = sent(&cache, asked("gzip"), &app).await;
        assert_eq!(
            (outcome, body),
            (CacheOutcome::Hit, Bytes::from_static(b"as the app sent it"))
        );
        let (plain, outcome, _) = sent(&cache, asked_with(Method::GET, "/", "dep-1", &[]), &app).await;
        assert_eq!(
            outcome,
            CacheOutcome::Miss,
            "the app's own gzip is not decoded for it"
        );
        assert!(!plain.headers().contains_key("content-encoding"));
        assert_eq!(app.woken(), 2);
    }

    #[tokio::test]
    async fn a_body_that_says_it_has_ended_before_it_is_polled_is_still_kept() {
        let kept = Arc::new(Mutex::new(None));
        let into = kept.clone();
        let _tee = Tee::new(empty(), 16, move |body| *into.lock().unwrap() = Some(body));
        assert_eq!(*kept.lock().unwrap(), Some(Bytes::new()));
    }

    // Autokit's pages as Deno Deploy served them on 2026-10-23.
    const FOR_THE_BROWSER: &str = "public, max-age=0, must-revalidate";
    const FOR_THE_CDN: &str = "public, s-maxage=86400, stale-while-revalidate=60, stale-if-error=86400";

    fn autokit(tags: &'static str) -> impl Fn(&request::Parts) -> Response<ProxyBody> + Send + Sync {
        move |_| {
            answer(
                &[
                    ("content-type", "text/html"),
                    ("cache-control", FOR_THE_BROWSER),
                    ("cdn-cache-control", FOR_THE_CDN),
                    ("cache-tag", tags),
                    ("etag", "\"v1\""),
                ],
                page(),
            )
        }
    }

    #[tokio::test]
    async fn cdn_cache_control_decides_what_is_stored_and_for_how_long_and_the_browser_keeps_its_own() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(autokit("t:page"));
        sent(&cache, asked("br"), &app).await;
        hands.forward(Duration::from_secs(3_600));
        let (hit, outcome, _) = sent(&cache, asked("br"), &app).await;
        assert_eq!((outcome, app.woken()), (CacheOutcome::Hit, 1));
        assert_eq!(hit.headers()[AGE], "3600");
        assert_eq!(hit.headers()[CACHE_CONTROL], FOR_THE_BROWSER);
        assert_eq!(hit.headers()[CDN_CACHE_CONTROL], FOR_THE_CDN);
        assert_eq!(hit.headers()[CACHE_TAG], "t:page");

        hands.forward(Duration::from_secs(86_400 - 3_600 + 30));
        let (_, outcome, _) = sent(&cache, asked("br"), &app).await;
        assert_eq!(
            outcome,
            CacheOutcome::Stale,
            "the browser's must-revalidate is not the cache's"
        );

        let canonical = asked("identity").canonical;
        let storable = |headers: &[(&str, &str)]| {
            let (parts, _) = answer(headers, Bytes::from_static(b"x")).into_parts();
            cache
                .stored(
                    &canonical,
                    parts,
                    Bytes::from_static(b"x"),
                    Provenance::default(),
                    hands.now(),
                    hands.now(),
                )
                .is_some()
        };
        let table: [(&[(&str, &str)], bool); 5] = [
            (
                &[
                    ("cache-control", FOR_THE_BROWSER),
                    ("cdn-cache-control", FOR_THE_CDN),
                ],
                true,
            ),
            (
                &[
                    ("cache-control", "no-store"),
                    ("cdn-cache-control", "s-maxage=60"),
                ],
                true,
            ),
            (
                &[
                    ("cache-control", "public, max-age=60"),
                    ("cdn-cache-control", "no-store"),
                ],
                false,
            ),
            (
                &[
                    ("cache-control", "public, max-age=60"),
                    ("cdn-cache-control", "private"),
                ],
                false,
            ),
            (
                &[
                    ("expires", "Thu, 01 Jan 2099 00:00:00 GMT"),
                    ("cdn-cache-control", "public"),
                ],
                false,
            ),
        ];
        for (headers, expected) in table {
            assert_eq!(storable(headers), expected, "{headers:?}");
        }
    }

    #[test]
    fn s_maxage_forbids_stale_answers_only_in_cache_control() {
        let windows = |headers: &[(&str, &str)]| {
            let (parts, _) = answer(headers, Bytes::new()).into_parts();
            Windows::of(&parts.headers)
        };
        assert_eq!(
            windows(&[("cache-control", "s-maxage=60, stale-while-revalidate=30")]),
            Windows::default()
        );
        assert_eq!(
            windows(&[(
                "cache-control",
                "max-age=60, stale-while-revalidate=30, stale-if-error=90"
            )]),
            Windows {
                while_revalidating: Duration::from_secs(30),
                if_error: Duration::from_secs(90),
            }
        );
        assert_eq!(
            windows(&[
                ("cache-control", "max-age=60, stale-while-revalidate=5"),
                ("cdn-cache-control", "s-maxage=60, stale-while-revalidate=30"),
            ]),
            Windows {
                while_revalidating: Duration::from_secs(30),
                if_error: Duration::ZERO,
            }
        );
        assert_eq!(
            windows(&[
                ("cache-control", "max-age=60, stale-while-revalidate=30"),
                ("cdn-cache-control", "max-age=60, must-revalidate"),
            ]),
            Windows::default()
        );
    }

    #[test]
    fn tags_are_read_from_cache_tag_and_surrogate_key_and_an_answer_with_too_many_has_none() {
        let tags = |headers: &[(&str, &str)]| {
            let (parts, _) = answer(headers, Bytes::new()).into_parts();
            tags_of(&parts.headers).map(|tags| tags.iter().map(|tag| tag.to_string()).collect::<Vec<_>>())
        };
        assert_eq!(
            tags(&[
                ("cache-tag", "t:page, t:price list,t:page"),
                ("surrogate-key", "s:a  s:b"),
                ("cache-tag", "t:other"),
            ]),
            Some(vec![
                "s:a".to_string(),
                "s:b".to_string(),
                "t:other".to_string(),
                "t:page".to_string(),
                "t:price list".to_string(),
            ])
        );
        assert_eq!(tags(&[]), Some(vec![]));
        let most = (0..MOST_TAGS)
            .map(|n| format!("t:{n}"))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            tags(&[("cache-tag", &most)]).map(|tags| tags.len()),
            Some(MOST_TAGS)
        );
        let one_more = format!("{most},t:one-more");
        assert_eq!(tags(&[("cache-tag", &one_more)]), None);
        let longest = "t".repeat(LONGEST_TAG);
        assert!(tags(&[("cache-tag", &longest)]).is_some());
        assert_eq!(tags(&[("cache-tag", &format!("{longest}t"))]), None);

        let mut unreadable = HeaderMap::new();
        unreadable.insert(CACHE_TAG, HeaderValue::from_bytes(b"t:\xff").unwrap());
        assert_eq!(tags_of(&unreadable), None);
    }

    #[tokio::test]
    async fn an_answer_whose_tags_no_purge_could_be_trusted_to_reach_is_not_stored() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(|_| {
            let tags = (0..=MOST_TAGS)
                .map(|n| format!("t:{n}"))
                .collect::<Vec<_>>()
                .join(",");
            answer(
                &[("cache-control", "public, max-age=60"), ("cache-tag", &tags)],
                page(),
            )
        });
        sent(&cache, asked("br"), &app).await;
        assert_eq!(sent(&cache, asked("br"), &app).await.1, CacheOutcome::Miss);
    }

    fn tenant_labelled(app_id: &str, deployment: &str, app_label: Option<&str>) -> Tenant {
        let labels = app_label.map_or_else(|| serde_json::json!({}), |app| serde_json::json!({ "app": app }));
        Tenant::of(
            &AppId::parse(app_id).unwrap(),
            &DeploymentId::parse(deployment).unwrap(),
            &serde_json::from_value(labels).unwrap(),
        )
    }

    fn asked_by(tenant: Tenant, path: &str) -> Asked {
        let (mut parts, ()) = Request::builder()
            .uri(path)
            .header(ACCEPT_ENCODING, "br")
            .body(())
            .unwrap()
            .into_parts();
        encode::as_negotiated(&mut parts.headers, Encoding::Br);
        Asked::new(parts, tenant, "app-1.apps.example.com", Encoding::Br)
    }

    fn tagged_by_path() -> Arc<App> {
        App::answering(|parts| {
            let tags = match parts.uri.path() {
                "/a" => "t:a, t:all",
                "/b" => "t:b, t:all",
                _ => "t:c",
            };
            answer(
                &[("cache-control", "public, max-age=600"), ("cache-tag", tags)],
                page(),
            )
        })
    }

    #[tokio::test]
    async fn a_purge_removes_every_answer_its_owner_tagged_and_nothing_of_anyone_else() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = tagged_by_path();
        let production = || tenant_labelled("shop-prod", "dep-1", Some("shop"));
        let preview = || tenant_labelled("shop-preview", "dep-2", Some("shop"));
        let other = || tenant_labelled("other", "dep-3", None);
        let all = [
            (production(), "/a"),
            (production(), "/b"),
            (production(), "/c"),
            (preview(), "/a"),
            (other(), "/a"),
        ];
        for (tenant, path) in all.clone() {
            sent(&cache, asked_by(tenant, path), &app).await;
        }
        assert_eq!(cache.purge("shop", &["t:a".to_string()]).await, 2);
        assert_eq!(cache.purge("shop", &["t:a".to_string()]).await, 0, "already gone");
        assert_eq!(
            cache.purge("shop-prod", &["t:b".to_string()]).await,
            0,
            "the owner is the label"
        );
        assert_eq!(cache.purge("nobody", &["t:all".to_string()]).await, 0);
        let mut outcomes = Vec::new();
        for (tenant, path) in all {
            outcomes.push(sent(&cache, asked_by(tenant, path), &app).await.1);
        }
        use CacheOutcome::{Hit, Miss};
        assert_eq!(outcomes, [Miss, Hit, Hit, Miss, Hit]);
        assert_eq!(
            cache
                .purge(
                    "shop",
                    &["t:all".to_string(), "t:c".to_string(), "t:b".to_string()]
                )
                .await,
            4,
            "each entry once, however many of its tags are named"
        );
        assert_eq!(cache.purge("other", &["t:a".to_string()]).await, 1);
        assert_eq!(cache.entries.entry_count(), 0);
    }

    #[tokio::test]
    async fn a_tag_leaves_the_index_with_the_answer_that_carried_it() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let answers = Arc::new(AtomicUsize::new(0));
        let counted = answers.clone();
        let app = App::answering(move |_| {
            let tag = match counted.fetch_add(1, Ordering::SeqCst) {
                0 => "t:first",
                _ => "t:second",
            };
            answer(
                &[("cache-control", "public, max-age=60"), ("cache-tag", tag)],
                page(),
            )
        });
        sent(&cache, asked("br"), &app).await;
        hands.forward(Duration::from_secs(61));
        assert_eq!(sent(&cache, asked("br"), &app).await.1, CacheOutcome::Miss);
        settled(&cache).await;
        assert_eq!(
            cache.purge("app-1", &["t:first".to_string()]).await,
            0,
            "the answer that carried it was replaced"
        );
        assert_eq!(sent(&cache, asked("br"), &app).await.1, CacheOutcome::Hit);

        cache.entries.invalidate_all();
        settled(&cache).await;
        assert!(locked(&cache.tags).0.is_empty(), "evicted with its entry");
        assert_eq!(cache.purge("app-1", &["t:second".to_string()]).await, 0);
    }

    #[tokio::test]
    async fn a_set_cookie_is_never_stored_whatever_the_app_says() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(|_| {
            answer(
                &[
                    ("cache-control", "public, max-age=60"),
                    ("cdn-cache-control", "public, s-maxage=60"),
                    ("set-cookie", "theme=dark"),
                ],
                page(),
            )
        });
        let (first, _, _) = sent(&cache, asked("br"), &app).await;
        assert_eq!(
            first.headers()[SET_COOKIE],
            "theme=dark",
            "the visitor still gets it"
        );
        let (_, outcome, _) = sent(&cache, asked("br"), &app).await;
        assert_eq!((outcome, app.woken()), (CacheOutcome::Miss, 2));
    }

    fn reported(response: &mut Response<ProxyBody>, outcome: CacheOutcome) -> String {
        report(response, "mf", outcome);
        response.headers()[CACHE_STATUS].to_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn cache_status_says_what_the_cache_did_as_deno_deploy_spells_it() {
        let hands = Hands::at_noon();
        let cache = cache_of(&hands, 8, 1024);
        let app = App::answering(autokit("t:page"));
        let (mut stored, outcome, _) = sent(&cache, asked("br"), &app).await;
        assert_eq!(reported(&mut stored, outcome), "mf; fwd=miss; stored");
        assert!(!stored.headers().contains_key(AGE));
        hands.forward(Duration::from_secs(13));
        let (mut hit, outcome, _) = sent(&cache, asked("br"), &app).await;
        assert_eq!(reported(&mut hit, outcome), "mf; hit");
        assert_eq!(hit.headers()[AGE], "13");

        let unstored = App::answering(html("no-store"));
        let (mut passed, outcome, _) = sent(
            &cache,
            asked_with(Method::GET, "/private", "dep-1", &[("accept-encoding", "br")]),
            &unstored,
        )
        .await;
        assert_eq!(reported(&mut passed, outcome), "mf; fwd=miss");

        let mut answered = answer(&[("cache-status", "deno; hit")], Bytes::new());
        assert_eq!(
            reported(&mut answered, CacheOutcome::Bypass),
            "deno; hit, mf; fwd=bypass"
        );
        for (outcome, status) in [
            (CacheOutcome::Stale, "mf; hit; detail=stale"),
            (CacheOutcome::Revalidated, "mf; fwd=stale; fwd-status=304"),
        ] {
            assert_eq!(reported(&mut answer(&[], Bytes::new()), outcome), status);
        }
    }
}
