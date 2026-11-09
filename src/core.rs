//! Runtime-agnostic pieces shared by the blocking and tokio clients: options, wire events, encoding,
//! retry policy, the bounded queue state and the flag state.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flate2::{Compression, write::GzEncoder};
use serde_json::{Map, Value, json};

use crate::defcache::FlagDefinitionCache;
use crate::eval::{FlagResult, FlagValue, REQUIRES_SERVER, evaluate_all};
use crate::hooks::{BeforeSend, OnError};
use crate::identity::TestSink;
use crate::snapshot::FlagSnapshot;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
const MIN_GZIP_BYTES: usize = 1024;
pub(crate) const MAX_BATCH: usize = 1000;

pub type Props = Map<String, Value>;

/// Client options; `Options::default()` picks the documented defaults.
#[derive(Debug, Clone)]
pub struct Options {
    pub host: String,
    /// Events per batch.
    pub flush_at: usize,
    pub flush_interval: Duration,
    /// Oldest events are dropped beyond this.
    pub max_queue_size: usize,
    pub max_retries: u32,
    /// `lc_sk_...`: enables local flag evaluation and `import_events`.
    pub secret_key: Option<String>,
    pub flag_poll_interval: Duration,
    /// With a secret key, follow `GET /v1/flags/stream` and reload on each change; interval
    /// polling only runs while the stream is down. Default on.
    pub flag_stream: bool,
    pub timeout: Duration,
    /// No events, no network, no threads/tasks.
    pub disabled: bool,
    /// Log to stderr.
    pub debug: bool,
    /// Events per request (1..=1000), independent of `flush_at`.
    pub max_batch_size: usize,
    /// Timeout for flag requests (definitions and `/v1/flags`); defaults to `timeout`.
    pub flag_timeout: Option<Duration>,
    /// Send `$geoip_disable: true` on every event (per event: [`Event::disable_geoip`]).
    pub disable_geoip: bool,
    /// Super properties on every event (see also `Client::register`); context and per-event properties win.
    pub default_properties: Props,
    /// Test mode: events land in this sink instead of the queue; no network, no flusher thread.
    pub test_sink: Option<TestSink>,
    pub before_send: Option<BeforeSend>,
    pub on_error: Option<OnError>,
    /// App contexts of this process; flags scoped to other app contexts are dropped from results
    /// (per call: [`FlagOptions::app_contexts`]).
    pub app_contexts: Vec<String>,
    /// Shares one definitions fetch between workers; see [`FlagDefinitionCache`].
    pub flag_definition_cache: Option<Arc<dyn FlagDefinitionCache>>,
    /// `flag key -> value` served (reason `fallback`, no exposure) when a flag read fails or the flag is unknown.
    pub fallbacks: HashMap<String, FlagValue>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            host: "https://capture.lucidace.com".into(),
            flush_at: 20,
            flush_interval: Duration::from_secs(5),
            max_queue_size: 1000,
            max_retries: 5,
            secret_key: None,
            flag_poll_interval: Duration::from_secs(30),
            flag_stream: true,
            timeout: Duration::from_secs(10),
            disabled: false,
            debug: false,
            max_batch_size: MAX_BATCH,
            flag_timeout: None,
            disable_geoip: false,
            default_properties: Props::new(),
            test_sink: None,
            before_send: None,
            on_error: None,
            app_contexts: Vec::new(),
            flag_definition_cache: None,
            fallbacks: HashMap::new(),
        }
    }
}

/// Errors of `import_events` and `reload_feature_flags`.
#[derive(Debug)]
pub enum Error {
    NoSecretKey,
    Http(String),
    Status(u16),
    Body(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoSecretKey => write!(f, "this call requires a secret key"),
            Self::Http(e) => write!(f, "request failed: {e}"),
            Self::Status(s) => write!(f, "unexpected status {s}"),
            Self::Body(e) => write!(f, "bad response body: {e}"),
        }
    }
}

impl std::error::Error for Error {}

/// One event. Timestamp and uuid default to now / a fresh `UUIDv7`.
#[derive(Debug, Clone, Default)]
pub struct Event {
    pub distinct_id: String,
    pub event: String,
    pub properties: Props,
    pub groups: Map<String, Value>,
    pub timestamp: Option<String>,
    pub uuid: Option<String>,
    /// Person properties (`$set`).
    pub set: Props,
    /// Person properties (`$set_once`).
    pub set_once: Props,
    /// Send `$process_person_profile: false`: no person is created or updated and `set` / `set_once` are dropped.
    pub personless: bool,
    /// Overrides [`Options::disable_geoip`] for this event.
    pub disable_geoip: Option<bool>,
    /// Flag values attached as `$feature/<key>` + `$active_feature_flags` (see [`Event::with_flags`]).
    pub flags: Option<HashMap<String, FlagValue>>,
}

impl Event {
    /// Attach a snapshot's flag values to the event (properties set on the event win).
    #[must_use]
    pub fn with_flags(mut self, s: &FlagSnapshot) -> Self {
        self.flags = Some(s.values().clone());
        self
    }

    #[must_use]
    pub fn new(distinct_id: &str, event: &str) -> Self {
        Self {
            distinct_id: distinct_id.into(),
            event: event.into(),
            ..Self::default()
        }
    }

    /// A personless event with a random `UUIDv7` distinct id.
    #[must_use]
    pub fn new_anon(event: &str) -> Self {
        Self {
            personless: true,
            ..Self::new(&uuid7(), event)
        }
    }

    /// An event whose distinct id comes from the active [`crate::RequestContext`].
    #[must_use]
    pub fn in_context(event: &str) -> Self {
        Self::new("", event)
    }

    /// A `$set` event updating person properties (`once`: only where unset, via `$set_once`).
    #[must_use]
    pub fn person_properties(distinct_id: &str, props: Props, once: bool) -> Self {
        let mut e = Self::new(distinct_id, "$set");
        if once {
            e.set_once = props;
        } else {
            e.set = props;
        }
        e
    }

    /// Per-event `$geoip_disable` override.
    #[must_use]
    pub fn geoip_disabled(mut self, disable: bool) -> Self {
        self.disable_geoip = Some(disable);
        self
    }

    #[must_use]
    pub fn property(mut self, k: &str, v: impl Into<Value>) -> Self {
        self.properties.insert(k.into(), v.into());
        self
    }

    #[must_use]
    pub fn group(mut self, group_type: &str, key: &str) -> Self {
        self.groups.insert(group_type.into(), key.into());
        self
    }

    /// Set the event time (kept as given by `import_events`).
    #[must_use]
    pub fn at(mut self, t: SystemTime) -> Self {
        self.timestamp = Some(iso(t));
        self
    }
}

/// Milliseconds-precision UTC ISO-8601, e.g. `2026-01-02T03:04:05.678Z`.
#[must_use]
pub fn iso(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = i64::try_from(d.as_secs()).unwrap_or(0);
    let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    // civil-from-days (H. Hinnant)
    let z = days + 719_468;
    let (era, doe) = (z.div_euclid(146_097), z.rem_euclid(146_097));
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        d.subsec_millis()
    )
}

pub(crate) fn uuid7() -> String {
    let ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(0);
    let mut b = [0u8; 16];
    b[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
    for x in &mut b[6..] {
        *x = fastrand::u8(..);
    }
    b[6] = 0x70 | (b[6] & 0x0f);
    b[8] = 0x80 | (b[8] & 0x3f);
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

pub(crate) fn wire(e: &Event) -> Value {
    let mut props = e.properties.clone();
    props.insert("$lib".into(), "lucida-rust".into());
    props.insert("$lib_version".into(), VERSION.into());
    let mut w = json!({
        "uuid": e.uuid.clone().unwrap_or_else(uuid7),
        "event": e.event,
        "distinct_id": e.distinct_id,
        "timestamp": e.timestamp.clone().unwrap_or_else(|| iso(SystemTime::now())),
        "properties": props,
    });
    for (k, m) in [
        ("$set", &e.set),
        ("$set_once", &e.set_once),
        ("$groups", &e.groups),
    ] {
        if !m.is_empty() {
            w[k] = Value::Object(m.clone());
        }
    }
    w
}

pub(crate) fn batch_payload(api_key: Option<&str>, batch: Vec<Value>) -> Value {
    let mut p = json!({"batch": batch, "sent_at": iso(SystemTime::now())});
    if let Some(k) = api_key {
        p["api_key"] = k.into();
    }
    p
}

/// JSON body; gzip above 1 KB flagged with `?compression=gzip`.
pub(crate) fn encode(url: &str, payload: &Value) -> (String, Vec<u8>) {
    let body = serde_json::to_vec(payload).unwrap_or_default();
    if body.len() < MIN_GZIP_BYTES {
        return (url.to_owned(), body);
    }
    let mut z = GzEncoder::new(Vec::new(), Compression::default());
    if z.write_all(&body).is_err() {
        return (url.to_owned(), body);
    }
    let sep = if url.contains('?') { '&' } else { '?' };
    (
        format!("{url}{sep}compression=gzip"),
        z.finish().unwrap_or(body),
    )
}

pub(crate) fn retryable(status: u16) -> bool {
    matches!(status, 0 | 408 | 429) || status >= 500
}

pub(crate) fn parse_retry_after(v: Option<&str>) -> Option<Duration> {
    let s: f64 = v?.trim().parse().ok()?;
    (s.is_finite() && s >= 0.0).then(|| Duration::from_secs_f64(s.min(60.0)))
}

/// Exponential backoff (1 s base, 30 s cap) with 50%..100% jitter; a longer `Retry-After` wins.
pub(crate) fn retry_delay(attempt: u32, retry_after: Option<Duration>) -> Duration {
    let step = Duration::from_secs(1u64 << attempt.min(10)).min(Duration::from_secs(30));
    let d = step.mul_f64(0.5 + 0.5 * fastrand::f64());
    retry_after.map_or(d, |r| d.max(r))
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Queue contents and flags, guarded by one mutex in each client.
#[derive(Default)]
pub(crate) struct QueueState {
    pub items: VecDeque<Value>,
    pub closed: bool,
    pub abort: bool,
    pub busy: bool,
    pub flush_req: bool,
    pub dropped: u64,
}

impl QueueState {
    /// Add an event; true when the flusher should be woken (first event, or a full batch).
    pub fn push(&mut self, e: Value, flush_at: usize, max: usize) -> bool {
        if self.closed {
            return false;
        }
        self.items.push_back(e);
        if self.items.len() > max {
            self.items.pop_front();
            self.dropped += 1;
        }
        matches!(self.items.len(), 1) || self.items.len() == flush_at
    }

    pub fn take(&mut self, max: usize) -> Vec<Value> {
        let n = self.items.len().min(max.clamp(1, MAX_BATCH));
        self.items.drain(..n).collect()
    }
}

// ---- flags ----------------------------------------------------------------------------------

/// Per-call flag inputs.
#[derive(Debug, Clone, Default)]
pub struct FlagOptions {
    pub person_properties: Props,
    pub groups: Map<String, Value>,
    pub group_properties: Map<String, Value>,
    /// Overrides [`Options::app_contexts`] for this call.
    pub app_contexts: Vec<String>,
    /// Never call the server: flags undecidable locally yield `None`.
    pub only_evaluate_locally: bool,
    /// Suppress `$feature_flag_called` / `$exposure` (`get_feature_flag` only).
    pub no_exposure: bool,
    /// Pre-identify id of this person; the gateway links it so flags keep their bucket after identify.
    pub anon_distinct_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct Resolved {
    pub value: FlagValue,
    pub payload: Option<Value>,
    pub locally: bool,
    pub request_id: Option<String>,
    pub reason: String,
    pub condition_index: i64,
    pub variant: Option<String>,
}

#[derive(Default)]
pub(crate) struct FlagState {
    pub defs: Mutex<(Option<Vec<Value>>, Option<String>)>, // (definitions, etag)
    /// The flag stream is connected, so the poller skips its interval reloads.
    pub streaming: AtomicBool,
    /// [`Options::app_contexts`]
    pub app_contexts: Vec<String>,
    seen: Mutex<Lru>,
}

/// Bounded set: inserting past `max` evicts the least recently seen key.
#[derive(Default)]
pub(crate) struct Lru {
    order: VecDeque<String>,
    set: HashSet<String>,
}

impl Lru {
    /// True when the key was new.
    pub fn insert(&mut self, k: String, max: usize) -> bool {
        let had = self.set.contains(&k);
        if had {
            self.order.retain(|x| *x != k);
        } else {
            self.set.insert(k.clone());
        }
        self.order.push_back(k);
        if self.order.len() > max
            && let Some(old) = self.order.pop_front()
        {
            self.set.remove(&old);
        }
        !had
    }
}

const SEEN_MAX: usize = 10_000;

impl FlagState {
    pub fn new(app_contexts: Vec<String>) -> Self {
        Self {
            app_contexts,
            ..Self::default()
        }
    }

    /// `o` with the client's app contexts filled in when the call names none.
    pub fn with_contexts(&self, o: &FlagOptions) -> FlagOptions {
        let mut o = o.clone();
        if o.app_contexts.is_empty() {
            o.app_contexts.clone_from(&self.app_contexts);
        }
        o
    }

    pub fn set_defs(&self, defs: Vec<Value>, etag: Option<String>) {
        *lock(&self.defs) = (Some(defs), etag);
    }

    pub fn etag(&self) -> Option<String> {
        lock(&self.defs).1.clone()
    }

    fn local(&self, id: &str, o: &FlagOptions) -> Option<HashMap<String, FlagResult>> {
        let g = lock(&self.defs);
        let defs = g.0.as_ref()?;
        let ctx = json!({
            "distinct_id": id, "groups": o.groups, "person_properties": o.person_properties,
            "group_properties": o.group_properties, "app_contexts": self.with_contexts(o).app_contexts,
        });
        let mut all = evaluate_all(defs, &ctx);
        all.retain(|_, r| r.reason != "app_context_excluded");
        Some(all)
    }

    pub fn local_one(&self, key: &str, id: &str, o: &FlagOptions) -> Option<Resolved> {
        let r = self.local(id, o)?.remove(key)?;
        (r.reason != REQUIRES_SERVER).then_some(Resolved {
            value: r.value,
            payload: r.payload,
            locally: true,
            request_id: None,
            reason: r.reason.to_owned(),
            condition_index: r.condition_index,
            variant: r.variant,
        })
    }

    /// Decided flags, and whether anything is left for the server.
    pub fn local_all(&self, id: &str, o: &FlagOptions) -> (HashMap<String, Resolved>, bool) {
        let Some(loc) = self.local(id, o) else {
            return (HashMap::new(), true);
        };
        let undecided = loc.values().any(|r| r.reason == REQUIRES_SERVER);
        let out = loc
            .into_iter()
            .filter(|(_, r)| r.reason != REQUIRES_SERVER)
            .map(|(k, r)| {
                (
                    k,
                    Resolved {
                        value: r.value,
                        payload: r.payload,
                        locally: true,
                        request_id: None,
                        reason: r.reason.to_owned(),
                        condition_index: r.condition_index,
                        variant: r.variant,
                    },
                )
            })
            .collect();
        (out, undecided)
    }

    /// True the first time `(id, key, value)` is seen (bounded LRU).
    pub fn first_seen(&self, id: &str, key: &str, value: &FlagValue) -> bool {
        let k = format!("{id}\0{key}\0{}", value.to_json());
        lock(&self.seen).insert(k, SEEN_MAX)
    }
}

pub(crate) fn remote_body(api_key: &str, id: &str, o: &FlagOptions, keys: Option<&str>) -> Value {
    let mut b = json!({
        "api_key": api_key, "distinct_id": id, "groups": o.groups,
        "person_properties": o.person_properties, "group_properties": o.group_properties,
    });
    if let Some(k) = keys {
        b["flag_keys"] = json!([k]);
    }
    if !o.app_contexts.is_empty() {
        b["app_contexts"] = json!(o.app_contexts);
    }
    if let Some(a) = &o.anon_distinct_id {
        b["anon_distinct_id"] = json!(a);
    }
    b
}

/// `GET /v1/flags/{key}/remote-config` with `key` percent-encoded (RFC 3986 unreserved bytes kept).
pub(crate) fn remote_config_url(host: &str, key: &str) -> String {
    let k: String = key
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    format!("{host}/v1/flags/{k}/remote-config")
}

/// The `payload` of a remote-config response; `None` on 404 (unknown / not remote-config) or any failure.
pub(crate) fn remote_config_payload(status: u16, body: &[u8]) -> Option<Value> {
    if status != 200 {
        return None;
    }
    let mut j = serde_json::from_slice::<Value>(body).ok()?;
    j.get_mut("payload")
        .map(Value::take)
        .filter(|v| !v.is_null())
}

pub(crate) fn parse_remote(j: &Value) -> HashMap<String, Resolved> {
    let rid = j
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let Some(flags) = j.get("flags").and_then(Value::as_object) else {
        return HashMap::new();
    };
    flags
        .iter()
        .map(|(k, f)| {
            let value = match f.get("value") {
                Some(Value::Bool(b)) => FlagValue::Bool(*b),
                Some(Value::String(s)) => FlagValue::Variant(s.clone()),
                _ => match f
                    .get("variant")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    Some(v) => FlagValue::Variant(v.to_owned()),
                    None => {
                        FlagValue::Bool(f.get("enabled").and_then(Value::as_bool).unwrap_or(false))
                    }
                },
            };
            let payload = f.get("payload").filter(|p| !p.is_null()).cloned();
            let reason = f.get("reason").and_then(Value::as_str).unwrap_or("remote");
            let variant = f
                .get("variant")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            (
                k.clone(),
                Resolved {
                    value,
                    payload,
                    locally: false,
                    request_id: rid.clone(),
                    reason: reason.to_owned(),
                    condition_index: f
                        .get("condition_index")
                        .and_then(Value::as_i64)
                        .unwrap_or(-1),
                    variant,
                },
            )
        })
        .collect()
}

pub(crate) fn exposure_events(id: &str, key: &str, r: &Resolved) -> Vec<Event> {
    ["$feature_flag_called", "$exposure"]
        .into_iter()
        .map(|name| {
            let mut e = Event::new(id, name)
                .property("$feature_flag", key)
                .property("$feature_flag_response", r.value.to_json())
                .property("locally_evaluated", r.locally);
            if let Some(rid) = &r.request_id {
                e = e.property("$feature_flag_request_id", rid.as_str());
            }
            e
        })
        .collect()
}

#[cfg(test)]
mod anon_tests {
    use super::*;

    #[test]
    fn remote_body_carries_anon_distinct_id_only_when_set() {
        let o = FlagOptions {
            anon_distinct_id: Some("anon-9".into()),
            ..FlagOptions::default()
        };
        assert_eq!(
            remote_body("k", "u", &o, None)["anon_distinct_id"],
            "anon-9"
        );
        assert!(
            remote_body("k", "u", &FlagOptions::default(), None)
                .get("anon_distinct_id")
                .is_none()
        );
    }
}
