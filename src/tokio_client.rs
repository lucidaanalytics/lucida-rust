//! Async client on tokio + async `reqwest` (feature `tokio`). Same behaviour as the blocking client.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::core::{
    Error, Event, FlagOptions, FlagState, MAX_BATCH, Options, Props, QueueState, Resolved,
    batch_payload, encode, exposure_events, lock, parse_remote, parse_retry_after, remote_body,
    remote_config_payload, remote_config_url, retry_delay, retryable, wire,
};
use crate::defcache::{adopt_cached, publish_cached, shutdown_cached};
use crate::details::{FALLBACK_REASON, FlagDetails, fallback};
use crate::eval::FlagValue;
use crate::hooks::build_wire;
use crate::identity::Shared;
use crate::snapshot::FlagSnapshot;
use crate::sse::SseEvents;

struct Inner {
    api_key: String,
    opts: Options,
    http: reqwest::Client,
    /// No overall timeout: the stream stays open.
    stream_http: reqwest::Client,
    q: Mutex<QueueState>,
    wake: Notify,
    flags: FlagState,
    shared: Shared,
    stop: watch::Sender<bool>,
    ready: watch::Sender<bool>,
}

enum Step {
    Send(Vec<Value>),
    Wait(Option<Duration>),
    Done,
}

/// Async server SDK client; must be created inside a tokio runtime. Call [`AsyncClient::shutdown`] to flush.
pub struct AsyncClient {
    inner: Arc<Inner>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Inner {
    fn log(&self, msg: &str) {
        if self.opts.debug {
            eprintln!("[lucida] {msg}");
        }
    }

    async fn request(
        &self,
        url: &str,
        payload: &Value,
        bearer: Option<&str>,
        timeout: Option<Duration>,
    ) -> (u16, Option<Duration>, Vec<u8>) {
        let (url, body) = encode(url, payload);
        let mut req = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body);
        if let Some(b) = bearer {
            req = req.header("Authorization", format!("Bearer {b}"));
        }
        if let Some(t) = timeout {
            req = req.timeout(t);
        }
        match req.send().await {
            Ok(r) => {
                let status = r.status().as_u16();
                let ra =
                    parse_retry_after(r.headers().get("retry-after").and_then(|v| v.to_str().ok()));
                (
                    status,
                    ra,
                    r.bytes().await.map(|b| b.to_vec()).unwrap_or_default(),
                )
            }
            Err(e) => {
                self.log(&format!("request failed: {e}"));
                (0, None, Vec::new())
            }
        }
    }

    fn step(&self, first_at: &mut Option<Instant>) -> Step {
        let mut q = lock(&self.q);
        let n = q.items.len();
        if n == 0 {
            *first_at = None;
            return if q.closed {
                Step::Done
            } else {
                Step::Wait(None)
            };
        }
        if !(n >= self.opts.flush_at || q.flush_req || q.closed) {
            let at = *first_at.get_or_insert_with(Instant::now);
            if let Some(rem) = (at + self.opts.flush_interval)
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
            {
                return Step::Wait(Some(rem));
            }
        }
        q.busy = true;
        *first_at = None;
        Step::Send(q.take(self.opts.max_batch_size))
    }

    async fn run(self: Arc<Self>) {
        let mut first_at = None;
        loop {
            match self.step(&mut first_at) {
                Step::Done => return,
                Step::Wait(d) => {
                    tokio::select! {
                        () = self.wake.notified() => {}
                        () = tokio::time::sleep(d.unwrap_or(Duration::from_secs(3600))) => {}
                    }
                }
                Step::Send(batch) => {
                    self.send(batch).await;
                    let mut q = lock(&self.q);
                    q.busy = false;
                    if q.items.is_empty() {
                        q.flush_req = false;
                    }
                }
            }
        }
    }

    async fn send(&self, batch: Vec<Value>) {
        let n = batch.len() as u64;
        let payload = batch_payload(Some(&self.api_key), batch);
        let url = format!("{}/v1/batch", self.opts.host);
        let mut stop = self.stop.subscribe();
        let mut attempt = 0;
        loop {
            let (status, ra, _) = self.request(&url, &payload, None, None).await;
            if (200..300).contains(&status) {
                return;
            }
            if !retryable(status) || attempt >= self.opts.max_retries || *stop.borrow() {
                lock(&self.q).dropped += n;
                self.log(&format!("dropping {n} events (status {status})"));
                self.opts.fail(
                    &format!("dropped {n} events (status {status})"),
                    &payload["batch"].as_array().cloned().unwrap_or_default(),
                );
                return;
            }
            let delay = retry_delay(attempt, ra);
            attempt += 1;
            self.log(&format!("retry in {delay:?}"));
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                _ = stop.changed() => {}
            }
        }
    }

    async fn poll(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        let mut first = true;
        loop {
            if first || !self.flags.streaming.load(Ordering::Relaxed) {
                let _ = self.reload().await;
            }
            first = false;
            self.ready.send_replace(true);
            tokio::select! {
                () = tokio::time::sleep(self.opts.flag_poll_interval) => {}
                _ = stop.changed() => return,
            }
        }
    }

    /// Follow the flag stream, reconnecting with backoff; `streaming` is false while it is down.
    async fn stream(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        let mut attempt = 0;
        loop {
            let ok = tokio::select! {
                ok = self.stream_once() => ok,
                _ = stop.changed() => return,
            };
            attempt = if ok { 0 } else { attempt + 1 };
            self.flags.streaming.store(false, Ordering::Relaxed);
            tokio::select! {
                () = tokio::time::sleep(retry_delay(attempt, None)) => {}
                _ = stop.changed() => return,
            }
        }
    }

    /// One connection; reports whether it was established.
    async fn stream_once(&self) -> bool {
        let Some(secret) = &self.opts.secret_key else {
            return false;
        };
        let r = self
            .stream_http
            .get(format!("{}/v1/flags/stream", self.opts.host))
            .header("Authorization", format!("Bearer {secret}"))
            .header("Accept", "text/event-stream")
            .send()
            .await;
        let mut r = match r {
            Ok(r) if r.status().as_u16() == 200 => r,
            Ok(r) => {
                self.log(&format!("flag stream: status {}", r.status().as_u16()));
                return false;
            }
            Err(e) => {
                self.log(&format!("flag stream failed: {e}"));
                return false;
            }
        };
        self.flags.streaming.store(true, Ordering::Relaxed);
        let mut sse = SseEvents::default();
        while let Ok(Some(chunk)) = r.chunk().await {
            if sse.push(&chunk) {
                let _ = self.reload().await;
                self.ready.send_replace(true);
            }
        }
        true
    }

    async fn reload(&self) -> Result<(), Error> {
        let secret = self.opts.secret_key.as_ref().ok_or(Error::NoSecretKey)?;
        let cache = self.opts.flag_definition_cache.as_deref();
        if let Some(defs) = adopt_cached(cache) {
            self.flags.set_defs(defs, None);
            return Ok(());
        }
        let mut req = self
            .http
            .get(format!("{}/v1/flags/definitions", self.opts.host))
            .header("Authorization", format!("Bearer {secret}"));
        if let Some(e) = self.flags.etag() {
            req = req.header("If-None-Match", e);
        }
        if let Some(t) = self.opts.flag_timeout {
            req = req.timeout(t);
        }
        let r = req.send().await.map_err(|e| Error::Http(e.to_string()))?;
        let status = r.status().as_u16();
        if status == 304 {
            return Ok(());
        }
        if status != 200 {
            return Err(Error::Status(status));
        }
        let etag = r
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let bytes = r.bytes().await.map_err(|e| Error::Http(e.to_string()))?;
        let j: Value = serde_json::from_slice(&bytes).map_err(|e| Error::Body(e.to_string()))?;
        let defs = j
            .get("flags")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::Body("no flags array".into()))?;
        self.flags.set_defs(defs.clone(), etag);
        publish_cached(cache, defs);
        Ok(())
    }

    async fn remote(
        &self,
        api_key: &str,
        id: &str,
        o: &FlagOptions,
        key: Option<&str>,
    ) -> HashMap<String, Resolved> {
        let url = format!("{}/v1/flags", self.opts.host);
        let (status, _, body) = self
            .request(
                &url,
                &remote_body(api_key, id, &self.flags.with_contexts(o), key),
                None,
                self.opts.flag_timeout,
            )
            .await;
        match serde_json::from_slice::<Value>(&body) {
            Ok(j) if status == 200 => parse_remote(&j),
            _ => {
                self.log(&format!("remote flags failed: status {status}"));
                HashMap::new()
            }
        }
    }
}

impl AsyncClient {
    /// Create a client and spawn its flusher (and poller, with a secret key) on the current runtime.
    ///
    /// # Panics
    /// Outside a tokio runtime.
    #[must_use]
    pub fn new(api_key: &str, opts: Options) -> Self {
        let mut opts = opts;
        opts.host = opts.host.trim_end_matches('/').into();
        let http = reqwest::Client::builder()
            .timeout(opts.timeout)
            .build()
            .unwrap_or_default();
        let stream_http = reqwest::Client::builder()
            .connect_timeout(opts.timeout)
            .build()
            .unwrap_or_default();
        let inner = Arc::new(Inner {
            api_key: api_key.into(),
            http,
            stream_http,
            q: Mutex::default(),
            wake: Notify::new(),
            flags: FlagState::new(opts.app_contexts.clone()),
            shared: Shared::new(opts.default_properties.clone()),
            stop: watch::channel(false).0,
            ready: watch::channel(opts.disabled || opts.secret_key.is_none()).0,
            opts,
        });
        let mut tasks = Vec::new();
        if !inner.opts.disabled && inner.opts.test_sink.is_none() {
            tasks.push(tokio::spawn(Arc::clone(&inner).run()));
            if inner.opts.secret_key.is_some() {
                tasks.push(tokio::spawn(Arc::clone(&inner).poll()));
                if inner.opts.flag_stream {
                    tasks.push(tokio::spawn(Arc::clone(&inner).stream()));
                }
            }
        }
        Self {
            inner,
            tasks: Mutex::new(tasks),
        }
    }

    #[must_use]
    pub fn dropped_events(&self) -> u64 {
        lock(&self.inner.q).dropped
    }

    pub(crate) fn shared(&self) -> &Shared {
        &self.inner.shared
    }

    /// Queue an event; never blocks on the network.
    pub fn capture(&self, e: &Event) {
        if self.inner.opts.disabled {
            return;
        }
        let o = &self.inner.opts;
        let Some(w) = build_wire(o, &self.inner.shared, e) else {
            return;
        };
        if let Some(sink) = &o.test_sink {
            sink.push(w);
            return;
        }
        if lock(&self.inner.q).push(w, o.flush_at, o.max_queue_size) {
            self.inner.wake.notify_one();
        }
    }

    /// `$identify`; an identical call (same id and properties) already sent by this client is skipped.
    pub fn identify(&self, distinct_id: &str, set: Props, set_once: Props) {
        if !self
            .inner
            .shared
            .first_identify(distinct_id, &set, &set_once)
        {
            return;
        }
        self.capture(&Event {
            set,
            set_once,
            ..Event::new(distinct_id, "$identify")
        });
    }

    pub fn alias(&self, distinct_id: &str, alias: &str) {
        self.capture(
            &Event::new(distinct_id, "$create_alias")
                .property("alias", alias)
                .property("distinct_id", distinct_id),
        );
    }

    /// `distinct_id` defaults to `$<type>_<key>`.
    pub fn group_identify(
        &self,
        group_type: &str,
        group_key: &str,
        properties: Props,
        distinct_id: Option<&str>,
    ) {
        if !self
            .inner
            .shared
            .first_group(group_type, group_key, distinct_id, &properties)
        {
            return;
        }
        let id = distinct_id.map_or_else(|| format!("${group_type}_{group_key}"), str::to_owned);
        self.capture(
            &Event::new(&id, "$groupidentify")
                .property("$group_type", group_type)
                .property("$group_key", group_key)
                .property("$group_set", properties),
        );
    }

    /// Wait until everything queued is sent or dropped; false if `timeout` elapsed first.
    pub async fn flush(&self, timeout: Option<Duration>) -> bool {
        {
            let mut q = lock(&self.inner.q);
            if !q.items.is_empty() || q.busy {
                q.flush_req = true; // not when idle: it would defeat batching for the next event
            }
        }
        self.inner.wake.notify_one();
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            {
                let q = lock(&self.inner.q);
                if q.items.is_empty() && !q.busy {
                    return true;
                }
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Stop polling, flush (up to 10 s), stop accepting events.
    pub async fn shutdown(&self) {
        let _ = self.flush(Some(Duration::from_secs(10))).await;
        {
            let mut q = lock(&self.inner.q);
            q.closed = true;
            q.abort = true;
            q.dropped += q.items.len() as u64;
            q.items.clear();
        }
        self.inner.stop.send_replace(true);
        self.inner.wake.notify_one();
        shutdown_cached(self.inner.opts.flag_definition_cache.as_deref());
        let tasks: Vec<_> = lock(&self.tasks).drain(..).collect();
        for t in tasks {
            let _ = t.await;
        }
    }

    /// Wait for the first definitions fetch attempt (immediately true without a secret key).
    pub async fn wait_ready(&self, timeout: Duration) -> bool {
        let mut rx = self.inner.ready.subscribe();
        tokio::time::timeout(timeout, rx.wait_for(|r| *r))
            .await
            .is_ok()
    }

    /// Fetch definitions now (`ETag` aware); keeps the previous ones on error.
    ///
    /// # Errors
    /// Without a secret key, or when the request or body is bad.
    pub async fn reload_feature_flags(&self) -> Result<(), Error> {
        self.inner.reload().await
    }

    /// Historical import via `POST /v1/s2s/batch` (secret key, timestamps kept), chunks of 1000.
    ///
    /// # Errors
    /// `NoSecretKey`, or `Status` at the first rejected chunk.
    pub async fn import_events(&self, events: &[Event]) -> Result<usize, Error> {
        let i = &self.inner;
        let secret = i.opts.secret_key.as_ref().ok_or(Error::NoSecretKey)?;
        if i.opts.disabled {
            return Ok(0);
        }
        let mut sent = 0;
        for chunk in events.chunks(MAX_BATCH) {
            let payload = batch_payload(None, chunk.iter().map(wire).collect());
            let (status, _, _) = i
                .request(
                    &format!("{}/v1/s2s/batch", i.opts.host),
                    &payload,
                    Some(secret),
                    None,
                )
                .await;
            if !(200..300).contains(&status) {
                return Err(Error::Status(status));
            }
            sent += chunk.len();
        }
        Ok(sent)
    }

    async fn resolve(&self, key: &str, id: &str, o: &FlagOptions) -> Option<Resolved> {
        let r = self.resolve_live(key, id, o).await;
        r.or_else(|| fallback(&self.inner.opts.fallbacks, key))
    }

    async fn resolve_live(&self, key: &str, id: &str, o: &FlagOptions) -> Option<Resolved> {
        if let Some(r) = self.inner.flags.local_one(key, id, o) {
            return Some(r);
        }
        if o.only_evaluate_locally {
            return None;
        }
        self.inner
            .remote(&self.inner.api_key, id, o, Some(key))
            .await
            .remove(key)
    }

    /// Flag value, or `None` when unknown / undecidable locally. Emits `$feature_flag_called` and
    /// `$exposure` once per `(distinct_id, flag, value)` unless `o.no_exposure`.
    pub async fn get_feature_flag(
        &self,
        key: &str,
        distinct_id: &str,
        o: &FlagOptions,
    ) -> Option<FlagValue> {
        self.get_feature_flag_details(key, distinct_id, o).await
            .map(|d| d.value)
    }

    /// [`Self::get_feature_flag`] with the full [`FlagDetails`]; an [`Options::fallbacks`] value has reason `fallback`.
    pub async fn get_feature_flag_details(
        &self,
        key: &str,
        distinct_id: &str,
        o: &FlagOptions,
    ) -> Option<FlagDetails> {
        if self.inner.opts.disabled {
            return None;
        }
        let r = self.resolve(key, distinct_id, o).await?;
        if !o.no_exposure
            && r.reason != FALLBACK_REASON
            && self.inner.flags.first_seen(distinct_id, key, &r.value)
        {
            for e in exposure_events(distinct_id, key, &r) {
                self.capture(&e);
            }
        }
        Some(FlagDetails::new(key, r))
    }

    pub async fn is_feature_enabled(
        &self,
        key: &str,
        distinct_id: &str,
        o: &FlagOptions,
    ) -> Option<bool> {
        self.get_feature_flag(key, distinct_id, o)
            .await
            .map(|v| v.is_on())
    }

    /// Payload of the flag's value; never emits events.
    pub async fn get_feature_flag_payload(
        &self,
        key: &str,
        distinct_id: &str,
        o: &FlagOptions,
    ) -> Option<Value> {
        if self.inner.opts.disabled {
            return None;
        }
        self.resolve(key, distinct_id, o).await?.payload
    }

    /// Payload of a remote-config flag, decrypted when stored encrypted (the server opens those for secret
    /// keys only). `None` when unknown or the call fails.
    ///
    /// # Errors
    /// `NoSecretKey` without a secret key.
    pub async fn get_remote_config_payload(&self, key: &str) -> Result<Option<Value>, Error> {
        let i = &self.inner;
        let secret = i.opts.secret_key.as_ref().ok_or(Error::NoSecretKey)?;
        if i.opts.disabled {
            return Ok(None);
        }
        let mut req = i
            .http
            .get(remote_config_url(&i.opts.host, key))
            .header("Authorization", format!("Bearer {secret}"));
        if let Some(t) = i.opts.flag_timeout {
            req = req.timeout(t);
        }
        let Ok(r) = req.send().await else {
            return Ok(None);
        };
        let status = r.status().as_u16();
        let body = r.bytes().await.map(|b| b.to_vec()).unwrap_or_default();
        Ok(remote_config_payload(status, &body))
    }

    async fn all_resolved(&self, id: &str, o: &FlagOptions) -> HashMap<String, Resolved> {
        let (mut res, undecided) = self.inner.flags.local_all(id, o);
        if undecided && !o.only_evaluate_locally && !self.inner.opts.disabled {
            for (k, r) in self.inner.remote(&self.inner.api_key, id, o, None).await {
                res.entry(k).or_insert(r);
            }
        }
        res
    }

    /// Every flag, locally where possible and remotely for the rest; never emits events.
    pub async fn get_all_flags(
        &self,
        distinct_id: &str,
        o: &FlagOptions,
    ) -> HashMap<String, FlagValue> {
        let all = self.all_resolved(distinct_id, o).await;
        all.into_iter().map(|(k, r)| (k, r.value)).collect()
    }

    /// Evaluate every flag once; read the snapshot freely and attach it with [`Event::with_flags`]. No events.
    pub async fn evaluate_flags(&self, distinct_id: &str, o: &FlagOptions) -> FlagSnapshot {
        FlagSnapshot::from_resolved(self.all_resolved(distinct_id, o).await)
    }

    /// Capture `e` with the distinct id's current flag values attached (`send_feature_flags`).
    pub async fn capture_with_flags(&self, e: &Event, o: &FlagOptions) {
        self.capture(
            &e.clone()
                .with_flags(&self.evaluate_flags(&e.distinct_id, o).await),
        );
    }
}
