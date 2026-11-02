//! Thread-based client on `reqwest::blocking`: one flusher thread, an optional definitions poller
//! and an optional (detached) flag stream reader.

mod sync;

use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::core::{
    Error, Event, FlagOptions, FlagState, MAX_BATCH, Options, Props, QueueState, Resolved,
    batch_payload, encode, exposure_events, lock, parse_retry_after, remote_config_payload,
    remote_config_url, retry_delay, retryable, wire,
};
use crate::defcache::shutdown_cached;
use crate::details::{FALLBACK_REASON, FlagDetails, fallback};
use crate::eval::FlagValue;
use crate::hooks::build_wire;

use crate::identity::Shared;
use crate::snapshot::FlagSnapshot;

struct Inner {
    api_key: String,
    opts: Options,
    http: reqwest::blocking::Client,
    /// No overall timeout: the stream stays open.
    stream_http: reqwest::blocking::Client,
    q: Mutex<QueueState>,
    cv: Condvar,
    flags: FlagState,
    shared: Shared,
    stop: (Mutex<bool>, Condvar),
    ready: (Mutex<bool>, Condvar),
}

/// Server SDK client. Every call names its distinct id. Call [`Client::shutdown`] to flush on exit.
pub struct Client {
    inner: Arc<Inner>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl Inner {
    fn log(&self, msg: &str) {
        if self.opts.debug {
            eprintln!("[lucida] {msg}");
        }
    }

    /// Returns (status or 0 on network error, Retry-After, body).
    fn request(
        &self,
        post: bool,
        url: &str,
        payload: Option<&Value>,
        headers: &[(&str, String)],
        timeout: Option<Duration>,
    ) -> (u16, Option<Duration>, Vec<u8>) {
        let (url, body) = payload.map_or((url.to_owned(), Vec::new()), |p| encode(url, p));
        let mut req = if post {
            self.http
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body)
        } else {
            self.http.get(&url)
        };
        for (k, v) in headers {
            req = req.header(*k, v);
        }
        if let Some(t) = timeout {
            req = req.timeout(t);
        }
        match req.send() {
            Ok(r) => {
                let status = r.status().as_u16();
                let ra =
                    parse_retry_after(r.headers().get("retry-after").and_then(|v| v.to_str().ok()));
                (
                    status,
                    ra,
                    r.bytes().map(|b| b.to_vec()).unwrap_or_default(),
                )
            }
            Err(e) => {
                self.log(&format!("request failed: {e}"));
                (0, None, Vec::new())
            }
        }
    }

    fn run(&self) {
        let mut first_at: Option<Instant> = None;
        loop {
            let batch = {
                let mut q = lock(&self.q);
                loop {
                    let n = q.items.len();
                    if n == 0 && q.closed {
                        return;
                    }
                    if n > 0 && (n >= self.opts.flush_at || q.flush_req || q.closed) {
                        break;
                    }
                    if n > 0 {
                        let at = *first_at.get_or_insert_with(Instant::now);
                        let Some(rem) = (at + self.opts.flush_interval)
                            .checked_duration_since(Instant::now())
                            .filter(|d| !d.is_zero())
                        else {
                            break;
                        };
                        q = self
                            .cv
                            .wait_timeout(q, rem)
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .0;
                    } else {
                        first_at = None;
                        q = self
                            .cv
                            .wait(q)
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                }
                q.busy = true;
                q.take(self.opts.max_batch_size)
            };
            first_at = None;
            self.send(batch);
            let mut q = lock(&self.q);
            q.busy = false;
            if q.items.is_empty() {
                q.flush_req = false;
            }
            self.cv.notify_all();
        }
    }

    fn send(&self, batch: Vec<Value>) {
        let n = batch.len() as u64;
        let payload = batch_payload(Some(&self.api_key), batch);
        let url = format!("{}/v1/batch", self.opts.host);
        let mut attempt = 0;
        loop {
            let (status, ra, _) = self.request(true, &url, Some(&payload), &[], None);
            if (200..300).contains(&status) {
                return;
            }
            if !retryable(status) || attempt >= self.opts.max_retries || lock(&self.q).abort {
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
            let q = lock(&self.q);
            drop(
                self.cv
                    .wait_timeout_while(q, delay, |q| !q.abort)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
    }

    fn mark_ready(&self) {
        let (m, c) = &self.ready;
        *lock(m) = true;
        c.notify_all();
    }

    /// Wait up to `d`; true once shutdown was requested.
    fn stopped_after(&self, d: Duration) -> bool {
        let (m, c) = &self.stop;
        *c.wait_timeout_while(lock(m), d, |s| !*s)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
    }
}

impl Client {
    /// Create a client and start its background threads (none when `opts.disabled`).
    #[must_use]
    pub fn new(api_key: &str, opts: Options) -> Self {
        let mut opts = opts;
        opts.host = opts.host.trim_end_matches('/').into();
        let http = reqwest::blocking::Client::builder()
            .timeout(opts.timeout)
            .build()
            .unwrap_or_default();
        let stream_http = reqwest::blocking::Client::builder()
            .timeout(None)
            .connect_timeout(opts.timeout)
            .build()
            .unwrap_or_default();
        let inner = Arc::new(Inner {
            api_key: api_key.into(),
            http,
            stream_http,
            q: Mutex::default(),
            cv: Condvar::new(),
            flags: FlagState::new(opts.app_contexts.clone()),
            shared: Shared::new(opts.default_properties.clone()),
            stop: (Mutex::new(false), Condvar::new()),
            ready: (
                Mutex::new(opts.disabled || opts.secret_key.is_none()),
                Condvar::new(),
            ),
            opts,
        });
        let mut threads = Vec::new();
        if !inner.opts.disabled && inner.opts.test_sink.is_none() {
            let i = Arc::clone(&inner);
            threads.push(
                thread::Builder::new()
                    .name("lucida-flusher".into())
                    .spawn(move || i.run())
                    .expect("spawn flusher"),
            );
            if inner.opts.secret_key.is_some() {
                let i = Arc::clone(&inner);
                threads.push(
                    thread::Builder::new()
                        .name("lucida-flags".into())
                        .spawn(move || i.poll())
                        .expect("spawn poller"),
                );
                if inner.opts.flag_stream {
                    let i = Arc::clone(&inner);
                    // detached: a blocking read cannot be interrupted, so shutdown does not wait for it
                    drop(
                        thread::Builder::new()
                            .name("lucida-flag-stream".into())
                            .spawn(move || i.stream())
                            .expect("spawn flag stream"),
                    );
                }
            }
        }
        Self {
            inner,
            threads: Mutex::new(threads),
        }
    }

    /// Events discarded because the queue was full or the server kept failing.
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
            self.inner.cv.notify_all();
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

    /// Block until everything queued is sent or dropped; false if `timeout` elapsed first.
    #[must_use]
    pub fn flush(&self, timeout: Option<Duration>) -> bool {
        let i = &self.inner;
        let mut q = lock(&i.q);
        if !q.items.is_empty() || q.busy {
            q.flush_req = true; // not when idle: it would defeat batching for the next event
        }
        i.cv.notify_all();
        let pending = |q: &mut QueueState| !q.items.is_empty() || q.busy;
        if let Some(t) = timeout {
            !i.cv
                .wait_timeout_while(q, t, pending)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .1
                .timed_out()
        } else {
            drop(
                i.cv.wait_while(q, pending)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
            true
        }
    }

    /// Stop polling, flush (up to 10 s), stop accepting events. Idempotent.
    pub fn shutdown(&self) {
        let i = &self.inner;
        let _ = self.flush(Some(Duration::from_secs(10)));
        {
            let mut q = lock(&i.q);
            q.closed = true;
            q.abort = true;
            q.dropped += q.items.len() as u64;
            q.items.clear();
            i.cv.notify_all();
        }
        *lock(&i.stop.0) = true;
        i.stop.1.notify_all();
        shutdown_cached(i.opts.flag_definition_cache.as_deref());
        for t in lock(&self.threads).drain(..) {
            let _ = t.join();
        }
    }

    /// Wait for the first definitions fetch attempt (immediately true without a secret key).
    #[must_use]
    pub fn wait_ready(&self, timeout: Duration) -> bool {
        let (m, c) = &self.inner.ready;
        *c.wait_timeout_while(lock(m), timeout, |r| !*r)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
    }

    /// Fetch definitions now (`ETag` aware); keeps the previous ones on error.
    ///
    /// # Errors
    /// Without a secret key, or when the request or body is bad.
    pub fn reload_feature_flags(&self) -> Result<(), Error> {
        self.inner.reload()
    }

    /// Historical import via `POST /v1/s2s/batch` (secret key, timestamps kept), chunks of 1000.
    ///
    /// # Errors
    /// `NoSecretKey`, or `Status` at the first rejected chunk.
    pub fn import_events(&self, events: &[Event]) -> Result<usize, Error> {
        let i = &self.inner;
        let secret = i.opts.secret_key.as_ref().ok_or(Error::NoSecretKey)?;
        if i.opts.disabled {
            return Ok(0);
        }
        let mut sent = 0;
        for chunk in events.chunks(MAX_BATCH) {
            let payload = batch_payload(None, chunk.iter().map(wire).collect());
            let (status, _, _) = i.request(
                true,
                &format!("{}/v1/s2s/batch", i.opts.host),
                Some(&payload),
                &[("Authorization", format!("Bearer {secret}"))],
                None,
            );
            if !(200..300).contains(&status) {
                return Err(Error::Status(status));
            }
            sent += chunk.len();
        }
        Ok(sent)
    }

    fn resolve(&self, key: &str, id: &str, o: &FlagOptions) -> Option<Resolved> {
        let i = &self.inner;
        i.flags
            .local_one(key, id, o)
            .or_else(|| {
                if o.only_evaluate_locally {
                    None
                } else {
                    i.remote(&i.api_key, id, o, Some(key)).remove(key)
                }
            })
            .or_else(|| fallback(&i.opts.fallbacks, key))
    }

    /// Flag value, or `None` when unknown / undecidable locally. Emits `$feature_flag_called` and
    /// `$exposure` once per `(distinct_id, flag, value)` unless `o.no_exposure`.
    #[must_use]
    pub fn get_feature_flag(
        &self,
        key: &str,
        distinct_id: &str,
        o: &FlagOptions,
    ) -> Option<FlagValue> {
        self.get_feature_flag_details(key, distinct_id, o)
            .map(|d| d.value)
    }

    /// [`Self::get_feature_flag`] with the full [`FlagDetails`]; an [`Options::fallbacks`] value has reason `fallback`.
    #[must_use]
    pub fn get_feature_flag_details(
        &self,
        key: &str,
        distinct_id: &str,
        o: &FlagOptions,
    ) -> Option<FlagDetails> {
        if self.inner.opts.disabled {
            return None;
        }
        let r = self.resolve(key, distinct_id, o)?;
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

    #[must_use]
    pub fn is_feature_enabled(
        &self,
        key: &str,
        distinct_id: &str,
        o: &FlagOptions,
    ) -> Option<bool> {
        self.get_feature_flag(key, distinct_id, o)
            .map(|v| v.is_on())
    }

    /// Payload of the flag's value; never emits events.
    #[must_use]
    pub fn get_feature_flag_payload(
        &self,
        key: &str,
        distinct_id: &str,
        o: &FlagOptions,
    ) -> Option<Value> {
        if self.inner.opts.disabled {
            return None;
        }
        self.resolve(key, distinct_id, o)?.payload
    }

    /// Payload of a remote-config flag, decrypted when stored encrypted (the server opens those for secret
    /// keys only). `None` when unknown or the call fails.
    ///
    /// # Errors
    /// `NoSecretKey` without a secret key.
    pub fn get_remote_config_payload(&self, key: &str) -> Result<Option<Value>, Error> {
        let i = &self.inner;
        let secret = i.opts.secret_key.as_ref().ok_or(Error::NoSecretKey)?;
        if i.opts.disabled {
            return Ok(None);
        }
        let (status, _, body) = i.request(
            false,
            &remote_config_url(&i.opts.host, key),
            None,
            &[("Authorization", format!("Bearer {secret}"))],
            i.opts.flag_timeout,
        );
        Ok(remote_config_payload(status, &body))
    }

    fn all_resolved(
        &self,
        id: &str,
        o: &FlagOptions,
    ) -> std::collections::HashMap<String, Resolved> {
        let (mut res, undecided) = self.inner.flags.local_all(id, o);
        if undecided && !o.only_evaluate_locally && !self.inner.opts.disabled {
            for (k, r) in self.inner.remote(&self.inner.api_key, id, o, None) {
                res.entry(k).or_insert(r);
            }
        }
        res
    }

    /// Every flag, locally where possible and remotely for the rest; never emits events.
    #[must_use]
    pub fn get_all_flags(
        &self,
        distinct_id: &str,
        o: &FlagOptions,
    ) -> std::collections::HashMap<String, FlagValue> {
        let all = self.all_resolved(distinct_id, o);
        all.into_iter().map(|(k, r)| (k, r.value)).collect()
    }

    /// Evaluate every flag once; read the snapshot freely and attach it with [`Event::with_flags`]. No events.
    #[must_use]
    pub fn evaluate_flags(&self, distinct_id: &str, o: &FlagOptions) -> FlagSnapshot {
        FlagSnapshot::from_resolved(self.all_resolved(distinct_id, o))
    }

    /// Capture `e` with the distinct id's current flag values attached (`send_feature_flags`).
    pub fn capture_with_flags(&self, e: &Event, o: &FlagOptions) {
        self.capture(
            &e.clone()
                .with_flags(&self.evaluate_flags(&e.distinct_id, o)),
        );
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.shutdown();
    }
}
