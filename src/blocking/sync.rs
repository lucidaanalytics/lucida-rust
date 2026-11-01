//! Flag definition sync of the blocking client: the poller, the change stream, the definitions reload and the
//! remote `/v1/flags` call.

use std::io::Read;
use std::sync::atomic::Ordering;

use serde_json::Value;

use super::Inner;
use crate::core::{Error, FlagOptions, Resolved, lock, parse_remote, remote_body, retry_delay};
use crate::defcache::{adopt_cached, publish_cached};
use crate::sse::SseEvents;

impl Inner {
    pub(super) fn poll(&self) {
        let mut first = true;
        loop {
            if first || !self.flags.streaming.load(Ordering::Relaxed) {
                let _ = self.reload();
            }
            first = false;
            self.mark_ready();
            if self.stopped_after(self.opts.flag_poll_interval) {
                return;
            }
        }
    }

    /// Follow the flag stream, reconnecting with backoff; `streaming` is false while it is down.
    pub(super) fn stream(&self) {
        let mut attempt = 0;
        loop {
            attempt = if self.stream_once() { 0 } else { attempt + 1 };
            self.flags.streaming.store(false, Ordering::Relaxed);
            if self.stopped_after(retry_delay(attempt, None)) {
                return;
            }
        }
    }

    /// One connection; reports whether it was established.
    pub(super) fn stream_once(&self) -> bool {
        let Some(secret) = &self.opts.secret_key else {
            return false;
        };
        let url = format!("{}/v1/flags/stream", self.opts.host);
        let r = self
            .stream_http
            .get(&url)
            .header("Authorization", format!("Bearer {secret}"))
            .header("Accept", "text/event-stream")
            .send();
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
        let mut buf = [0u8; 4096];
        // the server sends a keepalive every 25 s, so a stop is noticed within that
        while !*lock(&self.stop.0) {
            match r.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if sse.push(&buf[..n]) {
                        let _ = self.reload();
                        self.mark_ready();
                    }
                }
            }
        }
        true
    }

    pub(super) fn reload(&self) -> Result<(), Error> {
        let Some(secret) = &self.opts.secret_key else {
            return Err(Error::NoSecretKey);
        };
        let cache = self.opts.flag_definition_cache.as_deref();
        if let Some(defs) = adopt_cached(cache) {
            self.flags.set_defs(defs, None);
            return Ok(());
        }
        let mut h = vec![("Authorization", format!("Bearer {secret}"))];
        if let Some(e) = self.flags.etag() {
            h.push(("If-None-Match", e));
        }
        let url = format!("{}/v1/flags/definitions", self.opts.host);
        // the ETag lives in a response header, so this one bypasses `request`
        let mut req = self.http.get(&url);
        for (k, v) in &h {
            req = req.header(*k, v);
        }
        if let Some(t) = self.opts.flag_timeout {
            req = req.timeout(t);
        }
        let r = req.send().map_err(|e| Error::Http(e.to_string()))?;
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
        let j: Value = serde_json::from_slice(&r.bytes().map_err(|e| Error::Http(e.to_string()))?)
            .map_err(|e| Error::Body(e.to_string()))?;
        let defs = j
            .get("flags")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::Body("no flags array".into()))?;
        self.flags.set_defs(defs.clone(), etag);
        publish_cached(cache, defs);
        Ok(())
    }

    pub(super) fn remote(
        &self,
        api_key: &str,
        id: &str,
        o: &FlagOptions,
        key: Option<&str>,
    ) -> std::collections::HashMap<String, Resolved> {
        let url = format!("{}/v1/flags", self.opts.host);
        let (status, _, body) = self.request(
            true,
            &url,
            Some(&remote_body(api_key, id, &self.flags.with_contexts(o), key)),
            &[],
            self.opts.flag_timeout,
        );
        match serde_json::from_slice::<Value>(&body) {
            Ok(j) if status == 200 => parse_remote(&j),
            _ => {
                self.log(&format!("remote flags failed: status {status}"));
                std::collections::HashMap::new()
            }
        }
    }
}
