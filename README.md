# lucida (Rust)

Server SDK for Lucida: batched capture, local feature-flag evaluation and historical import. Name a distinct id per
event, or run inside `with_context` and use `Event::in_context`.

```rust
use lucida::{Client, Event, FlagOptions, Options};

let lc = Client::new("lc_pk_...", Options { secret_key: Some("lc_sk_...".into()), ..Options::default() });
lc.capture(&Event::new("u1", "purchase").property("total", 12).group("company", "acme"));
lc.identify("u1", props, Default::default());
if lc.is_feature_enabled("new-checkout", "u1", &FlagOptions::default()) == Some(true) { /* ... */ }
lc.shutdown(); // flushes and stops the background threads
```

Features: `blocking` (default, `Client` on threads) and `tokio` (`AsyncClient`, same surface, flag reads are async).

## Behaviour

- `capture` never blocks on the network; a background thread flushes at `flush_at` (20) events or `flush_interval` (5 s).
- Bodies over 1 KB are gzipped and sent with `?compression=gzip`.
- Retry (network error, 408, 429, 5xx) with jittered exponential backoff honouring `Retry-After`; after `max_retries` (5)
  the batch is dropped. The queue is bounded (`max_queue_size`, 1000): the oldest events are dropped; `dropped_events()` counts both.
- `flush`, `shutdown`, `Options::disabled`, `Options::debug`.
- `import_events`: `POST /v1/s2s/batch` with the secret key, timestamps preserved, chunks of 1000.

## Context, personless events and hooks

```rust
use lucida::{with_context, RequestContext, Event, BeforeSend, OnError, Options};

with_context(RequestContext::new().distinct_id("u1").session_id("s1").property("svc", "api"), || {
    lc.capture(&Event::in_context("purchase")); // id, $session_id and properties come from the context
});
// async: lucida::with_context_async(ctx, false, async { ... }).await  (feature `tokio`, task-local)
```

`with_context` (thread-local) nests: the child inherits unless `with_fresh_context`; the outer context is restored on exit.
Explicit event fields always win. `read_tracing_headers(|name| ..)` reads the browser SDK's `X-Lucida-Distinct-Id` /
`X-Lucida-Session-Id` (control chars stripped, trimmed, 1000 chars; client-controlled, never use for auth).

- `Event::new_anon("name")`: personless (`$process_person_profile: false`, random UUIDv7 distinct id, `set` dropped).
- `Options::before_send` (`BeforeSend::new(|json| Some(json))`; `None` drops; a panic keeps the original),
  `Options::on_error` (`OnError::new(|msg, events| ..)` when a batch is dropped or an event has no distinct id),
  `max_batch_size` (per request, separate from `flush_at`), `flag_timeout`, `disable_geoip` (+ `Event::geoip_disabled`,
  sends `$geoip_disable: true`).
- `init_global("lc_pk_...", Options::default())` once, then `global()` anywhere (blocking client; call
  `global().map(Client::shutdown)` before exit).
- `get_remote_config_payload(key)` (secret key) reads a remote-config flag's payload, decrypted if stored encrypted.
- No `log`/`tracing` dependency exists in this crate, so logging stays on `Options::debug` (stderr).

## Flags

With a `secret_key` the client polls `GET /v1/flags/definitions` (ETag, every `flag_poll_interval` = 30 s) and evaluates
locally; `wait_ready` blocks for the first attempt, `reload_feature_flags` forces one. `evaluate_all` and `hash01` are
exported for direct use. Flags that need the server (cohort filters) report `REQUIRES_SERVER`.
It also follows `GET /v1/flags/stream` (`flag_stream`, default on) and reloads on each `definitions` push; it
reconnects with backoff and the interval poll only reloads while the stream is down.

## Tests

`make sdk-rust`. Flag evaluation is pinned by the shared vectors in `testdata/flags`.

---

This repository is a read-only mirror of the Lucida monorepo (synced by a script, never edited by hand); releases and provenance attestations are built here by GitHub Actions. Issues are welcome, changes land upstream.
