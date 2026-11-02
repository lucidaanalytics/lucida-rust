//! Flag snapshot, event enrichment, definition cache provider and app contexts (LC-FLG-28, 29, 45).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lucida::{
    BeforeSend, Client, Event, FlagDefinitionCache, FlagOptions, InMemoryFlagDefinitionCache,
    Options,
};
use serde_json::{Value, json};

fn flag(key: &str, ctxs: &[&str], pct: u32) -> Value {
    json!({"key": key, "type": "boolean", "active": true, "salt": "s", "aggregation": {"kind": "person"},
           "app_contexts": ctxs, "conditions": [{"rollout_pct": pct}]})
}

fn defs() -> Vec<Value> {
    vec![
        flag("a", &[], 100),
        flag("b", &[], 100),
        flag("off", &[], 0),
        flag("web", &["web"], 100),
    ]
}

/// Serves `GET /v1/flags/definitions` with `defs()` and counts those requests; everything else gets `{}`.
fn server() -> (String, Arc<AtomicUsize>) {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let host = format!("http://{}", l.local_addr().expect("addr"));
    let loads = Arc::new(AtomicUsize::new(0));
    let n = Arc::clone(&loads);
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { return };
            let mut buf = [0u8; 4096];
            let k = s.read(&mut buf).unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..k]).into_owned();
            let body = if head.contains("/v1/flags/definitions") {
                n.fetch_add(1, Ordering::SeqCst);
                json!({"flags": defs()}).to_string()
            } else {
                "{}".into()
            };
            let _ = s.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
        }
    });
    (host, loads)
}

type Seen = Arc<Mutex<Vec<Value>>>;

fn client(host: String, extra: impl FnOnce(&mut Options)) -> (Client, Seen) {
    let seen: Seen = Arc::default();
    let s = Arc::clone(&seen);
    let mut o = Options {
        host,
        secret_key: Some("lc_sk_x".into()),
        flag_stream: false,
        flag_poll_interval: Duration::from_secs(3600),
        flush_interval: Duration::from_secs(3600),
        before_send: Some(BeforeSend::new(move |v| {
            s.lock().expect("lock").push(v);
            None
        })),
        ..Options::default()
    };
    extra(&mut o);
    let c = Client::new("lc_pk_x", o);
    assert!(c.wait_ready(Duration::from_secs(10)));
    (c, seen)
}

#[test]
fn snapshot_enrichment_and_app_contexts() {
    let (host, _) = server();
    let (c, seen) = client(host, |o| o.app_contexts = vec!["api".into()]);
    let o = FlagOptions::default();
    let snap = c.evaluate_flags("u", &o);
    assert!(snap.is_enabled("a"));
    assert_eq!(snap.keys(), ["a", "b", "off"]); // "web" is scoped to another app context
    c.capture(&Event::new("u", "e1").with_flags(&snap.only_accessed()));
    c.capture_with_flags(&Event::new("u", "buy").property("$feature/a", "mine"), &o);
    let seen = seen.lock().expect("lock");
    let p1 = &seen[0]["properties"];
    assert_eq!(p1["$feature/a"], true);
    assert!(p1.get("$feature/b").is_none());
    let p2 = &seen[1]["properties"];
    assert_eq!(p2["$feature/a"], "mine"); // caller wins
    assert_eq!(p2["$feature/off"], false);
    assert!(p2.get("$feature/web").is_none());
    assert_eq!(p2["$active_feature_flags"], json!(["a", "b"]));
    assert!(seen.iter().all(|e| e["event"] != "$feature_flag_called"));
}

#[test]
fn definition_cache_is_adopted_and_published() {
    let (host, loads) = server();
    let cache = Arc::new(InMemoryFlagDefinitionCache::new(Duration::from_secs(60)));
    cache.on_flag_definitions_received(&defs());
    let h = host.clone();
    let (c, _) = client(h, |o| o.flag_definition_cache = Some(cache.clone()));
    assert_eq!(
        loads.load(Ordering::SeqCst),
        0,
        "fresh cache means no fetch"
    );
    assert!(
        c.is_feature_enabled("a", "u", &FlagOptions::default())
            .unwrap_or(false)
    );
    c.shutdown();

    let empty = Arc::new(InMemoryFlagDefinitionCache::default());
    let (c, _) = client(host, |o| o.flag_definition_cache = Some(empty.clone()));
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(empty.flag_definitions().map(|d| d.len()), Some(4));
    c.shutdown();
}

#[derive(Debug)]
struct Broken;

impl FlagDefinitionCache for Broken {
    fn flag_definitions(&self) -> Option<Vec<Value>> {
        panic!("down")
    }
    fn should_fetch_flag_definitions(&self) -> bool {
        panic!("down")
    }
    fn on_flag_definitions_received(&self, _: &[Value]) {
        panic!("down")
    }
}

#[test]
fn broken_cache_means_fetch() {
    let (host, loads) = server();
    let (c, _) = client(host, |o| o.flag_definition_cache = Some(Arc::new(Broken)));
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(
        c.is_feature_enabled("a", "u", &FlagOptions::default()),
        Some(true)
    );
    c.shutdown();
}

#[test]
fn fallbacks_serve_without_exposure_and_details_carry_the_reason() {
    let (host, _) = server();
    let (c, seen) = client(host, |o| {
        o.fallbacks
            .insert("ghost".into(), lucida::FlagValue::Variant("safe".into()));
    });
    let o = FlagOptions::default();
    let d = c.get_feature_flag_details("ghost", "u", &o).expect("fallback");
    assert_eq!(d.value, lucida::FlagValue::Variant("safe".into()));
    assert_eq!(d.reason, "fallback");
    let a = c.get_feature_flag_details("a", "u", &o).expect("a");
    assert_eq!(a.value, lucida::FlagValue::Bool(true));
    assert_eq!((a.reason.as_str(), a.condition_index), ("condition_match", 0));
    assert!(c.get_feature_flag_details("nope", "u", &o).is_none());
    let seen = seen.lock().expect("lock");
    assert!(!seen.is_empty() && seen.iter().all(|e| e["properties"]["$feature_flag"] == "a"));
}

#[test]
fn network_failure_serves_the_fallback() {
    let c = Client::new(
        "lc_pk_x",
        Options {
            host: "http://127.0.0.1:1".into(),
            max_retries: 0,
            flush_interval: Duration::from_secs(3600),
            fallbacks: [("x".to_string(), lucida::FlagValue::Bool(true))].into(),
            ..Options::default()
        },
    );
    let d = c.get_feature_flag_details("x", "u", &FlagOptions::default());
    assert_eq!(d.expect("fallback").reason, "fallback");
}
