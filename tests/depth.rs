//! Test sink, super properties, person properties, identify dedupe, context propagation to spawned work.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lucida::{
    BeforeSend, Client, Event, Options, Props, RequestContext, TestSink, bind_context, with_context,
};
use serde_json::{Value, json};

fn client() -> (Client, TestSink) {
    let sink = TestSink::new();
    let c = Client::new(
        "lc_pk_x",
        Options {
            test_sink: Some(sink.clone()),
            default_properties: json!({"app": "a"}).as_object().cloned().unwrap_or_default(),
            ..Options::default()
        },
    );
    (c, sink)
}

fn props(v: serde_json::Value) -> Props {
    v.as_object().cloned().unwrap_or_default()
}

#[test]
fn sink_records_events_with_super_properties_per_call_winning() {
    let (c, sink) = client();
    c.register(props(json!({"env": "prod", "app": "b"})));
    c.unregister("env");
    c.capture(&Event::new("u", "e1"));
    c.capture(&Event::new("u", "e2").property("app", "mine"));
    let ev = sink.events();
    assert_eq!(ev[0]["properties"]["app"], "b");
    assert!(ev[0]["properties"].get("env").is_none());
    assert_eq!(ev[1]["properties"]["app"], "mine");
}

#[test]
fn person_properties_and_identify_group_dedupe() {
    let (c, sink) = client();
    c.set_person_properties("u", props(json!({"plan": "pro"})));
    c.set_person_properties_once("u", props(json!({"first": "x"})));
    for _ in 0..3 {
        c.identify("u", props(json!({"a": 1})), Props::new());
        c.group_identify("org", "acme", props(json!({"n": 1})), None);
    }
    c.identify("u", props(json!({"a": 2})), Props::new());
    let ev = sink.events();
    assert_eq!(ev[0]["$set"]["plan"], "pro");
    assert_eq!(ev[1]["$set_once"]["first"], "x");
    assert_eq!(
        sink.names(),
        ["$set", "$set", "$identify", "$groupidentify", "$identify"]
    );
}

/// Requests the stub saw, as `(path, body)`.
type Seen = Arc<Mutex<Vec<(String, Value)>>>;

/// One-thread HTTP stub: answers the n-th request with `statuses[n]` (200 after that) and records `(path, body)`.
fn stub(statuses: Vec<u16>) -> (String, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let host = format!("http://{}", listener.local_addr().expect("addr"));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    std::thread::spawn(move || {
        for (n, conn) in listener.incoming().enumerate() {
            let Ok(mut s) = conn else { return };
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            let (head_end, len) = loop {
                let Ok(r) = s.read(&mut chunk) else { return };
                if r == 0 {
                    return;
                }
                buf.extend_from_slice(&chunk[..r]);
                let text = String::from_utf8_lossy(&buf).to_string();
                if let Some(i) = text.find("\r\n\r\n") {
                    let len = text
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    break (i + 4, len);
                }
            };
            while buf.len() < head_end + len {
                let Ok(r) = s.read(&mut chunk) else { return };
                if r == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..r]);
            }
            let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
            let path = head
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_owned();
            let body = serde_json::from_slice(&buf[head_end..]).unwrap_or(Value::Null);
            log.lock().expect("log").push((path, body));
            let status = statuses.get(n).copied().unwrap_or(200);
            let _ = s.write_all(
                format!(
                    "HTTP/1.1 {status} X\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
                )
                .as_bytes(),
            );
        }
    });
    (host, seen)
}

fn names(seen: &Mutex<Vec<(String, Value)>>, path: &str) -> Vec<String> {
    seen.lock()
        .expect("log")
        .iter()
        .filter(|(p, _)| p == path)
        .flat_map(|(_, b)| b["batch"].as_array().cloned().unwrap_or_default())
        .filter_map(|e| e["event"].as_str().map(str::to_owned))
        .collect()
}

#[test]
fn identify_and_group_are_resent_after_their_batch_was_dropped() {
    let (host, seen) = stub(vec![400]);
    let c = Client::new(
        "lc_pk_x",
        Options {
            host,
            flush_at: 100,
            flush_interval: Duration::from_secs(3600),
            max_retries: 0,
            ..Options::default()
        },
    );
    for _ in 0..2 {
        c.identify("u", props(json!({"a": 1})), Props::new());
        c.group_identify("org", "acme", props(json!({"n": 1})), None);
        assert!(c.flush(Some(Duration::from_secs(5))));
    }
    // the rejected batch is recorded by the stub too; without the fix the repeat would never be sent
    assert_eq!(
        names(&seen, "/v1/batch"),
        ["$identify", "$groupidentify", "$identify", "$groupidentify"]
    );
}

#[test]
fn import_events_applies_defaults_and_before_send_unless_raw() {
    let (host, seen) = stub(vec![]);
    let c = Client::new(
        "lc_pk_x",
        Options {
            host,
            secret_key: Some("lc_sk_x".into()),
            default_properties: props(json!({"env": "prod"})),
            before_send: Some(BeforeSend::new(|e| (e["event"] != "drop").then_some(e))),
            ..Options::default()
        },
    );
    let ev = |name: &str| Event::new("u", name);
    assert_eq!(
        c.import_events(&[ev("keep"), ev("drop")]).expect("import"),
        1
    );
    assert_eq!(c.import_events_raw(&[ev("plain")]).expect("raw import"), 1);
    let sent: Vec<Value> = seen
        .lock()
        .expect("log")
        .iter()
        .filter(|(p, _)| p == "/v1/s2s/batch")
        .map(|(_, b)| b["batch"][0].clone())
        .collect();
    assert_eq!(sent[0]["event"], "keep");
    assert_eq!(sent[0]["properties"]["env"], "prod");
    assert_eq!(sent[1]["event"], "plain");
    assert!(sent[1]["properties"]["env"].is_null());
}

#[test]
fn bind_context_carries_the_scope_into_a_spawned_thread() {
    let (c, sink) = client();
    let c = std::sync::Arc::new(c);
    let c2 = std::sync::Arc::clone(&c);
    with_context(RequestContext::new().distinct_id("ctx-user"), || {
        std::thread::spawn(bind_context(move || {
            c2.capture(&Event::in_context("from_thread"));
        }))
        .join()
        .expect("thread");
    });
    assert_eq!(sink.events()[0]["distinct_id"], "ctx-user");
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn global_async_and_bound_spawn() {
    use lucida::{bind_context_async, global_async, init_global_async, with_context_async};
    let sink = TestSink::new();
    assert!(init_global_async(
        "lc_pk_x",
        Options {
            test_sink: Some(sink.clone()),
            ..Options::default()
        }
    ));
    assert!(!init_global_async("lc_pk_x", Options::default()));
    with_context_async(RequestContext::new().distinct_id("au"), false, async {
        tokio::spawn(bind_context_async(async {
            global_async()
                .expect("global")
                .capture(&Event::in_context("spawned"));
        }))
        .await
        .expect("join");
    })
    .await;
    assert_eq!(sink.events()[0]["distinct_id"], "au");
}
