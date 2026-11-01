//! Request context, personless events, delivery hooks and the global client. `before_send` records the wire
//! events (and drops them), so these tests need no server except the dropped-batch one.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lucida::{
    BeforeSend, Client, Event, OnError, Options, RequestContext, current_context, global,
    init_global, read_tracing_headers, with_context, with_fresh_context,
};
use serde_json::Value;

type Seen = Arc<Mutex<Vec<Value>>>;

fn recording(extra: impl FnOnce(&mut Options)) -> (Client, Seen, Arc<Mutex<Vec<String>>>) {
    let seen: Seen = Arc::default();
    let errs: Arc<Mutex<Vec<String>>> = Arc::default();
    let (s, e) = (Arc::clone(&seen), Arc::clone(&errs));
    let mut o = Options {
        flush_interval: Duration::from_secs(3600),
        before_send: Some(BeforeSend::new(move |v| {
            s.lock().expect("lock").push(v);
            None
        })),
        on_error: Some(OnError::new(move |m, _| {
            e.lock().expect("lock").push(m.to_owned());
        })),
        ..Options::default()
    };
    extra(&mut o);
    (Client::new("lc_pk_x", o), seen, errs)
}

#[test]
fn context_merges_nests_and_explicit_wins() {
    let (c, seen, _) = recording(|_| {});
    let ctx = RequestContext::new()
        .distinct_id("u1")
        .session_id("s1")
        .property("a", 1)
        .property("b", 1);
    with_context(ctx, || {
        with_context(RequestContext::new().property("b", 2), || {
            c.capture(&Event::in_context("inner"));
        });
        with_fresh_context(RequestContext::new(), || {
            assert!(current_context().is_some_and(|c| c.distinct_id.is_none()));
        });
        c.capture(&Event::new("explicit", "outer").property("a", 9));
    });
    assert!(current_context().is_none());
    let v = seen.lock().expect("lock");
    assert_eq!(v[0]["distinct_id"], "u1");
    assert_eq!(v[0]["properties"]["$session_id"], "s1");
    assert_eq!(
        (
            v[0]["properties"]["a"].clone(),
            v[0]["properties"]["b"].clone()
        ),
        (1.into(), 2.into())
    );
    assert_eq!(v[1]["distinct_id"], "explicit");
    assert_eq!(
        (
            v[1]["properties"]["a"].clone(),
            v[1]["properties"]["b"].clone()
        ),
        (9.into(), 1.into())
    );
    c.shutdown();
}

#[test]
fn personless_and_missing_distinct_id() {
    let (c, seen, errs) = recording(|_| {});
    c.capture(&Event::in_context("no id"));
    assert!(seen.lock().expect("lock").is_empty());
    assert_eq!(errs.lock().expect("lock").len(), 1);
    let mut e = Event::new_anon("anon");
    e.set.insert("a".into(), 1.into());
    c.capture(&e);
    let v = seen.lock().expect("lock");
    assert_eq!(v[0]["properties"]["$process_person_profile"], false);
    assert_eq!(v[0]["distinct_id"].as_str().map(str::len), Some(36));
    assert!(v[0].get("$set").is_none());
    c.shutdown();
}

#[test]
fn geoip_global_and_per_event() {
    let (c, seen, _) = recording(|o| o.disable_geoip = true);
    c.capture(&Event::new("u", "a"));
    c.capture(&Event::new("u", "b").geoip_disabled(false));
    let v = seen.lock().expect("lock");
    assert_eq!(v[0]["properties"]["$geoip_disable"], true);
    assert!(v[1]["properties"].get("$geoip_disable").is_none());
    c.shutdown();
    let (c2, seen2, _) = recording(|_| {});
    c2.capture(&Event::new("u", "c").geoip_disabled(true));
    assert_eq!(
        seen2.lock().expect("lock")[0]["properties"]["$geoip_disable"],
        true
    );
    c2.shutdown();
}

#[test]
fn before_send_edit_and_panic_keep_original() {
    let seen: Seen = Arc::default();
    let errs: Arc<Mutex<Vec<String>>> = Arc::default();
    let (s, e) = (Arc::clone(&seen), Arc::clone(&errs));
    let c = Client::new(
        "k",
        Options {
            flush_interval: Duration::from_secs(3600),
            disabled: false,
            before_send: Some(BeforeSend::new(|mut v| {
                assert!(v["event"] != "boom", "hook failure");
                v["properties"]["edited"] = true.into();
                Some(v)
            })),
            on_error: Some(OnError::new(move |m, ev| {
                e.lock().expect("lock").push(m.to_owned());
                s.lock().expect("lock").extend_from_slice(ev);
            })),
            host: "http://127.0.0.1:1".into(),
            max_retries: 0,
            ..Options::default()
        },
    );
    c.capture(&Event::new("u", "boom"));
    assert!(errs.lock().expect("lock")[0].contains("before_send panicked"));
    assert_eq!(seen.lock().expect("lock")[0]["event"], "boom"); // the original, unedited event
    assert!(
        seen.lock().expect("lock")[0]["properties"]
            .get("edited")
            .is_none()
    );
    c.shutdown();
}

#[test]
fn tracing_headers_are_cleaned() {
    let long = format!(" a\u{0}b{}", "z".repeat(2000));
    let (d, s) = read_tracing_headers(|n| match n {
        "X-Lucida-Distinct-Id" => Some(long.as_str()),
        "X-Lucida-Session-Id" => Some("   "),
        _ => None,
    });
    let d = d.expect("distinct id");
    assert_eq!((d.len(), d.starts_with("ab")), (1000, true));
    assert!(s.is_none());
}

#[test]
fn dropped_batch_reports_on_error_and_respects_max_batch_size() {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let host = format!("http://{}", l.local_addr().expect("addr"));
    let sizes: Arc<Mutex<Vec<usize>>> = Arc::default();
    let sz = Arc::clone(&sizes);
    std::thread::spawn(move || {
        for mut s in l.incoming().flatten() {
            let mut buf = vec![0u8; 65536];
            let n = s.read(&mut buf).unwrap_or(0);
            let text = String::from_utf8_lossy(&buf[..n]).to_string();
            if let Some(body) = text.split("\r\n\r\n").nth(1)
                && let Ok(v) = serde_json::from_str::<Value>(body)
            {
                sz.lock()
                    .expect("lock")
                    .push(v["batch"].as_array().map_or(0, Vec::len));
            }
            let _ = s.write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    let errs: Arc<Mutex<Vec<usize>>> = Arc::default();
    let e = Arc::clone(&errs);
    let c = Client::new(
        "k",
        Options {
            host,
            flush_at: 10,
            max_batch_size: 2,
            flush_interval: Duration::from_secs(3600),
            on_error: Some(OnError::new(move |_, ev| {
                e.lock().expect("lock").push(ev.len());
            })),
            ..Options::default()
        },
    );
    for i in 0..5 {
        c.capture(&Event::new("u", &format!("e{i}")));
    }
    assert!(c.flush(Some(Duration::from_secs(10))));
    assert_eq!(*sizes.lock().expect("lock"), vec![2, 2, 1]);
    assert_eq!(*errs.lock().expect("lock"), vec![2, 2, 1]);
    assert_eq!(c.dropped_events(), 5);
    c.shutdown();
}

#[test]
fn global_client_is_set_once() {
    assert!(global().is_none());
    let o = || Options {
        disabled: true,
        ..Options::default()
    };
    assert!(init_global("k", o()));
    assert!(!init_global("other", o()));
    assert!(global().is_some());
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn async_context_follows_the_task() {
    use lucida::{AsyncClient, with_context_async};
    let seen: Seen = Arc::default();
    let s = Arc::clone(&seen);
    let c = AsyncClient::new(
        "k",
        Options {
            flush_interval: Duration::from_secs(3600),
            before_send: Some(BeforeSend::new(move |v| {
                s.lock().expect("lock").push(v);
                None
            })),
            ..Options::default()
        },
    );
    with_context_async(
        RequestContext::new().distinct_id("t1").property("k", 1),
        false,
        async {
            tokio::task::yield_now().await;
            c.capture(&Event::in_context("in task"));
        },
    )
    .await;
    c.capture(&Event::new("x", "outside"));
    {
        let v = seen.lock().expect("lock");
        assert_eq!(
            (v[0]["distinct_id"].clone(), v[0]["properties"]["k"].clone()),
            ("t1".into(), 1.into())
        );
        assert!(v[1]["properties"].get("k").is_none());
    }
    c.shutdown().await;
}

#[test]
fn remote_config_payload_gets_the_escaped_route_with_the_secret() {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let host = format!("http://{}", l.local_addr().expect("addr"));
    let heads: Arc<Mutex<Vec<String>>> = Arc::default();
    let h = Arc::clone(&heads);
    std::thread::spawn(move || {
        for mut s in l.incoming().flatten() {
            let mut buf = vec![0u8; 65536];
            let n = s.read(&mut buf).unwrap_or(0);
            let text = String::from_utf8_lossy(&buf[..n]).to_string();
            let resp = if text.starts_with("GET /v1/flags/a%2Fb/remote-config ") {
                h.lock().expect("lock").push(text.to_lowercase());
                let body = r#"{"key":"a/b","enabled":true,"payload":{"a":1}}"#;
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
            } else {
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into()
            };
            let _ = s.write_all(resp.as_bytes());
        }
    });
    let c = Client::new(
        "lc_pk",
        Options {
            host,
            secret_key: Some("lc_sk".into()),
            flag_stream: false,
            ..Options::default()
        },
    );
    let p = c.get_remote_config_payload("a/b").expect("secret set");
    assert_eq!(p, Some(serde_json::json!({"a": 1})));
    assert!(heads.lock().expect("lock")[0].contains("authorization: bearer lc_sk"));
    assert_eq!(
        c.get_remote_config_payload("nope").expect("secret set"),
        None
    );
    c.shutdown();
    let none = Client::new(
        "k",
        Options {
            disabled: true,
            ..Options::default()
        },
    );
    assert!(none.get_remote_config_payload("x").is_err());
}
