//! Shared flag vectors (`testdata/flags`) and a capture round trip against a throwaway TCP server.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use lucida::{Client, Event, FlagValue, Options, evaluate_all, hash01};
use serde_json::{Value, json};

fn load(name: &str) -> Value {
    let root = std::env::var("LUCIDA_TESTDATA")
        .unwrap_or_else(|_| format!("{}/../../testdata", env!("CARGO_MANIFEST_DIR")));
    let p = format!("{root}/flags/{name}");
    serde_json::from_slice(&std::fs::read(p).expect("read vectors")).expect("parse vectors")
}

/// Keep only the context fields a vector declares in `needs`: anything else must not matter.
fn supplied(ctx: &Value, needs: &Value) -> Value {
    let Some(needs) = needs.as_array() else {
        return ctx.clone();
    };
    let mut out = serde_json::Map::new();
    for (k, v) in ctx.as_object().expect("context object") {
        let owner = if k == "bucketing_key" {
            "distinct_id"
        } else {
            k
        };
        if needs.iter().any(|n| n == owner) {
            out.insert(k.clone(), v.clone());
        }
    }
    Value::Object(out)
}

fn run(file: &str, min: usize) {
    let d = load(file);
    for h in d["hash_vectors"].as_array().expect("hash_vectors") {
        let input = h["input"].as_str().expect("input");
        assert!(
            (hash01(input) - h["hash"].as_f64().expect("hash")).abs() < 1e-15,
            "hash {input}"
        );
    }
    let flags = d["flags"].as_array().expect("flags");
    let vectors = d["vectors"].as_array().expect("vectors");
    assert!(vectors.len() >= min, "only {} vectors", vectors.len());
    for v in vectors {
        let name = v["name"].as_str().unwrap_or_default();
        let got = &evaluate_all(flags, &supplied(&v["context"], &v["needs"]))
            [v["flag"].as_str().expect("flag")];
        let e = &v["expected"];
        let value = match &got.value {
            FlagValue::Bool(b) => json!(b),
            FlagValue::Variant(s) => json!(s),
        };
        assert_eq!(value, e["value"], "{name}: value");
        assert_eq!(
            got.reason,
            e["reason"].as_str().unwrap_or_default(),
            "{name}: reason"
        );
        assert_eq!(
            got.condition_index,
            e["condition_index"].as_i64().unwrap_or(-1),
            "{name}: condition_index"
        );
        assert_eq!(
            got.variant.as_deref(),
            e["variant"].as_str(),
            "{name}: variant"
        );
        assert_eq!(
            got.payload.as_ref().unwrap_or(&Value::Null),
            &e["payload"],
            "{name}: payload"
        );
    }
}

#[test]
fn basic_vectors() {
    run("vectors_basic.json", 20);
}

#[test]
fn ops_v2_vectors() {
    run("vectors_ops.json", 100);
}

#[test]
fn g10_vectors() {
    run("vectors_g10.json", 15);
}

#[test]
fn sticky_vectors() {
    run("vectors_sticky.json", 19);
}

#[test]
fn generated_vectors() {
    run("vectors.json", 10_000);
}

#[test]
fn capture_posts_batch_on_flush() {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let host = format!("http://{}", l.local_addr().expect("addr"));
    let srv = std::thread::spawn(move || {
        let (mut s, _) = l.accept().expect("accept");
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let got = loop {
            let n = s.read(&mut chunk).expect("read");
            buf.extend_from_slice(&chunk[..n]);
            let text = String::from_utf8_lossy(&buf).into_owned();
            if let Some((head, body)) = text.split_once("\r\n\r\n") {
                let len: usize = head
                    .to_ascii_lowercase()
                    .split("content-length:")
                    .nth(1)
                    .and_then(|r| r.split_whitespace().next()?.parse().ok())
                    .unwrap_or(0);
                if body.len() >= len {
                    break (
                        head.lines().next().unwrap_or_default().to_owned(),
                        body.to_owned(),
                    );
                }
            }
        };
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .expect("reply");
        got
    });
    let c = Client::new(
        "lc_pk_x",
        Options {
            host,
            flush_interval: Duration::from_secs(3600),
            ..Options::default()
        },
    );
    c.capture(&Event::new("u1", "purchase").property("total", 12));
    assert!(c.flush(Some(Duration::from_secs(10))));
    c.shutdown();
    let (line, body) = srv.join().expect("server");
    assert!(line.starts_with("POST /v1/batch"), "{line}");
    let p: Value = serde_json::from_str(&body).expect("json body");
    assert_eq!(p["api_key"], "lc_pk_x");
    assert_eq!(p["batch"][0]["event"], "purchase");
    assert_eq!(p["batch"][0]["distinct_id"], "u1");
    assert_eq!(p["batch"][0]["properties"]["total"], 12);
}
