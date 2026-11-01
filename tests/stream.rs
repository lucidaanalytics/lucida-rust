//! The blocking client follows `GET /v1/flags/stream` and reloads definitions on a push,
//! without waiting for the (here hour-long) poll interval.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use lucida::{Client, FlagOptions, Options};

struct Gateway {
    on: AtomicBool,
    push: AtomicUsize,
    loads: AtomicUsize,
}

fn head(s: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut b = [0u8; 1024];
    while !String::from_utf8_lossy(&buf).contains("\r\n\r\n") {
        match s.read(&mut b) {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&b[..n]),
        }
    }
    String::from_utf8_lossy(&buf)
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned()
}

fn serve(g: &Gateway, mut s: TcpStream) {
    let line = head(&mut s);
    if line.contains("/v1/flags/stream") {
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\nevent: definitions\ndata: {}\n\n");
        let mut sent = 0;
        loop {
            let want = g.push.load(Ordering::SeqCst);
            if want > sent {
                sent = want;
                if s.write_all(b": keepalive\n\nevent: definitions\ndata: {}\n\n")
                    .is_err()
                {
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    let body = if line.contains("/v1/flags/definitions") {
        g.loads.fetch_add(1, Ordering::SeqCst);
        format!(
            r#"{{"flags":[{{"key":"f","type":"boolean","active":{},"salt":"s","aggregation":{{"kind":"person"}},"conditions":[{{"rollout_pct":100}}]}}]}}"#,
            g.on.load(Ordering::SeqCst)
        )
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

#[test]
fn stream_push_reloads_definitions() {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let host = format!("http://{}", l.local_addr().expect("addr"));
    let g = Arc::new(Gateway {
        on: AtomicBool::new(false),
        push: AtomicUsize::new(0),
        loads: AtomicUsize::new(0),
    });
    let gs = Arc::clone(&g);
    std::thread::spawn(move || {
        for s in l.incoming().flatten() {
            let g = Arc::clone(&gs);
            std::thread::spawn(move || serve(&g, s));
        }
    });
    let c = Client::new(
        "lc_pk_x",
        Options {
            host,
            secret_key: Some("lc_sk_x".into()),
            flag_poll_interval: Duration::from_secs(3600),
            flush_interval: Duration::from_secs(3600),
            ..Options::default()
        },
    );
    assert!(c.wait_ready(Duration::from_secs(5)));
    let o = FlagOptions::default();
    assert_eq!(c.is_feature_enabled("f", "u1", &o), Some(false));
    g.on.store(true, Ordering::SeqCst);
    g.push.fetch_add(1, Ordering::SeqCst);
    let end = Instant::now() + Duration::from_secs(5);
    while c.is_feature_enabled("f", "u1", &o) != Some(true) {
        assert!(Instant::now() < end, "no reload after a push");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(g.loads.load(Ordering::SeqCst) >= 2);
    c.shutdown();
}
