//! Test sink, super properties, person properties, identify dedupe, context propagation to spawned work.

use lucida::{Client, Event, Options, Props, RequestContext, TestSink, bind_context, with_context};
use serde_json::json;

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
