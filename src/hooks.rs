//! Delivery hooks (`before_send`, `on_error`) and the event assembly that applies them.

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use serde_json::Value;

use crate::context::current_context;
use crate::core::{Event, Options, uuid7, wire};
use crate::identity::Shared;
use crate::snapshot::flag_event_props;

type BeforeSendFn = dyn Fn(Value) -> Option<Value> + Send + Sync;
type OnErrorFn = dyn Fn(&str, &[Value]) + Send + Sync;

/// Edit or drop (`None`) each wire event before it is queued. A panicking hook keeps the original event.
#[derive(Clone)]
pub struct BeforeSend(Arc<BeforeSendFn>);

impl BeforeSend {
    pub fn new(f: impl Fn(Value) -> Option<Value> + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }
}

/// Called with a message and the affected wire events when a batch is given up on (or an event has no distinct id).
/// It runs on the delivery thread/task: keep it quick.
#[derive(Clone)]
pub struct OnError(Arc<OnErrorFn>);

impl OnError {
    pub fn new(f: impl Fn(&str, &[Value]) + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }
}

impl fmt::Debug for BeforeSend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BeforeSend(..)")
    }
}

impl fmt::Debug for OnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OnError(..)")
    }
}

impl Options {
    pub(crate) fn fail(&self, msg: &str, events: &[Value]) {
        if let Some(h) = &self.on_error {
            // a panicking callback must not break delivery
            let _ = catch_unwind(AssertUnwindSafe(|| (h.0)(msg, events)));
        }
    }
}

/// The wire event for `e`: super properties and context merged under its own fields, personless / geoip flags, then `before_send`.
/// `None` when it has no distinct id or the hook dropped it.
pub(crate) fn build_wire(opts: &Options, shared: &Shared, e: &Event) -> Option<Value> {
    let ctx = current_context().unwrap_or_default();
    let mut e = e.clone();
    if e.distinct_id.is_empty() {
        if let Some(id) = ctx.distinct_id {
            e.distinct_id = id;
        } else if e.personless {
            e.distinct_id = uuid7();
        } else {
            opts.fail("event dropped: no distinct id", &[]);
            return None;
        }
    }
    let mut props = shared.super_props();
    if let Some(values) = &e.flags {
        flag_event_props(values, &mut props);
    }
    props.extend(ctx.properties);
    if let Some(s) = ctx.session_id {
        props.insert("$session_id".into(), s.into());
    }
    props.extend(std::mem::take(&mut e.properties));
    if e.personless {
        props.insert("$process_person_profile".into(), false.into());
        e.set.clear();
        e.set_once.clear();
    }
    if e.disable_geoip.unwrap_or(opts.disable_geoip) {
        props.insert("$geoip_disable".into(), true.into());
    }
    e.properties = props;
    let w = wire(&e);
    let Some(h) = &opts.before_send else {
        return Some(w);
    };
    if let Ok(out) = catch_unwind(AssertUnwindSafe(|| (h.0)(w.clone()))) {
        out
    } else {
        opts.fail(
            "before_send panicked; the original event was kept",
            std::slice::from_ref(&w),
        );
        Some(w)
    }
}
