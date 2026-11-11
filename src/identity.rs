//! Per-client shared state: super properties, the bounded identify/group dedupe set, and the test-mode sink.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::core::{Event, Lru, Props, lock, uuid7};

const IDENTITY_DEDUPE_SIZE: usize = 1000;

/// Super properties plus identify/group dedupe, owned by one client.
pub(crate) struct Shared {
    super_props: Mutex<Props>,
    seen: Mutex<Lru>,
    /// Event uuid to dedupe key, until the event is dropped.
    keys: Mutex<HashMap<String, String>>,
}

impl Shared {
    pub fn new(defaults: Props) -> Self {
        Self {
            super_props: Mutex::new(defaults),
            seen: Mutex::default(),
            keys: Mutex::default(),
        }
    }

    pub fn register(&self, props: Props) {
        lock(&self.super_props).extend(props);
    }

    pub fn unregister(&self, key: &str) {
        lock(&self.super_props).remove(key);
    }

    pub fn super_props(&self) -> Props {
        lock(&self.super_props).clone()
    }

    /// The uuid to send an identify under, or `None` when this exact call was already sent.
    pub fn first_identify(&self, id: &str, set: &Props, set_once: &Props) -> Option<String> {
        self.first(&json!(["identify", id, set, set_once]))
    }

    pub fn first_group(
        &self,
        kind: &str,
        key: &str,
        id: Option<&str>,
        props: &Props,
    ) -> Option<String> {
        self.first(&json!(["group", kind, key, id, props]))
    }

    /// The uuid to send the event under the first time this exact call (kind, id and properties) is
    /// seen by the client, else `None`. [`Shared::release`] frees it again when the event is dropped.
    fn first(&self, call: &Value) -> Option<String> {
        let key = format!("{:x}", Sha256::digest(call.to_string().as_bytes()));
        if !lock(&self.seen).insert(key.clone(), IDENTITY_DEDUPE_SIZE) {
            return None;
        }
        let uuid = uuid7();
        let mut keys = lock(&self.keys);
        if keys.len() >= IDENTITY_DEDUPE_SIZE
            && let Some(old) = keys.keys().next().cloned()
        {
            keys.remove(&old);
        }
        keys.insert(uuid.clone(), key);
        Some(uuid)
    }

    /// Forget the calls whose events were in a dropped batch, so repeating them is sent again.
    pub fn release(&self, events: &[Value]) {
        let mut keys = lock(&self.keys);
        for e in events {
            if let Some(key) = e["uuid"].as_str().and_then(|u| keys.remove(u)) {
                lock(&self.seen).remove(&key);
            }
        }
    }
}

/// In-memory event sink for tests: set [`crate::Options::test_sink`] and read what the client captured
/// (wire events, in order, including exposure events). Nothing is sent.
#[derive(Debug, Clone, Default)]
pub struct TestSink(Arc<Mutex<Vec<Value>>>);

impl TestSink {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push(&self, w: Value) {
        lock(&self.0).push(w);
    }

    /// Every captured wire event so far.
    #[must_use]
    pub fn events(&self) -> Vec<Value> {
        lock(&self.0).clone()
    }

    /// The `event` names, in capture order.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        lock(&self.0)
            .iter()
            .filter_map(|e| e["event"].as_str().map(str::to_owned))
            .collect()
    }

    pub fn clear(&self) {
        lock(&self.0).clear();
    }
}

/// The same person-property and super-property methods on both clients.
macro_rules! person_api {
    ($client:ty) => {
        impl $client {
            /// Set person properties with a `$set` event.
            pub fn set_person_properties(&self, distinct_id: &str, props: Props) {
                self.capture(&Event::person_properties(distinct_id, props, false));
            }

            /// Set person properties only where unset (`$set_once`).
            pub fn set_person_properties_once(&self, distinct_id: &str, props: Props) {
                self.capture(&Event::person_properties(distinct_id, props, true));
            }

            /// Add super properties to every later event; context and per-event properties win.
            pub fn register(&self, props: Props) {
                self.shared().register(props);
            }

            pub fn unregister(&self, key: &str) {
                self.shared().unregister(key);
            }
        }
    };
}

#[cfg(feature = "blocking")]
person_api!(crate::Client);
#[cfg(feature = "tokio")]
person_api!(crate::AsyncClient);
