//! Evaluate-once flag snapshots and `$feature/<key>` event enrichment.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use serde_json::Value;

use crate::core::{Props, Resolved, lock};
use crate::eval::FlagValue;

/// Flags evaluated once for one distinct id. Reads never call the network or emit exposure events.
#[derive(Debug, Default)]
pub struct FlagSnapshot {
    values: HashMap<String, FlagValue>,
    payloads: HashMap<String, Value>,
    accessed: Mutex<HashSet<String>>,
}

/// `$feature/<key>` per flag plus sorted `$active_feature_flags`; properties already in `props` win.
pub(crate) fn flag_event_props(values: &HashMap<String, FlagValue>, props: &mut Props) {
    if values.is_empty() {
        return;
    }
    let mut active: Vec<&String> = values
        .iter()
        .filter(|(_, v)| **v != FlagValue::Bool(false))
        .map(|(k, _)| k)
        .collect();
    active.sort();
    for (k, v) in values {
        props
            .entry(format!("$feature/{k}"))
            .or_insert_with(|| v.to_json());
    }
    props
        .entry("$active_feature_flags")
        .or_insert_with(|| Value::from(active.iter().map(|k| k.as_str()).collect::<Vec<_>>()));
}

impl FlagSnapshot {
    pub(crate) fn from_resolved(all: HashMap<String, Resolved>) -> Self {
        let mut s = Self::default();
        for (k, r) in all {
            if let Some(p) = r.payload {
                s.payloads.insert(k.clone(), p);
            }
            s.values.insert(k, r.value);
        }
        s
    }

    /// The flag's value, or `None` when it is not in the snapshot.
    #[must_use]
    pub fn get_flag(&self, key: &str) -> Option<FlagValue> {
        lock(&self.accessed).insert(key.to_owned());
        self.values.get(key).cloned()
    }

    #[must_use]
    pub fn is_enabled(&self, key: &str) -> bool {
        self.get_flag(key).is_some_and(|v| v.is_on())
    }

    #[must_use]
    pub fn get_payload(&self, key: &str) -> Option<&Value> {
        self.payloads.get(key)
    }

    /// Sorted flag keys.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        let mut k: Vec<String> = self.values.keys().cloned().collect();
        k.sort();
        k
    }

    /// A snapshot holding only `keys`.
    #[must_use]
    pub fn only(&self, keys: &[&str]) -> Self {
        let keep = |k: &&String| keys.contains(&k.as_str());
        Self {
            values: self
                .values
                .iter()
                .filter(|(k, _)| keep(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            payloads: self
                .payloads
                .iter()
                .filter(|(k, _)| keep(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            accessed: Mutex::default(),
        }
    }

    /// A snapshot holding only the flags read through [`FlagSnapshot::get_flag`] / [`FlagSnapshot::is_enabled`] so far.
    #[must_use]
    pub fn only_accessed(&self) -> Self {
        let seen: Vec<String> = lock(&self.accessed).iter().cloned().collect();
        self.only(&seen.iter().map(String::as_str).collect::<Vec<_>>())
    }

    /// `props` plus this snapshot's `$feature/*` entries (`props` win).
    #[must_use]
    pub fn event_props(&self, mut props: Props) -> Props {
        flag_event_props(&self.values, &mut props);
        props
    }

    pub(crate) fn values(&self) -> &HashMap<String, FlagValue> {
        &self.values
    }
}
