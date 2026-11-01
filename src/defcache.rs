//! Flag definition cache provider: lets many workers share one definitions fetch (Redis, a database, files...).
//!
//! Before each reload the client asks [`FlagDefinitionCache::should_fetch_flag_definitions`]: when false it adopts
//! [`FlagDefinitionCache::flag_definitions`] instead of calling the server; the worker that did fetch calls
//! [`FlagDefinitionCache::on_flag_definitions_received`]. A provider that panics is treated as "fetch yourself".

use std::fmt::Debug;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::core::lock;

pub trait FlagDefinitionCache: Send + Sync + Debug {
    /// Cached definitions, or `None` when nothing is cached.
    fn flag_definitions(&self) -> Option<Vec<Value>>;
    /// True when this process should call the server (for example it won the fetch lock).
    fn should_fetch_flag_definitions(&self) -> bool;
    /// Store freshly fetched definitions for the other workers.
    fn on_flag_definitions_received(&self, definitions: &[Value]);
    /// Release locks and connections.
    fn shutdown(&self) {}
}

/// Reference provider: process-local, fetches again once `ttl` has passed.
#[derive(Debug)]
pub struct InMemoryFlagDefinitionCache {
    ttl: Duration,
    state: Mutex<Option<(Vec<Value>, Instant)>>,
}

impl InMemoryFlagDefinitionCache {
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            state: Mutex::new(None),
        }
    }
}

impl Default for InMemoryFlagDefinitionCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

impl FlagDefinitionCache for InMemoryFlagDefinitionCache {
    fn flag_definitions(&self) -> Option<Vec<Value>> {
        lock(&self.state).as_ref().map(|(d, _)| d.clone())
    }

    fn should_fetch_flag_definitions(&self) -> bool {
        lock(&self.state)
            .as_ref()
            .is_none_or(|(_, at)| at.elapsed() >= self.ttl)
    }

    fn on_flag_definitions_received(&self, definitions: &[Value]) {
        *lock(&self.state) = Some((definitions.to_vec(), Instant::now()));
    }
}

/// Definitions to use instead of fetching, or `None` when this process must fetch.
pub(crate) fn adopt_cached(p: Option<&dyn FlagDefinitionCache>) -> Option<Vec<Value>> {
    let p = p?;
    catch_unwind(AssertUnwindSafe(|| {
        if p.should_fetch_flag_definitions() {
            None
        } else {
            p.flag_definitions()
        }
    }))
    .ok()
    .flatten()
}

pub(crate) fn publish_cached(p: Option<&dyn FlagDefinitionCache>, defs: &[Value]) {
    if let Some(p) = p {
        // a failing cache must not break flag evaluation
        let _ = catch_unwind(AssertUnwindSafe(|| p.on_flag_definitions_received(defs)));
    }
}

pub(crate) fn shutdown_cached(p: Option<&dyn FlagDefinitionCache>) {
    if let Some(p) = p {
        let _ = catch_unwind(AssertUnwindSafe(|| p.shutdown()));
    }
}
