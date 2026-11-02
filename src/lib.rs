//! Lucida server SDK: batched event capture, local feature-flag evaluation (shared vectors in
//! `testdata/flags`) and historical import.
//!
//! Name a distinct id per event, or run inside [`with_context`] and use [`Event::in_context`]. [`Client`] (feature `blocking`,
//! default) runs on threads; [`AsyncClient`] (feature `tokio`) on a tokio runtime.

#[cfg(feature = "blocking")]
mod blocking;
mod context;
mod core;
mod defcache;
mod details;
mod eval;
mod eval_v2;
mod global;
mod hooks;
mod identity;
mod snapshot;
mod sse;
#[cfg(feature = "tokio")]
mod tokio_client;

#[cfg(feature = "blocking")]
pub use blocking::Client;
pub use context::{
    DISTINCT_ID_HEADER, RequestContext, SESSION_ID_HEADER, bind_context, clean_header,
    current_context, read_tracing_headers, with_context, with_fresh_context,
};
#[cfg(feature = "tokio")]
pub use context::{bind_context_async, with_context_async};
pub use core::{Error, Event, FlagOptions, Options, Props, VERSION, iso};
pub use details::{FALLBACK_REASON, FlagDetails};
pub use defcache::{FlagDefinitionCache, InMemoryFlagDefinitionCache};
pub use eval::{FlagResult, FlagValue, REQUIRES_SERVER, evaluate_all, hash01};
#[cfg(feature = "blocking")]
pub use global::{global, init_global};
#[cfg(feature = "tokio")]
pub use global::{global_async, init_global_async};
pub use hooks::{BeforeSend, OnError};
pub use identity::TestSink;
pub use snapshot::FlagSnapshot;
#[cfg(feature = "tokio")]
pub use tokio_client::AsyncClient;
