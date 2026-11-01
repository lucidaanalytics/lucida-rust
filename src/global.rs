//! Process-wide clients: `init_global` / `init_global_async` once at startup, `global()` / `global_async()` anywhere.

use std::sync::OnceLock;

use crate::Options;

#[cfg(feature = "blocking")]
static GLOBAL: OnceLock<crate::Client> = OnceLock::new();
#[cfg(feature = "tokio")]
static GLOBAL_ASYNC: OnceLock<crate::AsyncClient> = OnceLock::new();

/// Create the process-wide blocking client. Returns false (and drops `opts`) if one already exists.
/// A static is never dropped, so call `global().map(Client::shutdown)` before exit to flush.
#[cfg(feature = "blocking")]
pub fn init_global(api_key: &str, opts: Options) -> bool {
    GLOBAL.set(crate::Client::new(api_key, opts)).is_ok()
}

/// The client made by [`init_global`], if any.
#[cfg(feature = "blocking")]
#[must_use]
pub fn global() -> Option<&'static crate::Client> {
    GLOBAL.get()
}

/// Create the process-wide async client; call inside a tokio runtime. Returns false if one already exists.
/// Call `global_async()` then `.shutdown().await` before exit to flush.
#[cfg(feature = "tokio")]
pub fn init_global_async(api_key: &str, opts: Options) -> bool {
    GLOBAL_ASYNC
        .set(crate::AsyncClient::new(api_key, opts))
        .is_ok()
}

/// The client made by [`init_global_async`], if any.
#[cfg(feature = "tokio")]
#[must_use]
pub fn global_async() -> Option<&'static crate::AsyncClient> {
    GLOBAL_ASYNC.get()
}
