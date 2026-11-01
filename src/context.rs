//! Ambient request context: distinct id, session id and properties merged into events captured inside a
//! scope. Blocking code uses a thread-local scope ([`with_context`]); async code a task-local one
//! ([`with_context_async`], feature `tokio`). Explicit event fields always win over the context.

use std::cell::RefCell;

use crate::core::Props;

#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    pub distinct_id: Option<String>,
    pub session_id: Option<String>,
    pub properties: Props,
}

impl RequestContext {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn distinct_id(mut self, id: &str) -> Self {
        self.distinct_id = Some(id.into());
        self
    }

    #[must_use]
    pub fn session_id(mut self, id: &str) -> Self {
        self.session_id = Some(id.into());
        self
    }

    #[must_use]
    pub fn property(mut self, k: &str, v: impl Into<serde_json::Value>) -> Self {
        self.properties.insert(k.into(), v.into());
        self
    }

    /// `child` over `self`: set fields of the child win, properties merge key by key.
    fn merged(&self, child: Self) -> Self {
        let mut properties = self.properties.clone();
        properties.extend(child.properties);
        Self {
            distinct_id: child.distinct_id.or_else(|| self.distinct_id.clone()),
            session_id: child.session_id.or_else(|| self.session_id.clone()),
            properties,
        }
    }
}

thread_local! {
    static THREAD_CTX: RefCell<Option<RequestContext>> = const { RefCell::new(None) };
}

#[cfg(feature = "tokio")]
tokio::task_local! {
    static TASK_CTX: RequestContext;
}

/// The innermost active context (task scope first, then thread scope).
#[must_use]
pub fn current_context() -> Option<RequestContext> {
    #[cfg(feature = "tokio")]
    if let Ok(c) = TASK_CTX.try_with(Clone::clone) {
        return Some(c);
    }
    THREAD_CTX.with(|c| c.borrow().clone())
}

fn resolve(ctx: RequestContext, fresh: bool) -> RequestContext {
    match current_context() {
        Some(parent) if !fresh => parent.merged(ctx),
        _ => ctx,
    }
}

struct Restore(Option<RequestContext>);

impl Drop for Restore {
    fn drop(&mut self) {
        let prev = self.0.take();
        THREAD_CTX.with(|c| *c.borrow_mut() = prev);
    }
}

/// Run `f` with `ctx` merged over the enclosing context; the enclosing one is restored afterwards (also on panic).
pub fn with_context<R>(ctx: RequestContext, f: impl FnOnce() -> R) -> R {
    scoped(ctx, false, f)
}

/// Like [`with_context`] but does not inherit the enclosing context.
pub fn with_fresh_context<R>(ctx: RequestContext, f: impl FnOnce() -> R) -> R {
    scoped(ctx, true, f)
}

fn scoped<R>(ctx: RequestContext, fresh: bool, f: impl FnOnce() -> R) -> R {
    let next = resolve(ctx, fresh);
    let _restore = Restore(THREAD_CTX.with(|c| c.borrow_mut().replace(next)));
    f()
}

/// Async scope: the context follows the future across `.await` points and worker threads.
#[cfg(feature = "tokio")]
pub async fn with_context_async<F: std::future::Future>(
    ctx: RequestContext,
    fresh: bool,
    fut: F,
) -> F::Output {
    TASK_CTX.scope(resolve(ctx, fresh), fut).await
}

/// Capture the current context into `f`: the returned closure re-enters it wherever it runs, e.g. on a
/// spawned thread (`std::thread::spawn(bind_context(|| ...))`). Without an active context it runs `f` as is.
pub fn bind_context<R>(f: impl FnOnce() -> R) -> impl FnOnce() -> R {
    let ctx = current_context();
    move || match ctx {
        Some(c) => with_fresh_context(c, f),
        None => f(),
    }
}

/// Capture the current context into `fut` so it still applies after `tokio::spawn`
/// (`tokio::spawn(bind_context_async(async { ... }))`).
#[cfg(feature = "tokio")]
pub fn bind_context_async<F: std::future::Future>(
    fut: F,
) -> impl std::future::Future<Output = F::Output> {
    let ctx = current_context().unwrap_or_default();
    with_context_async(ctx, true, fut)
}

/// Header names the browser SDK adds to requests to your own backend.
pub const DISTINCT_ID_HEADER: &str = "X-Lucida-Distinct-Id";
pub const SESSION_ID_HEADER: &str = "X-Lucida-Session-Id";

/// Strip ASCII control chars, trim, cap at 1000 chars; empty becomes `None`.
#[must_use]
pub fn clean_header(v: &str) -> Option<String> {
    let s: String = v.chars().filter(|c| !c.is_ascii_control()).collect();
    let s: String = s.trim().chars().take(1000).collect();
    (!s.is_empty()).then_some(s)
}

/// `(distinct_id, session_id)` from the tracing headers; `get` looks a header up by (case-insensitive) name.
/// These headers are client-controlled: use them to join events to a browser session, never for authentication.
#[must_use]
pub fn read_tracing_headers<'a>(
    get: impl Fn(&str) -> Option<&'a str>,
) -> (Option<String>, Option<String>) {
    (
        get(DISTINCT_ID_HEADER).and_then(clean_header),
        get(SESSION_ID_HEADER).and_then(clean_header),
    )
}
