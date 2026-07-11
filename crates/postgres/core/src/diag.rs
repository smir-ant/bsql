//! Structured diagnostics — the dep-free observability seam.
//!
//! bsql emits structured operational events (a slow query, a server `NOTICE`, an
//! SSL downgrade, pool saturation, …) through ONE seam: a consumer-installed
//! callback ([`DiagSink`]) that receives a borrowed [`DiagEvent`]. The seam is
//! **dep-free** (no `tracing` / `log` / `metrics` in the runtime graph), so a
//! consumer chooses their own logging stack and bsql forces none; an optional
//! `tracing` adapter can be layered ON TOP of this callback later without
//! changing the core.
//!
//! # Zero-cost when off
//!
//! The sink is an [`Option<DiagSink>`](DiagSink): when a consumer installs none,
//! every emit site is a single `if let Some(..)` branch that is NOT taken — no
//! event is constructed, no wire body is parsed, no `Instant::now` is read, no
//! allocation happens. So a production build that never installs a sink pays only
//! a predictable, never-taken branch at each COLD lifecycle boundary. The events
//! fire ONLY at cold boundaries (a completed query's timing, a received `NOTICE`
//! frame, a pool checkout, a connect) — NEVER on the per-row hot path, so the
//! engine's `next_event` hot dispatch is untouched by construction. This is the
//! deliberate distinction from a per-row observation seam (which would tax every
//! row): a [`DiagEvent`] is a cold, rare, operator-facing signal, not a data-plane
//! callback.
//!
//! # No PII by default
//!
//! A [`DiagEvent`] NEVER carries a bound parameter VALUE. A slow-query event
//! carries the SQL TEXT (or a digest a consumer computes), never the values a
//! parameter placeholder stood for — those are the application's data and are the
//! consumer's to log deliberately, not bsql's to leak into a diagnostic stream.
//!
//! # Borrowed where possible
//!
//! Event fields borrow the transient wire body / the caller's SQL slice for the
//! duration of the sink call (`&'a str`, or a [`Cow`] that borrows when the bytes
//! are clean UTF-8 and owns ONLY on the rare non-UTF-8 field), so the common case
//! allocates nothing even when the sink IS installed. The sink receives
//! `&DiagEvent<'_>`, so it cannot smuggle the borrow out — an event that must
//! outlive the call is the consumer's to copy.

use std::borrow::Cow;
use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

thread_local! {
    /// `true` while a diagnostics sink is executing ON THIS THREAD. A diagnostic
    /// emitted from WITHIN a sink — a self-emitting sink that runs a query, checks
    /// out a pooled connection, or calls `Diagnostics::emit` directly, any of
    /// which can emit again — is SUPPRESSED (see [`dispatch`]), so a sink can
    /// never recurse into itself to an unbounded stack-overflow abort.
    static IN_DISPATCH: Cell<bool> = const { Cell::new(false) };
}

/// Clears [`IN_DISPATCH`] on scope exit — RAII, so the flag resets even if the
/// sink panics (the `catch_unwind` inside [`dispatch`] contains the panic, but
/// the reset must still run) or the one-time note path unwinds.
struct ReentryReset;

impl Drop for ReentryReset {
    fn drop(&mut self) {
        IN_DISPATCH.with(|flag| flag.set(false));
    }
}

/// Invoke a diagnostics sink with the consumer callback ISOLATED: a panicking
/// sink can never affect the driver, and a self-emitting sink can never recurse.
///
/// Diagnostics are strictly non-correctness — a buggy consumer callback that
/// panics must NOT poison the connection, abort the process, or alter any result.
/// The call is wrapped in [`catch_unwind`] (SAFE — no `unsafe`; the crate is
/// `#![forbid(unsafe_code)]`), so a panic is CONTAINED at the emit site and the
/// event dropped. Without this, a sink panic would (a) unwind a driver verb
/// before it restores its liveness token → a bricked standalone connection, and
/// (b) fire from a `Drop` during a later unwind → a double-panic process
/// `SIGABRT`. [`AssertUnwindSafe`] is required because the borrowed sink/event are
/// not `UnwindSafe`; asserting it is sound here — we DROP the caught panic and
/// touch no shared state that a partial callback could have corrupted (the sink
/// owns whatever it mutates).
///
/// A caught panic is noted to stderr exactly ONCE per process (a buggy sink is
/// not silent), then dropped — never propagated. Under a `panic = "abort"` build
/// there is nothing to catch (the panic aborts before reaching here — the
/// consumer's own global choice, consistent with all their code).
///
/// # Re-entrancy is suppressed (no unbounded recursion)
///
/// A sink may do ANYTHING — inspect the pool, run queries, block. If it emits a
/// diagnostic (directly via [`Diagnostics::emit`], or indirectly by running a
/// self-slow query → `SlowQuery`, or a `pool.get()` that times out →
/// `PoolAcquireTimeout`, whose sink emits again, …) it would recurse without
/// bound into a stack-overflow abort. A per-thread [`IN_DISPATCH`] flag breaks
/// the class STRUCTURALLY: while a sink runs on this thread, any diagnostic
/// emitted from WITHIN it is silently dropped, so a self-emitting sink fires
/// exactly once and returns. The flag is reset by an RAII [`ReentryReset`] guard
/// (so it clears even if the sink panics — the `catch_unwind` contains the panic,
/// but the reset must still run).
pub(crate) fn dispatch(sink: &DiagSink, event: &DiagEvent<'_>) {
    // Suppress a nested emit: if a sink is already running on this thread, this
    // call was reached from INSIDE it (directly or via a pool/query it drove) —
    // drop the event to break unbounded recursion. `replace(true)` reads the
    // prior state and arms the flag in one step; a nested call sees `true` and
    // returns WITHOUT a reset guard (the outer frame owns the flag).
    if IN_DISPATCH.with(|flag| flag.replace(true)) {
        return;
    }
    // Clear the flag on the way out (even on a sink panic — see `ReentryReset`).
    let _reset = ReentryReset;
    if catch_unwind(AssertUnwindSafe(|| sink(event))).is_err() {
        // Contain + note once. The default panic hook already printed the panic
        // itself; this adds the one-line context that it was a diagnostics sink
        // and was contained, so an operator is not misled into thinking the
        // driver faulted.
        static WARNED: AtomicBool = AtomicBool::new(false);
        if WARNED
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            eprintln!(
                "[bsql] WARNING: a diagnostics sink panicked; the panic was CONTAINED and the \
                 event dropped (diagnostics never affect the driver). A sink must not panic."
            );
        }
    }
}

/// A structured operational event surfaced through a [`DiagSink`].
///
/// `#[non_exhaustive]`: the event vocabulary grows over releases, so a consumer's
/// `match` always carries a `_ =>` arm and a new event never breaks it. Fields
/// borrow the wire body / caller SQL for the sink call (see the [module
/// docs](self)); no field ever carries a bound parameter value (no PII).
#[non_exhaustive]
#[derive(Debug)]
pub enum DiagEvent<'a> {
    /// A server `NoticeResponse` — a `RAISE NOTICE` / `WARNING` / `INFO` / `LOG`
    /// from a stored procedure or the server itself. The primary PL/pgSQL logging
    /// channel, which the driver would otherwise drop. `severity`/`code`/`message`
    /// are the parsed `S`/`C`/`M` fields (borrowed from the wire body when clean
    /// UTF-8); `code` is the 5-char SQLSTATE (e.g. `"00000"`).
    ServerNotice {
        /// Server-reported severity (`NOTICE`, `WARNING`, `INFO`, `LOG`, `DEBUG`),
        /// empty if the server omitted it.
        severity: Cow<'a, str>,
        /// The 5-character SQLSTATE the notice carried (empty if absent).
        code: Cow<'a, str>,
        /// The human-readable notice text.
        message: Cow<'a, str>,
    },
    /// An SSL DOWNGRADE: `SslMode::Prefer` was in effect, the server REFUSED TLS,
    /// and the driver fell back to a PLAINTEXT connection. A security event a
    /// headless / journald service must be able to capture — the structured,
    /// routable replacement for the bare stderr warning (which still fires when no
    /// sink is installed, so existing behaviour is preserved).
    SslDowngrade {
        /// The endpoint host the plaintext fallback connected to.
        host: &'a str,
    },
    /// A pool checkout waited out its acquire deadline without a permit/slot
    /// becoming free — classified backpressure (the pool is saturated), never an
    /// infinite block. The signal a connection-pool storm needs to shed load.
    PoolAcquireTimeout {
        /// The deadline the checkout waited (its configured acquire timeout).
        waited: Duration,
    },
    /// A pooled connection was REMOVED on checkout rather than handed out —
    /// either because its health-gate reset FAILED (a silently-vanished peer, a
    /// server that closed the idle socket) or because it was REAPED for outliving
    /// the pool's configured `max_lifetime` (age) / `idle_timeout` (idle). A steady
    /// stream from failed resets is a reconnect storm (server-side churn made
    /// visible); a steady stream from reaping is just the pool rotating aged
    /// connections as configured.
    PoolConnectionEvicted,
    /// A query's server round trip met or exceeded the configured slow-query
    /// threshold. Carries the SQL TEXT — never the bound parameter VALUES (no
    /// PII): a consumer that wants a digest computes it from `sql`.
    SlowQuery {
        /// The SQL text of the slow query (a `$N`-placeholder string for a
        /// parameterised verb — the values are NOT included).
        sql: &'a str,
        /// How long the query's round trip took (met/exceeded the threshold).
        elapsed: Duration,
    },
    /// A server `ParameterStatus` report during the session — a GUC the server
    /// echoes when it changes (`SET timezone`, `search_path`, `application_name`,
    /// …). Useful for correlating a session's runtime settings; otherwise silently
    /// dropped by the materializers.
    ParameterStatus {
        /// The GUC name (e.g. `"timezone"`).
        name: Cow<'a, str>,
        /// Its new value.
        value: Cow<'a, str>,
    },
    /// The migration runner is WAITING on the advisory lock (another instance is
    /// migrating). Emitted on each backoff poll — a silent deploy freeze looks
    /// like a hang, so a long wait is now visible with its elapsed time.
    MigrationLockWaiting {
        /// How long the lock acquire has been polling so far.
        elapsed: Duration,
    },
    /// The migration runner is about to APPLY a migration (before its DDL runs).
    MigrationApplying {
        /// The migration's name (its `/`-normalized relative file name).
        name: &'a str,
    },
    /// The migration runner finished APPLYING a migration (its DDL + ledger row
    /// committed).
    MigrationApplied {
        /// The migration's name.
        name: &'a str,
    },
    /// The transparent dynamic prepared-statement cache SELF-HEALED: a cached
    /// plan went stale (a schema change, an out-of-band `DEALLOCATE`, a server
    /// restart / proxy reset) and the driver silently re-prepared it. Invisible
    /// otherwise — the operator sees only extra latency, not the cause; a steady
    /// stream signals server-side churn.
    PreparedCacheSelfHeal {
        /// The SQL text whose cached plan was reclaimed and re-warmed (no param
        /// values).
        sql: &'a str,
    },
}

/// A snapshot of a pool's operational counters, read via a driver's
/// `Pool::stats`.
///
/// The counters are monotonic relaxed atomics incremented at the pool's cold
/// checkout boundary (a permit/slot wait, an eviction) — near-zero cost on a path
/// that already awaits or syscalls. A cheap health gauge for a metrics scrape,
/// complementing the push-style [`DiagEvent`] pool events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PoolStats {
    /// Idle (checked-in) connections currently held.
    pub idle: usize,
    /// The pool's maximum connection count.
    pub max_size: usize,
    /// Checkouts that waited out their acquire deadline (classified
    /// `PoolTimeout`) — monotonic. A rising value means sustained saturation.
    pub acquire_timeouts: u64,
    /// Pooled connections removed on checkout — either their health-gate reset
    /// failed, or they were reaped for outliving `max_lifetime` / `idle_timeout` —
    /// monotonic. A rising value from failed resets means server-side
    /// idle-connection churn (a reconnect storm); from reaping it is the pool
    /// rotating aged connections as configured.
    pub connections_evicted: u64,
    /// The high-water mark of concurrent checkouts blocked waiting for a
    /// permit/slot — the deepest the acquire queue ever got. `0` if a checkout
    /// never had to wait.
    pub waiters_high_water: u64,
}

impl PoolStats {
    /// Assemble a stats snapshot. The sole constructor (the struct is
    /// `#[non_exhaustive]`, so a driver — which lives in a different crate —
    /// builds it through this, and a future field is added here + defaulted,
    /// never breaking a consumer who only READS the fields).
    #[must_use]
    pub fn new(
        idle: usize,
        max_size: usize,
        acquire_timeouts: u64,
        connections_evicted: u64,
        waiters_high_water: u64,
    ) -> Self {
        Self {
            idle,
            max_size,
            acquire_timeouts,
            connections_evicted,
            waiters_high_water,
        }
    }
}

/// A consumer-installed diagnostic callback.
///
/// `Arc<dyn Fn(&DiagEvent<'_>) + Send + Sync>` — cloneable (so a pool hands the
/// same sink to every connection it mints), `Send + Sync + 'static` (so it rides a
/// `Send` connection and a `Send + Sync` pool across threads and `.await`s). It is
/// a 16-byte FAT pointer (data + vtable); `Option<DiagSink>` is niche-packed to
/// the same 16 bytes, and the whole [`Diagnostics`] handle (`Option<DiagSink>` +
/// `Option<Duration>`) is 32 bytes — carried on `Core`, never on the
/// footprint-pinned [`ConnectConfig`](crate::ConnectConfig).
pub type DiagSink = Arc<dyn Fn(&DiagEvent<'_>) + Send + Sync>;

/// The consumer-facing diagnostics configuration handle: the sink plus the
/// slow-query threshold.
///
/// Installed on a standalone connection (`Connection::connect_with` /
/// `set_diagnostics`) or on a pool (`Pool::builder(..).diagnostics(..)`), never on
/// [`ConnectConfig`](crate::ConnectConfig) (it is not a connection PARAMETER — the
/// 152-byte config footprint is untouched). Cheap to clone (an `Option<Arc>` plus
/// an `Option<Duration>`).
///
/// # Sink contract (the STRONGEST form — a sink can do anything)
///
/// A sink may do ANYTHING — inspect the pool (`pool.stats()`), run queries, check
/// out a connection, block, even panic — and the driver STRUCTURALLY absorbs the
/// consequences (no caller discipline required):
/// - a **panic** is contained by `catch_unwind` and dropped (never poisons a
///   connection or aborts the process);
/// - a pool event's sink runs OUTSIDE the pool lock, so **re-entering the pool**
///   (`pool.stats()`) cannot deadlock;
/// - any diagnostic emitted from WITHIN a sink (a self-slow query, a
///   `pool.get()` that times out, a direct `emit`) is **silently suppressed**, so
///   a **self-emitting** sink can never recurse into an unbounded stack overflow.
///
/// The only things a sink still owns: a diagnostic emitted from inside it does not
/// reach a sink (suppressed), and a slow sink serializes nothing but its own
/// (already-cold) emit site.
///
/// ```
/// use bsql_postgres_core::diag::{Diagnostics, DiagEvent};
/// use std::time::Duration;
/// let diag = Diagnostics::new()
///     .on_event(|ev: &DiagEvent<'_>| eprintln!("{ev:?}"))
///     .slow_query_threshold(Duration::from_millis(100));
/// assert!(diag.is_enabled());
/// ```
#[derive(Clone, Default)]
pub struct Diagnostics {
    /// The installed callback, or `None` (diagnostics off — zero cost).
    sink: Option<DiagSink>,
    /// The slow-query threshold; timing is read ONLY when this is `Some` AND a
    /// sink is installed, so an off build pays no `Instant::now`.
    slow_query_threshold: Option<Duration>,
}

impl std::fmt::Debug for Diagnostics {
    /// Hand-written: `DiagSink` is a `dyn Fn` (not `Debug`). Reports whether a
    /// sink is installed and the threshold, never the closure itself.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Diagnostics")
            .field("sink_installed", &self.sink.is_some())
            .field("slow_query_threshold", &self.slow_query_threshold)
            .finish()
    }
}

impl Diagnostics {
    /// An empty configuration — no sink, no slow-query threshold (diagnostics
    /// off, zero cost).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a callback closure, boxing it into the shared [`DiagSink`].
    ///
    /// The closure must be `Send + Sync + 'static` so it rides a `Send`
    /// connection / a `Send + Sync` pool across threads.
    #[must_use]
    pub fn on_event(mut self, sink: impl Fn(&DiagEvent<'_>) + Send + Sync + 'static) -> Self {
        self.sink = Some(Arc::new(sink));
        self
    }

    /// Install an already-boxed [`DiagSink`] (e.g. one shared across several
    /// pools/connections without re-boxing).
    #[must_use]
    pub fn with_sink(mut self, sink: DiagSink) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Set the slow-query threshold: a query whose server round trip meets or
    /// exceeds `threshold` emits a `SlowQuery` event. Off by default (no timing
    /// cost until set).
    #[must_use]
    pub fn slow_query_threshold(mut self, threshold: Duration) -> Self {
        self.slow_query_threshold = Some(threshold);
        self
    }

    /// Whether a sink is installed (diagnostics on).
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.sink.is_some()
    }

    /// Whether slow-query TIMING is armed: a threshold AND a sink are BOTH
    /// installed. The single gate the driver reads before touching a clock — when
    /// this is `false`, no `Instant::now` is read and no `Diagnostics` is cloned,
    /// so the off path is zero-cost. A threshold without a sink (nowhere to emit)
    /// or a sink without a threshold (slow-query off) is NOT armed.
    #[must_use]
    pub fn slow_query_armed(&self) -> bool {
        self.slow_query_threshold.is_some() && self.sink.is_some()
    }

    /// Emit `event` to the installed sink, or do nothing when off.
    ///
    /// The one-line front door a caller that already holds a `Diagnostics` uses;
    /// the zero-cost-off branch lives here. A panicking sink is CONTAINED (see
    /// [`dispatch`]) — it can never poison the connection or abort the process.
    pub fn emit(&self, event: &DiagEvent<'_>) {
        if let Some(sink) = &self.sink {
            dispatch(sink, event);
        }
    }

    /// The installed sink, if any — the borrowed form the pump-sink adapter
    /// threads (`capture_notify`) so a `NOTICE` frame can be surfaced without
    /// cloning the `Arc`. `None` (the common case) is a never-taken branch.
    pub(crate) fn sink(&self) -> Option<&DiagSink> {
        self.sink.as_ref()
    }

    /// The slow-query threshold, if set. The slow-query timing gate reads a clock
    /// ONLY when this is `Some` AND a sink is installed, so an off build pays no
    /// `Instant::now`.
    pub(crate) fn slow_threshold(&self) -> Option<Duration> {
        self.slow_query_threshold
    }
}

/// Parse a server `NoticeResponse` body and emit it as a
/// [`DiagEvent::ServerNotice`] on `sink`.
///
/// Runs ONLY when a sink is installed (the caller guards on `Option<&DiagSink>`),
/// so an off connection never reaches this parse. Borrows the wire body: the
/// `S`/`C`/`M` fields are lent as [`Cow`] that own ONLY on the rare non-UTF-8
/// field, so a clean notice allocates nothing. Drives the same
/// [`error_response_fields`](crate::materialize::error_response_fields) walk
/// `parse_error_response` uses (proven total by the decoder fuzz), so a hostile
/// notice body cannot panic here.
pub(crate) fn emit_server_notice(sink: &DiagSink, body: &[u8]) {
    let mut severity: Cow<'_, str> = Cow::Borrowed("");
    let mut code: Cow<'_, str> = Cow::Borrowed("");
    let mut message: Cow<'_, str> = Cow::Borrowed("");
    for (type_byte, value) in crate::materialize::error_response_fields(body) {
        match type_byte {
            // PG sends `S` (localized) then `V` (non-localized); the later wins.
            b'S' | b'V' => severity = String::from_utf8_lossy(value),
            b'C' => code = String::from_utf8_lossy(value),
            b'M' => message = String::from_utf8_lossy(value),
            _ => {}
        }
    }
    dispatch(sink, &DiagEvent::ServerNotice { severity, code, message });
}

/// Parse a server `ParameterStatus` body (`[name\0][value\0]`) and emit it as a
/// [`DiagEvent::ParameterStatus`] on `sink`. Runs ONLY when a sink is installed.
/// Total on any bytes (a missing NUL yields an empty tail, never a panic); the
/// fields are lent as [`Cow`] borrowing the body unless non-UTF-8.
pub(crate) fn emit_parameter_status(sink: &DiagSink, body: &[u8]) {
    let (name_bytes, rest) = split_first_cstr(body);
    let (value_bytes, _) = split_first_cstr(rest);
    dispatch(
        sink,
        &DiagEvent::ParameterStatus {
            name: String::from_utf8_lossy(name_bytes),
            value: String::from_utf8_lossy(value_bytes),
        },
    );
}

/// Split `body` at its first NUL into `(before, after)`; if there is no NUL,
/// `(body, &[])`. Bounds-checked (no indexing), so it is total on any input.
fn split_first_cstr(body: &[u8]) -> (&[u8], &[u8]) {
    match body.iter().position(|&b| b == 0) {
        Some(nul) => match (
            body.get(..nul),
            nul.checked_add(1).and_then(|start| body.get(start..)),
        ) {
            (Some(head), Some(tail)) => (head, tail),
            // Unreachable for a real slice (`nul` is a valid index), but total:
            // fall back to the whole body as the head, no tail.
            _ => (body, &[]),
        },
        None => (body, &[]),
    }
}

#[cfg(test)]
mod tests {
    //! The seam plumbing without a live server: a `DiagEvent` constructed and
    //! routed through a `Diagnostics` sink reaches the closure, and an off
    //! `Diagnostics` invokes nothing.

    use std::borrow::Cow;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::{DiagEvent, Diagnostics};

    #[test]
    fn an_installed_sink_receives_a_routed_event() {
        let seen = Arc::new(AtomicUsize::new(0));
        let seen_in = Arc::clone(&seen);
        let diag = Diagnostics::new().on_event(move |ev: &DiagEvent<'_>| {
            match ev {
                DiagEvent::ServerNotice { message, .. } => {
                    assert_eq!(message.as_ref(), "hello");
                    seen_in.fetch_add(1, Ordering::Relaxed);
                }
                other => panic!("expected a ServerNotice, got {other:?}"),
            }
        });
        assert!(diag.is_enabled());
        diag.emit(&DiagEvent::ServerNotice {
            severity: Cow::Borrowed("NOTICE"),
            code: Cow::Borrowed("00000"),
            message: Cow::Borrowed("hello"),
        });
        assert_eq!(seen.load(Ordering::Relaxed), 1, "the sink saw exactly one event");
    }

    /// Assemble a server `NoticeResponse`/`ErrorResponse` body:
    /// `[type:u8][value\0]…[0]`.
    fn notice_body(fields: &[(u8, &str)]) -> Vec<u8> {
        let mut b = Vec::new();
        for &(ty, val) in fields {
            b.push(ty);
            b.extend_from_slice(val.as_bytes());
            b.push(0);
        }
        b.push(0);
        b
    }

    #[test]
    fn emit_server_notice_parses_and_surfaces_fields() {
        // A hand-built NOTICE body (as PG frames a `RAISE NOTICE`) is parsed into a
        // ServerNotice with the S/V severity (V wins), the C SQLSTATE, and the M
        // message — no live server. The `S` then `V` order proves the later
        // non-localized severity wins, matching parse_error_response.
        let body = notice_body(&[
            (b'S', "LOG"),
            (b'V', "NOTICE"),
            (b'C', "00000"),
            (b'M', "hello from a raise notice"),
            (b'D', "some detail"),
        ]);
        let captured: Arc<std::sync::Mutex<Option<(String, String, String)>>> =
            Arc::new(std::sync::Mutex::new(None));
        let captured_in = Arc::clone(&captured);
        let diag = Diagnostics::new().on_event(move |ev: &DiagEvent<'_>| {
            if let DiagEvent::ServerNotice { severity, code, message } = ev {
                let mut slot = captured_in.lock().expect("test lock");
                *slot = Some((severity.to_string(), code.to_string(), message.to_string()));
            }
        });
        super::emit_server_notice(diag.sink().expect("sink installed"), &body);
        let got = captured.lock().expect("test lock").clone();
        assert_eq!(
            got,
            Some((
                "NOTICE".to_string(),
                "00000".to_string(),
                "hello from a raise notice".to_string(),
            )),
            "the notice's V-severity, SQLSTATE, and message must surface",
        );
    }

    #[test]
    fn emit_server_notice_on_a_truncated_body_does_not_panic() {
        // Untrusted/hostile bytes: a body with a field missing its NUL terminator
        // must surface a (possibly empty) event, never panic — the totality the
        // shared error_response_fields walk guarantees.
        let diag = Diagnostics::new().on_event(|_ev: &DiagEvent<'_>| {});
        let sink = diag.sink().expect("sink installed");
        super::emit_server_notice(sink, b"Mhi"); // no NUL, no terminator
        super::emit_server_notice(sink, &[]); // empty
        super::emit_server_notice(sink, &[0]); // immediate terminator
    }

    #[test]
    fn emit_parameter_status_parses_name_and_value() {
        // A `[name\0][value\0]` body surfaces as a ParameterStatus; a body missing
        // its trailing NUL (hostile/truncated) yields an empty value, never a
        // panic — the totality `split_first_cstr` guarantees.
        let captured: Arc<std::sync::Mutex<Vec<(String, String)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_in = Arc::clone(&captured);
        let diag = Diagnostics::new().on_event(move |ev: &DiagEvent<'_>| {
            if let DiagEvent::ParameterStatus { name, value } = ev {
                captured_in.lock().expect("lock").push((name.to_string(), value.to_string()));
            }
        });
        let sink = diag.sink().expect("sink installed");
        super::emit_parameter_status(sink, b"timezone\0UTC\0");
        super::emit_parameter_status(sink, b"application_name"); // no NUL — total
        super::emit_parameter_status(sink, b""); // empty — total
        let got = captured.lock().expect("lock").clone();
        assert_eq!(got.first(), Some(&("timezone".to_string(), "UTC".to_string())));
        assert_eq!(
            got.get(1),
            Some(&("application_name".to_string(), String::new())),
            "a NUL-less body is the whole name with an empty value",
        );
    }

    #[test]
    fn slow_query_arm_gate_is_zero_cost_off() {
        // ZERO-COST-OFF PROOF (C1d): the driver reads a clock (`Instant::now`) and
        // clones the `Diagnostics` ONLY when `slow_query_armed()` is true. Assert
        // it is FALSE in every off configuration, so the off path provably touches
        // no clock.
        let noop = |_ev: &DiagEvent<'_>| {};

        // Fully off: no sink, no threshold.
        assert!(!Diagnostics::new().slow_query_armed(), "default is not armed");

        // A sink but NO threshold: slow-query timing is off (other events still
        // flow, but no query is timed).
        assert!(
            !Diagnostics::new().on_event(noop).slow_query_armed(),
            "a sink without a threshold must not arm slow-query timing",
        );

        // A threshold but NO sink: nowhere to emit, so not armed.
        assert!(
            !Diagnostics::new()
                .slow_query_threshold(Duration::from_millis(1))
                .slow_query_armed(),
            "a threshold without a sink must not arm (nowhere to emit)",
        );

        // BOTH → armed (the only clock-reading configuration).
        assert!(
            Diagnostics::new()
                .on_event(noop)
                .slow_query_threshold(Duration::from_millis(1))
                .slow_query_armed(),
            "a sink + a threshold arms slow-query timing",
        );
    }

    #[test]
    fn a_self_reentering_sink_is_bounded_not_a_stack_overflow() {
        // A sink that EMITS from inside itself (the general shape of a sink that
        // runs a self-slow query, or a `pool.get()` that times out, whose own
        // event re-enters the sink) would recurse to a stack-overflow abort
        // without the `IN_DISPATCH` guard. With it, the nested emit is suppressed,
        // so the sink runs EXACTLY ONCE. Driver-agnostic (the guard is in core's
        // `dispatch`), so this offline test proves the mechanism directly.
        use std::sync::OnceLock;

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in = Arc::clone(&calls);
        // The sink re-enters via a Diagnostics handle shared through a OnceLock
        // set after build (it cannot capture the not-yet-built handle).
        let diag_cell: Arc<OnceLock<Diagnostics>> = Arc::new(OnceLock::new());
        let cell_in = Arc::clone(&diag_cell);
        let diag = Diagnostics::new().on_event(move |ev: &DiagEvent<'_>| {
            calls_in.fetch_add(1, Ordering::Relaxed);
            // Re-enter: emit the SAME event from inside the sink → back into
            // `dispatch`, which suppresses it (we are already IN_DISPATCH).
            if let Some(d) = cell_in.get() {
                d.emit(ev);
            }
        });
        diag_cell.set(diag.clone()).ok();

        diag.emit(&DiagEvent::PoolConnectionEvicted);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "the self-emitting sink ran exactly once; the nested emit was suppressed \
             (no unbounded recursion / stack overflow)",
        );

        // The flag is fully reset afterward, so a SUBSEQUENT (non-nested) emit
        // fires normally — the suppression is scoped to the re-entrant call, not
        // a permanent latch.
        diag.emit(&DiagEvent::PoolConnectionEvicted);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "a later top-level emit still fires (the guard reset on scope exit)",
        );
    }

    #[test]
    fn an_off_diagnostics_emits_nothing() {
        // The default (no sink) must invoke no closure and construct no owned data.
        let diag = Diagnostics::new();
        assert!(!diag.is_enabled());
        // No panic, no observable effect — the zero-cost-off branch.
        diag.emit(&DiagEvent::ServerNotice {
            severity: Cow::Borrowed(""),
            code: Cow::Borrowed(""),
            message: Cow::Borrowed(""),
        });
    }
}
