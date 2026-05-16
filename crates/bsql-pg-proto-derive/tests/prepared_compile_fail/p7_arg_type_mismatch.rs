//! DEF-244 hostile-bypass probe **P7** — pass arguments of the
//! wrong type to a `PreparedQuery`.
//!
//! # Tier
//!
//! Tier-1 by-construction. `BindPrepared<'q, P, R>` ties the
//! arg-tuple type `P` to `&PreparedQuery<P, R>::P`. Mismatched tuple
//! types fail to type-check at the `execute_prepared` boundary —
//! Rust's nominal tuple types make `(i32,)` and `(&str,)` distinct.
//!
//! # Expected diagnostic
//!
//! `error[E0308]: mismatched types: expected '(i32,)', found
//! '(&str,)'` (or similar — the wording can vary slightly across
//! rustc versions; the golden pins the current form).
//!
//! # Why this probe matters
//!
//! A query expecting `(i32,)` would, without this tier-1 enforcement,
//! silently accept `("hostile",)` and try to write an `&str` body
//! through the int4 encoder path — at best a wire-protocol error,
//! at worst undefined behaviour at the param encoder seam. Pinning
//! the type-equality at the call boundary closes the per-call
//! arg-shape mismatch class.
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P7.
//!
//! # DEF-246 Phase 2 migration (2026-05-16)
//!
//! Pre-DEF-246 the probe used `PgProtocol::new() -> <ActivePhase>` +
//! `as_ready()` to obtain a `ReadyGuard`. Post-DEF-246 the
//! constructor returns `<DisconnectedPhase>`; the handshake must
//! complete (`push_startup → ConnectingPhase` + auth frames +
//! `into_active`) to reach `<ActivePhase>`. The probe drives a
//! synthetic Trust handshake inline so the type-mismatch check at
//! `execute_prepared` is the FIRST compile-time failure.

extern crate bsql_pg_proto;

use bsql_pg_proto::{
    prepared, Credentials, FetchRows, Ident, IntoActiveError, PgProtocol, PreparedQuery, WriteBuf,
};
use bsql_pg_proto::reply_id::{QueryKind, StartupKind};

const Q_INT4: PreparedQuery<(i32,), ()> = prepared!(
    "DELETE FROM users WHERE id = $1::int4"
);

fn main() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let startup_reply = proto.next_reply_id::<StartupKind>();
    let user = match Ident::try_from_str("u") {
        Ok(u) => u,
        Err(_) => return,
    };
    let mut proto = {
        let (_, p) = match proto.push_startup(
            user,
            None,
            None,
            Credentials::Trust,
            startup_reply,
            &mut wb,
        ) {
            Ok(pair) => pair,
            Err(_) => return,
        };
        p
    };

    // Drive AuthOk + RFQ.
    if proto.feed_inbound(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).is_err() {
        return;
    }
    let _ = proto.advance_one_frame(&mut wb);
    if proto.feed_inbound(&[b'Z', 0, 0, 0, 5, b'I']).is_err() {
        return;
    }
    let _ = proto.advance_one_frame(&mut wb);

    let mut proto = match proto.into_active() {
        Ok(p) => p,
        Err(IntoActiveError::Closed(_)) => return,
        Err(IntoActiveError::StillConnecting(_)) => return,
    };

    let reply = proto.next_reply_id::<QueryKind>();
    let g = match proto.as_ready() {
        Some(g) => g,
        None => return,
    };
    // P7 attack: bind a string-tuple to a query expecting (i32,).
    // Should fail with E0308 — tuple types are nominally distinct.
    let _ = g.execute_prepared(
        &Q_INT4,
        ("hostile",),
        FetchRows::All,
        reply,
        &mut wb,
    );
}
