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

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, FetchRows, PgProtocol, PreparedQuery, WriteBuf};
use bsql_pg_proto::reply_id::QueryKind;

const Q_INT4: PreparedQuery<(i32,), ()> = prepared!(
    "DELETE FROM users WHERE id = $1::int4"
);

fn main() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
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
