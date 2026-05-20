//! Probe **P-D278D-6** — the `&[u8; 16]` lent into
//! the `with_cancel_request` closure cannot escape past the call.
//!
//! Tier-1 by construction: the closure type `impl FnOnce(&[u8; 16],
//! i32) -> R` carries an implicit HRTB `for<'a> FnOnce(&'a [u8; 16],
//! i32) -> R` so the `bytes: &'a [u8; 16]` borrow is quantified over
//! `'a` and cannot satisfy any outer-scope lifetime requirement.
//! Attempting to write the borrow into an outer slot is rejected at
//! compile time (`E0521` "borrowed data escapes outside of closure"
//! / variants).
//!
//! This is the closure-scope retention guarantee that elevates the
//! secret-bytes invariant from tier-1 by-Drop-fire to tier-1
//! by-closure-scope. Pinned here so a future refactor that drops the
//! HRTB (e.g. switching to `&'static [u8; 16]` or boxing the
//! closure) surfaces as a golden diff.
//!
//! What the caller MAY still do (intentionally allowed, documented
//! in `with_cancel_request`'s doc-comment): copy the bytes' CONTENTS
//! into caller-owned memory via `*bytes` (returns `[u8; 16]` by
//! value), `bytes.to_vec()`, `bytes.iter().copied()`, etc. Those
//! are memcpys into caller storage, not reference leaks; caller-side
//! scrubbing is the caller's responsibility (see doc-comment).

extern crate bsql_pg_proto;

use bsql_pg_proto::{
    ActivePhase, Credentials, DisconnectedPhase, Ident, IntoActiveError, PgProtocol, WriteBuf,
};

fn fresh_active() -> PgProtocol<ActivePhase> {
    let mut proto = PgProtocol::<DisconnectedPhase>::new();
    let mut wb = WriteBuf::new();
    let user = match Ident::try_from_str("u") {
        Ok(u) => u,
        Err(_) => panic!("u is a valid ident"),
    };
    let reply = proto.next_reply_id();
    let (_, mut proto_c) = match proto.push_startup(
        user,
        None,
        None,
        Credentials::Trust,
        reply,
        &mut wb,
    ) {
        Ok(p) => p,
        Err(_) => panic!("push_startup must succeed"),
    };
    // AuthOk
    let _ = proto_c.feed_inbound(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
    let _ = proto_c.advance_one_frame(&mut wb);
    // K (pid=1, secret=2)
    let _ = proto_c.feed_inbound(&[
        b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2,
    ]);
    let _ = proto_c.advance_one_frame(&mut wb);
    // Z (idle)
    let _ = proto_c.feed_inbound(&[b'Z', 0, 0, 0, 5, b'I']);
    let _ = proto_c.advance_one_frame(&mut wb);
    match proto_c.into_active() {
        Ok(p) => p,
        Err(IntoActiveError::Closed(_)) => panic!("Closed unexpected"),
        Err(IntoActiveError::StillConnecting(_)) => panic!("StillConnecting unexpected"),
    }
}

fn main() {
    let active = fresh_active();

    // Outer slot — Option<&[u8; 16]> with an implicit lifetime
    // anchored to the surrounding function scope. The closure body
    // assigns `bytes: &'a [u8; 16]` (HRTB-quantified) into the slot,
    // forcing `'a` to outlive the function scope — which the HRTB
    // forbids. Borrow-checker rejects with `E0521`-class diagnostic.
    let mut escape_slot: Option<&[u8; 16]> = None;
    let _ = active.with_cancel_request(|bytes, _pid| {
        escape_slot = Some(bytes);
    });
    if let Some(b) = escape_slot {
        // After `with_cancel_request` returned, the Zeroizing guard
        // dropped — `b` would point at scrubbed/freed memory. The
        // compile-fail above prevents reaching this line.
        let _ = b.get(0);
    }
}
