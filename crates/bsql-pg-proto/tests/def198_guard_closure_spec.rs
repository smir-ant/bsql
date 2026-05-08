//! DEF-198 — tier-1 closure verification for [`PgProtocol::as_ready`]
//! and [`PgProtocol::connection_status`].
//!
//! # What this file proves
//!
//! Both `as_ready` and `connection_status` dispatch on
//! `state.push_class()`, an exhaustive 5-variant classifier over every
//! `ProtoState` variant. Adding a new `ProtoState` variant forces an
//! update to `push_class` (compile failure if forgotten — already
//! pinned by `state.rs::push_class_tests`). The closure is thus
//! transitive: ProtoState change → StatePushClass change → as_ready /
//! connection_status reclassification.
//!
//! These integration tests verify the **behaviour** of as_ready /
//! connection_status for one representative state variant per
//! StatePushClass class:
//!
//! | StatePushClass  | Representative state                     | as_ready  | connection_status |
//! |-----------------|------------------------------------------|-----------|-------------------|
//! | Idle            | `ProtoState::Idle`                       | `Some(_)` | `Ready`           |
//! | PingAwaiting    | `ProtoState::PingAwaitingRfq(_)`         | `None`    | `Busy`            |
//! | BusyQuery       | `ProtoState::SimpleQueryAwaitingFirst..` | `None`    | `Busy`            |
//! | Connecting      | `ProtoState::ConnectingStartupTrust { .. }` | `None`  | `Handshaking`     |
//! | Errored         | `ProtoState::Errored(_)`                 | `None`    | `Errored(_)`      |
//!
//! The per-ProtoState-variant exhaustive grid lives in protocol.rs's
//! `compute_push_tests` module (private state-field access). This file
//! is the public-API contract pin.

#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::mem_forget,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::integer_division
)]
#![deny(unused_must_use, unused_lifetimes)]

use bsql_pg_proto::{
    Action, ConnectionStatus, Credentials, Ident, PgProtocol, PingKind, ProtoState,
    QueryKind, ReplyId, Sql, WriteBuf,
    wire::TAG_READY_FOR_QUERY,
};
use core::num::NonZeroU64;

mod common;
use common::PushOrPanic;

fn nz(v: u64) -> NonZeroU64 {
    assert!(v > 0, "nz(0) is a test bug");
    NonZeroU64::new(v).unwrap_or(NonZeroU64::MIN)
}

fn ping_id(v: u64) -> ReplyId<PingKind> {
    ReplyId::from_raw(nz(v))
}

fn query_id(v: u64) -> ReplyId<QueryKind> {
    ReplyId::from_raw(nz(v))
}

fn ident(s: &str) -> Ident {
    match Ident::try_from_str(s) {
        Ok(i) => i,
        Err(e) => panic!("test fixture: invalid ident `{s}`: {e:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════
// (1) Idle class — fresh proto, as_ready returns Some
// ═══════════════════════════════════════════════════════════════════

#[test]
fn def198_idle_state_yields_ready_guard() {
    let mut proto = PgProtocol::new();
    assert!(
        proto.as_ready().is_some(),
        "fresh PgProtocol::new() must yield Some(ReadyGuard)",
    );
    assert!(
        matches!(proto.state(), ProtoState::Idle),
        "fresh proto must be in Idle state",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Ready,
        "Idle state must map to ConnectionStatus::Ready",
    );
}

#[test]
fn def198_idle_after_drain_yields_ready_guard() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();

    // Push + drain Ping; final state should be Idle.
    proto.push_or_panic(bsql_pg_proto::push_command::Ping { reply: ping_id(1) }, &mut wb);
    let rfq = [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, b'I'];
    let _ = proto.feed_bytes(&rfq, &mut wb);

    assert!(
        proto.as_ready().is_some(),
        "after Ping drain, state returns to Idle",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Ready,
        "drained Ping returns to Ready",
    );
}

// ═══════════════════════════════════════════════════════════════════
// (2) PingAwaiting class — push Ping, no drain; as_ready returns None
// ═══════════════════════════════════════════════════════════════════

#[test]
fn def198_ping_awaiting_classifies_busy() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();

    proto.push_or_panic(bsql_pg_proto::push_command::Ping { reply: ping_id(1) }, &mut wb);

    assert!(matches!(proto.state(), ProtoState::PingAwaitingRfq(_)));
    assert!(
        proto.as_ready().is_none(),
        "DEF-198: as_ready returns None during PingAwaitingRfq",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Busy,
        "PingAwaitingRfq classifies as ConnectionStatus::Busy",
    );

    // Drain so Drop-guard is happy.
    let rfq = [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, b'I'];
    let _ = proto.feed_bytes(&rfq, &mut wb);
}

// ═══════════════════════════════════════════════════════════════════
// (3) BusyQuery class — push SimpleQuery, no drain
// ═══════════════════════════════════════════════════════════════════

#[test]
fn def198_simple_query_awaiting_classifies_busy() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();

    proto.push_or_panic(
        bsql_pg_proto::push_command::SimpleQuery {
            sql: Sql::from_str_truncating("SELECT 1"),
            reply: query_id(1),
        },
        &mut wb,
    );

    assert!(matches!(
        proto.state(),
        ProtoState::SimpleQueryAwaitingFirstResponse(_),
    ));
    assert!(
        proto.as_ready().is_none(),
        "DEF-198: as_ready returns None during SimpleQuery in-flight",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Busy,
        "BusyQuery class maps to ConnectionStatus::Busy",
    );

    // Drain via CommandComplete + RFQ.
    let mut drain = std::vec::Vec::new();
    let mut cc_body = b"SELECT 0".to_vec();
    cc_body.push(0); // NUL terminator
    drain.push(b'C');
    let total = u32::try_from(4 + cc_body.len()).unwrap_or(0);
    drain.extend_from_slice(&total.to_be_bytes());
    drain.extend_from_slice(&cc_body);
    drain.extend_from_slice(&[TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, b'I']);
    let _ = proto.feed_bytes(&drain, &mut wb);
}

// ═══════════════════════════════════════════════════════════════════
// (4) Connecting class — push Startup, no auth-ok yet
// ═══════════════════════════════════════════════════════════════════

#[test]
fn def198_connecting_startup_classifies_handshaking() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();

    proto.push_or_panic(
        bsql_pg_proto::push_command::Startup {
            user: ident("testuser"),
            database: None,
            app_name: None,
            credentials: Credentials::Trust,
            reply: ReplyId::from_raw(nz(1)),
        },
        &mut wb,
    );

    assert!(matches!(
        proto.state(),
        ProtoState::ConnectingStartupTrust { .. },
    ));
    assert!(
        proto.as_ready().is_none(),
        "DEF-198: as_ready returns None during Startup handshake",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Handshaking,
        "Connecting class maps to ConnectionStatus::Handshaking",
    );

    // Drain handshake.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]); // AuthOk
    drain.extend_from_slice(&[
        b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2,
    ]); // BackendKeyData
    drain.extend_from_slice(&[TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, b'I']);
    let _ = proto.feed_bytes(&drain, &mut wb);
}

// ═══════════════════════════════════════════════════════════════════
// (5) Errored class — drive into Errored, verify classification
// ═══════════════════════════════════════════════════════════════════

#[test]
fn def198_errored_classifies_errored_with_kind() {
    use bsql_pg_proto::error::ErrorKind;

    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();

    // Force Errored: unsolicited Z in Idle.
    let unsolicited = [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, b'I'];
    let out = proto.feed_bytes(&unsolicited, &mut wb);
    assert!(out.as_slice().iter().any(|a| matches!(a, Action::CloseSocket)));

    assert!(matches!(proto.state(), ProtoState::Errored(_)));
    assert!(
        proto.as_ready().is_none(),
        "DEF-198: as_ready returns None on Errored",
    );
    match proto.connection_status() {
        ConnectionStatus::Errored(state_err_kind) => {
            // Unsolicited Z classifies the connection as
            // tier-3 framing error. The kind is exposed via
            // ConnectionStatus for caller-side recovery.
            // We don't pin a specific kind here (drift-stable);
            // we pin the *shape* of the public-API exposure.
            let _ = state_err_kind.as_kind();
        }
        other => panic!("expected ConnectionStatus::Errored(_), got {other:?}"),
    }

    // Verify ErrorKind enum is reachable via the public re-export.
    let _: ErrorKind = ErrorKind::ServerError;
}

// ═══════════════════════════════════════════════════════════════════
// (6) Guard semantics — borrow exclusivity + consume on push
// ═══════════════════════════════════════════════════════════════════

#[test]
fn def198_ready_guard_consumes_on_push() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();

    // Acquire guard, push (consumes), state transitions to non-Idle.
    if let Some(guard) = proto.as_ready() {
        // DEF-212: explicit Ok arm — Ping push from Idle is
        // architecturally infallible (Ping body = pure const SYNC, no
        // builder Err path). The match preserves the
        // `clippy::let_underscore_must_use` discipline (DEF-211 SAFE-05)
        // by handling both arms; pre-(212) `let _out = ...` was a
        // bind-to-named-underscore which avoided the lint but left
        // failures silently unobserved at the test layer.
        match guard.push_command(bsql_pg_proto::push_command::Ping { reply: ping_id(1) }, &mut wb) {
            Ok(()) => {}
            Err(failure) => panic!(
                "Ping push from Idle must succeed (architecturally infallible); \
                 got Err({failure:?})",
            ),
        }
        // Guard consumed; cannot be reused. The compile-fail test in
        // `tests/def198_compile_fail/` proves the type-level lock.
    } else {
        panic!("fresh proto must yield ReadyGuard");
    }

    // Subsequent as_ready returns None — state transitioned via push.
    assert!(
        proto.as_ready().is_none(),
        "post-push state is non-Idle (PingAwaitingRfq); as_ready returns None",
    );

    // Drain.
    let rfq = [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, b'I'];
    let _ = proto.feed_bytes(&rfq, &mut wb);
}

#[test]
fn def198_ready_guard_drop_without_push_preserves_state() {
    let mut proto = PgProtocol::new();

    // Acquire and drop without pushing.
    drop(proto.as_ready());

    // State unchanged: still Idle, as_ready still returns Some.
    assert!(
        matches!(proto.state(), ProtoState::Idle),
        "ReadyGuard drop without push preserves Idle state",
    );
    assert!(
        proto.as_ready().is_some(),
        "Idle state preserved across drop-without-push; another guard available",
    );
}

// ═══════════════════════════════════════════════════════════════════
// (7) ConnectionStatus is Copy / Eq for ergonomic caller use
// ═══════════════════════════════════════════════════════════════════

#[test]
fn def198_connection_status_is_copy_eq() {
    let proto = PgProtocol::new();
    let s1 = proto.connection_status();
    let s2 = proto.connection_status();
    // Copy:
    let s3 = s1;
    let s4 = s1;
    assert_eq!(s1, s2);
    assert_eq!(s3, s4);
    assert_eq!(s1, ConnectionStatus::Ready);
}
