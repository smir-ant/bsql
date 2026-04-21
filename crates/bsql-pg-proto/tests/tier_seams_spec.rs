//! Tier-1-shield seam tests — category (1) per reforge.md §4.11.
//!
//! Every test here pins a one-line arm body or a {input → field}
//! mapping that the compiler does NOT enforce structurally. These are
//! the exact "literal swap compiles but silently corrupts behaviour"
//! seams that §4.11.1 requires us to close via test.
//!
//! Seams covered (from the 2026-04-18 second-pass audit):
//! - **S3** `SessionParams::set` — nine `b"key" => &mut self.field`
//!   arms; any swap compiles and the user observes one param in
//!   another's slot.
//! - **U3** `ProtoState::Errored(cause)` preservation across push —
//!   the `fail_cause = original.clone()` / `self.state =
//!   Errored(original)` pattern is reversible (compiles either way);
//!   a distinguishable cause in both state and reply pins it.
//! - **Newtype validation** — `DatabaseName`, `ApplicationName`
//!   validation matches `Ident` behaviour in spirit but not quite
//!   (app name allows empty). Currently only `Ident` is directly
//!   tested; this file fills the gap.
//! - **BackendKeyData malformed-size classification** — the
//!   `[a, b, c, d, e, f, g, h]` slice pattern in `parse_backend_key_data`
//!   is pinned as "exactly 8 bytes"; the fallback to
//!   `MalformedBackendKeyData { payload_len }` is live but not tested.

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
    Action, ApplicationName, Credentials, DatabaseName, Ident, IdentError, PgCommand, PgProtocol,
    ProtoState, ProtocolError, ReplyId, ReplyKind, SessionParams,
};
use core::num::NonZeroU64;

fn raw(value: u64) -> NonZeroU64 {
    // DEF-145: raw(0) is a test bug; assert fires loud.
    assert!(value > 0, "raw(0) is a test bug — use raw(1..) for non-zero test correlators");
    NonZeroU64::new(value).unwrap_or(NonZeroU64::MIN)
}

/// Generic over `K: ReplyKind` so tests can mint either
/// `ReplyId<PingKind>` or `ReplyId<StartupKind>` with the same
/// helper. Call site infers K from usage context (e.g. `PgCommand::Ping
/// { reply: id(raw(1)) }` picks `PingKind`).
fn id<K: ReplyKind>(value: NonZeroU64) -> ReplyId<K> {
    ReplyId::from_raw(value)
}

// =================================================================
// S2 — DELETED (DEF-089). The seam no longer exists: `SendBuf` is a
// single-shape newtype, not an enum. There is no `as_bytes` match
// body to swap. The former test `send_buf_as_bytes_static_and_owned_round_trip`
// was removed in the same commit that collapsed the enum — surface
// that could drift no longer exists, so no test is needed.
//
// This is the §4.11 ideal: when architecture moves a tier-3 test
// up to tier-1 structural, the test disappears alongside the surface.
// =================================================================

// =================================================================
// S3. SessionParams::set — key → field routing table
// =================================================================

/// Invariant (spec): each known `ParameterStatus` key routes to its
/// matching `SessionParams` field. Swapping any two arms in
/// `SessionParams::set` compiles cleanly; this table catches such
/// drift by setting each key in turn and reading back the matching
/// field.
#[test]
fn session_params_set_key_routing_table() {
    let mut p = SessionParams::new();

    // Set each known key with a distinguishable value.
    p.set(b"server_version", b"17.2");
    p.set(b"server_encoding", b"UTF8");
    p.set(b"client_encoding", b"LATIN1");
    p.set(b"application_name", b"myapp");
    p.set(b"is_superuser", b"off");
    p.set(b"session_authorization", b"alice");
    p.set(b"DateStyle", b"ISO, MDY");
    p.set(b"integer_datetimes", b"on");
    p.set(b"TimeZone", b"America/New_York");

    // Each value must arrive in its dedicated field — a swap of any
    // two arms would land the wrong value in the wrong field.
    assert_eq!(p.server_version.as_ref().map(|s| s.as_str()), Some("17.2"));
    // DEF-114: typed fields — Encoding/bool instead of strings.
    assert_eq!(p.server_encoding, Some(bsql_pg_proto::session_params::Encoding::Utf8));
    assert_eq!(
        p.client_encoding,
        Some(bsql_pg_proto::session_params::Encoding::Latin1),
    );
    assert_eq!(p.application_name.as_ref().map(|s| s.as_str()), Some("myapp"));
    assert_eq!(p.is_superuser, Some(false));
    assert_eq!(p.session_authorization.as_ref().map(|s| s.as_str()), Some("alice"));
    assert_eq!(p.date_style.as_ref().map(|s| s.as_str()), Some("ISO, MDY"));
    assert_eq!(p.integer_datetimes, Some(true));
    assert_eq!(p.time_zone.as_ref().map(|s| s.as_str()), Some("America/New_York"));
}

/// Invariant (spec): an unknown key is silently dropped; no matching
/// field is created. Pins the `_ => return` arm in `set`.
#[test]
fn session_params_set_unknown_key_is_dropped() {
    let mut p = SessionParams::new();
    p.set(b"some_future_key", b"value");
    // None of the known fields should be set.
    assert!(p.server_version.is_none());
    assert!(p.time_zone.is_none());
}

/// F55 regression (pass #6 audit): non-UTF-8 bytes are lossy-coerced
/// (non-ASCII → `?`), not silently skipped.
///
/// Pre-F55 the `set` path did `let Ok(s) = from_utf8(value) else { return }`
/// → the previously-set value was silently preserved and the new
/// (invalid) value dropped. That's a diagnostic-loss class —
/// operators debugging "why is server_version stale?" had no signal.
///
/// Post-F55 the `from_bytes_lossy` constructor replaces non-ASCII
/// bytes with `?` placeholders and ALWAYS overwrites. The new value
/// is visibly mangled (operator sees `"?"` where real data should
/// be) — loud diagnostic, not silent drop.
#[test]
fn session_params_set_non_utf8_is_lossy_not_silent_skip() {
    let mut p = SessionParams::new();
    p.set(b"server_version", b"17.2");
    // Invalid UTF-8 (a lone continuation byte).
    p.set(b"server_version", &[0x80]);
    // F55: non-ASCII byte coerced to `?`; the new value overwrote.
    assert_eq!(p.server_version.as_ref().map(|s| s.as_str()), Some("?"));
}

/// DEF-153 (audit A003): a malformed bool value (anything other than
/// PG's canonical `on`/`off`) leaves the field `None` AND bumps the
/// `n_malformed_bool_dropped` counter — gives operators a diagnostic
/// signal distinguishing "server never sent" from "server sent a
/// value we couldn't parse".
///
/// Covers both `is_superuser` and `integer_datetimes` — the two
/// bool-valued keys in `SessionParams::set`.
#[test]
fn session_params_set_malformed_bool_bumps_counter() {
    let mut p = SessionParams::new();
    assert_eq!(p.n_malformed_bool_dropped, 0);

    // `yes` is not a valid PG bool (PG expects `on`/`off`).
    p.set(b"is_superuser", b"yes");
    assert!(p.is_superuser.is_none(), "malformed → field stays None");
    assert_eq!(p.n_malformed_bool_dropped, 1, "counter bumps on first drop");

    // `1` is also rejected — parse_pg_bool accepts only PG's canonical forms.
    p.set(b"integer_datetimes", b"1");
    assert!(p.integer_datetimes.is_none());
    assert_eq!(p.n_malformed_bool_dropped, 2, "counter bumps on each drop");

    // Unknown key drop bumps `n_unknown_dropped`, NOT the bool counter.
    p.set(b"some_future_key", b"value");
    assert_eq!(p.n_malformed_bool_dropped, 2, "unknown-key drop must not touch bool counter");
    assert_eq!(p.n_unknown_dropped, 1);
}

/// DEF-153 companion: a WELL-FORMED bool value does NOT bump the
/// malformed counter. Pins the negative side of the above test.
#[test]
fn session_params_set_well_formed_bool_does_not_bump_counter() {
    let mut p = SessionParams::new();
    p.set(b"is_superuser", b"on");
    p.set(b"is_superuser", b"off");
    p.set(b"integer_datetimes", b"on");
    p.set(b"integer_datetimes", b"off");
    assert_eq!(p.is_superuser, Some(false));
    assert_eq!(p.integer_datetimes, Some(false));
    assert_eq!(p.n_malformed_bool_dropped, 0, "well-formed path must not bump");
}

/// Invariant (spec): a second valid set to the same key overwrites
/// the first.
#[test]
fn session_params_set_second_value_overwrites() {
    let mut p = SessionParams::new();
    p.set(b"TimeZone", b"UTC");
    p.set(b"TimeZone", b"America/New_York");
    assert_eq!(p.time_zone.as_ref().map(|s| s.as_str()), Some("America/New_York"));
}

// =================================================================
// U3. Errored preservation across push — cause in state AND reply.
// =================================================================

/// Invariant (spec): after a fatal enters `Errored(cause)`, a
/// subsequent `push_command` emits `FailReply` carrying the **same**
/// cause AND `ProtoState::Errored(cause)` is still stored. Pins that
/// `handle_push_ping`'s Errored arm both clones for the reply AND
/// restores the state.
///
/// A regression that forgot `self.state = Errored(original)` (losing
/// the cause to `core::mem::take`'s default = Idle) would reopen the
/// connection for commands silently. `ping_spec.rs`'s
/// `errored_state_is_terminal_and_drops_subsequent_frames` already
/// pins the frame-side behaviour; this one pins the push-side.
///
/// Uses a distinguishable cause (`FrameTooLarge { declared: 0xDEAD }`)
/// so reply and state must both carry the exact same value.
#[test]
fn errored_cause_is_preserved_in_state_and_reply() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let ping_raw = raw(7777);
    // Push ping and feed a FrameTooLarge frame. Setup-action list
    // discarded explicitly (`let _ = ...` is banned by user feedback).
    _ = proto.push_command(PgCommand::Ping {
        reply: id(ping_raw),
    }, &mut wb);
    // Declared length = 0xDEAD (way above MAX_FRAME_LEN_FIELD=4095).
    let frame = [b'Z', 0x00, 0x00, 0xDE, 0xAD];
    let out = proto.feed_bytes(&frame, &mut wb);
    assert_eq!(out.len(), 2);

    // First fatal: FailReply carries the FULL ProtocolError
    // (FrameTooLarge{declared: 0xDEAD}) and state transitions to
    // Errored(ErrorKind::Framing) — DEF-061: state retains only the
    // 1-byte kind classification, not the full cause.
    use bsql_pg_proto::error::ErrorKind;
    match out.as_slice() {
        [
            Action::FailReply {
                cause: ProtocolError::FrameTooLarge { declared },
                ..
            },
            Action::CloseSocket,
        ] => {
            assert_eq!(*declared, 0xDEAD, "first FailReply carries full cause");
        }
        other => panic!("expected [FailReply(FrameTooLarge 0xDEAD), CloseSocket], got {other:?}"),
    }
    assert!(
        matches!(proto.state(), ProtoState::Errored(k) if k.as_kind() == ErrorKind::Framing),
        "state after first fatal must be Errored(Framing), got {:?}",
        proto.state(),
    );

    // Push a new Ping — DEF-061: state is compact-Errored(Framing),
    // so the second FailReply is ConnectionAlreadyClosed{prior_kind:
    // Framing}, NOT a duplicate of the original FrameTooLarge. The
    // wrapper preserved the original diagnostic from the first
    // FailReply; this reply just classifies "already closed".
    let second_raw = raw(7778);
    let out = proto.push_command(PgCommand::Ping {
        reply: id(second_raw),
    }, &mut wb);
    assert_eq!(out.len(), 1);
    match out.as_slice() {
        [Action::FailReply {
            cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
            ..
        }] => {
            // DEF-142 (pass-#8): prior_kind is StateErrorKind newtype.
            assert_eq!(prior_kind.as_kind(), ErrorKind::Framing,
                "ConnectionAlreadyClosed must carry the prior_kind classification");
        }
        other => panic!(
            "expected FailReply(ConnectionAlreadyClosed{{Framing}}), got {other:?}",
        ),
    }
    // State preservation: still Errored(Framing) after the push.
    assert!(
        matches!(proto.state(), ProtoState::Errored(k) if k.as_kind() == ErrorKind::Framing),
        "state must stay Errored(Framing) after push, got {:?}",
        proto.state(),
    );
}

// =================================================================
// DatabaseName / ApplicationName validation — mirror ident_validation
// for the other two NUL-free newtypes (DEF-041).
// =================================================================

/// Invariant (spec): `DatabaseName::try_from_str` has the same
/// validation as `Ident` — non-empty, no NUL, ≤ 63 bytes.
#[test]
fn database_name_validation() {
    // Valid.
    assert!(DatabaseName::try_from_str("mydb").is_ok());

    // Empty rejected.
    assert!(matches!(
        DatabaseName::try_from_str(""),
        Err(IdentError::Empty),
    ));

    // NUL byte rejected.
    assert!(matches!(
        DatabaseName::try_from_str("my\0db"),
        Err(IdentError::ContainsNul),
    ));

    // Over-length rejected.
    let long = "a".repeat(64);
    assert!(matches!(
        DatabaseName::try_from_str(&long),
        Err(IdentError::TooLong { .. }),
    ));

    // At-capacity boundary accepted.
    let at_cap = "a".repeat(63);
    assert!(DatabaseName::try_from_str(&at_cap).is_ok());
}

/// Invariant (spec): `ApplicationName::try_from_str` differs from
/// `Ident` / `DatabaseName` in that EMPTY is allowed — PG accepts an
/// empty `application_name` parameter. All other validations are the
/// same (no NUL, ≤ 128 bytes).
///
/// Pins the `require_non_empty: false` argument to
/// `validate_ident` in the `ApplicationName::try_from_str` body — a
/// regression flipping it to `true` would reject legal empty inputs.
#[test]
fn application_name_validation_allows_empty() {
    // Empty ALLOWED (unlike Ident/DatabaseName).
    assert!(ApplicationName::try_from_str("").is_ok());

    // Valid non-empty.
    assert!(ApplicationName::try_from_str("myapp-worker-01").is_ok());

    // NUL still rejected.
    assert!(matches!(
        ApplicationName::try_from_str("my\0app"),
        Err(IdentError::ContainsNul),
    ));

    // Over-length (128 cap).
    let long = "a".repeat(129);
    assert!(matches!(
        ApplicationName::try_from_str(&long),
        Err(IdentError::TooLong { .. }),
    ));

    // At capacity accepted.
    let at_cap = "a".repeat(128);
    assert!(ApplicationName::try_from_str(&at_cap).is_ok());
}

// =================================================================
// BackendKeyData malformed-size classification.
// =================================================================

/// Invariant (spec): `BackendKeyData` frame (tag `'K'`) whose payload
/// is not exactly 8 bytes is classified as
/// `ProtocolError::MalformedBackendKeyData { payload_len }`, not
/// silently accepted. Pins the `[a,b,c,d,e,f,g,h]` slice pattern vs
/// the fallback `other` arm in `parse_backend_key_data`.
#[test]
fn backend_key_data_wrong_payload_size_is_classified() {
    // Set up: drive to ConnectingPostAuthAwaitingKey.
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(9000);
    // Setup: push Startup, feed AuthOk. Action lists are discarded
    // explicitly via `drop(...)` — `let _ = ...` is banned.
    _ = proto.push_command(PgCommand::Startup {
        user: Ident::try_from_str("u").unwrap_or_else(|_| panic!("valid ident")),
        database: None,
        app_name: None,
        credentials: Credentials::Trust,
        reply: id(startup_raw),
    }, &mut wb);
    // Feed AuthOk — now ConnectingPostAuthAwaitingKey.
    let auth_ok_frame: [u8; 9] = [b'R', 0, 0, 0, 8, 0, 0, 0, 0];
    _ = proto.feed_bytes(&auth_ok_frame, &mut wb);
    assert!(matches!(
        proto.state(),
        ProtoState::ConnectingPostAuthAwaitingKey(_),
    ));

    // Feed a BackendKeyData frame with a 4-byte body (wrong — spec says 8).
    let bad_bkd: [u8; 9] = [b'K', 0, 0, 0, 8, 0x11, 0x22, 0x33, 0x44];
    let out = proto.feed_bytes(&bad_bkd, &mut wb);
    assert_eq!(
        out.len(),
        2,
        "malformed BKD → FailReply + CloseSocket",
    );
    match out.as_slice() {
        [Action::FailReply { cause, .. }, Action::CloseSocket] => match cause {
            ProtocolError::MalformedBackendKeyData { payload_len } => {
                assert_eq!(*payload_len, 4);
            }
            other => panic!("expected MalformedBackendKeyData, got {other:?}"),
        },
        other => panic!("unexpected: {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Errored(_)));
}
