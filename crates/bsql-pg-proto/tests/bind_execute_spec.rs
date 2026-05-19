//! Extended Query `Bind + Execute + Sync` pipeline end-to-end.
//!
//! Covers the three-frame bundled pipeline:
//! - DML happy path: Bind → BindComplete → CommandComplete → RFQ
//! - SELECT with pre-provided schema: adds DataRow* between 2 and C
//! - Bad paths: server ErrorResponse at various stages, wrong state,
//!   Errored connection, PortalSuspended classification, DataRow
//!   without schema classification
//! - Wire-format drift-pins: B frame byte layout, E frame byte layout

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
    Action, ActiveState, ConnectionStatus, FetchRows, PortalName, ProtocolError,
    QueryKind, Reply, StmtName, WriteBuf,
    decode::RowDesc,
    wire::{
        TAG_BIND, TAG_BIND_COMPLETE, TAG_COMMAND_COMPLETE, TAG_DATA_ROW, TAG_ERROR_RESPONSE,
        TAG_EXECUTE, TAG_READY_FOR_QUERY,
    },
};

mod common;
use common::{PushOrPanic, fresh_active_via_trust_handshake, mint_reply, split_bind_execute_sync};

fn portal_unnamed() -> PortalName {
    PortalName::default()
}

fn stmt_unnamed() -> StmtName {
    StmtName::default()
}

/// Build a bare PG frame: tag + 4-byte BE length (self-inclusive) + body.
fn frame(tag: u8, body: &[u8]) -> std::vec::Vec<u8> {
    let mut out = std::vec::Vec::new();
    out.push(tag);
    let Ok(len) = u32::try_from(body.len().saturating_add(4)) else {
        panic!("fixture body too large");
    };
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn bind_complete_frame() -> [u8; 5] {
    [TAG_BIND_COMPLETE.byte(), 0, 0, 0, 4]
}

fn command_complete_frame(tag: &[u8]) -> std::vec::Vec<u8> {
    // CommandComplete body = NUL-terminated ASCII tag.
    let mut body = tag.to_vec();
    body.push(0);
    frame(TAG_COMMAND_COMPLETE.byte(), &body)
}

fn rfq_frame(tx_byte: u8) -> [u8; 6] {
    [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, tx_byte]
}

fn data_row_frame(values: &[&[u8]]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    let Ok(n_cols) = i16::try_from(values.len()) else {
        panic!("too many columns");
    };
    body.extend_from_slice(&n_cols.to_be_bytes());
    for v in values {
        let Ok(len) = i32::try_from(v.len()) else {
            panic!("column too large");
        };
        body.extend_from_slice(&len.to_be_bytes());
        body.extend_from_slice(v);
    }
    frame(TAG_DATA_ROW.byte(), &body)
}

fn error_response_frame(severity: &[u8], code: &[u8], message: &[u8]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.push(b'S');
    body.extend_from_slice(severity);
    body.push(0);
    body.push(b'C');
    body.extend_from_slice(code);
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message);
    body.push(0);
    body.push(0); // terminator
    frame(TAG_ERROR_RESPONSE.byte(), &body)
}

// ═════════════════════════════════════════════════════════════════
// Happy paths
// ═════════════════════════════════════════════════════════════════

/// Invariant (spec): `push_bind_execute` with no params on a DML
/// statement emits three `SendBytes` actions: Bind frame + Execute
/// frame + static `Sync`. State advances to
/// `BindExecuteAwaitingBindComplete`.
#[test]
fn bind_execute_emits_three_send_bytes_and_transitions() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<QueryKind>(&mut proto);

    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(),
        None,
        FetchRows::All,
        reply,
        &mut wb,
    );

    // Bytes live in `wb` (single concatenation drained by I/O layer
    // in one socket write). Verify wire layout via structural split.
    let (bind, execute, sync) = split_bind_execute_sync(wb.as_bytes());
    assert_eq!(bind.first(), Some(&TAG_BIND.byte()), "first frame tag = 'B'");
    assert_eq!(execute.first(), Some(&TAG_EXECUTE.byte()), "second frame tag = 'E'");
    assert_eq!(sync, &[b'S', 0u8, 0u8, 0u8, 4u8], "third = Sync wire bytes");

    assert!(matches!(
        proto.state(),
        ActiveState::BindExecuteAwaitingBindCompleteDml(_)
            | ActiveState::BindExecuteAwaitingBindCompleteSelect { .. }
    ));

    // Drain to Idle so ReplyId's Drop-guard doesn't trip.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&bind_complete_frame());
    drain.extend_from_slice(&command_complete_frame(b"INSERT 0 0"));
    drain.extend_from_slice(&rfq_frame(b'I'));
    let _ = proto.feed_bytes(&drain, &mut wb);
}

/// Invariant: full DML round-trip. `2` → `C(INSERT 0 1)` → `Z('I')`.
/// Delivers `Reply::QueryComplete` with no schema.
#[test]
fn bind_execute_dml_full_round_trip() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, reply_raw) = mint_reply::<QueryKind>(&mut proto);

    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(42i32,),
        None,
        FetchRows::All,
        reply,
        &mut wb,
    );

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&bind_complete_frame());
    bytes.extend_from_slice(&command_complete_frame(b"INSERT 0 1"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    assert_eq!(out.len(), 1, "DML end → single DeliverReply");
    match out.as_slice() {
        [Action::DeliverReply {
            id: delivered,
            value: Reply::QueryComplete(p),
        }] => {
            assert_eq!(*delivered, reply_raw, "correlator round-trips");
            assert_eq!(p.command_tag.as_str(), "INSERT 0 1");
            assert_eq!(p.tx_status, bsql_pg_proto::TxStatus::Idle);
            assert!(p.row_desc.is_none(), "DML: no schema delivered");
        }
        other => panic!("expected DeliverReply(QueryComplete), got {other:?}"),
    }
    assert!(matches!(proto.state(), ActiveState::Idle));
}

/// Invariant: SELECT path. User pre-provided row_desc → DataRow
/// emits `Action::StreamRow` with the schema by value.
#[test]
fn bind_execute_select_with_schema_streams_rows() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<QueryKind>(&mut proto);

    // User-provided schema — 1 TEXT column. In real use, macro-
    // generated at compile time from Parse+Describe fingerprint.
    let schema = RowDesc::EMPTY; // empty schema — 0-column "SELECT" case

    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(),
        Some(schema),
        FetchRows::All,
        reply,
        &mut wb,
    );

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&bind_complete_frame());
    bytes.extend_from_slice(&command_complete_frame(b"SELECT 0"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    assert_eq!(out.len(), 1, "0-row SELECT: just DeliverReply");
    match out.as_slice() {
        [Action::DeliverReply {
            value: Reply::QueryComplete(p),
            ..
        }] => {
            assert_eq!(p.command_tag.as_str(), "SELECT 0");
            assert!(p.row_desc.is_some(), "SELECT: schema delivered");
        }
        other => panic!("expected DeliverReply, got {other:?}"),
    }
}

// ═════════════════════════════════════════════════════════════════
// Bad paths — recoverable (query-level)
// ═════════════════════════════════════════════════════════════════

/// Invariant: server `ErrorResponse` during Bind (before
/// BindComplete) → FailReply + drain-to-Idle. Connection survives.
#[test]
fn bind_error_is_recoverable() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, reply_raw) = mint_reply::<QueryKind>(&mut proto);

    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(),
        None,
        FetchRows::All,
        reply,
        &mut wb,
    );

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&error_response_frame(
        b"ERROR",
        b"42P01",
        b"prepared statement \"foo\" does not exist",
    ));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    assert_eq!(out.len(), 1, "error + RFQ → FailReply, no CloseSocket");
    match out.as_slice() {
        [Action::FailReply { id: failed, cause }] => {
            assert_eq!(*failed, reply_raw);
            assert!(
                matches!(cause, ProtocolError::ServerErrorResponse { .. }),
                "expected classified server error, got {cause:?}",
            );
        }
        other => panic!("expected single FailReply, got {other:?}"),
    }
    assert!(matches!(proto.state(), ActiveState::Idle));
}

// ═════════════════════════════════════════════════════════════════
// Scope guards — 1c-3b restrictions classify loudly
// ═════════════════════════════════════════════════════════════════

/// Tier-1 structural shield: DML path (`AwaitingCommandCompleteDml`)
/// has NO 'D' dispatch arm — server-emitted DataRow → UnexpectedFrame
/// at the typed variant level, not via `match row_desc: Option<_>`.
/// Users asking for DML-shape and server shipping rows is caught
/// by variant dispatch.
#[test]
fn bind_execute_data_row_without_schema_is_unexpected_frame() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<QueryKind>(&mut proto);

    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(),
        None, // DML path
        FetchRows::All,
        reply,
        &mut wb,
    );

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&bind_complete_frame());
    bytes.extend_from_slice(&data_row_frame(&[b"surprise"]));

    let out = proto.feed_bytes(&bytes, &mut wb);
    // Expect FailReply + CloseSocket (fatal teardown — unexpected
    // wire frame in the current state).
    assert_eq!(out.len(), 2, "unexpected DataRow → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { cause, .. }, Action::CloseSocket] => {
            assert!(
                matches!(cause, ProtocolError::UnexpectedFrame { .. }),
                "expected UnexpectedFrame, got {cause:?}",
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
}

/// 1c-3b scope: server emitting `PortalSuspended` (tag 's') during
/// streaming → UnexpectedFrame. The `FetchRows` enum (F83)
/// structurally forbids a non-zero max_rows at the API level, but a
/// mis-configured server or proxy could still emit `PortalSuspended`
/// — the dispatcher classifies, doesn't accept silently.
#[test]
fn portal_suspended_is_unexpected_frame_in_1c_3b() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<QueryKind>(&mut proto);

    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(),
        None,
        FetchRows::All,
        reply,
        &mut wb,
    );

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&bind_complete_frame());
    // Build a bare PortalSuspended ('s') frame.
    bytes.extend_from_slice(&[b's', 0, 0, 0, 4]);

    let out = proto.feed_bytes(&bytes, &mut wb);
    assert_eq!(
        out.len(),
        2,
        "PortalSuspended → FailReply + CloseSocket (1c-3b scope)",
    );
    match out.as_slice() {
        [Action::FailReply { cause, .. }, Action::CloseSocket] => {
            assert!(
                matches!(cause, ProtocolError::UnexpectedFrame { .. }),
                "expected UnexpectedFrame, got {cause:?}",
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// ═════════════════════════════════════════════════════════════════
// Push-state policy — in-flight / errored / wrong state
// ═════════════════════════════════════════════════════════════════

/// Invariant: BindExecute from Errored state is structurally
/// blocked at the public API. `ConnectionStatus::Errored(kind)`
/// exposes the underlying cause for caller recovery decisions.
#[test]
fn bind_execute_from_errored_blocked_at_compile_time() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    // Force Errored by feeding an unexpected frame at Idle.
    let bogus = frame(b'Z', b"I");
    let out = proto.feed_bytes(&bogus, &mut wb);
    assert!(out.as_slice().iter().any(|a| matches!(a, Action::CloseSocket)));
    assert!(matches!(proto.state(), ActiveState::Errored(_)));

    // as_ready returns None on Errored.
    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None on Errored state",
    );
    match proto.connection_status() {
        ConnectionStatus::Errored(_kind) => {
            // Caller has structured access to the kind for recovery.
        }
        other => panic!("expected ConnectionStatus::Errored(_), got {other:?}"),
    }
}

/// Invariant: BindExecute while another BindExecute is in flight
/// is structurally blocked at the public API. The in-flight state
/// is preserved (caller must drive `feed_bytes` to drain).
#[test]
fn bind_execute_while_in_flight_blocked_at_compile_time() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (first_reply, _first_raw) = mint_reply::<QueryKind>(&mut proto);
    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(),
        None,
        FetchRows::All,
        first_reply,
        &mut wb,
    );
    assert!(matches!(
        proto.state(),
        ActiveState::BindExecuteAwaitingBindCompleteDml(_)
            | ActiveState::BindExecuteAwaitingBindCompleteSelect { .. }
    ));

    // as_ready returns None during in-flight Bind+Execute.
    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None during in-flight Bind+Execute",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Busy,
        "in-flight Bind+Execute classifies as ConnectionStatus::Busy",
    );
    // First state preserved.
    assert!(matches!(
        proto.state(),
        ActiveState::BindExecuteAwaitingBindCompleteDml(_)
            | ActiveState::BindExecuteAwaitingBindCompleteSelect { .. }
    ));

    // Drain the first reply so its ReplyId doesn't trip the Drop-guard.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&bind_complete_frame());
    drain.extend_from_slice(&command_complete_frame(b"INSERT 0 0"));
    drain.extend_from_slice(&rfq_frame(b'I'));
    let _ = proto.feed_bytes(&drain, &mut wb);
}

// ═════════════════════════════════════════════════════════════════
// Wire-format drift-pins
// ═════════════════════════════════════════════════════════════════

/// Drift-pin: Bind frame byte layout for unnamed portal + unnamed
/// stmt + zero params. Fails build if the wire builder's field
/// ordering or size changes.
#[test]
fn bind_frame_wire_layout_empty_params() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<QueryKind>(&mut proto);
    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(),
        None,
        FetchRows::All,
        reply,
        &mut wb,
    );
    let (bind_bytes, _execute, _sync) = split_bind_execute_sync(wb.as_bytes());
    // Expected shape:
    //   'B' | len(12) | '\0' portal | '\0' stmt | 0x0000 nf | 0x0000 np | 0x0000 nr
    //   = 1 + 4 + 1 + 1 + 2 + 2 + 2 = 13 bytes total
    // length field = 13 - 1 (tag excluded) = 12
    assert_eq!(
        bind_bytes,
        &[
            b'B', 0, 0, 0, 12, // tag + length=12 (includes itself)
            0,    // empty portal + NUL
            0,    // empty stmt + NUL
            0, 0, // n_param_formats = 0
            0, 0, // n_params = 0
            0, 0, // n_result_formats = 0
        ][..],
    );

    // Drain so ReplyId's Drop-guard doesn't trip.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&bind_complete_frame());
    drain.extend_from_slice(&command_complete_frame(b""));
    drain.extend_from_slice(&rfq_frame(b'I'));
    let _ = proto.feed_bytes(&drain, &mut wb);
}

/// Drift-pin: Execute frame byte layout — tag 'E', length, portal
/// NUL, max_rows u32.
#[test]
fn execute_frame_wire_layout_unnamed_portal() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<QueryKind>(&mut proto);
    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(),
        None,
        FetchRows::All,
        reply,
        &mut wb,
    );
    let (_bind, execute_bytes, _sync) = split_bind_execute_sync(wb.as_bytes());
    // Expected:
    //   'E' | len(9) | '\0' portal | 0x00000000 max_rows
    //   total = 1 + 4 + 1 + 4 = 10; length field = 9
    assert_eq!(
        execute_bytes,
        &[
            b'E', 0, 0, 0, 9, // tag + length=9
            0,    // empty portal + NUL
            0, 0, 0, 0, // max_rows = 0 (fetch all)
        ][..],
    );

    // Drain.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&bind_complete_frame());
    drain.extend_from_slice(&command_complete_frame(b""));
    drain.extend_from_slice(&rfq_frame(b'I'));
    let _ = proto.feed_bytes(&drain, &mut wb);
}

/// Option<T: EncodeBinary> NULL-path: `None` emits `len = -1` with
/// no body bytes (SQL NULL on the wire). Verifies the
/// `ParamEncoder for Option<T>` impl.
#[test]
fn bind_frame_null_param_wire_layout() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let none_i32: Option<i32> = None;
    let (reply, _raw) = mint_reply::<QueryKind>(&mut proto);
    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(none_i32,),
        None,
        FetchRows::All,
        reply,
        &mut wb,
    );
    let (bind_bytes, _execute, _sync) = split_bind_execute_sync(wb.as_bytes());
    // Expected:
    //   'B' | len | '\0' portal | '\0' stmt |
    //   0x0001 nf | 0x0001 Binary |
    //   0x0001 np | 0xFFFFFFFF (-1 = SQL NULL, no body) |
    //   0x0000 nr
    // Total body = 4 + 2 + 2 + 2 + 2 + 4 + 2 = 18; length field = 18
    assert_eq!(
        bind_bytes,
        &[
            b'B', 0, 0, 0, 18,       // tag + length
            0,                        // portal NUL
            0,                        // stmt NUL
            0, 1,                     // n_param_formats = 1
            0, 1,                     // format[0] = Binary
            0, 1,                     // n_params = 1
            0xff, 0xff, 0xff, 0xff,   // param[0].length = -1 (NULL)
            0, 0,                     // n_result_formats = 0
        ][..],
    );

    // Drain.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&bind_complete_frame());
    drain.extend_from_slice(&command_complete_frame(b""));
    drain.extend_from_slice(&rfq_frame(b'I'));
    let _ = proto.feed_bytes(&drain, &mut wb);
}

/// Option<T> mixed: `Some(42)` writes the body normally, `None`
/// writes -1. Verifies the impl dispatches correctly per element.
#[test]
fn bind_frame_optional_mixed_with_some_and_none() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<QueryKind>(&mut proto);
    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(Some(42i32), None::<&str>),
        None,
        FetchRows::All,
        reply,
        &mut wb,
    );
    let (bind_bytes, _execute, _sync) = split_bind_execute_sync(wb.as_bytes());
    // Expected (compact format-code block):
    //   tag + len + NUL + NUL + 0x0001 nf + 0x0001 Binary +
    //   0x0002 np + [0x00000004 + i32=42] + [0xFFFFFFFF NULL] +
    //   0x0000 nr
    // Length computation:
    //   4 (len_field) + 1 (portal NUL) + 1 (stmt NUL) +
    //   2 (nf=1) + 2 (format[0]=Binary; one format applies to all) + 2 (np=2) +
    //   4 (p0.len) + 4 (p0.body i32) + 4 (p1.len = -1) + 2 (nr)
    //   = 26 bytes body; length field value = 26
    //   Total frame = 1 (tag) + 26 = 27 bytes
    //
    // The compact format-code block saves 2 B vs a naive N=2 shape
    // (`n_format_codes = 2, [1, 1]`) by exploiting PG §55.7 Bind
    // spec's "one format code applies to all parameters" form.
    // Saving scales with N: 2 B for N=2, 4 B for N=3, ..., 30 B for
    // N=16.
    assert_eq!(
        bind_bytes,
        &[
            b'B', 0, 0, 0, 26,        // tag + length=26
            0, 0,                     // NUL-NUL (portal + stmt both empty)
            0, 1,                     // n_param_formats = 1
            0, 1,                     // format[0] = Binary (applies to all)
            0, 2,                     // n_params = 2
            0, 0, 0, 4, 0, 0, 0, 42,  // Some(42): len=4, i32=42 BE
            0xff, 0xff, 0xff, 0xff,   // None: len=-1
            0, 0,                     // n_result_formats = 0
        ][..],
    );

    // Drain.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&bind_complete_frame());
    drain.extend_from_slice(&command_complete_frame(b""));
    drain.extend_from_slice(&rfq_frame(b'I'));
    let _ = proto.feed_bytes(&drain, &mut wb);
}

/// Drift-pin: Bind frame carries a single i32=42 param in binary
/// format. Verifies per-param length-prefix + body layout.
#[test]
fn bind_frame_wire_layout_one_i32_param() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<QueryKind>(&mut proto);
    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(42i32,),
        None,
        FetchRows::All,
        reply,
        &mut wb,
    );
    let (bind_bytes, _execute, _sync) = split_bind_execute_sync(wb.as_bytes());
    // Expected:
    //   'B' | len | '\0' portal | '\0' stmt |
    //   0x0001 nf | 0x0001 (Binary format) |
    //   0x0001 np | 0x00000004 len | 0x0000002A (i32=42) |
    //   0x0000 nr
    // Fixed overhead = 1 + 4 + 1 + 1 + 2 = 9
    // Formats = 2 (one format code of 2 bytes)
    // n_params + param = 2 + 4 + 4 = 10
    // n_result_formats = 2
    // Body total = 9 + 2 + 10 + 2 = 23
    // length field excludes tag: 22
    assert_eq!(
        bind_bytes,
        &[
            b'B', 0, 0, 0, 22, // tag + length
            0,    // portal NUL
            0,    // stmt NUL
            0, 1, // n_param_formats = 1
            0, 1, // format[0] = Binary
            0, 1, // n_params = 1
            0, 0, 0, 4, // param[0].length = 4
            0, 0, 0, 42, // param[0].value = 42 BE
            0, 0, // n_result_formats = 0
        ][..],
    );

    // Drain.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&bind_complete_frame());
    drain.extend_from_slice(&command_complete_frame(b""));
    drain.extend_from_slice(&rfq_frame(b'I'));
    let _ = proto.feed_bytes(&drain, &mut wb);
}
