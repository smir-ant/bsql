//! Extended Query `Describe` command end-to-end.
//!
//! Covers both target shapes (statement / portal):
//!
//! - Statement-describe happy path:
//!   `D 'S' name + Sync` out, `ParameterDescription + RowDescription
//!   + RFQ` in → `Reply::DescribeStatementComplete` delivered.
//! - Statement-describe no-data path: `'t' + 'n' + 'Z'` →
//!   `DescribedRows::NoData` in the payload.
//! - Portal-describe happy path: `D 'P' name + Sync` out,
//!   `RowDescription + RFQ` in → `Reply::DescribePortalComplete`.
//! - Portal-describe no-data path: `'n' + 'Z'`.
//!
//! Bad paths (all recoverable via `DrainRfqAfterError` unless stated):
//!
//! - `'E'` at any describe stage → FailReply + drain + Idle.
//! - `'T'` (or `'n'`) before `'t'` in statement-describe →
//!   UnexpectedFrame → tear-down.
//! - `'t'` in portal-describe flow → UnexpectedFrame.
//! - Malformed `ParameterDescription` (count × 4 ≠ body len, negative
//!   count, count > `MAX_PARAMS_ARITY`) → tear-down (framing-level).
//! - Malformed RFQ payload → tear-down.
//!
//! Push-state policy:
//!
//! - `Describe{Statement,Portal}` in Idle → 2 SendBytes (Describe + Sync).
//! - `Describe` while another command in flight → `CommandInProgress`.
//! - `Describe` on Errored → `ConnectionAlreadyClosed { prior_kind }`.
//! - Other commands while Describe in flight → `CommandInProgress`,
//!   prior state preserved.
//!
//! Wire-format drift-pins:
//!
//! - Statement frame byte layout: `'D' | len | 'S' | name | NUL`.
//! - Portal frame byte layout: `'D' | len | 'P' | name | NUL`.
//! - `DescribeTargetByte::{Statement,Portal}.byte() == {b'S', b'P'}`.

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
    Action, ActiveState, ConnectionStatus, DescribePortalKind, DescribeStatementKind, DescribedRows, FetchRows,
    PgProtocol, PortalName, ProtocolError, Reply, ReplyId, StmtName,
    TxStatus, WriteBuf,
    wire::{
        DescribeTargetByte, TAG_BIND_COMPLETE, TAG_DATA_ROW, TAG_DESCRIBE, TAG_ERROR_RESPONSE,
        TAG_NO_DATA, TAG_PARAMETER_DESCRIPTION, TAG_PARSE_COMPLETE, TAG_READY_FOR_QUERY,
        TAG_ROW_DESCRIPTION,
    },
};

mod common;
use common::{
    PushOrPanic, fresh_active_via_trust_handshake, mint_reply, split_bind_execute_sync,
    split_frame_plus_sync,
};

fn stmt_unnamed() -> StmtName {
    StmtName::default()
}

fn portal_unnamed() -> PortalName {
    PortalName::default()
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

/// Build a `ParameterDescription` (`'t'`) frame carrying `oids`.
fn parameter_description_frame(oids: &[u32]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    let Ok(n) = i16::try_from(oids.len()) else {
        panic!("too many param oids for fixture");
    };
    body.extend_from_slice(&n.to_be_bytes());
    for oid in oids {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    frame(TAG_PARAMETER_DESCRIPTION.byte(), &body)
}

/// Build a minimal `RowDescription` (`'T'`) frame for `n` columns.
/// Body: i16 count + per-column (NUL name + i32 tbl_oid + i16 attnum +
/// i32 type_oid + i16 type_size + i32 type_mod + i16 format).
fn row_description_frame(n_columns: u16) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.extend_from_slice(&n_columns.to_be_bytes());
    for i in 0..n_columns {
        body.extend_from_slice(b"c");
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes()); // table_oid
        body.extend_from_slice(&i.to_be_bytes()); // attr_num
        body.extend_from_slice(&23i32.to_be_bytes()); // int4 oid
        body.extend_from_slice(&4i16.to_be_bytes()); // type_size
        body.extend_from_slice(&(-1i32).to_be_bytes()); // type_mod
        body.extend_from_slice(&1i16.to_be_bytes()); // binary format
    }
    frame(TAG_ROW_DESCRIPTION.byte(), &body)
}

fn no_data_frame() -> [u8; 5] {
    [TAG_NO_DATA.byte(), 0, 0, 0, 4]
}

fn rfq_frame(tx_byte: u8) -> [u8; 6] {
    [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, tx_byte]
}

fn error_response_frame(message: &[u8]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR");
    body.push(0);
    body.push(b'C');
    body.extend_from_slice(b"26000"); // invalid_sql_statement_name
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message);
    body.push(0);
    body.push(0); // terminator
    frame(TAG_ERROR_RESPONSE.byte(), &body)
}

/// Push a statement describe + verify wire layout in `wb.as_bytes()`.
/// Returns the D-frame bytes for further wire-layout inspection.
///
/// `push_command` returns `Result<(), PushFailure>`; bytes live in
/// `wb`. This helper thin-wraps `common::split_frame_plus_sync` with
/// the additional `'D'` tag check (PG §55.2.2 — Describe message).
#[track_caller]
fn describe_stmt_setup(
    proto: &mut PgProtocol,
    stmt_name: StmtName,
    reply: ReplyId<DescribeStatementKind>,
    wb: &mut WriteBuf,
) -> std::vec::Vec<u8> {
    proto.push_or_panic(
        bsql_pg_proto::push_command::DescribeStatement { stmt_name, reply },
        wb,
    );
    let (d_frame, _sync) = split_frame_plus_sync(wb.as_bytes());
    assert_eq!(
        d_frame.first(),
        Some(&TAG_DESCRIBE.byte()),
        "Describe-statement head must start with the 'D' tag",
    );
    d_frame.to_vec()
}

/// Push a portal describe + verify wire layout.
#[track_caller]
fn describe_portal_setup(
    proto: &mut PgProtocol,
    portal_name: PortalName,
    reply: ReplyId<DescribePortalKind>,
    wb: &mut WriteBuf,
) -> std::vec::Vec<u8> {
    proto.push_or_panic(
        bsql_pg_proto::push_command::DescribePortal { portal_name, reply },
        wb,
    );
    let (d_frame, _sync) = split_frame_plus_sync(wb.as_bytes());
    assert_eq!(
        d_frame.first(),
        Some(&TAG_DESCRIBE.byte()),
        "Describe-portal head must start with the 'D' tag",
    );
    d_frame.to_vec()
}

// ═════════════════════════════════════════════════════════════════
// (A) Statement-describe — spec conformance happy paths
// ═════════════════════════════════════════════════════════════════

/// Invariant (spec): statement-describe with 2 params + 3-column
/// result delivers `Reply::DescribeStatementComplete` carrying both
/// `param_oids` and `rows: DescribedRows::Rows(..)` on the terminal RFQ.
#[test]
fn describe_statement_with_rows_success_end_to_end() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    assert!(matches!(
        proto.state(),
        ActiveState::DescribeStatementAwaitingParamDesc(_),
    ));

    let mut bytes = std::vec::Vec::new();
    // 2 parameters (int4, text).
    bytes.extend_from_slice(&parameter_description_frame(&[23, 25]));
    // Row description with 3 columns.
    bytes.extend_from_slice(&row_description_frame(3));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    assert_eq!(out.len(), 1, "only terminal DeliverReply emitted");
    match out.as_slice() {
        [Action::DeliverReply {
            id: delivered_id,
            value: Reply::DescribeStatementComplete(_),
        }] => {
            assert_eq!(*delivered_id, reply_raw, "correlator round-trips");
        }
        other => panic!("expected DeliverReply(DescribeStatementComplete), got {other:?}"),
    }
    drop(out);
    // DEF-286 Φ-F*: payload fields externalised; query via accessors.
    let Some(param_oids) = proto.current_param_oids() else { panic!("param_oids slot populated"); };
    assert_eq!(param_oids.len(), 2);
    assert_eq!(param_oids.oids(), &[23, 25]);
    match proto.current_described_rows() {
        DescribedRows::Rows(desc) => {
            assert_eq!(desc.len(), 3, "expected 3 columns, got {}", desc.len());
        }
        DescribedRows::NoData => panic!("expected Rows(..), got NoData"),
    }
    // DEF-286 Φ-E: tx_status accessor instead of inline field.
    assert_eq!(proto.terminal_tx_status(), TxStatus::Idle);
    assert!(matches!(proto.state(), ActiveState::Idle));
}

/// Invariant (spec): statement-describe on a DML-without-RETURNING
/// statement receives `NoData` (`'n'`) instead of RowDescription;
/// payload carries `DescribedRows::NoData`.
#[test]
fn describe_statement_no_data_success_end_to_end() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&parameter_description_frame(&[23])); // int4
    bytes.extend_from_slice(&no_data_frame());
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    match out.as_slice() {
        [Action::DeliverReply {
            id: delivered_id,
            value: Reply::DescribeStatementComplete(_),
        }] => {
            assert_eq!(*delivered_id, reply_raw);
        }
        other => panic!("expected DescribeStatementComplete (NoData), got {other:?}"),
    }
    drop(out);
    let Some(param_oids) = proto.current_param_oids() else { panic!("param_oids slot populated"); };
    assert_eq!(param_oids.oids(), &[23]);
    assert!(matches!(proto.current_described_rows(), DescribedRows::NoData));
    assert!(matches!(proto.state(), ActiveState::Idle));
}

/// Invariant (spec): zero-parameter statement describes cleanly —
/// the `'t'` frame ships an empty OID list; payload has
/// `param_oids.is_empty() == true`.
#[test]
fn describe_statement_zero_params_ok() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&parameter_description_frame(&[]));
    bytes.extend_from_slice(&row_description_frame(1));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    match out.as_slice() {
        [Action::DeliverReply {
            value: Reply::DescribeStatementComplete(_),
            ..
        }] => {}
        other => panic!("expected DescribeStatementComplete, got {other:?}"),
    }
    drop(out);
    let Some(param_oids) = proto.current_param_oids() else { panic!("param_oids slot populated"); };
    assert!(param_oids.is_empty(), "zero-param statement");
    assert_eq!(param_oids.len(), 0);
    assert!(matches!(proto.current_described_rows(), DescribedRows::Rows(_)));
}

/// Invariant (spec): `MAX_PARAMS_ARITY` parameters parses cleanly.
/// Pins the upper-bound behaviour — the exact capacity boundary is
/// accepted, not rejected as too-many.
#[test]
fn describe_statement_max_params_ok() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    // 16 distinct OIDs — one per placeholder.
    let oids: std::vec::Vec<u32> = (1..=16u32).collect();
    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&parameter_description_frame(&oids));
    bytes.extend_from_slice(&no_data_frame());
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    match out.as_slice() {
        [Action::DeliverReply {
            value: Reply::DescribeStatementComplete(_),
            ..
        }] => {}
        other => panic!("expected DescribeStatementComplete, got {other:?}"),
    }
    drop(out);
    let Some(param_oids) = proto.current_param_oids() else { panic!("param_oids slot populated"); };
    assert_eq!(param_oids.len(), 16);
    assert_eq!(param_oids.oids(), oids.as_slice());
}

/// Invariant (spec): named statement — pin the on-wire layout.
/// `D | len | 'S' | name | NUL`.
#[test]
fn describe_statement_frame_wire_format_with_named_statement() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    let Ok(name) = StmtName::try_from_str("my_stmt") else {
        panic!("fixture: valid stmt name");
    };
    let d_bytes = describe_stmt_setup(&mut proto, name, reply, &mut wb);

    // Layout:
    //   byte 0: 'D'
    //   bytes 1..=4: BE u32 length = 4 + 1 + 7(name) + 1 = 13
    //   byte 5: 'S' (statement target)
    //   bytes 6..=12: "my_stmt"
    //   byte 13: NUL
    let expected_len_field = 4u32 + 1 + 7 + 1;
    assert_eq!(d_bytes.first(), Some(&TAG_DESCRIBE.byte()));
    assert_eq!(
        d_bytes.get(1..5),
        Some(&expected_len_field.to_be_bytes()[..]),
    );
    assert_eq!(d_bytes.get(5), Some(&b'S'));
    assert_eq!(d_bytes.get(6..13), Some(&b"my_stmt"[..]));
    assert_eq!(d_bytes.get(13), Some(&0u8));

    // Drain so the ReplyId doesn't trip the Drop-guard.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&parameter_description_frame(&[]));
    drain.extend_from_slice(&no_data_frame());
    drain.extend_from_slice(&rfq_frame(b'I'));
    let drain_out = proto.feed_bytes(&drain, &mut wb);
    assert!(matches!(drain_out.as_slice(), [Action::DeliverReply { .. }]));
}

// ═════════════════════════════════════════════════════════════════
// (B) Portal-describe — spec conformance happy paths
// ═════════════════════════════════════════════════════════════════

/// Invariant (spec): portal-describe does NOT receive a
/// ParameterDescription; `'T'` arrives directly and payload has
/// no `param_oids` field (type-level — can't be asked).
#[test]
fn describe_portal_with_rows_success_end_to_end() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, reply_raw) = mint_reply::<DescribePortalKind>(&mut proto);
    describe_portal_setup(&mut proto, portal_unnamed(), reply, &mut wb);

    assert!(matches!(
        proto.state(),
        ActiveState::DescribePortalAwaitingRowDescOrNoData(_),
    ));

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(2));
    bytes.extend_from_slice(&rfq_frame(b'T'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    match out.as_slice() {
        [Action::DeliverReply {
            id: delivered_id,
            value: Reply::DescribePortalComplete(_),
        }] => {
            assert_eq!(*delivered_id, reply_raw);
        }
        other => panic!("expected DescribePortalComplete, got {other:?}"),
    }
    drop(out);
    match proto.current_described_rows() {
        DescribedRows::Rows(desc) => {
            assert_eq!(desc.len(), 2);
        }
        DescribedRows::NoData => panic!("expected Rows(..)"),
    }
    // DEF-286 Φ-E: tx_status accessor instead of inline field.
    assert_eq!(proto.terminal_tx_status(), TxStatus::InTransaction);
    assert!(matches!(proto.state(), ActiveState::Idle));
}

/// Invariant (spec): portal-describe NoData branch.
#[test]
fn describe_portal_no_data_success_end_to_end() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribePortalKind>(&mut proto);
    describe_portal_setup(&mut proto, portal_unnamed(), reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&no_data_frame());
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    match out.as_slice() {
        [Action::DeliverReply {
            value: Reply::DescribePortalComplete(_),
            ..
        }] => {}
        other => panic!("expected DescribePortalComplete (NoData), got {other:?}"),
    }
    drop(out);
    assert!(matches!(proto.current_described_rows(), DescribedRows::NoData));
}

/// Invariant (spec): named portal — pin the on-wire layout.
/// `D | len | 'P' | name | NUL`.
#[test]
fn describe_portal_frame_wire_format_with_named_portal() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribePortalKind>(&mut proto);
    let Ok(name) = PortalName::try_from_str("my_portal") else {
        panic!("fixture: valid portal name");
    };
    let d_bytes = describe_portal_setup(&mut proto, name, reply, &mut wb);

    let expected_len_field = 4u32 + 1 + 9 + 1;
    assert_eq!(d_bytes.first(), Some(&TAG_DESCRIBE.byte()));
    assert_eq!(
        d_bytes.get(1..5),
        Some(&expected_len_field.to_be_bytes()[..]),
    );
    assert_eq!(d_bytes.get(5), Some(&b'P'));
    assert_eq!(d_bytes.get(6..15), Some(&b"my_portal"[..]));
    assert_eq!(d_bytes.get(15), Some(&0u8));

    // Drain.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&no_data_frame());
    drain.extend_from_slice(&rfq_frame(b'I'));
    let drain_out = proto.feed_bytes(&drain, &mut wb);
    assert!(matches!(drain_out.as_slice(), [Action::DeliverReply { .. }]));
}

// ═════════════════════════════════════════════════════════════════
// (C) Recoverable errors — server ErrorResponse at each stage
// ═════════════════════════════════════════════════════════════════

/// Invariant (spec): `'E'` before `'t'` in statement-describe (e.g.,
/// unknown statement name) → FailReply + drain + Idle.
#[test]
fn describe_statement_error_at_param_desc_is_recoverable() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&error_response_frame(b"prepared statement \"foo\" does not exist"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    let actions = out.as_slice();
    assert_eq!(actions.len(), 1, "E emits FailReply; Z drained silently");
    match actions.first() {
        Some(Action::FailReply { id: failed_id }) => {
            assert_eq!(*failed_id, reply_raw);
        }
        other => panic!("expected FailReply, got {other:?}"),
    }
    for a in actions {
        assert!(
            !matches!(a, Action::CloseSocket),
            "describe-error must not close socket: {a:?}",
        );
    }
    drop(out);
    // DEF-286 Φ-I.b: query cause via slot.
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated post-FailReply"); };
    assert!(
        matches!(cause, ProtocolError::ServerErrorResponse { .. }),
        "expected ServerErrorResponse, got {cause:?}",
    );
    assert!(
        matches!(proto.state(), ActiveState::Idle),
        "state returns to Idle after drain; got {:?}", proto.state(),
    );
}

/// Invariant (spec): `'E'` after `'t'` but before `'T'`/`'n'` →
/// FailReply + drain + Idle. Late error classification is
/// indistinguishable from early for recoverable-vs-fatal purposes.
#[test]
fn describe_statement_error_at_row_desc_stage_is_recoverable() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&parameter_description_frame(&[23]));
    bytes.extend_from_slice(&error_response_frame(b"unexpected server error"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    match out.as_slice() {
        [Action::FailReply { id: failed_id, .. }] => {
            assert_eq!(*failed_id, reply_raw);
        }
        other => panic!("expected single FailReply, got {other:?}"),
    }
    assert!(matches!(proto.state(), ActiveState::Idle));
}

/// Invariant (spec): portal-describe `'E'` → FailReply + drain + Idle.
#[test]
fn describe_portal_error_response_is_recoverable() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, reply_raw) = mint_reply::<DescribePortalKind>(&mut proto);
    describe_portal_setup(&mut proto, portal_unnamed(), reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&error_response_frame(b"portal \"bar\" does not exist"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    match out.as_slice() {
        [Action::FailReply { id: failed_id, .. }] => {
            assert_eq!(*failed_id, reply_raw);
        }
        other => panic!("expected FailReply, got {other:?}"),
    }
    assert!(matches!(proto.state(), ActiveState::Idle));
}

// ═════════════════════════════════════════════════════════════════
// (D) Wire-desync — unexpected frames tear the connection down
// ═════════════════════════════════════════════════════════════════

/// Invariant (spec): `'T'` arriving in
/// `DescribeStatementAwaitingParamDesc` is an UnexpectedFrame.
/// The server must emit `'t'` before any row-desc/no-data per
/// PG §55.2.2; a jump to `'T'` means the server violated the
/// sequence.
#[test]
fn describe_statement_row_desc_before_param_desc_tears_down() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    let out = proto.feed_bytes(&row_description_frame(1), &mut wb);
    let actions = out.as_slice();
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::FailReply { .. },
        )),
        "expected FailReply(UnexpectedFrame), got {actions:?}",
    );
    assert!(actions.iter().any(|a| matches!(a, Action::CloseSocket)));
    assert!(matches!(proto.state(), ActiveState::Errored(_)));
}

/// Invariant (spec): `'n'` arriving in
/// `DescribeStatementAwaitingParamDesc` is also UnexpectedFrame —
/// statement-describe MUST emit `'t'` first.
#[test]
fn describe_statement_no_data_before_param_desc_tears_down() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    let out = proto.feed_bytes(&no_data_frame(), &mut wb);
    let actions = out.as_slice();
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::FailReply { .. },
        )),
        "expected FailReply(UnexpectedFrame), got {actions:?}",
    );
    assert!(actions.iter().any(|a| matches!(a, Action::CloseSocket)));
}

/// Invariant (spec): portal-describe never expects a `'t'` frame
/// (portals are bound, parameters were fixed at Bind time per PG
/// §55.2.2). A `'t'` arrival is UnexpectedFrame → tear-down.
#[test]
fn describe_portal_param_desc_is_unexpected_and_tears_down() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribePortalKind>(&mut proto);
    describe_portal_setup(&mut proto, portal_unnamed(), reply, &mut wb);

    let out = proto.feed_bytes(&parameter_description_frame(&[23]), &mut wb);
    let actions = out.as_slice();
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::FailReply { .. },
        )),
        "expected FailReply(UnexpectedFrame), got {actions:?}",
    );
    assert!(actions.iter().any(|a| matches!(a, Action::CloseSocket)));
}

/// Invariant (spec): `'D'` (DataRow) during describe flow is
/// UnexpectedFrame — describe never streams rows (PG sends
/// RowDescription which DECLARES columns, not actual rows).
#[test]
fn describe_statement_data_row_tears_down() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    // A pretend DataRow frame — body has i16 column count + column data,
    // but the tag is what matters for the dispatcher.
    let fake_data_row = frame(TAG_DATA_ROW.byte(), &[0, 0]);
    let out = proto.feed_bytes(&fake_data_row, &mut wb);
    let actions = out.as_slice();
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::FailReply { .. },
    )));
    assert!(actions.iter().any(|a| matches!(a, Action::CloseSocket)));
}

// ═════════════════════════════════════════════════════════════════
// (E) Malformed wire payloads — framing-level errors
// ═════════════════════════════════════════════════════════════════

/// Invariant (spec): `'t'` body with `count × 4 ≠ body_len` classifies
/// as `MalformedParameterDescription`, not as a silent parse.
#[test]
fn describe_statement_malformed_param_desc_tears_down() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    // Body: count=2 but only 4 bytes of OID data (one OID) — length
    // mismatch.
    let mut body = std::vec::Vec::new();
    body.extend_from_slice(&2i16.to_be_bytes());
    body.extend_from_slice(&23u32.to_be_bytes());
    let bad = frame(TAG_PARAMETER_DESCRIPTION.byte(), &body);

    let out = proto.feed_bytes(&bad, &mut wb);
    let actions = out.as_slice();
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::FailReply { .. },
        )),
        "expected MalformedParameterDescription, got {actions:?}",
    );
    assert!(actions.iter().any(|a| matches!(a, Action::CloseSocket)));
}

/// Invariant (spec): `'t'` body with count > `MAX_PARAMS_ARITY` (=16)
/// classifies as `TooManyParameters`. Pins the BindExecute-arity
/// matching cap — receiving more OIDs than we can ever Bind against
/// is a structural rejection.
#[test]
fn describe_statement_too_many_params_tears_down() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    // 17 OIDs — one over the cap.
    let oids: std::vec::Vec<u32> = (1..=17u32).collect();
    let bad = parameter_description_frame(&oids);

    let out = proto.feed_bytes(&bad, &mut wb);
    let actions = out.as_slice();
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::FailReply { .. },
        )),
        "expected TooManyParameters {{ count: 17, max: 16 }}, got {actions:?}",
    );
    assert!(actions.iter().any(|a| matches!(a, Action::CloseSocket)));
}

/// Invariant (spec): malformed RFQ payload (length != 1) in the
/// final describe-stage RFQ classifies as MalformedReadyForQuery.
#[test]
fn describe_statement_malformed_rfq_tears_down() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&parameter_description_frame(&[]));
    bytes.extend_from_slice(&no_data_frame());
    // RFQ with 2 bytes instead of 1 — out of spec.
    bytes.extend_from_slice(&[TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 6, b'I', b'X']);

    let out = proto.feed_bytes(&bytes, &mut wb);
    let actions = out.as_slice();
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::FailReply { .. },
    )));
    assert!(actions.iter().any(|a| matches!(a, Action::CloseSocket)));
}

// ═════════════════════════════════════════════════════════════════
// (F) Push-state policy
// ═════════════════════════════════════════════════════════════════

/// Invariant: DescribeStatement on Errored is structurally
/// blocked at the public API. `ConnectionStatus::Errored(kind)`
/// surfaces the underlying cause for caller recovery.
#[test]
fn describe_statement_on_errored_blocked_at_compile_time() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    // Force Errored via an unsolicited Z in Idle.
    let unsolicited = frame(TAG_READY_FOR_QUERY.byte(), b"I");
    let out = proto.feed_bytes(&unsolicited, &mut wb);
    assert!(out.as_slice().iter().any(|a| matches!(a, Action::CloseSocket)));
    assert!(matches!(proto.state(), ActiveState::Errored(_)));

    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None on Errored",
    );
    match proto.connection_status() {
        ConnectionStatus::Errored(_kind) => {}
        other => panic!("expected ConnectionStatus::Errored(_), got {other:?}"),
    }
}

/// Invariant: DescribePortal on Errored is structurally
/// blocked at the public API.
#[test]
fn describe_portal_on_errored_blocked_at_compile_time() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let unsolicited = frame(TAG_READY_FOR_QUERY.byte(), b"I");
    let out = proto.feed_bytes(&unsolicited, &mut wb);
    assert!(out.as_slice().iter().any(|a| matches!(a, Action::CloseSocket)));

    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None on Errored",
    );
    match proto.connection_status() {
        ConnectionStatus::Errored(_kind) => {}
        other => panic!("expected ConnectionStatus::Errored(_), got {other:?}"),
    }
}

/// Invariant: pushing any other command while Describe is in
/// flight is structurally blocked. The in-flight state is preserved
/// (caller must drive `feed_bytes` to drain).
#[test]
fn parse_while_describe_statement_in_flight_blocked_at_compile_time() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (first_reply, _first_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), first_reply, &mut wb);

    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None during in-flight Describe",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Busy,
        "in-flight Describe classifies as ConnectionStatus::Busy",
    );
    assert!(matches!(
        proto.state(),
        ActiveState::DescribeStatementAwaitingParamDesc(_),
    ));

    // Drain describe so the Drop-guard is happy.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&parameter_description_frame(&[]));
    drain.extend_from_slice(&no_data_frame());
    drain.extend_from_slice(&rfq_frame(b'I'));
    let drain_out = proto.feed_bytes(&drain, &mut wb);
    assert!(matches!(drain_out.as_slice(), [Action::DeliverReply { .. }]));
}

/// Invariant: DescribeStatement while DescribePortal in flight
/// is structurally blocked. Two describe targets are mutually
/// exclusive single-command shapes; pipelining lands in 1c-5.
#[test]
fn describe_statement_while_describe_portal_in_flight_blocked() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (first_reply, _first_raw) = mint_reply::<DescribePortalKind>(&mut proto);
    describe_portal_setup(&mut proto, portal_unnamed(), first_reply, &mut wb);

    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None during in-flight DescribePortal",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Busy,
        "in-flight Describe classifies as ConnectionStatus::Busy",
    );

    // Drain.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&no_data_frame());
    drain.extend_from_slice(&rfq_frame(b'I'));
    let drain_out = proto.feed_bytes(&drain, &mut wb);
    assert!(matches!(drain_out.as_slice(), [Action::DeliverReply { .. }]));
}

/// Invariant: DescribeStatement while BindExecute (DML) in
/// flight is structurally blocked.
#[test]
fn describe_statement_while_bind_execute_in_flight_blocked() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    // Start a BindExecute (DML path, row_desc=None).
    use bsql_pg_proto::QueryKind as QK;
    let (be_reply, be_raw) = mint_reply::<QK>(&mut proto);
    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(),
        None,
        FetchRows::All,
        be_reply,
        &mut wb,
    );
    // Verify `wb` contains B+E+Sync structurally.
    let (_bind, _execute, _sync) = split_bind_execute_sync(wb.as_bytes());

    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None during in-flight Bind+Execute",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Busy,
        "in-flight Bind+Execute classifies as ConnectionStatus::Busy",
    );

    // Drain the BindExecute — and verify the in-flight id round-trips
    // to the terminal DeliverReply correlator. Closing the be_raw use
    // here avoids the `let _ = be_raw;` pattern (user-banned) and
    // produces a stronger assertion than a presence check.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&[TAG_BIND_COMPLETE.byte(), 0, 0, 0, 4]);
    let mut cc_body = b"INSERT 0 1".to_vec();
    cc_body.push(0);
    drain.extend_from_slice(&frame(b'C', &cc_body));
    drain.extend_from_slice(&rfq_frame(b'I'));
    let drain_out = proto.feed_bytes(&drain, &mut wb);
    let delivered = drain_out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            Action::DeliverReply { id, .. } => Some(*id),
            _ => None,
        });
    match delivered {
        Some(id) => assert_eq!(
            id, be_raw,
            "BindExecute drain delivered with wrong correlator",
        ),
        None => panic!("expected DeliverReply in BindExecute drain, got {:?}", drain_out.as_slice()),
    }
}

// ═════════════════════════════════════════════════════════════════
// (G) Drift pins — typed wire bytes + prior-Parse unaffected
// ═════════════════════════════════════════════════════════════════

/// Invariant (drift pin): `DescribeTargetByte::Statement.byte()` is
/// the PG-documented `b'S'`. An arm-body swap in `wire.rs` that
/// flipped `Statement` ↔ `Portal` would compile but fail here.
#[test]
fn describe_target_byte_statement_pins_to_s() {
    assert_eq!(DescribeTargetByte::Statement.byte(), b'S');
}

/// Invariant (drift pin): `DescribeTargetByte::Portal.byte()` is
/// the PG-documented `b'P'`.
#[test]
fn describe_target_byte_portal_pins_to_p() {
    assert_eq!(DescribeTargetByte::Portal.byte(), b'P');
}

/// Invariant (spec): a prior `ParseComplete` for a completed Parse
/// must not leak into an immediately-following Describe's initial
/// state. Pins that Describe's state machine starts fresh from
/// `Idle` even after another command ended cleanly.
#[test]
fn describe_after_completed_parse_starts_clean() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    // Run a Parse to completion.
    use bsql_pg_proto::ParseKind;
    let parse_reply = proto.next_reply_id::<ParseKind>();
    proto.push_or_panic(
        bsql_pg_proto::push_command::Parse {
            stmt_name: stmt_unnamed(),
            sql: "SELECT 1",
            reply: parse_reply,
        },
        &mut wb,
    );
    // Verify Parse+Sync layout.
    let (_p_frame, _sync) = split_frame_plus_sync(wb.as_bytes());

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&[TAG_PARSE_COMPLETE.byte(), 0, 0, 0, 4]);
    bytes.extend_from_slice(&rfq_frame(b'I'));
    let out = proto.feed_bytes(&bytes, &mut wb);
    assert!(matches!(out.as_slice(), [Action::DeliverReply { .. }]));
    assert!(matches!(proto.state(), ActiveState::Idle));

    // Now describe — should proceed normally from Idle.
    let (describe_reply, describe_raw) = mint_reply::<DescribeStatementKind>(&mut proto);
    describe_stmt_setup(&mut proto, stmt_unnamed(), describe_reply, &mut wb);
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&parameter_description_frame(&[]));
    drain.extend_from_slice(&no_data_frame());
    drain.extend_from_slice(&rfq_frame(b'I'));
    let drain_out = proto.feed_bytes(&drain, &mut wb);
    match drain_out.as_slice() {
        [Action::DeliverReply {
            id,
            value: Reply::DescribeStatementComplete(_),
        }] => {
            assert_eq!(*id, describe_raw);
        }
        other => panic!("expected DescribeStatementComplete after Parse, got {other:?}"),
    }
}

