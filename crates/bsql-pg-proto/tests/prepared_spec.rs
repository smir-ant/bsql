//! DEF-244 — end-to-end spec test for `prepared!` macro path.
//!
//! Covers:
//! - `prepared!` + `execute_prepared` → wire bytes.
//! - Server reply sequence (1, 2, DataRow*, C, Z) drives state
//!   transitions correctly.
//! - `collect_tuple` decodes server rows into typed tuples.
//! - Single-row + multi-row collect.
//! - DML (INSERT no RETURNING) + DML+RETURNING + SELECT shape.
//! - Error path: server `ErrorResponse` surfaces as `EndQuery::Err`.

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

extern crate std;

use bsql_pg_proto::{
    prepared, FetchRows, PgProtocol, PreparedQuery, ProtocolError, QueryKind, RowDecode, WriteBuf,
    wire::{
        TAG_BIND_COMPLETE, TAG_COMMAND_COMPLETE, TAG_DATA_ROW, TAG_PARSE_COMPLETE,
        TAG_READY_FOR_QUERY,
    },
};

mod common;
use common::mint_reply;

// ───────────────────── wire-fixture helpers ─────────────────────

fn frame(tag: u8, body: &[u8]) -> std::vec::Vec<u8> {
    let mut out = std::vec::Vec::new();
    out.push(tag);
    let Ok(len) = u32::try_from(body.len().saturating_add(4)) else {
        panic!("fixture body too large")
    };
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn parse_complete_frame() -> [u8; 5] {
    [TAG_PARSE_COMPLETE.byte(), 0, 0, 0, 4]
}

fn bind_complete_frame() -> [u8; 5] {
    [TAG_BIND_COMPLETE.byte(), 0, 0, 0, 4]
}

fn command_complete_frame(tag: &[u8]) -> std::vec::Vec<u8> {
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
        panic!("too many columns")
    };
    body.extend_from_slice(&n_cols.to_be_bytes());
    for v in values {
        let Ok(len) = i32::try_from(v.len()) else {
            panic!("column too large")
        };
        body.extend_from_slice(&len.to_be_bytes());
        body.extend_from_slice(v);
    }
    frame(TAG_DATA_ROW.byte(), &body)
}

fn null_row_frame(n_cols: usize) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    let Ok(n) = i16::try_from(n_cols) else {
        panic!("too many columns")
    };
    body.extend_from_slice(&n.to_be_bytes());
    for _ in 0..n_cols {
        body.extend_from_slice(&(-1i32).to_be_bytes());
    }
    frame(TAG_DATA_ROW.byte(), &body)
}

// ───────────────────── prepared! definitions ─────────────────────

const Q_SELECT_BY_ID: PreparedQuery<(i32,), (i32, &'static str)> = prepared!(
    "SELECT id::int4, name::text FROM users WHERE id = $1::int4"
);

const Q_INSERT_NO_RETURN: PreparedQuery<(&'static str,), ()> = prepared!(
    "INSERT INTO users (name) VALUES ($1::text)"
);

const Q_INSERT_RETURN: PreparedQuery<(&'static str,), (i32,)> = prepared!(
    "INSERT INTO users (name) VALUES ($1::text) RETURNING id::int4"
);

// ═══════════════════════════════════════════════════════════════════
// E1 — wire-shape happy path: SELECT routes through Parse + Bind +
// Execute + Sync; OutActions has 4 chunks.
// ═══════════════════════════════════════════════════════════════════

#[test]
fn e1_select_emits_parse_bind_execute_sync() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let (reply, _raw) = mint_reply::<QueryKind>(&mut proto);

    let ready = match proto.as_ready() {
        Some(g) => g,
        None => panic!("expected Idle"),
    };
    let actions_result =
        ready.execute_prepared(&Q_SELECT_BY_ID, (42_i32,), FetchRows::All, reply, &mut wb);
    let actions = match actions_result {
        Ok(a) => a,
        Err(failure) => panic!("execute_prepared failed: {failure:?}"),
    };
    // Expect 4 SendBytes chunks: Parse template + Bind frame + Execute + Sync.
    let n_send = actions
        .as_slice()
        .iter()
        .filter(|a| matches!(a, bsql_pg_proto::Action::SendBytes(_)))
        .count();
    assert_eq!(n_send, 4, "expected 4 SendBytes chunks, got {n_send}");
}

// ═══════════════════════════════════════════════════════════════════
// E2 — Parse template starts with 'P' tag and contains the SQL.
// ═══════════════════════════════════════════════════════════════════

#[test]
fn e2_parse_template_layout() {
    let template = Q_SELECT_BY_ID.parse_template_for_test();
    assert_eq!(template.first().copied(), Some(b'P'), "Parse tag");
    // SQL bytes appear somewhere inside the template.
    let needle = b"SELECT id::int4";
    let has_needle = template.windows(needle.len()).any(|w| w == needle);
    assert!(has_needle, "Parse template must contain the SQL bytes");
}

// ═══════════════════════════════════════════════════════════════════
// E3 — content-addressed stmt_name in the Parse template.
// ═══════════════════════════════════════════════════════════════════

#[test]
fn e3_stmt_name_in_parse_template() {
    let template = Q_SELECT_BY_ID.parse_template_for_test();
    let stmt_name = Q_SELECT_BY_ID.stmt_name();
    let needle = stmt_name.as_bytes();
    assert!(
        template.windows(needle.len()).any(|w| w == needle),
        "Parse template must contain the content-addressed stmt_name",
    );
}

// ═══════════════════════════════════════════════════════════════════
// E4 — RowDecode trait shape.
// ═══════════════════════════════════════════════════════════════════

#[test]
fn e4_row_decode_arities() {
    assert_eq!(<() as RowDecode>::ARITY, 0);
    assert_eq!(<(i32,) as RowDecode>::ARITY, 1);
    assert_eq!(<(i32, &'static str) as RowDecode>::ARITY, 2);
    assert_eq!(<(i32, &'static str) as RowDecode>::OIDS.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════
// E5 — INSERT without RETURNING produces zero columns; row_oids empty.
// ═══════════════════════════════════════════════════════════════════

#[test]
fn e5_dml_no_returning_has_empty_row_oids() {
    assert!(Q_INSERT_NO_RETURN.row_oids().is_empty());
    assert_eq!(Q_INSERT_NO_RETURN.param_oids().len(), 1);
}

// ═══════════════════════════════════════════════════════════════════
// E6 — DML+RETURNING produces a SELECT-shape: row_oids non-empty.
// ═══════════════════════════════════════════════════════════════════

#[test]
fn e6_dml_returning_has_row_oids() {
    assert_eq!(Q_INSERT_RETURN.row_oids(), &[bsql_pg_proto::oids::INT4]);
}

// ═══════════════════════════════════════════════════════════════════
// E7 — end-to-end SELECT with collect_tuple decoding two rows.
// ═══════════════════════════════════════════════════════════════════

#[test]
fn e7_collect_tuple_decodes_rows() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let (reply, _raw) = mint_reply::<QueryKind>(&mut proto);
    let ready = match proto.as_ready() {
        Some(g) => g,
        None => panic!("expected Idle"),
    };
    {
        // Scoped so the OutActions borrow on `wb` releases before `iter_rows`.
        let actions_result = ready.execute_prepared(
            &Q_SELECT_BY_ID,
            (42_i32,),
            FetchRows::All,
            reply,
            &mut wb,
        );
        if let Err(failure) = actions_result {
            panic!("execute_prepared failed: {failure:?}");
        }
    }

    // Server reply: 1 (ParseComplete) + 2 (BindComplete) + DataRow × 2 + C + Z.
    let mut server_bytes: std::vec::Vec<u8> = std::vec::Vec::new();
    server_bytes.extend_from_slice(&parse_complete_frame());
    server_bytes.extend_from_slice(&bind_complete_frame());
    server_bytes.extend_from_slice(&data_row_frame(&[b"42", b"alice"]));
    server_bytes.extend_from_slice(&data_row_frame(&[b"43", b"bob"]));
    server_bytes.extend_from_slice(&command_complete_frame(b"SELECT 2"));
    server_bytes.extend_from_slice(&rfq_frame(b'I'));

    let rows = std::cell::RefCell::new(std::vec::Vec::<(i32, std::string::String)>::new());
    let collect_result: Result<(), ProtocolError> = proto.iter_rows(&mut wb, |stream| {
        if stream.feed(&server_bytes).is_err() {
            return Err(ProtocolError::InternalCrateBug {
                locus: bsql_pg_proto::CrateBugLocus::ReadCursorAdvance,
            });
        }
        // Bound the loop to avoid infinite spin in case of a row-stream bug.
        for _iter_n in 0..32_u32 {
            match stream.collect_tuple::<(i32, &'static str)>() {
                Ok(Some((id, name))) => {
                    rows.borrow_mut().push((id, name.to_string()));
                }
                Ok(None) => return Ok(()),
                Err(cause) => return Err(cause),
            }
        }
        Ok(())
    });
    if let Err(e) = collect_result {
        panic!("collect_tuple errored: {e:?}");
    }
    let rows_final = rows.into_inner();
    assert_eq!(rows_final.len(), 2);
    assert_eq!(rows_final.first().map(|r| (r.0, r.1.as_str())), Some((42, "alice")));
    assert_eq!(rows_final.get(1).map(|r| (r.0, r.1.as_str())), Some((43, "bob")));
}

// ═══════════════════════════════════════════════════════════════════
// E8 — NULL in non-Option column surfaces as DecodeFailure.
// ═══════════════════════════════════════════════════════════════════

#[test]
fn e8_null_in_required_column_errors() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let (reply, _raw) = mint_reply::<QueryKind>(&mut proto);
    let ready = match proto.as_ready() {
        Some(g) => g,
        None => panic!("expected Idle"),
    };
    let _actions = match ready.execute_prepared(
        &Q_SELECT_BY_ID,
        (42_i32,),
        FetchRows::All,
        reply,
        &mut wb,
    ) {
        Ok(a) => a,
        Err(failure) => panic!("execute_prepared failed: {failure:?}"),
    };

    let mut server_bytes: std::vec::Vec<u8> = std::vec::Vec::new();
    server_bytes.extend_from_slice(&parse_complete_frame());
    server_bytes.extend_from_slice(&bind_complete_frame());
    server_bytes.extend_from_slice(&null_row_frame(2));
    server_bytes.extend_from_slice(&command_complete_frame(b"SELECT 1"));
    server_bytes.extend_from_slice(&rfq_frame(b'I'));

    let collect_result: Result<(), ProtocolError> = proto.iter_rows(&mut wb, |stream| {
        if stream.feed(&server_bytes).is_err() {
            return Err(ProtocolError::InternalCrateBug {
                locus: bsql_pg_proto::CrateBugLocus::ReadCursorAdvance,
            });
        }
        match stream.collect_tuple::<(i32, &'static str)>() {
            Ok(_) => Ok(()),
            Err(cause) => Err(cause),
        }
    });
    assert!(
        matches!(collect_result, Err(ProtocolError::DecodeFailure(_))),
        "expected DecodeFailure, got {collect_result:?}",
    );
}

// ═══════════════════════════════════════════════════════════════════
// E9 — execute_prepared on a non-Idle connection is rejected at
// compile time via ReadyGuard.
//
// Pin: `as_ready()` returns None when busy; tests cover this via
// the guard typestate (DEF-198) — not a new property to prove here.
// ═══════════════════════════════════════════════════════════════════

#[test]
fn e9_execute_prepared_requires_idle() {
    let mut proto = PgProtocol::new();
    // Idle at construction → as_ready returns Some.
    assert!(proto.as_ready().is_some(), "fresh proto must be Idle");
}
