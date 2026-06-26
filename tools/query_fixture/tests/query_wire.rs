//! `query!` const wire-artifact: the Parse template, Bind prefix, OID
//! lists, and content-addressed statement name are derived from the
//! inferred query shape at COMPILE TIME and validated through the
//! proto-owned `run` boundary.
//!
//! Each `query!` below is typed and its wire artifact baked against the
//! catalog that `build.rs` -> `bsql-build` replays from `migrations/`,
//! with NO live server. The artifact is the validated `PreparedQuery`
//! const `<Name>Query::PREPARED`; these tests pin its bytes exactly, so
//! any non-determinism or layout drift in the macro's emission is a
//! failing assertion.

use bsql_postgres_proto::oids;

// One `$1` parameter (the `int8` PK) and two projected columns
// (`int8` PK + NOT NULL `text`). Exercises the param-OID section of the
// Parse template and a text-bearing row.
bsql_query_macros::query!(UserById, "SELECT id, email FROM users WHERE id = $1");

// No parameters; two `int8` columns. Exercises the zero-param Parse tail
// (n_param_types = 0, no OID words).
bsql_query_macros::query!(OrderKey, "SELECT id, user_id FROM orders");

#[test]
fn parse_template_is_deterministic_one_param() {
    // Rebuild the documented Parse-frame layout from the artifact's own
    // SQL + statement name + the known param OID, and assert the macro
    // baked exactly those bytes — pinning the const wire bytes as
    // deterministic: `b'P' | len_i32_be | stmt_name | NUL | sql | NUL |
    // n_params_i16_be | oid_i32_be × n`.
    let q = UserByIdQuery::PREPARED;
    let sql = q.sql();
    let stmt = q.stmt_name();
    let length = (4 + stmt.len() + 1 + sql.len() + 1 + 2 + 4) as u32;
    let mut expected = vec![b'P'];
    expected.extend_from_slice(&length.to_be_bytes());
    expected.extend_from_slice(stmt.as_bytes());
    expected.push(0);
    expected.extend_from_slice(sql.as_bytes());
    expected.push(0);
    expected.extend_from_slice(&1u16.to_be_bytes());
    expected.extend_from_slice(&oids::INT8.to_be_bytes());
    assert_eq!(
        q.parse_template_for_test(),
        expected.as_slice(),
        "Parse template bytes must match the documented layout exactly",
    );
}

#[test]
fn parse_template_is_deterministic_zero_params() {
    let q = OrderKeyQuery::PREPARED;
    let sql = q.sql();
    let stmt = q.stmt_name();
    let length = (4 + stmt.len() + 1 + sql.len() + 1 + 2) as u32;
    let mut expected = vec![b'P'];
    expected.extend_from_slice(&length.to_be_bytes());
    expected.extend_from_slice(stmt.as_bytes());
    expected.push(0);
    expected.extend_from_slice(sql.as_bytes());
    expected.push(0);
    expected.extend_from_slice(&0u16.to_be_bytes());
    assert_eq!(q.parse_template_for_test(), expected.as_slice());
    // Zero-param tail: the last two bytes are n_param_types = 0, with no
    // trailing OID words.
    let template = q.parse_template_for_test();
    assert_eq!(&template[template.len() - 2..], &[0x00, 0x00]);
}

#[test]
fn param_and_row_oids_track_the_inferred_types() {
    let q = UserByIdQuery::PREPARED;
    // `$1` binds the `int8` PK; the projection is `int8` + `text`.
    assert_eq!(q.param_oids(), &[oids::INT8]);
    assert_eq!(q.row_oids(), &[oids::INT8, oids::TEXT]);

    let q0 = OrderKeyQuery::PREPARED;
    assert!(q0.param_oids().is_empty());
    assert_eq!(q0.row_oids(), &[oids::INT8, oids::INT8]);
}

#[test]
fn stmt_name_is_content_addressed_and_baked() {
    let q = UserByIdQuery::PREPARED;
    let stmt_name = q.stmt_name();
    assert!(
        stmt_name.starts_with("bsql_q_"),
        "statement name must carry the content-address prefix",
    );
    // 7-char prefix + 24 hex chars (96-bit SHA-256 truncation).
    assert_eq!(stmt_name.len(), 7 + 24);
    let template = q.parse_template_for_test();
    let needle = stmt_name.as_bytes();
    assert!(
        template.windows(needle.len()).any(|w| w == needle),
        "the content-addressed stmt_name must appear in the Parse template",
    );
    // Two distinct queries content-address to distinct names.
    assert_ne!(UserByIdQuery::PREPARED.stmt_name(), OrderKeyQuery::PREPARED.stmt_name());
}

#[test]
fn bind_prefix_is_portal_and_stmt_name_only() {
    let q = UserByIdQuery::PREPARED;
    let stmt_name = q.stmt_name();
    let mut expected = vec![0u8]; // empty portal NUL
    expected.extend_from_slice(stmt_name.as_bytes());
    expected.push(0u8); // stmt_name NUL
    assert_eq!(q.bind_execute_prefix_for_test(), expected.as_slice());
}

// ── the carrier is a zero-size, value-less marker ───────────────────────
//
// Mirrors the `wire_pin!` the macro emits in every expansion. The wire
// data lives in `.rodata`; the carrier itself is 0 bytes.
const _: () = {
    use core::mem::{align_of, size_of};
    assert!(size_of::<UserByIdQuery>() == 0);
    assert!(align_of::<UserByIdQuery>() == 1);
    assert!(size_of::<OrderKeyQuery>() == 0);
};
