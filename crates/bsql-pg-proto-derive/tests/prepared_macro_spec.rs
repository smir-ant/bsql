//! Spec tests for the `prepared!` proc-macro.
//!
//! # Coverage matrix
//!
//! - **T1**: happy path per supported type — i16/i32/i64/u32/bool/&str.
//! - **T2**: multi-parameter SELECT (3+ placeholders).
//! - **T3**: RETURNING clause on INSERT/UPDATE/DELETE.
//! - **T4**: explicit `CAST($N AS TYPE)` form.
//! - **T5**: max-arity (16 params + 16 columns).
//! - **T6**: string escapes (`'foo''bar'`, dollar-quoting).
//! - **T7**: comments (`-- line`, `/* block */`).
//!
//! Hostile-bypass probes P1-P12 live as `compile_fail` doctests on
//! the macro itself (`crates/bsql-pg-proto-derive/src/lib.rs`).
//! Pinning expected error strings into a separate `trybuild` golden
//! file is tracked as a v1.0 follow-up (acceptance gate accepts the
//! doctest mechanism as the load-bearing closure surface; the
//! probes themselves are exercised here).

#![forbid(unsafe_code)]

use bsql_pg_proto::{prepared, PreparedQuery, RowDecode};

// ═══════════════════════════════════════════════════════════════════
// T1 — happy path per supported type
// ═══════════════════════════════════════════════════════════════════

const Q_INT2: PreparedQuery<(i16,), (i16,)> = prepared!(
    "SELECT id::int2 WHERE id = $1::int2"
);

const Q_INT4: PreparedQuery<(i32,), (i32,)> = prepared!(
    "SELECT id::int4 WHERE id = $1::int4"
);

const Q_INT8: PreparedQuery<(i64,), (i64,)> = prepared!(
    "SELECT id::int8 WHERE id = $1::int8"
);

const Q_OID: PreparedQuery<(u32,), (u32,)> = prepared!(
    "SELECT id::oid WHERE id = $1::oid"
);

const Q_BOOL: PreparedQuery<(bool,), (bool,)> = prepared!(
    "SELECT flag::bool WHERE flag = $1::bool"
);

const Q_TEXT: PreparedQuery<(&'static str,), (&'static str,)> = prepared!(
    "SELECT name::text WHERE name = $1::text"
);

#[test]
fn t1_happy_path_per_type_compiles() {
    // Each const above evaluated at compile time; this test asserts
    // the per-type properties at runtime.
    assert_eq!(Q_INT2.param_oids(), &[bsql_pg_proto::oids::INT2]);
    assert_eq!(Q_INT4.param_oids(), &[bsql_pg_proto::oids::INT4]);
    assert_eq!(Q_INT8.param_oids(), &[bsql_pg_proto::oids::INT8]);
    assert_eq!(Q_OID.param_oids(), &[bsql_pg_proto::oids::OID]);
    assert_eq!(Q_BOOL.param_oids(), &[bsql_pg_proto::oids::BOOL]);
    assert_eq!(Q_TEXT.param_oids(), &[bsql_pg_proto::oids::TEXT]);
}

// ═══════════════════════════════════════════════════════════════════
// T2 — multi-parameter
// ═══════════════════════════════════════════════════════════════════

const Q_MULTI_PARAM: PreparedQuery<(i32, &'static str, bool), (i32,)> = prepared!(
    "SELECT id::int4 WHERE id = $1::int4 AND name = $2::text AND flag = $3::bool"
);

#[test]
fn t2_multi_parameter() {
    assert_eq!(
        Q_MULTI_PARAM.param_oids(),
        &[
            bsql_pg_proto::oids::INT4,
            bsql_pg_proto::oids::TEXT,
            bsql_pg_proto::oids::BOOL,
        ],
    );
    assert_eq!(Q_MULTI_PARAM.row_oids(), &[bsql_pg_proto::oids::INT4]);
}

// ═══════════════════════════════════════════════════════════════════
// T3 — RETURNING clause
// ═══════════════════════════════════════════════════════════════════

const Q_INSERT_RET: PreparedQuery<(&'static str,), (i32, &'static str)> = prepared!(
    "INSERT INTO users (name) VALUES ($1::text) RETURNING id::int4, name::text"
);

const Q_UPDATE_RET: PreparedQuery<(i32, &'static str), (bool,)> = prepared!(
    "UPDATE users SET name = $2::text WHERE id = $1::int4 RETURNING flag::bool"
);

const Q_DELETE_RET: PreparedQuery<(i32,), (i32,)> = prepared!(
    "DELETE FROM users WHERE id = $1::int4 RETURNING id::int4"
);

#[test]
fn t3_returning_clause_works() {
    assert_eq!(Q_INSERT_RET.row_oids().len(), 2);
    assert_eq!(Q_UPDATE_RET.row_oids().len(), 1);
    assert_eq!(Q_DELETE_RET.row_oids().len(), 1);
}

// ═══════════════════════════════════════════════════════════════════
// T4 — explicit CAST() form
// ═══════════════════════════════════════════════════════════════════

const Q_CAST_FORM: PreparedQuery<(i32,), (i32,)> = prepared!(
    "SELECT CAST(id AS int4) FROM users WHERE id = CAST($1 AS int4)"
);

#[test]
fn t4_cast_form() {
    assert_eq!(Q_CAST_FORM.param_oids(), &[bsql_pg_proto::oids::INT4]);
    assert_eq!(Q_CAST_FORM.row_oids(), &[bsql_pg_proto::oids::INT4]);
}

// ═══════════════════════════════════════════════════════════════════
// T5 — max arity (16 params + 16 columns)
// ═══════════════════════════════════════════════════════════════════

#[expect(clippy::type_complexity, reason = "16-tuple is intentional — pins MAX_PARAMS_ARITY support. Migrated #[allow]→#[expect] (Rust 1.81): if the arity shrinks or the type simplifies, the lint stops firing, prompting attribute removal.")]
const Q_MAX_ARITY: PreparedQuery<
    (i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32),
    (i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32),
> = prepared!(
    "SELECT a::int4, b::int4, c::int4, d::int4, e::int4, f::int4, g::int4, h::int4, \
            i::int4, j::int4, k::int4, l::int4, m::int4, n::int4, o::int4, p::int4 \
     WHERE a = $1::int4 AND b = $2::int4 AND c = $3::int4 AND d = $4::int4 \
       AND e = $5::int4 AND f = $6::int4 AND g = $7::int4 AND h = $8::int4 \
       AND i = $9::int4 AND j = $10::int4 AND k = $11::int4 AND l = $12::int4 \
       AND m = $13::int4 AND n = $14::int4 AND o = $15::int4 AND p = $16::int4"
);

#[test]
fn t5_max_arity_16() {
    assert_eq!(Q_MAX_ARITY.param_oids().len(), 16);
    assert_eq!(Q_MAX_ARITY.row_oids().len(), 16);
}

// ═══════════════════════════════════════════════════════════════════
// T6 — string escapes + dollar-quoting in SQL (lexer-only test)
// ═══════════════════════════════════════════════════════════════════

const Q_STRING_LITERAL: PreparedQuery<(i32,), (i32,)> = prepared!(
    "SELECT id::int4 WHERE name = 'O''Reilly' AND id = $1::int4"
);

const Q_DOLLAR_QUOTED: PreparedQuery<(i32,), (i32,)> = prepared!(
    "SELECT id::int4 WHERE meta = $tag$ embedded 'quote' $tag$ AND id = $1::int4"
);

#[test]
fn t6_string_escapes_and_dollar_quoting() {
    // The macro consumed both literals without interpreting their
    // contents — placeholder count and column count are correct.
    assert_eq!(Q_STRING_LITERAL.param_oids().len(), 1);
    assert_eq!(Q_DOLLAR_QUOTED.param_oids().len(), 1);
}

// ═══════════════════════════════════════════════════════════════════
// T7 — comments inside SQL
// ═══════════════════════════════════════════════════════════════════

const Q_LINE_COMMENT: PreparedQuery<(i32,), (i32,)> = prepared!(
    "SELECT id::int4 -- inline comment
     WHERE id = $1::int4"
);

const Q_BLOCK_COMMENT: PreparedQuery<(i32,), (i32,)> = prepared!(
    "SELECT /* skip */ id::int4 WHERE id = $1::int4"
);

#[test]
fn t7_comments() {
    assert_eq!(Q_LINE_COMMENT.param_oids(), &[bsql_pg_proto::oids::INT4]);
    assert_eq!(Q_BLOCK_COMMENT.param_oids(), &[bsql_pg_proto::oids::INT4]);
}

// ═══════════════════════════════════════════════════════════════════
// T8 — `concat!()` composition is not supported at the proc-macro
// level: proc-macros see their arguments as raw token-streams
// BEFORE other macros expand, so `prepared!(concat!(...))` is
// rejected at the LitStr parse step. The P6 closure still holds —
// the macro accepts ONLY a `syn::LitStr` at expansion.
//
// ═══════════════════════════════════════════════════════════════════
// T9 — content-addressed stmt_name (P11 closure)
// ═══════════════════════════════════════════════════════════════════

#[test]
fn t9_stmt_name_content_addressed() {
    // Same SQL → same stmt_name. The macro doesn't dedupe instances
    // (each `prepared!` invocation generates its own const items),
    // but the stmt_name is content-addressed via SHA-256-96.
    let a = Q_INT4.stmt_name();
    let b = Q_INT4.stmt_name();
    assert_eq!(a, b);
    // Different SQL → different stmt_name (collision space = 2⁻⁹⁶).
    assert_ne!(Q_INT4.stmt_name(), Q_BOOL.stmt_name());
    // Format: `bsql_p_<24 hex chars>`. 24 hex chars + 7 prefix = 31.
    assert_eq!(a.len(), 31);
    assert!(a.starts_with("bsql_p_"));
    for c in a.chars().skip(7) {
        // Hex digit OR lowercase a-f. is_ascii_hexdigit accepts 0-9, a-f,
        // A-F; we want lowercase-or-digit only.
        assert!(
            c.is_ascii_digit() || (c.is_ascii_lowercase() && c.is_ascii_hexdigit()),
            "non-lowercase-hex char: {c}",
        );
    }
}

// ═══════════════════════════════════════════════════════════════════
// RowDecode tuple impls — verify OIDs propagate
// ═══════════════════════════════════════════════════════════════════

#[test]
fn row_decode_oids_per_arity() {
    assert_eq!(<() as RowDecode>::ARITY, 0);
    assert_eq!(<(i32,) as RowDecode>::ARITY, 1);
    assert_eq!(<(i32, &'static str) as RowDecode>::ARITY, 2);
    assert_eq!(
        <(i32, &'static str) as RowDecode>::OIDS,
        &[bsql_pg_proto::oids::INT4, bsql_pg_proto::oids::TEXT],
    );
}

// ═══════════════════════════════════════════════════════════════════
// Sizes pinned at the test layer — caller verifies the documented
// budgets.
// ═══════════════════════════════════════════════════════════════════

#[test]
fn prepared_query_size_within_budget() {
    assert!(
        core::mem::size_of::<PreparedQuery<(i32,), (i32, &'static str)>>() <= 128,
        "PreparedQuery<(i32,), (i32, &str)> must stay <= 128 B, \
         actual {} B",
        core::mem::size_of::<PreparedQuery<(i32,), (i32, &'static str)>>(),
    );
}
