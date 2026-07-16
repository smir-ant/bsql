#![forbid(unsafe_code)]
//! NaN-parameter bind is a LOUD classified error, not a silent divergence.
//!
//! `sqlite3_bind_double` silently coerces a `NaN` to SQL `NULL` (only `NaN` —
//! `±INF` binds as a normal `REAL`), so a bound `NaN` would vanish and a
//! `WHERE x = ?` match nothing. PostgreSQL round-trips `NaN` bit-identically; on
//! SQLite that parity is unreachable, so the driver rejects a `NaN` bind at the
//! ONE `ValueRef` bind seam (shared by the typed `raw_bind_parameter` path and
//! the dynamic `params_from_iter` path) as [`SqliteError::NanBind`]. `±INF` still
//! binds as `REAL`, so only the genuinely-lossy case is refused.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test harness — unwrap/expect surface failures loudly"
)]

use bsql_sqlite::{Connection, SqliteError, ValueRef};

#[test]
fn nan_bind_is_a_classified_error_dynamic_path() {
    let conn = Connection::open_in_memory().expect("open");
    // The dynamic `*_params` path binds each `ValueRef` through `ToSql` — the
    // same seam the typed tuple path uses — so a NaN is rejected here too.
    let err = conn
        .query_params("SELECT ?1", &[ValueRef::Real(f64::NAN)])
        .expect_err("a NaN bind must fail loudly, not coerce to NULL");
    assert!(
        matches!(err, SqliteError::NanBind),
        "expected NanBind, got {err:?}",
    );
}

#[test]
fn positive_and_negative_infinity_bind_as_real() {
    let conn = Connection::open_in_memory().expect("open");
    // Only NaN nulls; ±INF is a valid REAL and round-trips.
    for f in [f64::INFINITY, f64::NEG_INFINITY] {
        let result = conn
            .query_params("SELECT ?1 AS x", &[ValueRef::Real(f)])
            .expect("±INF binds as REAL");
        let row = result.get(0).expect("one row");
        assert_eq!(row.get::<f64>(0).expect("real column"), f);
    }
}
