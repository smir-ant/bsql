#![forbid(unsafe_code)]
//! A TYPED bind whose parameter tuple arity disagrees with the SQL's `?N`
//! placeholders is a LOUD classified error, not a silent under-bind.
//!
//! The macro-generated `query!` carriers always agree by construction, but a
//! HAND-WRITTEN [`SqliteTypedQuery`] carrier could declare a `Params` tuple
//! shorter than its `SQL`'s placeholders — SQLite would then leave the unbound
//! `?N` as `NULL` and run silently. The typed verbs guard against this with
//! `ensure_param_count`, mirroring the dynamic path (which already errors on a
//! bind-count mismatch).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test harness — unwrap/expect surface failures loudly"
)]

use bsql_sqlite::{ColumnSource, Connection, SqliteError, SqliteTypedQuery, read_required};

/// A hand-written carrier whose `Params` tuple (`COUNT == 1`) disagrees with its
/// SQL's TWO `?N` placeholders — the exact shape the macro never emits.
struct UnderBound;

impl SqliteTypedQuery for UnderBound {
    type Params<'p> = (i64,);
    type Record<'q> = i64;
    type Owned = i64;
    const SQL: &'static str = "SELECT ?1 + ?2";

    fn decode_row<'q, S: ColumnSource<'q>>(src: &S) -> Result<i64, SqliteError> {
        read_required(src, 0)
    }
    fn decode_row_owned<'a, S: ColumnSource<'a>>(src: &S) -> Result<i64, SqliteError> {
        read_required(src, 0)
    }
}

#[test]
fn under_bound_typed_carrier_is_a_classified_error() {
    let conn = Connection::open_in_memory().expect("open");
    // The SQL binds two placeholders; the tuple binds one. Loud, not silent NULL.
    let result = conn.query::<UnderBound>((5,));
    assert!(
        matches!(
            result,
            Err(SqliteError::ParameterCountMismatch { expected: 2, bound: 1 })
        ),
        "expected ParameterCountMismatch {{ expected: 2, bound: 1 }}",
    );
}
