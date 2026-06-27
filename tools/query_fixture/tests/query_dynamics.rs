//! `query!` DYNAMIC forms, end-to-end against the migration-replayed
//! catalog (no live server):
//!
//!   * `OPTIONAL(col = $N)` toggled filter — the parameter becomes
//!     `Option<T>`, the SQL gains a `($N IS NULL OR ...)` term, and the
//!     baked param OID stays the column's scalar OID.
//!   * `col = ANY($N)` in-list — the parameter becomes a SINGLE array
//!     (`&[T]`) and the baked param OID is the element's array OID.
//!   * runtime `ORDER BY { a ASC | b DESC | ... }` allow-set — the macro
//!     emits one prepared query per ordering and a closed selector enum;
//!     each ordering bakes a distinct content-addressed statement.
//!
//! These pin the lowered wire artifacts the macro emits; any drift in the
//! lowering or the OID baking is a failing assertion.

use bsql_postgres_proto::oids;

// Toggled optional filter: `id` is `int8`, so `$1` types from it and
// becomes `Option<i64>`. The baked param OID is the scalar `INT8`.
bsql_query_macros::query!(
    OptUser,
    "SELECT id, email FROM users WHERE OPTIONAL(id = $1)"
);

// `= ANY($1)` in-list: `id` is `int8`, so the single array parameter is
// `&[i64]` and the baked param OID is `INT8_ARRAY`.
bsql_query_macros::query!(AnyOrders, "SELECT id FROM orders WHERE id = ANY($1)");

// `= ANY($1)` over a text column: the array parameter is `&[&str]` and the
// param OID is `TEXT_ARRAY`.
bsql_query_macros::query!(
    AnyEmails,
    "SELECT id FROM users WHERE email = ANY($1)"
);

// Runtime ORDER BY allow-set: three orderings over `orders`, one bound
// scalar parameter (`user_id` is `int8`). The macro emits the selector
// enum `SortedOrdersOrderBy { IdAsc, TotalDesc, IdDesc }`.
bsql_query_macros::query!(
    SortedOrders,
    "SELECT id, total FROM orders WHERE user_id = $1 ORDER BY { id ASC | total DESC | id DESC }"
);

#[test]
fn optional_filter_param_is_scalar_oid_and_sql_expands() {
    let q = OptUserQuery::PREPARED;
    assert_eq!(
        q.param_oids(),
        &[oids::INT8],
        "a toggled Option<i64> keeps the column's scalar OID on the wire"
    );
    assert!(
        q.sql().contains("$1 IS NULL OR id = $1"),
        "the lowered SQL carries the `($1 IS NULL OR ...)` toggle term, got: {}",
        q.sql()
    );
}

#[test]
fn optional_filter_decodes_like_a_plain_row() {
    // The toggled filter does not change the projected row shape; the
    // record decodes a normal `DataRow`.
    let row: &[u8] = &[
        0x00, 0x02, // 2 columns
        0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 9, // id = 9
        0x00, 0x00, 0x00, 0x03, b'a', b'@', b'b', // email = "a@b"
    ];
    let decoded = OptUser::decode(row).expect("decode");
    assert_eq!(decoded.id, 9);
    assert_eq!(decoded.email, "a@b");
}

#[test]
fn any_in_list_param_is_array_oid() {
    let q = AnyOrdersQuery::PREPARED;
    assert_eq!(
        q.param_oids(),
        &[oids::INT8_ARRAY],
        "a `= ANY($1)` in-list over int8 sends one int8[] array param"
    );
    assert!(
        q.sql().contains("id = ANY($1)"),
        "the wire SQL keeps the `= ANY($1)` form, got: {}",
        q.sql()
    );
}

#[test]
fn any_in_list_over_text_is_text_array() {
    let q = AnyEmailsQuery::PREPARED;
    assert_eq!(q.param_oids(), &[oids::TEXT_ARRAY]);
}

#[test]
fn order_by_allow_set_emits_distinct_baked_statements() {
    // Each ordering is its own content-addressed prepared statement.
    let asc = SortedOrdersOrderBy::IdAsc.prepared();
    let total_desc = SortedOrdersOrderBy::TotalDesc.prepared();
    let id_desc = SortedOrdersOrderBy::IdDesc.prepared();

    assert!(asc.sql().ends_with("ORDER BY id ASC"), "got: {}", asc.sql());
    assert!(
        total_desc.sql().ends_with("ORDER BY total DESC"),
        "got: {}",
        total_desc.sql()
    );
    assert!(id_desc.sql().ends_with("ORDER BY id DESC"));

    // Distinct SQL -> distinct content-addressed statement names.
    assert_ne!(asc.stmt_name(), total_desc.stmt_name());
    assert_ne!(asc.stmt_name(), id_desc.stmt_name());
    assert_ne!(total_desc.stmt_name(), id_desc.stmt_name());

    // Every ordering shares one parameter shape: the bound `user_id`.
    assert_eq!(asc.param_oids(), &[oids::INT8]);
    assert_eq!(total_desc.param_oids(), &[oids::INT8]);

    // And one row shape (id int8, total int4).
    assert_eq!(asc.row_oids(), &[oids::INT8, oids::INT4]);
}

#[test]
fn order_by_selector_is_a_closed_set() {
    // The selector enum has exactly the declared variants; an ordering
    // outside the set is unrepresentable (it is not a variant).
    let all = [
        SortedOrdersOrderBy::IdAsc,
        SortedOrdersOrderBy::TotalDesc,
        SortedOrdersOrderBy::IdDesc,
    ];
    assert_eq!(all.len(), 3);
    // Each variant maps to a prepared query whose SQL sorts as declared.
    for choice in all {
        let prepared = choice.prepared();
        assert!(prepared.sql().contains("ORDER BY"));
    }
}
