//! Fixture exercising the compile-checked query chain WITH the SQLite
//! build-time conformance cross-check enabled.
//!
//! Each `query!` below is typed at compile time against the PostgreSQL
//! catalog (the inference lattice) AND cross-checked against the SQLite
//! template database that `build.rs` replayed from `migrations/`: the macro
//! opens the template under a deny-all-but-readonly authorizer, `prepare`s
//! the query, reads each result column's declared type and base NOT NULL
//! flag, and asserts they agree with the lattice's `(type, nullable)`. That
//! this crate compiles at all is the end-to-end proof that real SQLite
//! conforms to the shared lattice for the portable subset.

// All-portable row: `id` is a NOT NULL `BIGINT` (the PK), `email` is a NOT
// NULL `TEXT`, `name` is a nullable `TEXT`. SQLite agrees on all three
// (the PK is reconciled to NOT NULL despite SQLite's table_info quirk).
bsql_query_macros::query!(UserRow, "SELECT id, email, name FROM users");

// Mixed nullability: `id`/`user_id` NOT NULL `int8`, `total` nullable
// `int4`. SQLite's decltype + table_info agree with the lattice.
bsql_query_macros::query!(OrderRow, "SELECT id, user_id, total FROM orders");

// A dynamic OPTIONAL(...) toggle filter whose enabled form forces a
// full-table scan (the `$1 IS NULL OR ...` shape defeats every index). The
// scan is ACKNOWLEDGED with the recognized marker + a documented return
// plan, so the conformance check accepts it. (Without the marker this is a
// build error — see the trybuild gate.)
bsql_query_macros::query!(
    OptUser,
    "SELECT id, email FROM users WHERE OPTIONAL(name = $1) \
     /* bsql:allow-scan: `users` is a small lookup table, so a full scan \
        when the optional filter is enabled is acceptable; revisit and add \
        an index on `name` if the table grows past a few thousand rows */"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_a_user_row() {
        // DataRow payload: id = 1 (int8), email = "a@b" (text), name = NULL.
        let body: &[u8] = &[
            0x00, 0x03, // 3 columns
            0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 1, // id = 1
            0x00, 0x00, 0x00, 0x03, b'a', b'@', b'b', // email = "a@b"
            0xFF, 0xFF, 0xFF, 0xFF, // name = NULL
        ];
        let row = UserRow::decode(body).expect("decode a conforming user row");
        assert_eq!(row.id, 1);
        assert_eq!(row.email, "a@b");
        assert_eq!(row.name, None);
    }

    #[test]
    fn decodes_an_order_row() {
        // DataRow payload: id = 5, user_id = 9, total = NULL.
        let body: &[u8] = &[
            0x00, 0x03, // 3 columns
            0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 5, // id = 5
            0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 9, // user_id = 9
            0xFF, 0xFF, 0xFF, 0xFF, // total = NULL
        ];
        let row = OrderRowOwned::decode(body).expect("decode an order row");
        assert_eq!(row.id, 5);
        assert_eq!(row.user_id, 9);
        assert_eq!(row.total, None);
    }

    #[test]
    fn acknowledged_toggle_query_prepared_exists() {
        // The acknowledged OPTIONAL(...) query still emits its baked
        // prepared-query artifact.
        let prepared = OptUserQuery::PREPARED;
        assert!(!prepared.param_oids().is_empty());
    }
}
