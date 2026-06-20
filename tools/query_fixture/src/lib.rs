//! Fixture exercising the compile-checked query chain.
//!
//! Each `schema_check!` below is validated AT COMPILE TIME against the
//! catalog that `build.rs` -> `bsql-build` replays from `migrations/`. If
//! any referenced `table.column` did not exist in the migration-replayed
//! schema, this crate would fail to compile. That this crate builds at
//! all is the end-to-end proof that the chain works for a real consumer.

/// Valid references — these columns exist in the replayed schema, so the
/// macro expands to `()` and the consts compile.
pub const USERS_ID_OK: () = bsql_query_macros::schema_check!(users.id);
/// `users.email` exists (added NOT NULL in 0001).
pub const USERS_EMAIL_OK: () = bsql_query_macros::schema_check!(users.email);
/// `users.name` exists (added by ALTER TABLE in 0002) — proves ALTER ADD
/// COLUMN reached the catalog.
pub const USERS_NAME_OK: () = bsql_query_macros::schema_check!(users.name);
/// `orders.status` exists (added by ALTER TABLE in 0003).
pub const ORDERS_STATUS_OK: () = bsql_query_macros::schema_check!(orders.status);

/// `accounts.id` exists ONLY under the post-RENAME name: 0004 creates
/// `legacy_accounts`, 0005 renames it to `accounts`. That this compiles
/// proves `RENAME TO` re-keyed the catalog to the new name. The companion
/// compile-fail golden proves the OLD name (`legacy_accounts`) no longer
/// resolves.
pub const ACCOUNTS_ID_OK: () = bsql_query_macros::schema_check!(accounts.id);
/// `accounts.balance` survived the rename with its column intact.
pub const ACCOUNTS_BALANCE_OK: () = bsql_query_macros::schema_check!(accounts.balance);

/// `audit.event` lives in a migration nested under `migrations/regional/`.
/// That this resolves proves the build helper RECURSES into
/// subdirectories. The table is written `Audit` / `Event` (unquoted), so
/// it resolves under the lowercase-folded `audit.event` — proving the
/// unquoted-identifier case folding end-to-end.
pub const AUDIT_EVENT_OK: () = bsql_query_macros::schema_check!(audit.event);

#[cfg(test)]
mod tests {
    #[test]
    fn valid_column_references_compiled() {
        // Reaching this test at all means every `schema_check!` above
        // expanded successfully — i.e. the catalog was read and the
        // columns validated. Name the consts so they are not dead; each
        // is the unit value `()`, so asserting equality keeps them live
        // without a unit-valued let-binding.
        assert_eq!(super::USERS_ID_OK, ());
        assert_eq!(super::USERS_EMAIL_OK, ());
        assert_eq!(super::USERS_NAME_OK, ());
        assert_eq!(super::ORDERS_STATUS_OK, ());
        assert_eq!(super::ACCOUNTS_ID_OK, ());
        assert_eq!(super::ACCOUNTS_BALANCE_OK, ());
        assert_eq!(super::AUDIT_EVENT_OK, ());
    }
}
