//! Fixture exercising the compile-checked query chain.
//!
//! Each `query!` below types a SELECT AT COMPILE TIME against the catalog
//! that `build.rs` -> `bsql-build` replays from `migrations/`. If any
//! referenced table or column did not exist in the migration-replayed
//! schema, this crate would fail to compile. That this crate builds at all
//! is the end-to-end proof that the chain works for a real consumer: each
//! query below also stands in for a specific catalog-replay property
//! (`ALTER TABLE ADD COLUMN`, `RENAME TO`, subdirectory recursion,
//! unquoted-identifier case folding) that resolves through the macro.

// Valid references — these columns exist in the replayed schema, so the
// query types and its records compile.
bsql_query_macros::query!(UsersId, "SELECT id FROM users");
bsql_query_macros::query!(UsersEmail, "SELECT email FROM users");
// `users.name` was added NOT NULL by `ALTER TABLE ADD COLUMN` in 0002 —
// resolving it proves ALTER ADD COLUMN reached the catalog.
bsql_query_macros::query!(UsersName, "SELECT name FROM users");
// `orders.status` was added by `ALTER TABLE ADD COLUMN` in 0003.
bsql_query_macros::query!(OrdersStatus, "SELECT status FROM orders");

// `accounts` exists ONLY under the post-RENAME name: 0004 creates
// `legacy_accounts`, 0005 renames it to `accounts`. That these type at all
// proves `RENAME TO` re-keyed the catalog to the new name. The companion
// compile-fail golden (`query_renamed_away_table`) proves the OLD name
// (`legacy_accounts`) no longer resolves.
bsql_query_macros::query!(AccountsId, "SELECT id FROM accounts");
// `accounts.balance` survived the rename with its column intact.
bsql_query_macros::query!(AccountsBalance, "SELECT balance FROM accounts");

// `audit.event` lives in a migration nested under `migrations/regional/`.
// That this resolves proves the build helper RECURSES into subdirectories.
// The table is written `Audit` / `Event` (unquoted), so it resolves under
// the lowercase-folded `audit.event` — proving the unquoted-identifier case
// folding end-to-end.
bsql_query_macros::query!(AuditEvent, "SELECT event FROM audit");
