// `legacy_accounts` was created in 0004 then renamed to `accounts` in
// 0005 via `ALTER TABLE ... RENAME TO`. The old name must no longer
// resolve — a `query!` against it is a compile_error, proving RENAME TO
// re-keyed the catalog (the old name was removed).
fn main() {
    bsql_query_macros::query!(Row, "SELECT id FROM legacy_accounts");
}
