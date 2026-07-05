// EXPECT: E0308 — a splice site takes a `SafeIdent`, not a `&str`. Passing a raw
// (unvalidated) identifier where the injection-safe newtype is required is a type
// error: "forgot to validate before splicing" cannot compile. This is the wall
// every internal SQL-splice helper (`listen_sql`, `unlisten_sql`, `copy_out_sql`,
// `copy_in_sql`) relies on.
use bsql_postgres_core::SafeIdent;

// Stands for the crate-internal splice helpers, each of which takes a
// `SafeIdent` / `SafeTable` (never a `&str`), so a new identifier-splicing verb
// physically cannot assemble SQL from an unvalidated name.
fn splice_into_sql(_channel: SafeIdent<'_>) -> String {
    String::new()
}

fn main() {
    // A raw &str is NOT a `SafeIdent`: no validator ran, so the type wall rejects
    // it. There is no `From<&str>` / coercion — the only door is `validate`.
    let _ = splice_into_sql("events; DROP TABLE users --");
}
