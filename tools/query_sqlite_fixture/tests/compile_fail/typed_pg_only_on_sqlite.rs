// A query using `OPTIONAL(...)` dynamic sugar: it types on PostgreSQL and PASSES
// the SQLite conformance oracle (the toggle's full scan is acknowledged), but it
// is NOT a SQLite-decodable query — the toggle is PostgreSQL-runtime sugar with
// no SQLite lowering — so the macro emits NO `SqliteTypedQuery` impl for it.
//
// Executing it on the SQLite driver is therefore a LOCATED compile error at the
// call site (the `SqliteTypedQuery` bound is not satisfied), carrying the
// diagnostic note that names why it is PostgreSQL-only — never a silent runtime
// mis-run of a form SQLite cannot execute. This is the runtime peer of the
// build-time "unknown column is a compile error" guarantee.
bsql::query!(
    OptUser,
    "SELECT id FROM users WHERE OPTIONAL(name = $1) \
     /* bsql:allow-scan: small lookup table; revisit when it grows */"
);

fn main() {
    let conn = bsql::sqlite::Connection::open_in_memory().expect("open");
    // `OptUserQuery` does not implement `SqliteTypedQuery` — this does not compile.
    let _ = conn.query::<OptUserQuery>(());
}
