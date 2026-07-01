//! Seal probe — `PreparedQuery::new(...)` does not exist as a public
//! constructor. The struct has zero public constructors; the only path to
//! mint a value is the validating `new_prepared_query`, which the
//! compile-checked `query!` macro routes through. A defensive `pub fn new`
//! added for ergonomics would re-open the SQL-injection class, so this probe
//! pins the absence with `error[E0599]`.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::PreparedQuery;

fn main() {
    let _hostile: PreparedQuery<(), ()> = PreparedQuery::new(
        "DROP TABLE users; --",
        "x",
        &[],
        &[],
        &[],
        &[],
    );
}
