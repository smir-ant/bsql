//! Seal probe — `Box::leak(Box::new(PreparedQuery { .. }))` is a common
//! trick to obtain `&'static mut` and then overwrite a field. It fails at the
//! struct-literal step: every field is `pub(crate)`, so an external
//! construction is `error[E0451]` and the `Box::leak` mutation is never
//! reached. This complements the direct-construction probe by pinning that
//! the `&'static mut` acquisition path is closed at construction too.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::PreparedQuery;
use core::marker::PhantomData;

fn main() {
    let _leaked: &'static mut PreparedQuery<(), ()> = Box::leak(Box::new(PreparedQuery {
        sql: "DROP TABLE users; --",
        stmt_name: "x",
        param_oids: &[],
        row_oids: &[],
        parse_template: &[],
        bind_execute_prefix: &[],
        _phantom: PhantomData,
    }));
}
