// SOLE-PATH SEAM PROBE — external struct-literal construction of a
// text-bearing PushCommand must be a compile error.
//
// `SimpleQuery::sql` / `Parse::sql` are `pub(crate)`, so a bare struct
// literal from this (external) crate fails with E0451. The `reply` /
// `stmt_name` values arrive as function parameters, so the ONLY error is
// the private `sql` field — no correlator construction is needed to
// demonstrate the seam. The functions are never called; rustc type-checks
// their bodies regardless, which is where E0451 fires.

#![allow(dead_code)]

use bsql_postgres_proto::push_command::{Parse, SimpleQuery};
use bsql_postgres_proto::reply_id::{ParseKind, QueryKind, ReplyId};
use bsql_postgres_proto::StmtName;

fn simple_query(reply: ReplyId<QueryKind>) {
    let _q = SimpleQuery { sql: "SELECT 1", reply };
}

fn parse(stmt_name: StmtName, reply: ReplyId<ParseKind>) {
    let _p = Parse { stmt_name, sql: "SELECT 1", reply };
}

fn main() {}
