// The sanctioned construction path: the explicit `::new` seam compiles
// from an external crate. This is the leg proving the macro / Fragment
// builder CAN construct a wire-bound command (it routes through exactly
// this). It also documents the honest tier: `::new` is `pub` and
// hand-callable (tier-3-by-discipline), but it is the single explicit
// raw-SQL entry point — struct-literal construction is sealed (see
// `struct_literal_sealed.rs`).

#![allow(dead_code)]

use bsql_postgres_proto::push_command::{Parse, SimpleQuery};
use bsql_postgres_proto::reply_id::{ParseKind, QueryKind, ReplyId};
use bsql_postgres_proto::StmtName;

fn simple_query(reply: ReplyId<QueryKind>) -> SimpleQuery<'static> {
    SimpleQuery::new("SELECT 1", reply)
}

fn parse(stmt_name: StmtName, reply: ReplyId<ParseKind>) -> Parse<'static> {
    Parse::new(stmt_name, "SELECT 1", reply)
}

fn main() {}
