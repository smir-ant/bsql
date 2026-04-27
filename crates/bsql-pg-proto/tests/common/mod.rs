//! Shared helpers for DEF-198 ReadyGuard pattern in integration tests.
//!
//! Pre-DEF-198 tests called `proto.push_command(cmd, wb)` directly.
//! Post-DEF-198 the public surface is `ReadyGuard::push_command`,
//! reachable only via `proto.as_ready()`. The [`PushOrPanic`] trait
//! provides a method-style helper that panics on non-Idle state —
//! mirrors the pre-DEF-198 ergonomics for happy-path tests.
//!
//! Tests that intentionally test the non-Idle branch (e.g.,
//! "pushing while busy returns FailReply") now test
//! `proto.as_ready().is_none()` + `proto.connection_status()`
//! directly — no helper needed for those.

#![allow(dead_code, reason = "shared helper module — not every test uses every helper")]

use bsql_pg_proto::{
    FetchRows, OutActions, PgCommand, PgProtocol, PortalName, QueryKind, ReplyId, RowDesc,
    StmtName, WriteBuf, params::ParamsWriter,
};

/// Extension trait: pre-DEF-198 ergonomics for happy-path tests.
///
/// `proto.push_or_panic(cmd, wb)` is equivalent to
/// `proto.as_ready().expect("...").push_command(cmd, wb)` but uses
/// the let-else + panic! idiom (which is allowed in tests but the
/// `expect_used` lint is forbidden across the workspace).
pub trait PushOrPanic {
    fn push_or_panic<'p, 'w>(
        &'p mut self,
        cmd: PgCommand,
        wb: &'w mut WriteBuf,
    ) -> OutActions<'w, 'p>;

    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors push_bind_execute wire-args 1:1"
    )]
    fn push_bind_execute_or_panic<'p, 'w, P: ParamsWriter>(
        &'p mut self,
        portal_name: &PortalName,
        stmt_name: &StmtName,
        params: &P,
        row_desc: Option<RowDesc>,
        fetch: FetchRows,
        reply: ReplyId<QueryKind>,
        wb: &'w mut WriteBuf,
    ) -> OutActions<'w, 'p>;
}

impl PushOrPanic for PgProtocol {
    fn push_or_panic<'p, 'w>(
        &'p mut self,
        cmd: PgCommand,
        wb: &'w mut WriteBuf,
    ) -> OutActions<'w, 'p> {
        let status = self.connection_status();
        let Some(g) = self.as_ready() else {
            panic!(
                "test fixture: proto must be Idle for push (status = {status:?})",
            );
        };
        g.push_command(cmd, wb)
    }

    fn push_bind_execute_or_panic<'p, 'w, P: ParamsWriter>(
        &'p mut self,
        portal_name: &PortalName,
        stmt_name: &StmtName,
        params: &P,
        row_desc: Option<RowDesc>,
        fetch: FetchRows,
        reply: ReplyId<QueryKind>,
        wb: &'w mut WriteBuf,
    ) -> OutActions<'w, 'p> {
        let status = self.connection_status();
        let Some(g) = self.as_ready() else {
            panic!(
                "test fixture: proto must be Idle for push_bind_execute (status = {status:?})",
            );
        };
        g.push_bind_execute(portal_name, stmt_name, params, row_desc, fetch, reply, wb)
    }
}
