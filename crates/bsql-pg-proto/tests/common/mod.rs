//! Shared helpers for DEF-198 ReadyGuard pattern in integration tests.
//!
//! # Pre-DEF-212 (pre-2026-05-04)
//!
//! Pre-DEF-212 tests called `proto.push_command(cmd, wb)` directly,
//! got back `OutActions<'_, '_>`, and inspected the action list.
//!
//! # DEF-212 (Alt Y', architect-vetted impl plan, audit 2026-05-04)
//!
//! Post-DEF-212 push paths return `Result<(), PushFailure>` (~80 B
//! return frame down from 800 B `OutActions`). On `Ok(())` bytes
//! live in the caller's `WriteBuf`; tests verify via `wb.as_bytes()`.
//! On `Err(PushFailure { id, cause })` state has transitioned to
//! `Errored` and the test asserts on `id` + `cause`.
//!
//! Tests that intentionally test the non-Idle branch (e.g.,
//! "pushing while busy returns FailReply") now test
//! `proto.as_ready().is_none()` + `proto.connection_status()`
//! directly — no helper needed for those.

#![allow(dead_code, reason = "shared helper module — not every test uses every helper")]

use bsql_pg_proto::{
    FetchRows, HeaderParse, PgProtocol, PortalName, PushFailure, QueryKind, ReplyId,
    ReplyKind, RowDesc, StmtName, WriteBuf, params::ParamsWriter, parse_header,
    push_command::{BindExecute, PushCommand},
};
use core::num::NonZeroU64;

/// DEF-270 (U letter) — test-friendly mint of a fresh `ReplyId<K>` and
/// its underlying raw `NonZeroU64`. Pre-DEF-270 tests minted via
/// `ReplyId::from_raw(raw_value)`; that constructor is now `pub(crate)`
/// (external fabrication closed at tier-1 by-visibility — see the U
/// letter in deferred.md `DEF-270`).
///
/// Returns `(id, raw)` so the test can:
/// - move `id` into a command's `reply` field, and
/// - retain `raw` for later state-pattern assertions
///   (e.g. `expect_awaiting_ping_reply(state, raw)`).
///
/// The minted value is the protocol's monotonic counter — first call
/// returns 1, second 2, etc. Tests that want SPECIFIC raw values (e.g.
/// for fixture-distinguishability across multiple commands in one
/// scenario) can mint sequentially and capture the actual values.
pub fn mint_reply<K: ReplyKind>(proto: &mut PgProtocol) -> (ReplyId<K>, NonZeroU64) {
    let id = proto.next_reply_id::<K>();
    let raw = id.get();
    (id, raw)
}

/// Extension trait: pre-DEF-198 ergonomics for happy-path tests.
///
/// DEF-269 v2: generic over `C: PushCommand` — tests pass per-command
/// structs (e.g. `Ping { reply }`) directly, no `PgCommand` enum.
///
/// `proto.push_or_panic(cmd, wb)` panics on:
/// - Non-Idle state (caller's test fixture is malformed; the helper
///   exists for happy-path tests, not non-Idle path tests).
/// - `Err(PushFailure)` — happy-path tests expect builder success;
///   if the test wants to assert on a failure, it should call
///   `g.push_command(...)` directly and pattern-match the Result.
///
/// On success, returns `()`. The caller verifies via `wb.as_bytes()`.
pub trait PushOrPanic {
    fn push_or_panic<C: PushCommand>(&mut self, cmd: C, wb: &mut WriteBuf);

    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors push_bind_execute wire-args 1:1"
    )]
    fn push_bind_execute_or_panic<P: ParamsWriter>(
        &mut self,
        portal_name: &PortalName,
        stmt_name: &StmtName,
        params: &P,
        row_desc: Option<RowDesc>,
        fetch: FetchRows,
        reply: ReplyId<QueryKind>,
        wb: &mut WriteBuf,
    );

    /// DEF-212: variant that returns the typed failure for tests that
    /// EXPECT a `PushFailure` (e.g., builder-overflow classification
    /// tests). Panics on non-Idle (same fixture-malformed semantics
    /// as `push_or_panic`).
    fn push_expect_failure<C: PushCommand>(
        &mut self,
        cmd: C,
        wb: &mut WriteBuf,
    ) -> PushFailure;
}

impl PushOrPanic for PgProtocol {
    fn push_or_panic<C: PushCommand>(&mut self, cmd: C, wb: &mut WriteBuf) {
        let status = self.connection_status();
        let Some(g) = self.as_ready() else {
            panic!(
                "test fixture: proto must be Idle for push (status = {status:?})",
            );
        };
        match g.push_command(cmd, wb) {
            Ok(()) => {}
            Err(f) => panic!(
                "test fixture: push_or_panic expected Ok but got {f:?}"
            ),
        }
    }

    fn push_bind_execute_or_panic<P: ParamsWriter>(
        &mut self,
        portal_name: &PortalName,
        stmt_name: &StmtName,
        params: &P,
        row_desc: Option<RowDesc>,
        fetch: FetchRows,
        reply: ReplyId<QueryKind>,
        wb: &mut WriteBuf,
    ) {
        let status = self.connection_status();
        let Some(g) = self.as_ready() else {
            panic!(
                "test fixture: proto must be Idle for push_bind_execute (status = {status:?})",
            );
        };
        match g.push_command(
            BindExecute {
                portal_name,
                stmt_name,
                params,
                row_desc,
                fetch,
                reply,
            },
            wb,
        ) {
            Ok(()) => {}
            Err(f) => panic!(
                "test fixture: push_bind_execute_or_panic expected Ok but got {f:?}"
            ),
        }
    }

    fn push_expect_failure<C: PushCommand>(
        &mut self,
        cmd: C,
        wb: &mut WriteBuf,
    ) -> PushFailure {
        let status = self.connection_status();
        let Some(g) = self.as_ready() else {
            panic!(
                "test fixture: proto must be Idle for push_expect_failure (status = {status:?})",
            );
        };
        match g.push_command(cmd, wb) {
            Ok(()) => panic!(
                "test fixture: push_expect_failure expected Err but got Ok"
            ),
            Err(f) => f,
        }
    }
}

/// DEF-212 (Alt Y'): split `wb.as_bytes()` of a single-frame-plus-Sync
/// push (Parse / Describe / etc.) into the leading frame and the
/// trailing Sync wire bytes.
///
/// Layout: `[frame: tag + length(4) + body] + [Sync (5 B literal:
/// tag 'S' + BE u32 length=4)]`. Assumes the trailing 5 bytes are
/// always the Sync constant — the production path ALWAYS emits a
/// trailing `Sync` post-Parse / post-Describe per PG §55.2.4 + the
/// const-assert chain in `write_buf.rs`.
///
/// Panics if the layout violates the contract (test-fixture failure,
/// not protocol failure).
#[track_caller]
pub fn split_frame_plus_sync(bytes: &[u8]) -> (&[u8], &[u8]) {
    let total_len = bytes.len();
    assert!(
        total_len >= 5,
        "push must emit at least the trailing Sync (5 B); got {total_len} B",
    );
    let split = total_len.saturating_sub(5);
    let Some((frame, sync)) = bytes.split_at_checked(split) else {
        panic!("wb split unreachable post-assert(total_len >= 5): split={split} total={total_len}");
    };
    assert_eq!(
        sync, &[b'S', 0u8, 0u8, 0u8, 4u8],
        "tail must be the PG Sync wire bytes (tag 'S' + BE u32 length=4)",
    );
    (frame, sync)
}

/// DEF-212 (Alt Y'): split `wb.as_bytes()` of a Bind+Execute+Sync push
/// into the three constituent wire frames.
///
/// Layout (PG §55.7 + DEF-094 staged push):
///   `[B frame: tag 'B' + length(4) + body] +
///    [E frame: tag 'E' + length(4) + body] +
///    [Sync wire bytes (5 B literal)]`
///
/// Returns `(bind, execute, sync)` slices into the input. Uses
/// `parse_header` to discover the Bind / Execute frame boundaries —
/// no hardcoded length math. Panics if the layout violates the
/// contract.
#[track_caller]
pub fn split_bind_execute_sync(bytes: &[u8]) -> (&[u8], &[u8], &[u8]) {
    let HeaderParse::Ok { total_len: bind_total, .. } = parse_header(bytes) else {
        panic!("first frame must be a parseable Bind header; got bytes len={}", bytes.len());
    };
    let bind_total_usize = usize::from(bind_total);
    let Some((bind, rest)) = bytes.split_at_checked(bind_total_usize) else {
        panic!(
            "bytes too short to hold Bind frame (declared total_len={bind_total_usize}, available={})",
            bytes.len(),
        );
    };
    let HeaderParse::Ok { total_len: exec_total, .. } = parse_header(rest) else {
        panic!("second frame must be a parseable Execute header; got rest len={}", rest.len());
    };
    let exec_total_usize = usize::from(exec_total);
    let Some((execute, sync)) = rest.split_at_checked(exec_total_usize) else {
        panic!(
            "bytes too short to hold Execute frame (declared total_len={exec_total_usize}, available={})",
            rest.len(),
        );
    };
    assert_eq!(
        sync, &[b'S', 0u8, 0u8, 0u8, 4u8],
        "tail must be the PG Sync wire bytes (tag 'S' + BE u32 length=4)",
    );
    (bind, execute, sync)
}
