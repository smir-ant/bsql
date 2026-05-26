//! Shared helpers for the `ReadyGuard` pattern in integration tests.
//!
//! # Push-path return shape
//!
//! Push paths return `Result<OutActions<'_>, PushFailure>`
//! to surface the zero-copy SQL chunk (caller-owned `&str` borrowed
//! via `SendBytesBorrowed`) alongside the header/trailer ranges in
//! `WriteBuf`. Tests still want to assert on the FULL wire frame, so
//! the helpers below drain `OutActions` into a local scratch buffer
//! and rebuild `wb` with the concatenated bytes BEFORE returning —
//! preserving the test invariant `wb.as_bytes() == on-wire frame`.
//! This costs one extra memcpy in tests only; production callers
//! drain `OutActions` directly to socket via `writev` / `IoSlice`.
//!
//! Tests that intentionally test the non-Idle branch (e.g.,
//! "pushing while busy returns FailReply") use
//! `proto.as_ready().is_none()` + `proto.connection_status()`
//! directly — no helper needed for those.

#![allow(dead_code, reason = "shared helper module — not every test uses every helper")]

use bsql_postgres_proto::{
    Action, ActivePhase, Credentials, DisconnectedPhase, FetchRows, HeaderParse, Ident,
    IntoActiveError, OutActions, PgProtocol, PortalName, PushFailure, QueryKind, ReplyId,
    ReplyKind, RowDesc, StartupKind, StmtName, WriteBuf, params::ParamsWriter, parse_header,
    push_command::{BindExecute, PushCommand},
};
use core::num::NonZeroU64;

/// Test-friendly mint of a fresh `ReplyId<K>` and its underlying raw
/// `NonZeroU64`. `ReplyId::from_raw` is `pub(crate)` (external
/// fabrication closed at tier-1 by-visibility); tests mint via the
/// production `proto.next_reply_id::<K>()` API.
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
///
/// Default-phase shape is on `<ActivePhase>`. For
/// `<DisconnectedPhase>` (pre-Startup mint) callers use
/// [`mint_reply_disconnected`] (same body, different phase type).
pub fn mint_reply<K: ReplyKind>(proto: &mut PgProtocol<ActivePhase>) -> (ReplyId<K>, NonZeroU64) {
    let id = proto.next_reply_id::<K>();
    let raw = id.get();
    (id, raw)
}

/// Mint a fresh `ReplyId<K>` on a `<DisconnectedPhase>` protocol
/// (pre-Startup). Mirror of [`mint_reply`] but typed for the
/// disconnect-phase shape.
pub fn mint_reply_disconnected<K: ReplyKind>(
    proto: &mut PgProtocol<DisconnectedPhase>,
) -> (ReplyId<K>, NonZeroU64) {
    let id = proto.next_reply_id::<K>();
    let raw = id.get();
    (id, raw)
}

// ═══════════════════════════════════════════════════════════════════
// Handshake-driver helper
// ═══════════════════════════════════════════════════════════════════
//
// `fresh_active_via_trust_handshake()` drives a fresh
// `PgProtocol<DisconnectedPhase>` through a synthetic Trust-auth
// handshake using ONLY the public API:
//
//   1. PgProtocol::new() → <DisconnectedPhase>
//   2. push_startup(user="testuser", trust)
//      → (OutActions, <ConnectingPhase>)
//   3. feed_inbound + advance_one_frame loop over synthetic wire
//      bytes (AuthOk, ParameterStatus×N, BackendKeyData, RFQ)
//   4. into_active() → <ActivePhase>
//
// Tests that need an Active protocol immediately (most spec-conformance
// tests) call this helper to keep their fixtures terse. Tests that
// observe handshake progression directly drive the public API without
// this helper.

/// Build an AuthenticationOk frame: tag 'R', length 8, sub-code 0.
fn auth_ok_frame() -> [u8; 9] {
    [b'R', 0, 0, 0, 8, 0, 0, 0, 0]
}

/// Build a ParameterStatus frame: tag 'S', key\0value\0.
fn param_status_frame(key: &str, value: &str) -> Vec<u8> {
    let body_len = key.len().saturating_add(1).saturating_add(value.len()).saturating_add(1);
    let declared = u32::try_from(body_len).unwrap_or(0).saturating_add(4);
    let mut frame = Vec::new();
    frame.push(b'S');
    frame.extend_from_slice(&declared.to_be_bytes());
    frame.extend_from_slice(key.as_bytes());
    frame.push(0);
    frame.extend_from_slice(value.as_bytes());
    frame.push(0);
    frame
}

/// Build a BackendKeyData frame: tag 'K', 8-byte payload (pid + secret_key).
fn backend_key_data_frame(pid: i32, secret_key: i32) -> [u8; 13] {
    let pid_bytes = pid.to_be_bytes();
    let key_bytes = secret_key.to_be_bytes();
    [
        b'K', 0, 0, 0, 12,
        pid_bytes[0], pid_bytes[1], pid_bytes[2], pid_bytes[3],
        key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3],
    ]
}

/// Build a ReadyForQuery frame.
fn rfq_frame(tx_status: u8) -> [u8; 6] {
    [b'Z', 0, 0, 0, 5, tx_status]
}

/// Drive a fresh `PgProtocol` through a synthetic Trust-auth
/// handshake to `<ActivePhase>`. Uses ONLY the public API:
///
/// - `PgProtocol::new()` produces `<DisconnectedPhase>`.
/// - `push_startup` consumes it → `<ConnectingPhase>`.
/// - `feed_inbound` + `advance_one_frame` drive the synthetic
///   AuthOk + 2× ParameterStatus + BackendKeyData + RFQ chain.
/// - `into_active` consumes the `<ConnectingPhase>` → `<ActivePhase>`.
///
/// No `_for_test`, no `__test_bypass_*`, no `#[doc(hidden)]` —
/// every step is a publicly-callable method.
///
/// **Panics** on any non-Idle terminal state — the caller is in a
/// happy-path fixture context. Tests that observe failure paths
/// drive the API directly.
#[track_caller]
pub fn fresh_active_via_trust_handshake() -> PgProtocol<ActivePhase> {
    let mut proto = PgProtocol::<DisconnectedPhase>::new();
    let mut wb = WriteBuf::new();
    let user = match Ident::try_from_str("testuser") {
        Ok(u) => u,
        Err(e) => panic!("test fixture: 'testuser' is a valid Ident, got {e}"),
    };
    let (reply, _raw) = mint_reply_disconnected::<StartupKind>(&mut proto);
    let mut proto_connecting = {
        let (_actions, p) = match proto.push_startup(
            user,
            None,
            None,
            Credentials::Trust,
            reply,
            &mut wb,
        ) {
            Ok((a, p)) => (a, p),
            Err(f) => panic!(
                "test fixture: push_startup must succeed for Trust auth, got {:?}",
                f.cause,
            ),
        };
        // `_actions` borrows into `wb`; drop it (block scope) before
        // re-using `wb` for the subsequent handshake-drive calls.
        let _ = _actions;
        p
    };

    // Drive AuthOk → ParameterStatus×N → BackendKeyData → RFQ.
    if let Err(e) = proto_connecting.feed_inbound(&auth_ok_frame()) {
        panic!("test fixture: feed_inbound(AuthOk) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    if let Err(e) = proto_connecting.feed_inbound(&param_status_frame("server_version", "17.2")) {
        panic!("test fixture: feed_inbound(ParameterStatus server_version) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    if let Err(e) = proto_connecting.feed_inbound(&param_status_frame("client_encoding", "UTF8")) {
        panic!("test fixture: feed_inbound(ParameterStatus client_encoding) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    if let Err(e) = proto_connecting.feed_inbound(&backend_key_data_frame(12345, 67890)) {
        panic!("test fixture: feed_inbound(BackendKeyData) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    if let Err(e) = proto_connecting.feed_inbound(&rfq_frame(b'I')) {
        panic!("test fixture: feed_inbound(RFQ) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    match proto_connecting.into_active() {
        Ok(p) => p,
        Err(IntoActiveError::Closed(_)) => panic!(
            "test fixture: trust handshake landed in Closed unexpectedly",
        ),
        Err(IntoActiveError::StillConnecting(_)) => panic!(
            "test fixture: trust handshake landed in StillConnecting unexpectedly",
        ),
    }
}

/// Extension trait: ergonomics for happy-path tests.
///
/// Generic over `C: PushCommand` — tests pass per-command structs
/// (e.g. `Ping { reply }`) directly, no `PgCommand` enum.
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

    /// Variant that returns the typed failure for tests that EXPECT
    /// a `PushFailure` (e.g., builder-overflow classification tests).
    /// Panics on non-Idle (same fixture-malformed semantics as
    /// `push_or_panic`).
    fn push_expect_failure<C: PushCommand>(
        &mut self,
        cmd: C,
        wb: &mut WriteBuf,
    ) -> PushFailure;
}

/// Drain `OutActions` chunks into an owned scratch buffer.
///
/// `OutActions` chunks span BOTH `wb` (header / trailer ranges via
/// `SendBytesRange`) AND caller memory (SQL borrow via
/// `SendBytesBorrowed`) AND static memory (Sync trailer via
/// `SendBytesStatic`). Tests check `wb.as_bytes()` for the full
/// frame; preserving that invariant requires one extra memcpy here
/// (production drains chunks directly via `writev`).
///
/// Consumes `actions` by value so the caller can re-mut-borrow `wb`
/// after the call returns (the `'w` lifetime that flowed into
/// `OutActions` is released on drop of the iterator).
///
/// Panics on unexpected action variants (push paths emit only
/// `SendBytes` per the architecturally-pinned push contract).
#[track_caller]
fn actions_to_scratch(actions: OutActions<'_>) -> std::vec::Vec<u8> {
    let mut scratch: std::vec::Vec<u8> = std::vec::Vec::with_capacity(8192);
    for action in actions {
        match action {
            Action::SendBytes(b) => scratch.extend_from_slice(b),
            Action::DeliverReply { .. } => panic!(
                "test fixture: push paths must NEVER emit DeliverReply",
            ),
            Action::FailReply { .. } => panic!(
                "test fixture: FailReply short-circuits via Result::Err; \
                 reaching the action list indicates a push_command_internal \
                 contract regression",
            ),
            Action::CloseSocket => panic!(
                "test fixture: push paths must NEVER emit CloseSocket on Ok",
            ),
            // `Action` is `#[non_exhaustive]`. A future variant addition
            // (e.g., a new feed-side reply class) would fail the explicit
            // arm coverage above, surfacing a test-fixture update need at
            // compile time — but the non_exhaustive marker forces a
            // wildcard arm to satisfy exhaustiveness. We classify the
            // wildcard as a test-fixture drift signal rather than silently
            // accepting unknown action bytes into the rebuilt frame.
            _ => panic!(
                "test fixture: unhandled Action variant — \
                 update tests/common/mod.rs after adding a new Action arm",
            ),
        }
    }
    scratch
}

impl PushOrPanic for PgProtocol<ActivePhase> {
    fn push_or_panic<C: PushCommand>(&mut self, cmd: C, wb: &mut WriteBuf) {
        let status = self.connection_status();
        let Some(g) = self.as_ready() else {
            panic!(
                "test fixture: proto must be Idle for push (status = {status:?})",
            );
        };
        match g.push_command(cmd, wb) {
            Ok(actions) => {
                let scratch = actions_to_scratch(actions);
                wb.clear();
                if wb.push_bytes(&scratch).is_err() {
                    panic!(
                        "test fixture: rebuilt frame ({} B) must fit WriteBuf capacity",
                        scratch.len(),
                    );
                }
            }
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
            Ok(actions) => {
                let scratch = actions_to_scratch(actions);
                wb.clear();
                if wb.push_bytes(&scratch).is_err() {
                    panic!(
                        "test fixture: rebuilt frame ({} B) must fit WriteBuf capacity",
                        scratch.len(),
                    );
                }
            }
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
            Ok(_actions) => panic!(
                "test fixture: push_expect_failure expected Err but got Ok"
            ),
            Err(f) => f,
        }
    }
}

/// Split `wb.as_bytes()` of a single-frame-plus-Sync push (Parse /
/// Describe / etc.) into the leading frame and the trailing Sync
/// wire bytes.
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

/// Split `wb.as_bytes()` of a Bind+Execute+Sync push into the three
/// constituent wire frames.
///
/// Layout (PG §55.7 staged push):
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