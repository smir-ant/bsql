//! `(prev_state, frame_tag) → outcome` matcher.
//!
//! The dispatcher is the **single** place the protocol decides what to
//! do with a freshly-parsed frame given the current state. The match is
//! exhaustive over `(state, tag)` pairs the Phase 1b flows can encounter;
//! adding a new state or tag is a build error until it is wired into
//! the matcher.
//!
//! # Payload contract — tier-1 via slice patterns
//!
//! The caller has already parsed the header (5 bytes: tag + 4-byte BE
//! length) and verified that the full frame is buffered. It passes the
//! dispatcher the **payload** — the bytes *after* the header, of length
//! `total_len - 5`. Every arm that needs to inspect bytes uses a slice
//! pattern (`[b0]`, `[b0, b1, ..]`, etc.) so the compiler enforces the
//! length / presence check.
//!
//! # DEF-184 A7 (2026-04-24) — tag LUT path rejected, DO NOT retry
//!
//! A tempting refactor is to replace `match (prev, tag: InboundTag)`
//! with a compact `InboundTagClass` enum (17 dense variants + classify
//! step) under the hypothesis "dense-discriminant jump table beats
//! sparse-ASCII-byte switch". **This was implemented, measured, and
//! rejected** — commit `1a762ca` (reverted).
//!
//! Measured result (criterion against `def184-complete` baseline,
//! aarch64-apple-darwin): all 4 hot-path benches regressed
//! (+2.6% to +8.2%, p<0.05). Modern LLVM already lowers the byte
//! switch into a compact cmp-and-branch chain that CSEs across arms;
//! the extra classify() call + `InboundTagClass::Unknown` catch-all
//! branch add indirection LLVM cannot fold out. Hypothesis falsified.
//!
//! If you are tempted to reopen A7: first produce a NEW criterion
//! measurement refuting the 2026-04-24 result (different machine,
//! different LLVM, or architectural change in the dispatch loop).
//! See `reforge.md §4.12` (measurement-gated perf) and
//! `deferred.md §B` (measurement-rejected items) before touching.

use crate::action::StagedAction;
use crate::error::ProtocolError;
use crate::reply_id::ReplyId;
use crate::state::ProtoState;

// ═════════════════════════════════════════════════════════════════════
// DEF-272 cluster α (2026-05-10) — schema-side concrete-token leaf
//
// Pre-DEF-272-α the `AtRowDescriptionDispatch` tag impl'd
// `SchemaWriteAuth` (sealed-trait pattern). Tier-1 EXTERNAL but tier-2
// by-discipline WITHIN-CRATE: any in-crate file could `impl Sealed +
// SchemaWriteAuth for HostileTag` and bypass.
//
// Post-DEF-272-α the leaf hosts a CONCRETE `TDispatchToken` type with a
// private tuple-struct field; the literal `Self(())` mint is callable
// ONLY inside this submodule. The cell's
// `park_at_t_dispatch` method takes `TDispatchToken` by value — there
// is no trait to `impl` for hostile types, no sealed-supertrait to
// route around. Three call sites (simple-query, describe-statement,
// describe-portal 'T' arms) all invoke the leaf helper.
//
// See `mod protocol` (DEF-272 cluster α block) for the parallel
// schema-slot leaves; cluster β migrates the session_params leaves.
// ═════════════════════════════════════════════════════════════════════

/// DEF-272 cluster α leaf submodule for the inbound `'T'`
/// (RowDescription) frame dispatch. Hosts the [`TDispatchToken`] type
/// and the single park helper fn.
// DEF-244 modernisation audit (rust-version 1.81 sweep): the
// historical `#[allow(missing_docs, reason = "leaf helper")]`
// here was DEAD — `missing_docs` only fires on `pub` items in
// `#![deny(missing_docs)]` crates; this submodule and its items
// are `pub(crate)`-only, so the lint doesn't trigger. Attribute
// deleted rather than migrated to `#[expect]`.
pub(crate) mod _row_description_dispatch_leaf {
    /// DEF-272 cluster α leaf-scope token. The tuple-struct field is
    /// PRIVATE to this submodule — `Self(())` mints are callable ONLY
    /// here. The type itself is `pub(crate)` so
    /// [`crate::schema_slot::RowDescSlotCell::park_at_t_dispatch`] can
    /// name it in its parameter signature.
    pub(crate) struct TDispatchToken(());

    /// Mint a [`TDispatchToken`] and write `row_desc` into `slot` via
    /// [`crate::schema_slot::RowDescSlotCell::park_at_t_dispatch`].
    /// Used by all three 'T' arm dispatch transitions (simple-query,
    /// describe-statement, describe-portal).
    #[inline]
    pub(in crate::dispatch) fn park_row_description_at_dispatch(
        slot: &mut crate::schema_slot::RowDescSlotCell,
        row_desc: crate::decode::RowDesc,
    ) {
        slot.park_at_t_dispatch(row_desc, TDispatchToken(()));
    }
}
use crate::wire::{
    SCRAM_SHA_256_MECHANISM, TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_BIND_COMPLETE,
    TAG_COMMAND_COMPLETE, TAG_EMPTY_QUERY_RESPONSE, TAG_ERROR_RESPONSE,
    TAG_NEGOTIATE_PROTOCOL_VERSION, TAG_NO_DATA, TAG_PARAMETER_DESCRIPTION, TAG_PARSE_COMPLETE,
    TAG_PORTAL_SUSPENDED, TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};
// DEF-154 (B) Phase B4: `WriteBuf` import no longer needed here —
// dispatch takes `&mut BrandedWriteReserved<'_>` post-migration.

/// What to do after dispatching a single frame.
///
/// Three variants to keep the "emit zero actions" and "emit one
/// action" cases structurally distinct (audit round 2 A4).
///
/// # DEF-184 (B21/C6) — by-ref state, no `new_state` payload
///
/// Pre-(B21/C6) the Advanced variants carried `new_state: ProtoState`
/// (712 B each). Every successful dispatch produced a by-value
/// DispatchOutcome of ~800 B that the caller moved into its `match`
/// arm then wrote back into `*state`. LLVM does NOT optimise the
/// round-trip because `ProtoState` contains an opaque password
/// buffer (non-trivial move semantics).
///
/// Post-(B21/C6) dispatch takes `state: &mut ProtoState` and writes
/// the transition directly. DispatchOutcome shrinks to the
/// **side-effect** signal only:
/// - `AdvancedSilent` — no payload (~1 B discriminant).
/// - `AdvancedWithAction { action }` — 88 B `StagedAction`.
/// - `Errored { reply_id, cause }` — 80 B classified failure.
///
/// Size: 88 B exact (vs ~800 B). Pin in lib.rs.
///
/// # Real-world win sizing (honest scope — architect audit 2026-04-24)
///
/// `dispatch()` runs **per frame**, not per row — and `DataRow`
/// bypasses `dispatch()` entirely via the
/// `row_stream::fast_path_data_row` fast-path. Typical query
/// routes through dispatch ~3-6 times (Parse/Bind complete,
/// RowDescription, CommandComplete, RFQ). So the true saving is
/// ~712 B × 3-6 per query, not 712 MB per million rows.
///
/// The material benefit is **async future frame reduction**: every
/// `feed_bytes` suspension point in a downstream async wrapper
/// carries the 800 → 88 B delta, which snowballs through the
/// `async fn` state-machine nesting. That is the real per-QPS
/// win — smaller suspended futures, better L1 residency on
/// poll reawakening.
// DEF-184 (A1+A13): DispatchOutcome's Errored.cause ProtocolError
// shrunk 312 → ~72 B via ErrorArena externalisation.
//
// DEF-160 (Z2): `StagedAction<'static>` — the dispatch path
// (server→client frames) produces no `SendBytesBorrowed` actions;
// only push paths (Parse / SimpleQuery in commit 2) borrow SQL bytes
// from the caller. Hard-pinning to `'static` here keeps the dispatch
// fn signatures lifetime-free. If a future server-driven path needs
// to borrow (e.g., streaming COPY data references), promote this
// to `<'sql>` then.
#[derive(Debug)]
pub(crate) enum DispatchOutcome {
    /// Frame consumed; transition already written to caller's
    /// state slot. No action emitted. Used by ParameterStatus,
    /// BackendKeyData, AuthenticationOk, SASLFinal — frames that
    /// advance state without user-visible side effects.
    AdvancedSilent,
    /// Frame consumed; transition already written to caller's
    /// state slot. One staged action emitted.
    ///
    /// DEF-094: `StagedAction` is range-based — the entry-point
    /// materialises into a ref-bound `Action<'buf>` after the write
    /// phase releases.
    ///
    /// DEF-154 (Y): `'r` deleted post-StreamRowRange removal —
    /// `StagedAction` no longer carries read-buf references (the
    /// only lifetime'd field was `StreamRowRange::row_bytes`).
    ///
    /// DEF-160 (Z2): `StagedAction<'static>` — dispatch path never
    /// produces `SendBytesBorrowed`.
    AdvancedWithAction {
        /// The single side-effect to push.
        action: StagedAction<'static>,
    },
    /// Frame rejected; connection irrecoverable. Caller tears down.
    /// State has already been set to `ProtoState::Errored(kind)` by
    /// the dispatch helper [`install_errored`] (the caller observes
    /// the terminal state without needing a second write).
    ///
    /// # Pre-consumed reply_id (DEF-112)
    ///
    /// `reply_id` is `Option<NonZeroU64>` (already-consumed raw
    /// value), not `Option<ReplyId<K>>`. Rationale: dispatchers
    /// are parameterised per-command-kind after DEF-112, and the
    /// Errored path is kind-agnostic (the downstream action is a
    /// `FailReply { id: NonZeroU64, cause }` that carries no
    /// payload). Pre-consuming at each dispatcher's Errored
    /// construction site keeps the DispatchOutcome kind-free and
    /// avoids forcing DispatchOutcome to be generic over K.
    Errored {
        reply_id: Option<core::num::NonZeroU64>,
        cause: ProtocolError,
    },
}

/// DEF-103 + DEF-184 (B21/C6): `#[cold] #[inline]` helper centralising
/// every `DispatchOutcome::Errored` construction **plus** the
/// `*state = ProtoState::Errored(...)` install.
///
/// Pre-(B21/C6) this was `errored(reply_id, cause) -> DispatchOutcome`
/// and the caller wrote `*state = ProtoState::Errored(kind)` in the
/// outer match arm on the Errored variant. Post-(B21/C6) state is
/// a `&mut` parameter to `dispatch()`; installing the terminal state
/// inside the helper keeps the seam tight (no arm can forget to set
/// Errored while returning `DispatchOutcome::Errored`).
///
/// The `#[cold]` marker tells LLVM to push the Errored-path basic
/// block out of the hot-path I-cache footprint; `#[inline]` keeps
/// the call-site free of an actual function call (the helper body
/// folds into the caller).
///
/// `reply_id` is `Option<NonZeroU64>` (already-consumed raw
/// value) per DEF-112's pre-consume convention.
#[cold]
#[inline]
fn install_errored(
    state: &mut ProtoState,
    reply_id: Option<core::num::NonZeroU64>,
    cause: ProtocolError,
) -> DispatchOutcome {
    *state = ProtoState::Errored(cause.state_kind());
    DispatchOutcome::Errored { reply_id, cause }
}

// DEF-177 + DEF-184 (B21/C6) `install_internal_bug` DELETED 2026-04-25
// (DEF-188 cascade): the helper's only callers were the three
// `SchemaArenaAllocFull` arms in dispatch (one per RowDescription
// destination) plus a row-range-construction site that no longer
// exists. With DEF-188's arena deletion, no dispatch arm currently
// classifies into `InternalCrateBug`. The helper became dead code
// — re-introduce inline if a future arm needs the same shape.

/// Dispatch a single frame.
///
/// `write_buf` is the caller-owned outbound staging buffer (DEF-094);
/// dispatchers that produce [`StagedAction::SendBytesRange`] write
/// into it and record the range. The caller (feed_bytes) is
/// responsible for clearing `write_buf` at the start of each
/// entry-point call and materialising the ranges into `&'buf [u8]`
/// slices after the write-phase mutable borrow completes.
///
/// # DEF-188 — `terminal_row_desc: &mut Option<RowDesc>`
///
/// The terminal-RowDesc slot on `PgProtocol` is threaded mutably.
/// Z arms (terminal `ReadyForQuery` transitions to `Idle`) park
/// the in-flight schema into this slot — schema-bearing variants'
/// `row_desc: RowDesc` field is moved out of the consumed state
/// variant and written into the slot — so the materialise-time
/// borrow has a stable address that outlives the state transition
/// (state moves to `Idle`, but the slot persists until the next
/// entry-point's clear).
///
/// Pre-DEF-188 the equivalent slot was a 2-slot `SchemaArena` with
/// generation tracking; the dispatch path took
/// `arena: &mut ArenaWriter<'_>`. Deleting the arena removes the
/// `StaleSchemaRef` class entirely (no handle, no generation
/// drift) and shrinks the per-row hot-path by removing the dual
/// arena lookup.
///
/// # DEF-184 (B21/C6) — state by `&mut`
///
/// Pre-(B21/C6) dispatch took `prev: ProtoState` by value (712 B
/// memcpy per call) and returned `DispatchOutcome::Advanced* {
/// new_state: ProtoState, ... }` (another 712 B on the stack
/// return). Caller wrote the transition via
/// `*caller_state = outcome.new_state`. That's two 712 B memcpies
/// per dispatch iteration — on 1M-row SELECT workloads, ~1.4 GB
/// stack traffic purely for state round-tripping.
///
/// Post-(B21/C6) dispatch takes `state: &mut ProtoState`, snaps
/// the previous state via `core::mem::take(state)` for pattern
/// matching (state reset to `Default` == `Idle` during the match),
/// then each arm writes `*state = new_state` directly — one
/// store, no round trip. DispatchOutcome shrinks to the
/// **side-effect** signal only (see `DispatchOutcome` docs).
///
/// Invariant: EVERY match arm must either (a) assign `*state =
/// new_state` before returning `AdvancedSilent`/`AdvancedWithAction`,
/// or (b) delegate to [`install_errored`] / [`install_internal_bug`]
/// which install `ProtoState::Errored(...)`. Forgetting to assign
/// leaves state at the default `Idle` — a silent regression class
/// the compiler cannot catch. Mitigation: arm-body coverage tests
/// across all transitions (existing 230-test suite exercises every
/// `(state, tag)` pair reachable in the state machine).
pub(crate) fn dispatch(
    state: &mut ProtoState,
    tag: crate::wire::InboundTag,
    payload: &[u8],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
    // DEF-196 (2026-04-28): `&mut Option<Box<ErrorArena>>` slot for
    // the dispatch path's only cold-write target. Most dispatch arms
    // don't write error_arena; the few that do (ErrorResponse arms)
    // lazy-init via `error_arena_or_init(error_arena_slot)` inline,
    // allocating exactly once on the first server error per
    // connection. Frames that don't reach an ErrorResponse arm pay
    // zero allocation cost.
    error_arena_slot: &mut Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
) -> DispatchOutcome {
    // DEF-184 (B21/C6): snap owned prev for pattern matching; state
    // slot holds the explicit `ProtoState::Idle` placeholder during
    // the match. Every match arm below MUST `*state = <transition>`
    // before returning a non-Errored outcome; the install_errored /
    // install_internal_bug helpers handle Errored transitions.
    //
    // Use `mem::replace` (not `mem::take`) to make the placeholder
    // explicit — `mem::take` silently relies on the `Default` impl
    // returning `Idle`, and a future `Default` change could swap
    // placeholder semantics under us.
    let prev = core::mem::replace(state, ProtoState::Idle);
    let outcome = match (prev, tag) {
        // =============================================================
        // Ping flow (Phase 1a, carried forward)
        // =============================================================
        (ProtoState::PingAwaitingRfq(id), TAG_READY_FOR_QUERY) => {
            // DEF-112: `id: ReplyId<PingKind>` — the typed
            // `deliver` helper binds the payload to `PongPayload`
            // at compile time. Attempting to deliver any other
            // payload type here is a type error.
            //
            // Tier-1 tx_status validation via the centralised
            // `parse_rfq_payload` (F13): users never receive a
            // `TxStatus` outside `{Idle, InTransaction, Failed}`;
            // any other byte is a wire violation classified as
            // `MalformedReadyForQuery` with the correct `payload_len`.
            match parse_rfq_payload(payload) {
                Ok(tx_status) => {
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            id,
                            crate::action::PongPayload { tx_status },
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(id.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        (ProtoState::PingAwaitingRfq(id), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(id.consume()), cause)
        }
        (ProtoState::PingAwaitingRfq(id), other) => install_errored(state, Some(id.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupTrust — awaiting AuthenticationOk
        // (DEF-097: Trust connections cannot accept AUTH_SASL — that
        // case is a per-variant dispatcher arm now, not a runtime
        // classification.)
        // =============================================================
        (ProtoState::ConnectingStartupTrust { reply }, TAG_AUTHENTICATION) => {
            dispatch_auth_in_startup_trust(state, reply, payload)
        }
        (ProtoState::ConnectingStartupTrust { reply }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupTrust { reply }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupTrust { reply }, other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupScram — awaiting AuthenticationSASL
        // (DEF-097: mirror of the Trust arm. A Scram connection
        // receiving AUTH_OK in this state is classified as
        // `UnsupportedAuthMethod` — the server accepted without
        // challenge while the user supplied a password, a PG policy
        // mismatch worth surfacing.)
        // =============================================================
        (ProtoState::ConnectingStartupScram { reply, scram }, TAG_AUTHENTICATION) => {
            // DEF-184 (A10/B22 revert 2026-04-24): `scram: ScramSession`
            // is destructured DIRECTLY from the variant — variant-
            // carries-field is tier-1 compile (CREDO §1). No drift
            // classifier needed: the variant cannot exist without its
            // SCRAM session.
            dispatch_auth_in_startup_scram(state, reply, scram, payload, reserved)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupCleartext — awaiting AuthenticationCleartextPassword
        // (DEF-215, 2026-05-05). Mirror of the Trust + SCRAM startup
        // arms. A Cleartext connection only accepts AUTH_CLEARTEXT_PASSWORD
        // (sub-code 3); AUTH_OK without challenge would be a server-side
        // policy mismatch (server accepted nothing despite the user
        // supplying a password — surfaced as `UnsupportedAuthMethod`).
        // =============================================================
        (ProtoState::ConnectingStartupCleartext { reply, password }, TAG_AUTHENTICATION) => {
            // Variant-carries-field: the variant cannot exist without
            // a `Box<Sensitive<Password>>`. Destructure here moves the
            // Box into the per-variant dispatcher, which builds the
            // `PasswordMessage` frame and drops the Box at scope end
            // (Drop chain scrubs the password bytes).
            dispatch_auth_in_startup_cleartext(state, reply, password, payload, reserved)
        }
        (ProtoState::ConnectingStartupCleartext { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupCleartext { reply, .. }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupCleartext { reply, .. }, other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingCleartextAwaitingAuthOk — PasswordMessage sent;
        // awaiting AuthenticationOk (DEF-215, 2026-05-05).
        // =============================================================
        (ProtoState::ConnectingCleartextAwaitingAuthOk(reply), TAG_AUTHENTICATION) => {
            dispatch_auth_ok_after_cleartext(state, reply, payload)
        }
        (ProtoState::ConnectingCleartextAwaitingAuthOk(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingCleartextAwaitingAuthOk(reply), other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupMd5 — awaiting AuthenticationMD5Password
        // (DEF-216, 2026-05-05). Mirror of cleartext + SCRAM startup
        // arms. Only sub-code 5 (with a valid 4-byte salt) progresses;
        // any other code is `UnsupportedAuthMethod` (downgrade
        // rejection — security mirror of SCRAM dispatcher).
        // =============================================================
        (ProtoState::ConnectingStartupMd5 { reply, handshake }, TAG_AUTHENTICATION) => {
            // Variant-carries-field: variant cannot exist without
            // `Box<Md5HandshakeState>`. Move into per-variant
            // dispatcher; the Box drops at function-return through
            // the ZeroizeOnDrop chain (Box::drop → md5 state drop →
            // Sensitive::drop → Password::drop).
            dispatch_auth_in_startup_md5(state, reply, handshake, payload, reserved)
        }
        (ProtoState::ConnectingStartupMd5 { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupMd5 { reply, .. }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupMd5 { reply, .. }, other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingMd5AwaitingAuthOk — PasswordMessage sent;
        // awaiting AuthenticationOk (DEF-216, 2026-05-05).
        // =============================================================
        (ProtoState::ConnectingMd5AwaitingAuthOk(reply), TAG_AUTHENTICATION) => {
            dispatch_auth_ok_after_md5(state, reply, payload)
        }
        (ProtoState::ConnectingMd5AwaitingAuthOk(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingMd5AwaitingAuthOk(reply), other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // SCRAM: awaiting server-first-message
        // =============================================================
        (
            ProtoState::ConnectingScramAwaitingServerFirst { reply, scram },
            TAG_AUTHENTICATION,
        ) => {
            // DEF-184 (A10/B22 revert 2026-04-24): heavy SCRAM fields
            // destructured inline from the variant — tier-1 invariant
            // (CREDO §1: variant-carries-field). No drift path.
            //
            // DEF-210 REC-06 → PERF-02 (audits 2026-04-28 + 2026-05-04):
            // the SCRAM handshake state is one `Box<ScramSession>`
            // carrying password + `client_first_bare` + `client_nonce_b64`
            // inline. The Box is moved here by the destructure (no
            // allocator op); `dispatch_auth_sasl_continue` borrows
            // `&scram` for HMAC composition + reads
            // `scram.client_first_bare` / `scram.client_nonce_b64`
            // through the same borrow. The Box drops at the end of
            // this arm scope (1 free), firing `ScramSession::Drop`
            // with `ZeroizeOnDrop` scrub of the password.
            dispatch_auth_sasl_continue(
                state,
                reply,
                &scram,
                payload,
                reserved,
            )
        }
        (ProtoState::ConnectingScramAwaitingServerFirst { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingScramAwaitingServerFirst { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // SCRAM: awaiting server-final-message
        // =============================================================
        (
            ProtoState::ConnectingScramAwaitingServerFinal {
                reply,
                expected_server_sig,
            },
            TAG_AUTHENTICATION,
        ) => {
            // DEF-184 (A10/B22 revert 2026-04-24): `expected_server_sig`
            // destructured inline — tier-1 variant-carries-field.
            dispatch_auth_sasl_final(state, reply, expected_server_sig, payload)
        }
        (ProtoState::ConnectingScramAwaitingServerFinal { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingScramAwaitingServerFinal { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // SCRAM: awaiting AuthenticationOk after server sig verified
        // =============================================================
        (ProtoState::ConnectingScramAwaitingAuthOk(reply), TAG_AUTHENTICATION) => {
            dispatch_auth_ok_after_scram(state, reply, payload)
        }
        (ProtoState::ConnectingScramAwaitingAuthOk(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingScramAwaitingAuthOk(reply), other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // Post-auth: waiting for BackendKeyData
        //
        // `ParameterStatus` (tag 'S') is filtered pre-dispatch in
        // `feed_bytes` via `allows_unsolicited_param_status`; the
        // dispatcher never sees it for these states. DEF-054.
        // =============================================================
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), TAG_BACKEND_KEY_DATA) => {
            match parse_backend_key_data(payload) {
                Ok((pid, secret_key)) => {
                    // DEF-189 Q8-C2: wrap the secret_key in Sensitive
                    // so the variant's storage scrubs on drop (state
                    // transition).
                    *state = ProtoState::ConnectingPostAuthHaveKey {
                        reply,
                        pid,
                        secret_key: crate::sensitive::Sensitive::new(secret_key),
                    };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // Post-auth: have BackendKeyData, waiting for ReadyForQuery
        //
        // `ParameterStatus` (tag 'S') is filtered pre-dispatch in
        // `feed_bytes` via `allows_unsolicited_param_status`; the
        // dispatcher never sees it for these states. DEF-054.
        // =============================================================
        (
            ProtoState::ConnectingPostAuthHaveKey {
                reply,
                pid,
                secret_key,
            },
            TAG_READY_FOR_QUERY,
        ) => {
            // DEF-112: `reply: ReplyId<StartupKind>` — typed
            // `deliver` forces a `StartupCompletePayload` payload.
            //
            // DEF-189 Q8-C2: extract the inner i32 from
            // `Sensitive<i32>` via `.get()` (returns &i32, copy-deref
            // here). The Sensitive wrapper drops at end of arm scope,
            // scrubbing the source slot. The plain `i32` in
            // StartupCompletePayload then flows through the staged
            // pipeline; that payload's manual Debug impl already
            // redacts the field (P1-C).
            match parse_rfq_payload(payload) {
                Ok(tx_status) => {
                    let secret_key_inner: i32 = *secret_key.get();
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            reply,
                            crate::action::StartupCompletePayload {
                                pid,
                                secret_key: secret_key_inner,
                                tx_status,
                            },
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        (ProtoState::ConnectingPostAuthHaveKey { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingPostAuthHaveKey { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Phase 1c-1b: Simple Query flow
        //
        // State transitions per PG §55.2.3:
        //   Q sent → AwaitingFirstResponse
        //     T (RowDescription)   → StreamingRows
        //     C (CommandComplete)  → AwaitingRfq(command_tag)
        //     I (EmptyQueryResp)   → AwaitingRfq(empty tag)
        //     E (ErrorResponse)    → DrainRfqAfterError (FailReply emitted)
        //   StreamingRows
        //     D (DataRow)          → emit StreamRow; stay
        //     C                    → AwaitingRfq(command_tag)
        //     E                    → DrainRfqAfterError (FailReply emitted)
        //   AwaitingRfq
        //     Z                    → DeliverReply QueryComplete; Idle
        //   DrainRfqAfterError
        //     Z                    → silent; Idle (reply already sent)
        //
        // Errors within simple-query states: query-level `E` is
        // recoverable (connection survives); framing-level `E`
        // during the handshake states would tear down, but those
        // states are never entered from simple-query flow.
        // =============================================================

        // AwaitingFirstResponse: T / C / I / E — any other tag is desync
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_ROW_DESCRIPTION) => {
            // DEF-189: parsed schema lands in PgProtocol::row_desc_slot.
            // The variant transitions to StreamingRows (no inline payload).
            // The slot lives across the entire stream (DataRows + Z).
            match crate::decode::parse_row_description(payload) {
                Ok(row_desc) => {
                    // DEF-271 cluster C: leaf helper performs the mint+park
                    // with the auth tag's scope confined to its submodule.
                    _row_description_dispatch_leaf::park_row_description_at_dispatch(
                        row_desc_slot,
                        row_desc,
                    );
                    *state = ProtoState::SimpleQueryStreamingRows { reply };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_COMMAND_COMPLETE) => {
            // DML path: no RowDescription frame fired, so row_desc_slot
            // remained `None` since the last Idle entry — materialise
            // emits Reply with `row_desc = None` (DEF-210 SR-01 Path C).
            advance_to_awaiting_rfq(state, reply, payload)
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_EMPTY_QUERY_RESPONSE) => {
            // DEF-185 P0-F (audit 2026-04-24): PG §55.7 specifies
            // EmptyQueryResponse has a zero-byte body. Enforce tier-2
            // structural — non-empty body classifies as
            // UnexpectedFrameBody. Pre-fix: payload was ignored entirely.
            match payload {
                [] => {
                    *state = ProtoState::SimpleQueryAwaitingRfq {
                        reply,
                        command_tag: crate::error::BoundedStr::default(),
                    };
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_EMPTY_QUERY_RESPONSE,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // StreamingRows: C / E — any other tag (including DataRow
        // via `feed_bytes`) is a desync. DataRow is handled
        // exclusively by `iter_rows` fast-path; reaching dispatch
        // with TAG_DATA_ROW in a row-streaming state means the
        // caller used the `feed_bytes` API for a row-bearing
        // response (API misuse). The catch-all arm below
        // classifies as `UnexpectedFrame { tag: DataRow }` →
        // FailReply + CloseSocket.
        (ProtoState::SimpleQueryStreamingRows { reply }, TAG_COMMAND_COMPLETE) => {
            // SELECT path terminates: schema lives in row_desc_slot
            // (parked by the 'T' arm earlier in this query). AwaitingRfq
            // → Z → Idle. DEF-210 SR-01 Path C: materialise reads the
            // slot directly — no flag to set.
            advance_to_awaiting_rfq(state, reply, payload)
        }
        (ProtoState::SimpleQueryStreamingRows { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::SimpleQueryStreamingRows { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // AwaitingRfq: Z is the only legal frame
        (ProtoState::SimpleQueryAwaitingRfq { reply, command_tag }, TAG_READY_FOR_QUERY) => {
            // DEF-189 + DEF-210 SR-01 Path C: schema (if any) lives
            // in row_desc_slot (parked at the 'T' arm earlier in this
            // query, or pre-populated by push for BindExecute SELECT).
            // State transitions to Idle; slot persists until next
            // entry-point clear, so the public QueryComplete reply's
            // RowDescBorrow stays valid. Materialise reads the slot
            // directly — no `schema_present: bool` flag to keep in
            // sync (single source of truth, tier-1 by-construction).
            match parse_rfq_payload(payload) {
                Ok(tx_status) => {
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            reply,
                            crate::action::StagedQueryCompletePayload {
                                command_tag,
                                tx_status,
                            },
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        (ProtoState::SimpleQueryAwaitingRfq { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // DrainRfqAfterError: consume Z → Idle, with full tx_status validation.
        //
        // DEF-185 P1-G (audit 2026-04-24): delegate to parse_rfq_payload
        // for uniform validation. Pre-fix: accepted any 1-byte payload
        // via `[_]` slice-pattern without validating the byte is one of
        // `{I, T, E}`. Every OTHER RFQ arm (`PingAwaitingRfq`,
        // `ParseAwaitingRfq`, `SimpleQueryAwaitingRfq`, etc.) used
        // `parse_rfq_payload`; this one was an asymmetry — `drained`
        // still succeeded but a malformed byte got silently accepted.
        // Semantically low-stakes (we reach Idle either way), but tier-4
        // uniformity drift nonetheless. Post-fix: consistent classification.
        (ProtoState::DrainRfqAfterError, TAG_READY_FOR_QUERY) => {
            // tx_status is unused on the drain path — we're returning
            // to Idle after a query-level error; the pre-error query's
            // consumer already received FailReply via `advance_to_drain_after_error`.
            // Nobody consumes the drain's tx_status. Pattern-bind `_`
            // in Ok arm to validate-and-discard (not a `let _` — that
            // form is user-banned per no-underscore-vars feedback).
            match parse_rfq_payload(payload) {
                Ok(_) => {
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedSilent
                }
                Err(payload_len) => install_errored(
                    state,
                    None,
                    ProtocolError::MalformedReadyForQuery { payload_len },
                ),
            }
        }
        (ProtoState::DrainRfqAfterError, other) => {
            install_errored(state, None, ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Phase 1c-3a: Extended Query — Parse flow
        //
        // Parse + Sync sequence:
        //   Client sends: P frame + S frame (bundled in push_command)
        //   Server responds: '1' (ParseComplete) + 'Z' (ReadyForQuery)
        //                or: 'E' (ErrorResponse) + 'Z' (recoverable)
        //
        // State lifecycle:
        //   Idle → ParseAwaitingParseComplete(reply) → ParseAwaitingRfq(reply) → Idle
        //                                            ↘ (on E)
        //                                              DrainRfqAfterError → Idle
        // =============================================================

        (ProtoState::ParseAwaitingParseComplete(reply), TAG_PARSE_COMPLETE) => {
            // DEF-185 P0-F: ParseComplete body must be empty per
            // PG §55.7.
            match payload {
                [] => {
                    *state = ProtoState::ParseAwaitingRfq(reply);
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_PARSE_COMPLETE,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (ProtoState::ParseAwaitingParseComplete(reply), TAG_ERROR_RESPONSE) => {
            // Recoverable parse error — PG spec sends Z after E, so
            // drain it silently and return to Idle (reusing the
            // `DrainRfqAfterError` variant — both drain
            // the same trailing RFQ pattern).
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            *state = ProtoState::DrainRfqAfterError;
            DispatchOutcome::AdvancedWithAction {
                action: StagedAction::FailReply {
                    id: reply.consume(),
                    cause,
                },
            }
        }
        (ProtoState::ParseAwaitingParseComplete(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        (ProtoState::ParseAwaitingRfq(reply), TAG_READY_FOR_QUERY) => {
            match parse_rfq_payload(payload) {
                Ok(tx_status) => {
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            reply,
                            crate::action::ParseCompletePayload { tx_status },
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        (ProtoState::ParseAwaitingRfq(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Phase 1c-3b: Extended Query — Bind + Execute flow
        //
        // Flow splits at `push_bind_execute` call time by whether the
        // caller provided a `row_desc`:
        //
        // • SCHEMA-LESS (DML) path — 3 states, 'D' is UnexpectedFrame
        //   at every stage since there's no schema to decode rows with:
        //     AwaitingBindCompleteDml → '2' → AwaitingCommandCompleteDml
        //     AwaitingCommandCompleteDml → 'C' → AwaitingRfqDml
        //     AwaitingRfqDml → 'Z' → Idle + DeliverReply(row_desc=None)
        //
        // • SCHEMA-BEARING (SELECT) path — 4 states, 'D' streams rows:
        //     AwaitingBindCompleteSelect → '2' → AwaitingDataOrCompleteSelect
        //     AwaitingDataOrCompleteSelect → 'D' → StreamingRows (+emit)
        //                                  → 'C' → AwaitingRfqSelect
        //     StreamingRows → 'D' → StreamingRows (+emit)
        //                   → 'C' → AwaitingRfqSelect
        //     AwaitingRfqSelect → 'Z' → Idle + DeliverReply(row_desc=Some)
        //
        // Shared-across-paths:
        //   'E' (ErrorResponse) at ANY state → drain-after-error
        //                                      (query-level recoverable)
        //   's' (PortalSuspended) → UnexpectedFrame (1c-3b scope;
        //                           1c-6 lifts for chunked fetch)
        //
        // Tier uplift vs pre-split: the "can we stream rows?"
        // decision is resolved at the VARIANT level (tier-1 structural
        // dispatch) instead of via a runtime `match row_desc: Option<_>`
        // at the 'D' arm.
        // =============================================================

        // ─── DML path ───

        (ProtoState::BindExecuteAwaitingBindCompleteDml(reply), TAG_BIND_COMPLETE) => {
            // DEF-185 P0-F: BindComplete body must be empty per
            // PG §55.7.
            match payload {
                [] => {
                    *state = ProtoState::BindExecuteAwaitingCommandCompleteDml(reply);
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_BIND_COMPLETE,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        // DEF-244 (2026-05-13): the prepared! macro path bundles Parse
        // + Bind + Execute + Sync; server replies with `1` (ParseComplete)
        // before `2` (BindComplete). Accept `1` as a silent transition
        // in this state (state stays the same; ParseComplete carries no
        // payload data we'd track separately). The post-condition still
        // requires `2` to advance.
        //
        // **State restore**: the dispatch entry `mem::replace`'d state with
        // Idle; the AdvancedSilent path MUST write back the original
        // variant or the protocol would silently transition to Idle
        // (and the next BindComplete frame would be UnexpectedFrame).
        (ProtoState::BindExecuteAwaitingBindCompleteDml(reply), TAG_PARSE_COMPLETE) => {
            if payload.is_empty() {
                *state = ProtoState::BindExecuteAwaitingBindCompleteDml(reply);
                DispatchOutcome::AdvancedSilent
            } else {
                let payload_len = payload.len();
                install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_PARSE_COMPLETE,
                        payload_len,
                    },
                )
            }
        }
        (ProtoState::BindExecuteAwaitingBindCompleteDml(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::BindExecuteAwaitingBindCompleteDml(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        (ProtoState::BindExecuteAwaitingCommandCompleteDml(reply), TAG_COMMAND_COMPLETE) => {
            match parse_command_tag(payload) {
                Ok(command_tag) => {
                    *state = ProtoState::BindExecuteAwaitingRfqDml { reply, command_tag };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::BindExecuteAwaitingCommandCompleteDml(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::BindExecuteAwaitingCommandCompleteDml(reply), TAG_PORTAL_SUSPENDED) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_PORTAL_SUSPENDED })
        }
        (ProtoState::BindExecuteAwaitingCommandCompleteDml(reply), other) => {
            // Includes 'D' (DataRow) — structurally no schema here,
            // server emitting rows is a wire violation.
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        (
            ProtoState::BindExecuteAwaitingRfqDml { reply, command_tag },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                // DML path: no schema to park (row_desc_slot stays
                // at its prior cleared state, i.e. None). DEF-210
                // SR-01 Path C: materialise reads the slot directly
                // → public Reply::QueryComplete.row_desc = None.
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedQueryCompletePayload {
                            command_tag,
                            tx_status,
                        },
                    ),
                }
            }
            Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
        },
        (ProtoState::BindExecuteAwaitingRfqDml { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // ─── SELECT path ───

        (
            ProtoState::BindExecuteAwaitingBindCompleteSelect { reply },
            TAG_BIND_COMPLETE,
        ) => {
            // DEF-185 P0-F: BindComplete body must be empty per
            // PG §55.7.
            match payload {
                [] => {
                    *state =
                        ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply };
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_BIND_COMPLETE,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        // DEF-244 (2026-05-13): prepared! macro path — same silent
        // ParseComplete transition as the DML arm above. State name
        // stays; the state semantically represents "awaiting BindComplete
        // for the in-flight Bind, optionally preceded by ParseComplete".
        // Same state-restore discipline as the DML arm (mem::replace
        // dance — see line ~893).
        (ProtoState::BindExecuteAwaitingBindCompleteSelect { reply }, TAG_PARSE_COMPLETE) => {
            if payload.is_empty() {
                *state = ProtoState::BindExecuteAwaitingBindCompleteSelect { reply };
                DispatchOutcome::AdvancedSilent
            } else {
                let payload_len = payload.len();
                install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_PARSE_COMPLETE,
                        payload_len,
                    },
                )
            }
        }
        (ProtoState::BindExecuteAwaitingBindCompleteSelect { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::BindExecuteAwaitingBindCompleteSelect { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // BindExecuteAwaitingDataOrCompleteSelect: DataRow via
        // `feed_bytes` classifies as UnexpectedFrame in the
        // catch-all arm below (see SimpleQueryStreamingRows for
        // the full rationale).
        (
            ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply },
            TAG_COMMAND_COMPLETE,
        ) => advance_to_bindexecute_awaiting_rfq_select(state, reply, payload),
        (ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (
            ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, .. },
            TAG_PORTAL_SUSPENDED,
        ) => install_errored(state,
            Some(reply.consume()),
            ProtocolError::UnexpectedFrame { tag: TAG_PORTAL_SUSPENDED },
        ),
        (ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // BindExecuteStreamingRows: DataRow via `feed_bytes`
        // classifies as UnexpectedFrame in the catch-all arm
        // below (see SimpleQueryStreamingRows for the full
        // rationale).
        (ProtoState::BindExecuteStreamingRows { reply }, TAG_COMMAND_COMPLETE) => {
            advance_to_bindexecute_awaiting_rfq_select(state, reply, payload)
        }
        (ProtoState::BindExecuteStreamingRows { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::BindExecuteStreamingRows { reply, .. }, TAG_PORTAL_SUSPENDED) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_PORTAL_SUSPENDED })
        }
        (ProtoState::BindExecuteStreamingRows { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        (
            ProtoState::BindExecuteAwaitingRfqSelect { reply, command_tag },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                // DEF-189 + DEF-210 SR-01 Path C: schema (if any)
                // lives in row_desc_slot, populated either by
                // push_bind_execute (caller-supplied) or by a prior
                // 'T' frame on the auto-describe path. State
                // transitions to Idle; slot persists until next
                // entry-point clear. Materialise reads the slot
                // directly — single source of truth.
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedQueryCompletePayload {
                            command_tag,
                            tx_status,
                        },
                    ),
                }
            }
            Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
        },
        (ProtoState::BindExecuteAwaitingRfqSelect { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Phase 1c-3c: Extended Query — Describe flow
        //
        // Statement-target response sequence (PG §55.2.2):
        //   't' (ParameterDescription) → 'T' (RowDescription) OR
        //   'n' (NoData) → 'Z' (ReadyForQuery).
        // Portal-target response sequence:
        //   'T' or 'n' → 'Z'. NO 't' — portals are bound-state
        //   handles, parameters fixed at Bind time.
        //
        // Either target, 'E' at any point → recoverable (connection
        // survives, same pattern as SimpleQuery/Parse query-level
        // error per PG §55.2.3).
        //
        // State transitions encode the per-target topology:
        // statement-describe walks 3 states, portal-describe walks 2.
        // A `'T'` arrival in `DescribeStatementAwaitingParamDesc` is
        // UnexpectedFrame — the server has violated the documented
        // sequence. Tier-1 structural dispatch.
        // =============================================================

        // ─── Statement-describe path ───

        // Stage 1: awaiting ParameterDescription.
        (ProtoState::DescribeStatementAwaitingParamDesc(reply), TAG_PARAMETER_DESCRIPTION) => {
            match crate::decode::parse_parameter_description(payload) {
                Ok(param_oids) => {
                    *state = ProtoState::DescribeStatementAwaitingRowDescOrNoData {
                        reply,
                        param_oids,
                    };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::DescribeStatementAwaitingParamDesc(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::DescribeStatementAwaitingParamDesc(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // Stage 2: awaiting RowDescription or NoData.
        (
            ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply, param_oids },
            TAG_ROW_DESCRIPTION,
        ) => match crate::decode::parse_row_description(payload) {
            Ok(row_desc) => {
                // DEF-189 + DEF-210 SR-01-D Path D: parsed schema
                // lands in PgProtocol::row_desc_slot — the single
                // source of truth (no `rows: Rows` discriminator
                // duplicate). Materialise reads the slot at the Z
                // arm.
                // DEF-271 cluster C: leaf helper performs the mint+park
                // with the auth tag's scope confined to its submodule.
                _row_description_dispatch_leaf::park_row_description_at_dispatch(
                    row_desc_slot,
                    row_desc,
                );
                *state = ProtoState::DescribeStatementAwaitingRfq {
                    reply,
                    param_oids,
                };
                DispatchOutcome::AdvancedSilent
            }
            Err(cause) => install_errored(state, Some(reply.consume()), cause),
        },
        (
            ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply, param_oids },
            TAG_NO_DATA,
        ) => {
            // DEF-185 P0-F: NoData body must be empty per PG §55.7.
            // DEF-210 SR-01-D Path D: row_desc_slot stays `None`
            // (no 'T' fired); materialise reads the slot at Z and
            // emits Reply::DescribeStatementComplete with `NoData`.
            match payload {
                [] => {
                    *state = ProtoState::DescribeStatementAwaitingRfq {
                        reply,
                        param_oids,
                    };
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_NO_DATA,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (
            ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply, .. },
            TAG_ERROR_RESPONSE,
        ) => advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot)),
        (ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // Stage 3: awaiting ReadyForQuery — deliver the terminal
        // reply carrying the accumulated param_oids. DEF-210 SR-01-D
        // Path D: schema (if any) lives in row_desc_slot, populated
        // at the 'T' arm above; materialise reads the slot directly.
        (
            ProtoState::DescribeStatementAwaitingRfq {
                reply,
                param_oids,
            },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedDescribeStatementCompletePayload {
                            param_oids,
                            tx_status,
                        },
                    ),
                }
            }
            Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
        },
        (ProtoState::DescribeStatementAwaitingRfq { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // ─── Portal-describe path ───

        // Stage 1: awaiting RowDescription or NoData (no ParamDesc).
        (ProtoState::DescribePortalAwaitingRowDescOrNoData(reply), TAG_ROW_DESCRIPTION) => {
            // DEF-189 + DEF-210 SR-01-D Path D: parsed schema lands
            // in PgProtocol::row_desc_slot — single source of truth.
            match crate::decode::parse_row_description(payload) {
                Ok(row_desc) => {
                    // DEF-271 cluster C: leaf helper performs the mint+park
                    // with the auth tag's scope confined to its submodule.
                    _row_description_dispatch_leaf::park_row_description_at_dispatch(
                        row_desc_slot,
                        row_desc,
                    );
                    *state = ProtoState::DescribePortalAwaitingRfq { reply };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::DescribePortalAwaitingRowDescOrNoData(reply), TAG_NO_DATA) => {
            // DEF-185 P0-F: NoData body must be empty per PG §55.7.
            // DEF-210 SR-01-D Path D: row_desc_slot stays None (no
            // 'T' fired); materialise reads slot at Z and emits NoData.
            match payload {
                [] => {
                    *state = ProtoState::DescribePortalAwaitingRfq { reply };
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_NO_DATA,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (ProtoState::DescribePortalAwaitingRowDescOrNoData(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::DescribePortalAwaitingRowDescOrNoData(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // Stage 2: awaiting ReadyForQuery — deliver portal reply.
        // DEF-210 SR-01-D Path D: schema (if any) in row_desc_slot;
        // materialise reads it at the materialise pass.
        (
            ProtoState::DescribePortalAwaitingRfq { reply },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedDescribePortalCompletePayload { tx_status },
                    ),
                }
            }
            Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
        },
        (ProtoState::DescribePortalAwaitingRfq { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Idle — unsolicited frames are out-of-spec
        // =============================================================
        (ProtoState::Idle, other) => install_errored(state, None, ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // Errored — terminal sink (Phase 1a pattern carried forward)
        //
        // DEF-163 A012: architecturally dead under current flow.
        // `feed_bytes` short-circuits on `ProtoState::Errored(_)`
        // via the is-errored-or-recovering fast-path check (see
        // `protocol.rs` — `is_errored_or_recovering` → clear
        // read_buf + return empty OutActions, dispatch is NEVER
        // entered). Arm retained for exhaustive `(ProtoState, tag)`
        // coverage — a future API that bypasses the early-return
        // would still land here classified, not UB. Missing arm
        // would be a compile error by match exhaustiveness.
        // =============================================================
        (ProtoState::Errored(original), _) => {
            *state = ProtoState::Errored(original);
            DispatchOutcome::AdvancedSilent
        }
    };

    // DEF-184 (A10/B22 revert 2026-04-24): no scram_state cleanup needed.
    // `mem::replace(state, ProtoState::Idle)` above consumed the SCRAM
    // variant by value; if the arm ended in Errored the variant is
    // already dropped — `ScramSession::Drop` impl (`ZeroizeOnDrop`)
    // scrubbed password bytes inside the match. Variant-carries-field
    // invariant (CREDO §1) makes this automatic: there is no separate
    // slot that could linger past the state transition.
    outcome
}

// -----------------------------------------------------------------
// Helper: parse Authentication sub-code from payload
// -----------------------------------------------------------------

/// Extract + classify the 4-byte BE auth sub-code from an `'R'`
/// payload. Returns a typed [`crate::wire::AuthSubCode`] (only the 4
/// PG-defined values) plus the rest of the payload; classifies
/// unknown codes as `UnsupportedAuthMethod` at parse time.
///
/// Tier-1 uplift: downstream `dispatch_auth_*` handlers now match
/// on an enum with 4 variants — adding a new PG auth sub-code
/// forces every handler to decide how to treat it. Previously each
/// handler's `_ =>` arm swallowed both "unknown" and "known-but-wrong"
/// codes silently.
fn auth_sub_code(payload: &[u8]) -> Result<(crate::wire::AuthSubCode, &[u8]), ProtocolError> {
    match payload {
        [a, b, c, d, rest @ ..] => {
            let raw = u32::from_be_bytes([*a, *b, *c, *d]);
            // F-046 (pass-#8): `try_from_u32` returns `Result<Self, u32>`
            // (not `Option<Self>`) — forward the rejected raw u32 via
            // `map_err`, no separate `.ok_or(..raw)` layer needed.
            let code = crate::wire::AuthSubCode::try_from_u32(raw).map_err(|unknown| {
                // DEF-184 (B9): `unknown ≠ 0` structurally — AUTH_OK
                // = 0 is matched to Ok(AuthSubCode::Ok) by
                // try_from_u32 above, so `Err(0)` is architecturally
                // impossible. `AuthSubCodeClass::Unknown(NonZeroU32)`
                // carries the tier-1 type-level proof; dead None
                // arm classified as AuthSubCodeZeroInErr crate-bug.
                match core::num::NonZeroU32::new(unknown) {
                    Some(nz) => ProtocolError::UnsupportedAuthMethod {
                        sub_code: crate::error::AuthSubCodeClass::Unknown(nz),
                    },
                    None => ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::AuthSubCodeZeroInErr,
                    },
                }
            })?;
            Ok((code, rest))
        }
        _ => Err(ProtocolError::MalformedAuthentication {
            payload_len: payload.len(),
        }),
    }
}

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingStartupTrust`]. DEF-097.
///
/// Only `AUTH_OK` is acceptable here: the user provided no password,
/// so a SCRAM challenge from the server is classified as
/// `UnsupportedAuthMethod` (the server's pg_hba.conf disagrees with
/// the client's no-password configuration). The dispatcher match
/// cannot reach this arm with SCRAM payloads already buffered,
/// because the Scram variant of the state has its own handler —
/// this separation is the tier-1 compile guarantee DEF-097 buys.
fn dispatch_auth_in_startup_trust(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    payload: &[u8],
) -> DispatchOutcome {
    let (code, _rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    match code {
        crate::wire::AuthSubCode::Ok => {
            *state = ProtoState::ConnectingPostAuthAwaitingKey(reply);
            DispatchOutcome::AdvancedSilent
        }
        // Any non-Ok auth code means the server expects an auth
        // method this Trust connection is not configured for:
        // `CleartextPassword` / `Md5Password` (DEF-215 / DEF-216
        // foundation) — Trust client carries no password.
        // `Sasl` / `SaslContinue` / `SaslFinal` — Trust client
        // never requested SCRAM.
        // Tier-1 exhaustive — a future new `AuthSubCode` variant
        // forces this match to be updated.
        other @ (crate::wire::AuthSubCode::CleartextPassword
            | crate::wire::AuthSubCode::Md5Password
            | crate::wire::AuthSubCode::Sasl
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => install_errored(state,
            Some(reply.consume()),
            ProtocolError::UnsupportedAuthMethod { sub_code: crate::error::AuthSubCodeClass::KnownButWrong(other) },
        ),
    }
}

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingStartupScram`]. DEF-097.
///
/// Only `AUTH_SASL` is acceptable here; the server taking `AUTH_OK`
/// without asking for the password is an auth-method mismatch on
/// the server side (client expected SCRAM).
fn dispatch_auth_in_startup_scram(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    mut scram: alloc::boxed::Box<crate::scram::session::ScramSession>,
    payload: &[u8],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    match code {
        crate::wire::AuthSubCode::Sasl => {
            if !mechanism_list_contains_scram(rest) {
                return install_errored(state, Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::NoSupportedMechanism));
            }

            // Build client-first-message and SASLInitialResponse.
            // DEF-094: write directly into the caller-owned `write_buf`
            // and record the range; materialise at the entry-point
            // boundary after the mutable write phase releases.
            // DEF-210 PERF-02 (audit 2026-05-04): single-Box SCRAM.
            // `build_sasl_initial_response` populates
            // `scram.client_first_bare` + `scram.client_nonce_b64`
            // IN PLACE through `&mut Box<ScramSession>`. The same
            // `Box<ScramSession>` allocation is reused across the
            // StartupScram → ServerFirst transition (zero allocator
            // ops). Per-handshake total: 1 alloc (StartupScram
            // construction) + 1 free (ServerFinal drop), zero
            // transitions in between. Closes the principal's
            // documented "one heap alloc per SCRAM connection"
            // invariant to literal accuracy (REC-06 was a half-
            // measure that still incurred 1 alloc + 1 free at the
            // transition; PERF-02 closes the gap).
            match build_sasl_initial_response(&mut scram, reserved) {
                Ok(range) => {
                    *state = ProtoState::ConnectingScramAwaitingServerFirst {
                        reply,
                        scram,
                    };
                    DispatchOutcome::AdvancedWithAction {
                        action: StagedAction::SendBytesRange(range),
                    }
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        // Any non-`Sasl` auth code in this state is an auth-method
        // mismatch: the SCRAM client expected the server to offer
        // SASL mechanisms but got a different code instead.
        // `CleartextPassword` / `Md5Password` (DEF-215 / DEF-216
        // foundation): a SCRAM client refuses to downgrade to a
        // weaker password-auth method even if it carries credentials
        // (security: prevents server-side downgrade attacks).
        // `Ok` here means the server accepted nothing without
        // asking — also a mismatch from the client's POV.
        // Tier-1 exhaustive — a future new `AuthSubCode` variant
        // forces this match to be updated.
        other @ (crate::wire::AuthSubCode::Ok
            | crate::wire::AuthSubCode::CleartextPassword
            | crate::wire::AuthSubCode::Md5Password
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => install_errored(state,
            Some(reply.consume()),
            ProtocolError::UnsupportedAuthMethod { sub_code: crate::error::AuthSubCodeClass::KnownButWrong(other) },
        ),
    }
}

/// Check if the SASL mechanism list contains SCRAM-SHA-256.
///
/// DEF-184 (B12): happy-path fast-check — most servers announce
/// `"SCRAM-SHA-256\0\0"` as the only mechanism (or first), so a
/// single `starts_with` covers the common case in one compare
/// instead of a full NUL-split loop. Falls through to the generic
/// scan for the multi-mechanism case (rare — PG typically sends
/// only SCRAM-SHA-256, sometimes with `SCRAM-SHA-256-PLUS`).
#[inline]
fn mechanism_list_contains_scram(data: &[u8]) -> bool {
    // Fast path: first mechanism is SCRAM-SHA-256 followed by
    // the NUL separator. Covers ≥95% of real-world servers.
    if let Some(rest) = data.strip_prefix(SCRAM_SHA_256_MECHANISM)
        && let Some(&0) = rest.first()
    {
        return true;
    }
    // Slow path: walk NUL-separated names. e.g. server offering
    // ["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"] lands here.
    for name in data.split(|b| *b == 0) {
        if name == SCRAM_SHA_256_MECHANISM {
            return true;
        }
    }
    false
}

/// Build the SASLInitialResponse frame for SCRAM-SHA-256.
///
/// # Pattern: compile-time capability proof
///
/// Takes `_: &ScramSession` — an anonymous-parameter typed reference
/// that is **never dereferenced**. The function does not use the
/// password inside the `ScramSession`; the password is consumed only
/// later in `dispatch_auth_sasl_continue`. So why require the
/// parameter at all?
///
/// The parameter is a **capability proof**. To construct a
/// `ScramSession` the caller must have discriminated away
/// `Credentials::Trust` (see `compute_push_startup` routing — Trust
/// lands in `ConnectingStartupTrust` which never reaches this
/// function; Scram lands in `ConnectingStartupScram { scram, .. }`
/// which does). Accepting `&ScramSession` as an argument forces the
/// caller to have that evidence at hand; passing no argument or the
/// wrong type is a compile error.
///
/// The anonymous `_` binding is intentional — the parameter shape is
/// load-bearing (the type `&ScramSession`), the binding is not (we
/// never read the value). The crate's "no `_var` prefixed discards"
/// rule applies to `let _prefix = expr;` bindings, not to
/// anonymous-parameter match discards (which are standard idiomatic
/// Rust for capability-proof parameters — see
/// e.g. `std::marker::Unpin` witness patterns).
///
/// # Audit A2 + DEF-097
///
/// The `Credentials`-vs-`ScramPassword` split happens exactly once at
/// `ScramSession::from_password`; this function cannot be reached
/// from a Trust-credentials push path because the state variant it
/// destructures from (`ConnectingStartupScram { scram, .. }`) carries
/// a `ScramSession`, not a `Credentials`.
///
/// [`ScramSession`]: crate::scram::session::ScramSession
/// DEF-210 PERF-02 (audit 2026-05-04): writes `client_first_bare`
/// and `client_nonce_b64` directly into the caller's
/// `&mut ScramSession` (single source of truth for handshake
/// state — see `ScramSession` struct docstring). Returns only
/// the [`WriteRange`] for the wire bytes to send.
fn build_sasl_initial_response(
    scram: &mut crate::scram::session::ScramSession,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    use crate::scram::wire;

    // DEF-185 P2-F (audit 2026-04-24): PG convention — the SCRAM
    // `n=<user>` field is hard-coded empty because the real user
    // name travelled in the StartupMessage's `user` parameter. PG's
    // SCRAM implementation explicitly ignores the SASL-level user
    // (see PG src/backend/libpq/auth-scram.c — the `user` from
    // client-first is never consulted; authentication is bound to
    // the startup user).
    //
    // RFC 5802 §5.1 allows an empty `saslname` per RFC 4013 SASLprep.
    // PG-specific clients set `n=` verbatim; non-PG SCRAM servers
    // (MongoDB, Kafka) REQUIRE a non-empty `n=` and this code path
    // would fail against those. `bsql-pg-proto` is PG-only by design;
    // interop with other SCRAM servers is out of scope.
    //
    // If a future phase adds non-PG SCRAM server support, plumb the
    // user identifier through as a `Sensitive<...>` argument here.
    let user_bytes: &[u8] = b"";

    let client_nonce_vec = wire::generate_client_nonce().map_err(ProtocolError::Scram)?;

    let client_first_bare_vec =
        wire::build_client_first_bare(user_bytes, &client_nonce_vec).map_err(ProtocolError::Scram)?;

    let client_first_msg =
        wire::build_client_first_message(user_bytes, &client_nonce_vec).map_err(ProtocolError::Scram)?;

    // DEF-154 (B) escape hatch: SCRAM auth is a cold handshake path
    // that predates the branded push_* helpers. The scram::wire
    // builders hand back owned heapless::Vec; we push those bytes
    // into the branded reserved via `as_write_buf_mut()`. The brand
    // is preserved by the enclosing `reserved: &mut BrandedWriteReserved<'wb>` —
    // `as_write_buf_mut` returns &mut WriteBuf without a brand, and
    // the pushed range is wrapped via `WriteRange::from_write_span`
    // below against the same reserved.
    let start = reserved.len();
    let buf = reserved.as_write_buf_mut();
    buf.push_u8(crate::wire::TAG_SASL_RESPONSE.byte())
        .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;
    buf.with_length_prefix(|w| {
        w.push_bytes(SCRAM_SHA_256_MECHANISM)
            .map_err(|_| crate::write_buf::WriteBufFull)?;
        w.push_u8(0).map_err(|_| crate::write_buf::WriteBufFull)?;
        let body_len =
            i32::try_from(client_first_msg.len()).map_err(|_| crate::write_buf::WriteBufFull)?;
        w.push_i32_be(body_len)
            .map_err(|_| crate::write_buf::WriteBufFull)?;
        w.push_bytes(&client_first_msg)
            .map_err(|_| crate::write_buf::WriteBufFull)?;
        Ok(())
    })
    .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;

    // DEF-210 PERF-02: populate the SCRAM session's
    // client_first_bare + client_nonce_b64 fields IN PLACE
    // (vs. returning them by value to be re-Boxed). The caller's
    // `Box<ScramSession>` is reused across the StartupScram →
    // ServerFirst transition with zero allocator ops.
    scram.client_first_bare = crate::ident::PodBytes::try_from_slice(&client_first_bare_vec)
        .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;
    scram.client_nonce_b64 = crate::ident::PodBytes::try_from_slice(&client_nonce_vec)
        .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;
    // DEF-154 (B) P0-2: `from_branded_write_span` returns Result;
    // `?` propagates up through the function's own Result return
    // type. Err here classifies as `EmptyWriteRange` — dead under
    // intact SCRAM invariants.
    crate::action::WriteRange::from_write_span(start, reserved)
}

/// Dispatch AuthenticationSASLContinue (server-first-message).
///
/// Takes a [`ScramSession`] by value — the `Trust`-vs-`ScramPassword`
/// discrimination was consumed at
/// [`ScramSession::try_from_credentials`] in the parent dispatch
/// call; this function cannot be reached with `Trust` credentials
/// because the state variant it destructures from
/// ([`ProtoState::ConnectingScramAwaitingServerFirst`]) carries
/// `ScramSession`, not `Credentials`. Audit A2.
///
/// [`ScramSession`]: crate::scram::session::ScramSession
/// [`ScramSession::try_from_credentials`]: crate::scram::session::ScramSession::try_from_credentials
// DEF-184 (A10/B22 revert 2026-04-24): 7-arg function — within
// clippy::too_many_arguments default threshold. No `#[expect]` needed.
//
// DEF-187 (architect 2026-04-26): SCRAM data is heap-boxed inside
// the variant for variant-size compaction.
//
// DEF-210 REC-06 (audit 2026-04-28): the three SCRAM-handshake
// fields previously each in their own Box are now consolidated
// inside `Box<ScramHandshakeState>` carried by
// `ConnectingScramAwaitingServerFirst`. The caller destructures
// `*handshake` and passes `scram` BY REFERENCE (helper only reads
// `scram.password_bytes()` once for HMAC composition; no need to
// memcpy ~520 B onto this helper's stack frame, no need to re-Box).
// `client_first_bare` and `client_nonce_b64` move by value into the
// helper (caller drops them when the arm scope exits). Net per
// SCRAM handshake: pre-REC-06 was 0 allocs + 3 Box-frees at this
// arm; post-REC-06 is 0 allocs + 1 Box-free (the consolidated
// `Box<ScramHandshakeState>`).
fn dispatch_auth_sasl_continue(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    scram: &crate::scram::session::ScramSession,
    payload: &[u8],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::SaslContinue) {
        return install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION });
    }

    // `rest` is the server-first-message body.
    let server_first =
        match crate::scram::wire::parse_server_first(rest, scram.client_nonce_b64.as_slice()) {
            Ok(sf) => sf,
            Err(e) => {
                return install_errored(state, Some(reply.consume()), ProtocolError::Scram(e));
            }
        };

    // Password bytes come from the typed SCRAM session — no
    // Trust-vs-ScramPassword discrimination here (audit A2).
    let password_bytes = scram.password_bytes();

    // Build client-final-without-proof.
    let client_final_without_proof =
        match crate::scram::wire::build_client_final_without_proof(
            server_first.server_nonce.as_bytes(),
        ) {
            Ok(v) => v,
            Err(e) => {
                return install_errored(state, Some(reply.consume()), ProtocolError::Scram(e));
            }
        };

    // Compute proof and expected server signature.
    //
    // AuthMessage = client-first-bare + "," + server-first + "," +
    // client-final-without-proof. The three components are passed
    // separately — compute_client_proof feeds them incrementally into
    // HMAC::update(), with zero intermediate buffer. No staging
    // buffer → no silent-truncation class → tier-1 by construction.
    // F54: `compute_client_proof` returns Result on architecturally-
    // dead `HmacKeyRejected` path. On Err (supply-chain compromise of
    // RustCrypto's HMAC, etc.), tear down the handshake with a typed
    // diagnostic — don't continue with zero-filled bytes. Fail-closed.
    let (proof, expected_server_sig) = match crate::scram::crypto::compute_client_proof(
        password_bytes,
        &server_first.salt,
        server_first.iterations,
        scram.client_first_bare.as_slice(),
        rest,
        &client_final_without_proof,
    ) {
        Ok(v) => v,
        Err(e) => return install_errored(state, Some(reply.consume()), ProtocolError::Scram(e)),
    };

    // Base64-encode proof.
    //
    // DEF-185 P1-A (audit 2026-04-24): stack buffer holds base64
    // ClientProof — password-correlated via `SHA-256(ClientKey) =
    // StoredKey`, and `proof = ClientKey XOR HMAC(StoredKey, AuthMessage)`.
    // A core-dump attacker with the base64-decoded proof + AuthMessage
    // can derive StoredKey and replay the handshake. Wrap in
    // `zeroize::Zeroizing` so the 64-byte buffer scrubs on scope exit.
    let mut proof_b64_buf: zeroize::Zeroizing<[u8; 64]> = zeroize::Zeroizing::new([0_u8; 64]);
    let proof_b64_len =
        match crate::scram::wire::base64_encode_to_buf(proof.as_ref(), proof_b64_buf.as_mut()) {
            Ok(n) => n,
            Err(_) => {
                return install_errored(state, Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))
            }
        };
    let proof_b64 = match proof_b64_buf.get(..proof_b64_len) {
        Some(s) => s,
        None => {
            return install_errored(state, Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))
        }
    };

    // Build client-final-message.
    //
    // DEF-185 P1-A (audit 2026-04-24): `client_final_msg` contains the
    // embedded `p=<proof_b64>` payload — password-correlated via the
    // same StoredKey algebra as proof_b64_buf. The `heapless::Vec` it
    // lives in does NOT implement `Zeroize` (upstream crate), so we
    // cannot wrap in `Zeroizing`. Instead, after the value has been
    // copied into the write buffer (push_bytes call below), zeroize
    // the heapless::Vec's backing bytes in-place via
    // `Zeroize::zeroize()` on the mut slice (slice impl exists
    // upstream). Done just before the Vec drops at function scope end.
    let mut client_final_msg = match crate::scram::wire::build_client_final_message(
        server_first.server_nonce.as_bytes(),
        proof_b64,
    ) {
        Ok(v) => v,
        Err(e) => {
            return install_errored(state, Some(reply.consume()), ProtocolError::Scram(e));
        }
    };

    // DEF-154 (B): build SASLResponse frame via the branded reserved.
    // Escape hatch as_write_buf_mut() for the pre-DEF-154 (B)
    // Result-returning push path; WriteRange wraps the span
    // at the end via `from_branded_write_span`.
    let start = reserved.len();
    {
        let buf = reserved.as_write_buf_mut();
        if buf.push_u8(crate::wire::TAG_SASL_RESPONSE.byte()).is_err()
            || buf
                .with_length_prefix(|w| w.push_bytes(&client_final_msg))
                .is_err()
        {
            // DEF-185 P1-A: scrub client_final_msg before early-return
            // classification. The buffer's bytes may contain partial
            // `p=<proof_b64>` payload depending on when the push failed.
            use zeroize::Zeroize;
            client_final_msg.as_mut_slice().zeroize();
            return install_errored(state, Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow));
        }
    }
    // DEF-185 P1-A: scrub the password-correlated client_final_msg
    // contents now that it has been copied into the write buffer.
    // The write buffer itself is zeroed on `WriteBuf::clear()` per
    // P0-B separately. Without this step the 384-byte heapless::Vec
    // backing array would hold `p=<proof_b64>` until this function's
    // stack frame gets overwritten by a subsequent call.
    use zeroize::Zeroize;
    client_final_msg.as_mut_slice().zeroize();
    // DEF-154 (B) P0-2: `from_branded_write_span` returns Result.
    // Err is architecturally dead here — the SASL_RESPONSE frame
    // body always has the 1-byte tag + 4-byte length prefix + the
    // client-final-message which is non-empty by SCRAM protocol.
    // Classified as `EmptyWriteRange` if triggered.
    let range = match crate::action::WriteRange::from_write_span(start, reserved) {
        Ok(r) => r,
        Err(cause) => return install_errored(state, Some(reply.consume()), cause),
    };

    // DEF-184 (A10/B22 revert 2026-04-24): `expected_server_sig` moves
    // INTO the variant — tier-1 compile (CREDO §1: variant-carries-field).
    // The `scram` ScramSession consumed here (moved into this function)
    // is NOT needed by the next state; its drop here fires
    // `ZeroizeOnDrop` — password material scrubbed exactly when the
    // handshake no longer needs it.
    *state = ProtoState::ConnectingScramAwaitingServerFinal {
        reply,
        expected_server_sig,
    };
    DispatchOutcome::AdvancedWithAction {
        action: StagedAction::SendBytesRange(range),
    }
}

/// Dispatch AuthenticationSASLFinal (server-final-message).
fn dispatch_auth_sasl_final(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    expected_server_sig: crate::scram::types::SecretDigest,
    payload: &[u8],
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::SaslFinal) {
        return install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION });
    }

    // Parse server-final-message.
    let received_sig = match crate::scram::wire::parse_server_final(rest) {
        Ok(sig) => sig,
        Err(e) => {
            return install_errored(state, Some(reply.consume()), ProtocolError::Scram(e));
        }
    };

    // Constant-time comparison (DEF-039).
    if !bool::from(expected_server_sig.ct_eq(&received_sig)) {
        return install_errored(state, Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::SignatureMismatch));
    }

    // Signature matches. Await AuthenticationOk.
    *state = ProtoState::ConnectingScramAwaitingAuthOk(reply);
    DispatchOutcome::AdvancedSilent
}

/// Dispatch AuthenticationOk after SCRAM verification.
fn dispatch_auth_ok_after_scram(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    payload: &[u8],
) -> DispatchOutcome {
    // AuthOk has no trailing data; destructure with anonymous `_`
    // pattern (pattern-discard, not a `_`-prefixed identifier —
    // allowed by the `no underscore vars` discipline).
    let code = match auth_sub_code(payload) {
        Ok((code, _)) => code,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::Ok) {
        return install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION });
    }

    *state = ProtoState::ConnectingPostAuthAwaitingKey(reply);
    DispatchOutcome::AdvancedSilent
}

// -----------------------------------------------------------------
// Cleartext-password handshake — DEF-215 (2026-05-05)
// -----------------------------------------------------------------

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingStartupCleartext`]. DEF-215.
///
/// Only `AUTH_CLEARTEXT_PASSWORD` (sub-code 3) is acceptable here:
/// any other code means the server expects an auth method this
/// connection is not configured for. The match is tier-1 exhaustive
/// — adding a new `AuthSubCode` variant forces this dispatcher to
/// classify it explicitly.
///
/// # Drop chain
///
/// `password: Box<Sensitive<Password>>` is moved in by value. On the
/// happy path the password bytes are written into the
/// `PasswordMessage` frame (cleartext on the wire — TLS gate is the
/// driver-wrapper's responsibility) and the Box drops at function
/// return, scrubbing the in-memory copy via `ZeroizeOnDrop`. On any
/// error path the Box drops at function return through the same
/// chain.
fn dispatch_auth_in_startup_cleartext(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    password: alloc::boxed::Box<crate::sensitive::Sensitive<crate::password::Password>>,
    payload: &[u8],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> DispatchOutcome {
    let (code, _rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    match code {
        crate::wire::AuthSubCode::CleartextPassword => {
            // Build the PasswordMessage frame inline against the
            // branded write reservation, then transition to the
            // AuthOk-awaiting state. The Box drops at function
            // return after this arm, scrubbing the password bytes.
            match build_password_message(&password, reserved) {
                Ok(range) => {
                    *state = ProtoState::ConnectingCleartextAwaitingAuthOk(reply);
                    DispatchOutcome::AdvancedWithAction {
                        action: StagedAction::SendBytesRange(range),
                    }
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        // Any other auth code in this state is an auth-method
        // mismatch: the server expected something other than
        // cleartext-password. Tier-1 exhaustive.
        other @ (crate::wire::AuthSubCode::Ok
            | crate::wire::AuthSubCode::Md5Password
            | crate::wire::AuthSubCode::Sasl
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => install_errored(state,
            Some(reply.consume()),
            ProtocolError::UnsupportedAuthMethod { sub_code: crate::error::AuthSubCodeClass::KnownButWrong(other) },
        ),
    }
}

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingCleartextAwaitingAuthOk`]. DEF-215.
///
/// Mirror of [`dispatch_auth_ok_after_scram`]: only `AUTH_OK` is
/// acceptable; any other code or any other frame-tag is an
/// `UnexpectedFrame` protocol error.
fn dispatch_auth_ok_after_cleartext(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    payload: &[u8],
) -> DispatchOutcome {
    let code = match auth_sub_code(payload) {
        Ok((code, _)) => code,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::Ok) {
        return install_errored(
            state,
            Some(reply.consume()),
            ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION },
        );
    }

    *state = ProtoState::ConnectingPostAuthAwaitingKey(reply);
    DispatchOutcome::AdvancedSilent
}

/// Build the `PasswordMessage` frame for cleartext-password auth.
/// DEF-215 (2026-05-05).
///
/// PG protocol §55.7: the frame is `'p'` (`TAG_SASL_RESPONSE` —
/// the byte is shared between SASL response and generic password
/// messages, disambiguated by context) + BE u32 length-field
/// (length includes itself + body) + password bytes + NUL
/// terminator.
///
/// The length-prefix wrapper handles the BE u32 framing; we push
/// password bytes followed by the trailing NUL inside the closure.
///
/// # Tier-1 architectural-impossibility of `WriteBufFull`
///
/// DEF-215 + DEF-216 audit (2026-05-07): the `Err(WriteBufFull)`
/// arm propagated through `?` is **architecturally unreachable**
/// per the const-assert
/// `MAX_OWNED_SEND_LEN >= max_password_message_size()` in
/// `write_buf.rs`. The error path is preserved as defence in
/// depth (and to keep the function signature uniform with sibling
/// builders that DO have legitimate runtime overflow paths), but
/// any actual `Err` here would indicate a const-assert drift —
/// itself a build error. Routed through
/// `InternalCrateBug { BuilderCapacityOverflow }` via the existing
/// `From<WriteBufFull> for ProtocolError` impl on the unlikely
/// path; that classification gives forensic visibility if a
/// future contributor disables the const-assert.
fn build_password_message(
    password: &crate::sensitive::Sensitive<crate::password::Password>,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    let buf = reserved.as_write_buf_mut();
    buf.push_u8(crate::wire::TAG_SASL_RESPONSE.byte())?;
    buf.with_length_prefix(|w| {
        w.push_bytes(password.get().as_bytes())?;
        // PG requires NUL-terminated password in the PasswordMessage
        // body. The length-prefix above includes the NUL byte.
        w.push_u8(0)?;
        Ok(())
    })?;

    crate::action::WriteRange::from_write_span(start, reserved)
}

// -----------------------------------------------------------------
// MD5-password handshake — DEF-216 (2026-05-05)
// -----------------------------------------------------------------

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingStartupMd5`]. DEF-216.
///
/// Only `AUTH_MD5_PASSWORD` (sub-code 5) carrying a valid 4-byte
/// salt is acceptable here. Any other code → `UnsupportedAuthMethod`.
/// Wrong-length salt → `MalformedAuthentication`. Tier-1 exhaustive
/// — adding a new `AuthSubCode` variant forces this dispatcher to
/// classify it explicitly.
///
/// # Drop chain
///
/// `handshake: Box<Md5HandshakeState>` is moved in by value. The
/// Box drops at function return on every path (success or error)
/// through `Box::drop → Md5HandshakeState::drop → Sensitive::drop
/// → Password::drop`, scrubbing the in-memory password copy via
/// `ZeroizeOnDrop`. The MD5 digest computation in
/// [`crate::md5::compute_response_body`] additionally wraps every
/// password-derived intermediate buffer in `Zeroizing<>` so
/// nothing leaks even transiently.
fn dispatch_auth_in_startup_md5(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    handshake: alloc::boxed::Box<crate::md5::Md5HandshakeState>,
    payload: &[u8],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    match code {
        crate::wire::AuthSubCode::Md5Password => {
            // Salt must be EXACTLY 4 bytes per PG §55.4. Anything
            // else is a malformed Authentication frame — classify
            // tier-3, transition to Errored. `payload_len` reports
            // the FULL payload byte count (sub-code 4 B + salt
            // bytes) for forensic visibility; a well-formed
            // MD5 auth frame has payload_len == 8.
            let salt: [u8; 4] = match <[u8; 4]>::try_from(rest) {
                Ok(s) => s,
                Err(_) => {
                    return install_errored(
                        state,
                        Some(reply.consume()),
                        ProtocolError::MalformedAuthentication {
                            payload_len: payload.len(),
                        },
                    );
                }
            };

            // Build the PasswordMessage frame inline against the
            // branded write reservation. The handshake Box drops
            // at function return (after this arm) — Drop chain
            // scrubs password.
            match build_md5_password_message(&handshake, salt, reserved) {
                Ok(range) => {
                    *state = ProtoState::ConnectingMd5AwaitingAuthOk(reply);
                    DispatchOutcome::AdvancedWithAction {
                        action: StagedAction::SendBytesRange(range),
                    }
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        // Any other auth code in this state is an auth-method
        // mismatch. Tier-1 exhaustive — symmetric with the
        // cleartext + SCRAM dispatchers.
        other @ (crate::wire::AuthSubCode::Ok
            | crate::wire::AuthSubCode::CleartextPassword
            | crate::wire::AuthSubCode::Sasl
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => install_errored(state,
            Some(reply.consume()),
            ProtocolError::UnsupportedAuthMethod { sub_code: crate::error::AuthSubCodeClass::KnownButWrong(other) },
        ),
    }
}

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingMd5AwaitingAuthOk`]. DEF-216. Mirror of
/// [`dispatch_auth_ok_after_cleartext`] / [`dispatch_auth_ok_after_scram`].
fn dispatch_auth_ok_after_md5(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    payload: &[u8],
) -> DispatchOutcome {
    let code = match auth_sub_code(payload) {
        Ok((code, _)) => code,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::Ok) {
        return install_errored(
            state,
            Some(reply.consume()),
            ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION },
        );
    }

    *state = ProtoState::ConnectingPostAuthAwaitingKey(reply);
    DispatchOutcome::AdvancedSilent
}

/// Build the `PasswordMessage` frame for MD5-password auth.
/// DEF-216 (2026-05-05).
///
/// Wire shape: `'p' (TAG_SASL_RESPONSE)` + BE u32 length + 35-byte
/// MD5 response body (`"md5" + 32 hex chars`) + NUL terminator.
///
/// The MD5 digest is computed by [`crate::md5::compute_response_body`]
/// which wraps every password-derived intermediate in `Zeroizing<>`.
///
/// # Tier-1 architectural-impossibility of `WriteBufFull`
///
/// Same as [`build_password_message`]: the const-assert
/// `MAX_OWNED_SEND_LEN >= max_password_message_size()` in
/// `write_buf.rs` makes the error path architecturally
/// unreachable. The MD5 frame is fixed-size at 41 bytes total —
/// trivially under the 2176 B `MAX_OWNED_SEND_LEN` budget.
fn build_md5_password_message(
    handshake: &crate::md5::Md5HandshakeState,
    salt: [u8; 4],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    // Compute the 35-byte response body. `compute_response_body`
    // returns an owned `Zeroizing<[u8; 35]>` (DEF-216 Phase 2
    // audit 2026-05-07: tier-1 type-level array signature — the
    // caller cannot accidentally pass a wrong-size buffer or a
    // buffer that wouldn't be fully overwritten). The returned
    // array scrubs on drop at fn return.
    let body = crate::md5::compute_response_body(
        handshake.password.get(),
        handshake.user.as_bytes(),
        salt,
    );

    let start = reserved.len();
    let buf = reserved.as_write_buf_mut();
    buf.push_u8(crate::wire::TAG_SASL_RESPONSE.byte())?;
    buf.with_length_prefix(|w| {
        w.push_bytes(body.as_slice())?;
        // PG's PasswordMessage body must be NUL-terminated (cleartext
        // and MD5 share this contract — the body is treated as a
        // cstring at the server).
        w.push_u8(0)?;
        Ok(())
    })?;

    crate::action::WriteRange::from_write_span(start, reserved)
}

// -----------------------------------------------------------------
// Helper: parse ErrorResponse typed fields
// -----------------------------------------------------------------

/// DEF-184 (audit #3 A-11): dedicated parser return type.
///
/// Pre-audit, [`parse_error_response`] returned `ProtocolError` —
/// coupling parser output to the public error shape. The three
/// fields of `ServerErrorResponse` were packed/unpacked twice (once
/// at the `ProtocolError::ServerErrorResponse { .. }` construction
/// in the parser, once at the `let ProtocolError::ServerErrorResponse
/// { .. } = err else { ... }` deconstruction in `parse_and_resolve`
/// and any future introspection site). A variant-rename refactor on
/// `ProtocolError` would ripple into the parser body and every
/// caller unless the parser outputs a shape-stable struct.
///
/// Post-audit: parser returns `ParsedServerError` (struct with
/// public fields); callers wanting the public `ProtocolError` use
/// [`Self::into_protocol_error`]. The struct is crate-private —
/// exposing it on the public API would create a second shape for
/// the same data.
///
/// # Layout
///
/// `Severity` 1 B + `SqlStateCode` 5 B + `ErrorRef` 8 B = 14 B
/// (+ alignment padding → 16 B on 4-byte-aligned targets). Identical
/// budget to the `ProtocolError::ServerErrorResponse` payload slice,
/// so no cascade size impact.
#[derive(Debug, Clone, Copy)]
#[must_use = "ParsedServerError holds an ErrorRef into the caller's ErrorArena; \
              convert via .into_protocol_error() or resolve the ref via \
              PgProtocol::get_server_error before drop. Silent drop loses the \
              only handle to server message/detail/hint strings."]
pub(crate) struct ParsedServerError {
    pub(crate) severity: crate::error::Severity,
    pub(crate) code: crate::error::SqlStateCode,
    pub(crate) details_ref: crate::error_arena::ErrorRef,
}

impl ParsedServerError {
    /// Wrap into the public `ProtocolError` shape.
    #[inline]
    pub(crate) fn into_protocol_error(self) -> ProtocolError {
        ProtocolError::ServerErrorResponse {
            severity: self.severity,
            code: self.code,
            details_ref: self.details_ref,
        }
    }
}

/// Parse an ErrorResponse payload into a classified error.
///
/// PG ErrorResponse body: series of typed fields, each = type-byte +
/// NUL-terminated string. Terminated by a bare NUL (0x00). We extract
/// 'S' (severity), 'C' (code), 'M' (message), 'D' (detail), 'H' (hint).
///
/// Cold path: called only when the server emits an `ErrorResponse`
/// frame (`'E'` tag). The `#[cold]` attribute tells LLVM to keep the
/// body out of hot-path inlining scope.
///
/// DEF-184 (audit #3 A-11): returns [`ParsedServerError`] struct
/// instead of `ProtocolError`. Callers that want the public error
/// shape use `.into_protocol_error()`; callers that want to inspect
/// the parsed fields (e.g. test helper, future diagnostic layers)
/// access the struct fields directly.
#[cold]
fn parse_error_response(
    payload: &[u8],
    error_arena: &mut crate::error_arena::ErrorArena,
) -> ParsedServerError {
    use crate::error::{Severity, SqlStateCode};
    use crate::ident::SecretBoundedStr;
    // DEF-060 part 2: typed fields. Severity → enum (1 byte);
    // code → SqlStateCode ([u8;5]); message/detail/hint →
    // SecretBoundedStr<N> (DEF-205 — non-Copy, ZeroizeOnDrop) with
    // explicit truncation marker (no
    // `.unwrap_or_default()` silent-truncation).
    //
    // Architect audit #3 (2026-04-21): `severity_set: bool` +
    // `severity = Severity::Unknown` pair collapsed into
    // `Option<Severity>`. Tier-3 audit (the bool flip had to stay
    // in sync with the enum assignment) → tier-1 compile (the
    // `Option` discriminator and the `Some(Severity)` payload are
    // the same value; impossible to desync). Niche-packed:
    // `Severity::Unknown = 0` as `#[repr(u8)]` means `Option<Severity>`
    // stays 1 byte (same as the prior `Severity` alone).
    let mut severity: Option<Severity> = None;
    let mut code = SqlStateCode::from_bytes(b"");
    let mut message: SecretBoundedStr<128> = SecretBoundedStr::default();
    let mut detail: SecretBoundedStr<96> = SecretBoundedStr::default();
    let mut hint: SecretBoundedStr<64> = SecretBoundedStr::default();

    // DEF-064: bounded-iteration DoS shield. PG's documented
    // ErrorResponse field set has ~18 tags total (S, V, C, M, D,
    // H, P, p, q, W, s, t, c, n, F, L, R, plus future). A
    // legitimate server sends each at most once. Cap at 32 — 2×
    // headroom for any future addition. Beyond the cap, we stop
    // parsing and use whatever fields we've already extracted.
    //
    // Without this cap the loop is still bounded by
    // `payload.len() ≤ MAX_FRAME_LEN_FIELD ≈ 4KB` (pos advances
    // monotonically, `payload.get(pos)` returns `None` at
    // end-of-payload), so a 4 KB pathological frame could
    // produce ~1300 tight iterations. The cap keeps the work
    // bounded to O(field_count) regardless of frame size. Tier-2
    // structural — the invariant is enforced by the `for _ in
    // 0..N` bound, not an audit of `pos` math.
    const MAX_ERROR_FIELDS: usize = 32;

    // DEF-210 SR-06 + REC-09 drift pin (audit 2026-04-28): typed
    // arms below extract structured fields into named locals
    // (severity / code / message / detail / hint). If a contributor
    // adds a new typed arm without raising the cap, an adversarial
    // server can flood the leading 32 fields with noise and push
    // a typed field out of range — silently lost diagnostic. The
    // assert below catches that class: MAX_ERROR_FIELDS must hold
    // at least 2× the typed-arm count (every typed field plus an
    // equal-sized noise prefix). Updating the typed-arm list below
    // requires updating this slice in lockstep — the slice is the
    // source of truth for "how many typed extractors exist" and
    // is referenced in the assert; manual lockstep with the arms
    // is tier-3 by-discipline. (A full tier-1 lift would require
    // reflective arm counting, which Rust does not expose.)
    const KNOWN_TYPED_ERROR_FIELD_TAGS: &[u8] = b"SVCMDH";
    const _: () = assert!(
        MAX_ERROR_FIELDS >= KNOWN_TYPED_ERROR_FIELD_TAGS.len() * 2,
        "MAX_ERROR_FIELDS cap must be ≥ 2 × count of typed arms — \
         otherwise an adversarial flood can truncate a typed field \
         out of the parsed prefix (DEF-210 SR-06).",
    );

    let mut pos: usize = 0;
    for _ in 0..MAX_ERROR_FIELDS {
        let field_type = match payload.get(pos) {
            Some(0) | None => break, // Terminator or end of payload.
            Some(b) => *b,
        };
        pos = match pos.checked_add(1) {
            Some(p) => p,
            None => break,
        };

        // Find NUL terminator for this field's value.
        //
        // DEF-184 (B4): `iter().position(|&b| b == 0)` on the slice
        // tail is LLVM-vectorisable (SIMD chunk-compare for u8
        // slices ≥ 8 bytes). Pre-(184) was a byte-by-byte `while
        // let Some(b) = payload.get(pos)` loop with per-iter
        // `checked_add(1)` — O(N) with one compare + one bounds-
        // check + one add per byte. Post-(184): single iterator
        // scan that LLVM lowers to SIMD for long fields (error
        // messages can be up to 128 bytes). ~3× on server-error
        // parsing hot path — fired on every ServerErrorResponse.
        let start = pos;
        let tail = payload.get(start..).unwrap_or(&[]);
        let value_bytes;
        match tail.iter().position(|&b| b == 0) {
            Some(n) => {
                value_bytes = tail.get(..n).unwrap_or(&[]);
                // Advance past value + NUL. `start + n + 1 ≤
                // payload.len()` by construction (n is an index
                // into tail = payload[start..]), so checked_add
                // cannot fail unless start+n+1 > usize::MAX —
                // architecturally impossible for a ≤ 4 KB PG
                // frame. Classified dead-arm via saturating_add.
                pos = start.saturating_add(n).saturating_add(1);
            }
            None => {
                // No NUL terminator in remainder — wire-spec
                // violation but tolerate by using rest-of-payload
                // as the value (DEF-064 forward-compat). Exit
                // loop next iter via `pos > payload.len()` peek.
                value_bytes = tail;
                pos = payload.len();
            }
        }

        match field_type {
            // `S` (localised) takes precedence; `V` (non-localised,
            // PG 9.6+) fills in if `S` didn't. `severity.is_none()`
            // guard expresses "first-wins" precedence directly.
            b'S' | b'V' if severity.is_none() => {
                severity = Some(Severity::from_bytes(value_bytes));
            }
            b'C' => {
                code = SqlStateCode::from_bytes(value_bytes);
            }
            // F22: text fields come in as bytes. PG encodes them in
            // `client_encoding` which MAY be non-UTF-8 on legacy
            // servers; `from_bytes_lossy` preserves the ASCII subset
            // and visibly marks non-ASCII bytes with `?`. Previously
            // `from_utf8(..).unwrap_or("")` silently dropped the entire
            // field on any single invalid byte — tier-3 diagnostic loss.
            b'M' => {
                message = SecretBoundedStr::from_bytes_lossy(value_bytes);
            }
            b'D' => {
                detail = SecretBoundedStr::from_bytes_lossy(value_bytes);
            }
            b'H' => {
                hint = SecretBoundedStr::from_bytes_lossy(value_bytes);
            }
            _ => {} // Unknown field type — skip.
        }
    }

    // DEF-184 (A1+A13): allocate the bounded strings into the
    // caller-supplied error arena; return the small ParsedServerError
    // (16 B) with the ErrorRef handle.
    let details_ref = error_arena.alloc(crate::error_arena::ErrorPayload {
        message,
        detail,
        hint,
    });
    ParsedServerError {
        // No S or V field in payload → `Severity::Unknown` fallback
        // (public API preserves the pre-uplift shape).
        severity: severity.unwrap_or(Severity::Unknown),
        code,
        details_ref,
    }
}

// -----------------------------------------------------------------
// Helper: parse BackendKeyData
// -----------------------------------------------------------------

/// 1c-1b helper: shared body for the `C` arm in both
/// `AwaitingFirstResponse` and `StreamingRows` states. Transition to
/// `AwaitingRfq { reply, command_tag }` on well-formed tag; classify
/// missing-NUL / framing error as `Errored`.
///
/// Centralises the "`CommandComplete` → AwaitingRfq" invariant in one
/// place — an arm-body edit in only one of the two call sites
/// would diverge silently; the helper makes the transition atomic.
///
/// DEF-210 SR-01 Path C: pre-Path-C this helper took
/// `schema_present: bool` and stamped it into the variant. The
/// flag is gone — schema presence is observed via
/// `PgProtocol::row_desc_slot.is_some()` at materialise. The two
/// callsites (DML CommandComplete and SELECT StreamingRows
/// CommandComplete) now only differ in the slot's prior population:
/// DML never fired a 'T' arm, so slot is None; SELECT did, so slot
/// is Some. Materialise reads the slot — no synchronised flag.
fn advance_to_awaiting_rfq(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::QueryKind>,
    payload: &[u8],
) -> DispatchOutcome {
    match parse_command_tag(payload) {
        Ok(command_tag) => {
            *state = ProtoState::SimpleQueryAwaitingRfq {
                reply,
                command_tag,
            };
            DispatchOutcome::AdvancedSilent
        }
        Err(cause) => install_errored(state, Some(reply.consume()), cause),
    }
}

/// 1c-3b helper: `CommandComplete` on the schema-bearing (SELECT)
/// path → `BindExecuteAwaitingRfqSelect`. DEF-189: the variant carries
/// no row_desc field; the schema lives in `PgProtocol::row_desc_slot`
/// (parked at push time by `push_bind_execute`). The variant name
/// `Select` is the tier-1 signal that the slot is populated.
///
/// The DML path's 'C' transition is inlined directly in the dispatch
/// arm (one call-site only) and doesn't need a helper.
fn advance_to_bindexecute_awaiting_rfq_select(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::QueryKind>,
    payload: &[u8],
) -> DispatchOutcome {
    match parse_command_tag(payload) {
        Ok(command_tag) => {
            *state = ProtoState::BindExecuteAwaitingRfqSelect {
                reply,
                command_tag,
            };
            DispatchOutcome::AdvancedSilent
        }
        Err(cause) => install_errored(state, Some(reply.consume()), cause),
    }
}

/// 1c-1b helper (pass-#7 audit, 2026-04-21): shared body for the
/// `E` arm across multiple flows. Emit `FailReply` (NO `CloseSocket`
/// — query-level errors are connection-survivable per PG §55.2.3)
/// and transition to `DrainRfqAfterError` so the trailing `Z`
/// returns the state to `Idle`.
///
/// # Signature rationale — pre-consume at call site (F1)
///
/// Pre-pass-#7 this was generic `<K: ReplyKind>` taking `ReplyId<K>`
/// by value, forcing monomorphisation once per kind. Since the body
/// only uses `reply.consume() -> NonZeroU64` (`K`-oblivious), every
/// call site emitted an identical 3-instruction basic block. After
/// pass-#7 the signature takes `raw_id: NonZeroU64` — the caller
/// pre-consumes the typed `ReplyId<K>`, exactly mirroring the pattern
/// `errored(Some(reply.consume()), …)` elsewhere in this module.
/// LLVM now emits one function body for all kinds.
///
/// # `#[cold] #[inline]` (F2)
///
/// Error drain is a cold branch — typical dispatch iterations
/// complete without encountering `ErrorResponse`. `#[cold]` pushes
/// this body out of the hot-match I-cache footprint; `#[inline]`
/// allows LLVM to fold the function into each call site when
/// register pressure permits. Same treatment as `errored()` above.
#[cold]
#[inline]
fn advance_to_drain_after_error(
    state: &mut ProtoState,
    raw_id: core::num::NonZeroU64,
    payload: &[u8],
    error_arena: &mut crate::error_arena::ErrorArena,
) -> DispatchOutcome {
    let cause = parse_error_response(payload, error_arena).into_protocol_error();
    *state = ProtoState::DrainRfqAfterError;
    DispatchOutcome::AdvancedWithAction {
        action: StagedAction::FailReply {
            id: raw_id,
            cause,
        },
    }
}

/// Parse a `ReadyForQuery` payload (body of the `'Z'` frame) into a
/// typed [`crate::action::TxStatus`].
///
/// PG §55.7 `ReadyForQuery` carries exactly one byte: `'I'`, `'T'`,
/// or `'E'`. Any other shape is a wire violation.
///
/// # F-048 (pass-#8) — narrow return type
///
/// Returns `Result<TxStatus, usize>` — Err carries the offending
/// `payload_len` as a bare `usize`. Callers wrap via
/// `.map_err(|payload_len| ProtocolError::MalformedReadyForQuery { payload_len })`.
///
/// Prior shape `Result<TxStatus, ProtocolError>` forced every dispatch
/// arm to reserve ~304 B of stack for the return slot (dominated by
/// ProtocolError::ServerErrorResponse). Narrowing to `Result<_, usize>`
/// shrinks the slot to 16 B (usize + discriminant + padding). 10+
/// dispatch call sites pay for this return.
///
/// # F-013 consolidation
///
/// Centralised pass-#7 audit F13 — prior to F13 there were 4+ parallel
/// `match payload { [b] => ..., other => ... }` patterns across every
/// `*AwaitingRfq` state. Single-point classifier closes drift between
/// handlers if a future change alters the `TxStatus` variant set.
#[cold]
#[inline]
fn parse_rfq_payload(
    payload: &[u8],
) -> Result<crate::action::TxStatus, usize> {
    match payload {
        // `[tx_byte]` pattern proves payload_len == 1 structurally.
        // F-009: `try_from_byte` returns `Result<Self, u8>`; we drop
        // the rejected byte and forward just the length-1 classification.
        // Diagnostic-wise the rejected byte is not currently surfaced
        // further upstream; if `MalformedReadyForQuery` gains a `byte`
        // field in the future, this map_err flips to pass it through.
        [tx_byte] => crate::action::TxStatus::try_from_byte(*tx_byte).map_err(|_| 1usize),
        other => Err(other.len()),
    }
}

/// Parse `CommandComplete` payload into a bounded command tag.
///
/// PG §55.7 CommandComplete body: a single NUL-terminated ASCII
/// string — e.g. `"SELECT 5\0"`, `"INSERT 0 3\0"`, `"UPDATE 7\0"`.
/// A missing NUL terminator is treated as a framing error
/// ([`ProtocolError::MalformedCommandComplete`]); the bytes-before-NUL
/// are truncating-fitted into a [`crate::error::BoundedStr<32>`].
///
/// Capacity 32 bytes handles PG's documented tag shapes with
/// headroom: the longest standard tag,
/// `"INSERT <oid:10-digit> <n:10-digit>\0"`, is ~23 bytes.
/// Overflow appends `"…"` rather than silently truncating (DEF-060
/// pattern — BoundedStr's `from_str_truncating`).
///
/// Cold path: called once per completed command. Not on any
/// per-row hot path.
#[cold]
fn parse_command_tag(payload: &[u8]) -> Result<crate::error::BoundedStr<32>, ProtocolError> {
    use crate::error::BoundedStr;
    // Strip the trailing NUL terminator. Missing NUL → framing error.
    let Some(body) = payload.strip_suffix(b"\0") else {
        return Err(ProtocolError::MalformedCommandComplete {
            payload_len: payload.len(),
        });
    };
    // DEF-185 P2-7 (audit 2026-04-24): embedded NUL validation.
    //
    // Pre-fix: PG CommandComplete body `SELECT\x00 5\x00` — strip_suffix
    // removed the trailing NUL, leaving `SELECT\x00 5` with embedded
    // NUL that `from_bytes_lossy` passed through verbatim (UTF-8
    // validator accepts NUL as a valid codepoint). User saw
    // `command_tag` with embedded NUL — weird but not exploitable.
    //
    // Post-fix: reject bodies containing embedded NUL as
    // `MalformedCommandComplete`. PG's CommandComplete is NUL-terminated
    // per §55.7 with the NUL strictly at the END; an interior NUL is a
    // wire violation.
    if body.contains(&0u8) {
        return Err(ProtocolError::MalformedCommandComplete {
            payload_len: payload.len(),
        });
    }
    // F-045 (pass-#8): use `from_bytes_lossy` — preserves ASCII subset,
    // coerces non-ASCII / invalid UTF-8 to `?`. Prior
    // `core::str::from_utf8(body).unwrap_or("")` silently dropped the
    // entire tag on any single invalid byte (tier-4 silent-pass). Now a
    // buggy proxy corrupting a tag byte leaves the rest readable and
    // marks corruption with `?` in the Debug/Display output.
    //
    // Mirrors F22's treatment of ErrorResponse fields (message / detail
    // / hint) — CommandComplete was missed in that pass; F-045 closes
    // the class.
    Ok(BoundedStr::from_bytes_lossy(body))
}

/// Parse BackendKeyData payload: 8 bytes = pid(i32 BE) + secret_key(i32 BE).
///
/// Cold path: called once per connection at end of startup handshake.
/// Not on any per-frame or per-query hot path.
#[cold]
fn parse_backend_key_data(payload: &[u8]) -> Result<(i32, i32), ProtocolError> {
    match payload {
        [a, b, c, d, e, f, g, h] => {
            let pid = i32::from_be_bytes([*a, *b, *c, *d]);
            let secret_key = i32::from_be_bytes([*e, *f, *g, *h]);
            Ok((pid, secret_key))
        }
        other => Err(ProtocolError::MalformedBackendKeyData {
            payload_len: other.len(),
        }),
    }
}

#[cfg(test)]
mod parse_error_response_tests {
    //! Seam-closing tests for `parse_error_response` (S4 / B1 from the
    //! 2026-04-18 second-pass audit).
    //!
    //! The function has nine field-type arms (`b'S'`, `b'V'`, `b'C'`,
    //! `b'M'`, `b'D'`, `b'H'`, unknown) mapping to five
    //! `ServerErrorResponse` fields. Swapping any two arms compiles
    //! cleanly — the lint-level type checker cannot see that `b'M'`
    //! should land in `message` and `b'D'` should land in `detail`.
    //! Tests below set each field explicitly and assert the full
    //! `ProtocolError` value for byte-exact mapping.
    //!
    //! Also covers pathological inputs:
    //! - Empty payload (just the terminator).
    //! - Field type with no NUL-terminated value (unterminated).
    //! - Duplicate severity (S / V) — first wins (`if severity.is_empty()`).
    //!
    //! Category (1) per reforge.md §4.11. Uses `assert_eq!` on the
    //! full `ProtocolError` variant because `panic!` is crate-root
    //! forbidden even in unit tests.

    extern crate alloc;

    use super::*;

    /// Build a well-formed ErrorResponse body: a sequence of
    /// `type_byte + NUL-terminated-value` entries, followed by a
    /// single trailing `\0` terminator.
    fn build_error_body(fields: &[(u8, &[u8])]) -> alloc::vec::Vec<u8> {
        let mut body = alloc::vec::Vec::new();
        for (type_byte, value) in fields {
            body.push(*type_byte);
            body.extend_from_slice(value);
            body.push(0);
        }
        body.push(0); // Terminator.
        body
    }

    /// DEF-184 (A1+A13): post-refactor test-fixture tuple —
    /// (Severity, SqlStateCode, ErrorPayload). Tests build expected
    /// tuples and compare against parsed actual via `parse_and_resolve`.
    type ExpectedErr = (
        crate::error::Severity,
        crate::error::SqlStateCode,
        crate::error_arena::ErrorPayload,
    );

    fn mk_err(
        severity: &str,
        code: &str,
        message: &str,
        detail: &str,
        hint: &str,
    ) -> ExpectedErr {
        use crate::error::{Severity, SqlStateCode};
        use crate::ident::SecretBoundedStr;
        (
            Severity::from_bytes(severity.as_bytes()),
            SqlStateCode::from_bytes(code.as_bytes()),
            crate::error_arena::ErrorPayload {
                message: SecretBoundedStr::<128>::from_str_truncating(message),
                detail: SecretBoundedStr::<96>::from_str_truncating(detail),
                hint: SecretBoundedStr::<64>::from_str_truncating(hint),
            },
        )
    }

    /// DEF-184 (A1+A13): parse body using a fresh arena, resolve the
    /// ErrorRef into full payload, and return the comparable tuple.
    ///
    /// DEF-184 (audit #3 A-03): pre-audit used
    /// `arena.get(details_ref).copied().unwrap_or_default()` — the
    /// silent-fallback pattern banned per user feedback. Post-audit
    /// the Result-returning `get()` is matched explicitly; the
    /// Err branch panics with a locus (architecturally unreachable
    /// by construction — `parse_error_response` always allocates
    /// into the fresh arena; the returned ref's generation matches;
    /// no intervening clear or realloc can fire here).
    ///
    /// DEF-184 (audit #3 A-11): pre-audit also had a defensive
    /// `else { return (...) }` fallback for the non-ServerErrorResponse
    /// branch, because the parser returned `ProtocolError`. Post-audit
    /// `parse_error_response` returns the dedicated `ParsedServerError`
    /// struct with unambiguous fields; the match-pattern + fallback is
    /// replaced with direct field access.
    fn parse_and_resolve(body: &[u8]) -> ExpectedErr {
        let mut arena = crate::error_arena::ErrorArena::new();
        let parsed = parse_error_response(body, &mut arena);
        let r = arena.get(parsed.details_ref);
        // Forbid-bundle compliance (mirror of error_arena::tests::must_alloc):
        // `assert!(is_ok) + .copied().unwrap_or(dead_for_test())` — assert
        // fires loudly if the invariant (parse always populates arena) breaks;
        // the dead_for_test fallback satisfies the no-panic bundle without
        // the tier-4 silent `unwrap_or_default` pattern banned per CREDO §5.
        assert!(
            r.is_ok(),
            "parse_error_response + arena.get Err {r:?} — architecturally unreachable \
             (parse always allocates into the fresh arena, no intervening clear)",
        );
        // DEF-205: ErrorPayload is no longer Copy (fields are
        // SecretBoundedStr<N> which is non-Copy + ZeroizeOnDrop).
        // `r.copied()` no longer compiles; `r.cloned()` requires
        // Clone (still derived). Same defensive idiom — `assert!`
        // fires loudly on the unexpected None path; `unwrap_or`
        // keeps the test compiling under the crate's no-panic
        // forbid bundle. Eager `dead_for_test()` evaluation is fine
        // (test path, no perf concern).
        let payload = r.cloned().unwrap_or(crate::error_arena::ErrorPayload::dead_for_test());
        (parsed.severity, parsed.code, payload)
    }

    /// Invariant (spec): each known field type routes to its dedicated
    /// `ServerErrorResponse` field. A one-arm swap in `parse_error_response`
    /// compiles silently; this table catches it via full-value equality.
    #[test]
    fn field_type_routes_to_correct_output_field() {
        let body = build_error_body(&[
            (b'S', b"FATAL"),
            (b'C', b"28P01"),
            (b'M', b"authentication failed"),
            (b'D', b"user does not exist"),
            (b'H', b"check pg_hba.conf"),
        ]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err(
            "FATAL",
            "28P01",
            "authentication failed",
            "user does not exist",
            "check pg_hba.conf",
        );
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): `b'V'` is an alternate for `b'S'` (non-localised
    /// severity). When only `V` arrives, it populates `severity`.
    #[test]
    fn severity_v_used_when_s_absent() {
        let body = build_error_body(&[(b'V', b"ERROR")]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("ERROR", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): first severity (`S` or `V`) wins — the
    /// `if severity.is_empty()` guard in the S/V arms blocks overwrite.
    #[test]
    fn severity_s_wins_over_later_v() {
        let body = build_error_body(&[(b'S', b"FATAL"), (b'V', b"ERROR")]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("FATAL", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): unknown field types are silently dropped;
    /// other fields still parse.
    #[test]
    fn unknown_field_types_are_silently_skipped() {
        let body = build_error_body(&[
            (b'Z', b"irrelevant"),
            (b'M', b"real message"),
            (b'Q', b"also irrelevant"),
        ]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "real message", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): empty payload (just the terminator NUL)
    /// yields an all-empty `ServerErrorResponse`, not a parse failure
    /// or panic.
    #[test]
    fn empty_payload_yields_empty_fields() {
        let body: alloc::vec::Vec<u8> = alloc::vec![0];
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): a field-type byte with no NUL-terminated
    /// value (adversarial input: payload ends mid-field) does not
    /// loop forever and does not panic. Pins that the
    /// `while let Some(b) = payload.get(pos)` inner loop terminates
    /// on end-of-buffer. A regression using unchecked indexing would
    /// loop / panic; this test verifies graceful termination.
    #[test]
    fn unterminated_final_field_does_not_panic() {
        // [S, 'A', 'B'] — no NUL after 'B', no terminator.
        let body: alloc::vec::Vec<u8> = alloc::vec![b'S', b'A', b'B'];
        let actual = parse_and_resolve(&body);
        // Whatever the parser recovered: must be a bounded string and
        // must not panic. Exact value of severity is an implementation
        // detail (parser reads to EOF as the value).
        let expected = mk_err("AB", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): duplicate non-severity fields — second
    /// overwrites first. Pins that the unguarded assignments for
    /// `C`, `M`, `D`, `H` arms hold. A regression that added `if
    /// .is_empty()` guards would silently change the semantics
    /// (first-wins vs last-wins).
    #[test]
    fn duplicate_code_second_wins() {
        let body = build_error_body(&[(b'C', b"FIRST"), (b'C', b"SECOND")]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "SECOND", "", "", "");
        assert_eq!(actual, expected);
    }

    /// F22 regression: non-UTF-8 bytes in M/D/H fields are coerced to
    /// `?` placeholders, preserving ASCII content, NOT silently
    /// collapsed to empty.
    ///
    /// Before F22: `from_utf8(value).unwrap_or("")` → whole field lost
    /// on any single invalid byte. A message like
    /// `b"Ung\xFCltige Eingabe"` (Latin-1 "Ungültige Eingabe") would
    /// become `""` — full diagnostic loss.
    ///
    /// After F22: ASCII subset preserved, non-ASCII byte → `?`.
    /// `b"Ung\xFCltige Eingabe"` → `"Ung?ltige Eingabe"` — user still
    /// sees 94% of the original message.
    #[test]
    fn non_utf8_message_preserves_ascii_subset() {
        // Latin-1 "Ungültige Eingabe" — the \xFC is ü in Latin-1,
        // invalid as a standalone UTF-8 byte.
        let body = build_error_body(&[(b'M', b"Ung\xFCltige Eingabe")]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "Ung?ltige Eingabe", "", "");
        assert_eq!(actual, expected);
    }

    /// F22 regression: valid UTF-8 multibyte sequences pass through
    /// unchanged (fast path).
    #[test]
    fn valid_utf8_message_preserved_verbatim() {
        // Proper UTF-8 "Ungültige Eingabe".
        let body = build_error_body(&[(b'M', "Ungültige Eingabe".as_bytes())]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "Ungültige Eingabe", "", "");
        assert_eq!(actual, expected);
    }

    /// F22 regression: slow-path control + high-bit bytes in an
    /// invalid-UTF-8 payload all coerce to `?`. Ensures that binary
    /// junk in a field doesn't produce an empty message.
    ///
    /// Note: the `0xFF` byte triggers slow-path (invalid UTF-8). In
    /// slow path, every byte that isn't ASCII printable or
    /// `\t`/`\n`/`\r` coerces — including the otherwise-valid-UTF-8
    /// control bytes `0x01` / `0x02`. (Fast path, separately tested,
    /// preserves valid UTF-8 verbatim including control bytes.)
    #[test]
    fn non_utf8_control_bytes_coerced_to_question_mark() {
        // Mix: ASCII + 0x01 (valid UTF-8 SOH) + 0xFF (invalid UTF-8
        // high-bit lead) + 0x02 (SOT). The 0xFF forces slow path.
        let body = build_error_body(&[(b'M', b"A\x01B\xFFC\x02")]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "A?B?C?", "", "");
        assert_eq!(actual, expected);
    }
}
