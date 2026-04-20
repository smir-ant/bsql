//! PostgreSQL wire-protocol byte constants.
//!
//! These are compile-time `const` literals from the published PG wire
//! specification (PostgreSQL 17 §55.7, "Message Formats"). Any
//! modification is a protocol break and must be reviewed against the
//! upstream spec.
//!
//! The full PG protocol uses dozens of message tags; **Phase 1a only
//! ships the tags the Ping flow actually traverses**: `Sync` outbound,
//! `ReadyForQuery` and `ErrorResponse` inbound. Other tags land with
//! their drivers per reforge.md §3.5 (no manufactured constants).

/// Frontend `Sync` message tag (`'S'`).
///
/// Sent by the client to flush a pipelined batch. In Phase 1a we use it
/// as the Ping primitive: the only legal server response to a `Sync` in
/// `Idle` is a `ReadyForQuery`. PG protocol spec §55.2.4 (Extended Query).
pub const TAG_SYNC: u8 = b'S';

/// Backend `ReadyForQuery` message tag (`'Z'`).
///
/// Carries one byte of payload — the transaction status indicator
/// (`'I'` idle, `'T'` in-transaction, `'E'` failed transaction). In
/// Phase 1a we accept any of the three (we are layer-below the
/// transaction state machine; it lands in 1c).
pub const TAG_READY_FOR_QUERY: u8 = b'Z';

/// Backend `ErrorResponse` message tag (`'E'`).
///
/// A server-side error. Variable-length payload of typed fields. The
/// dispatcher's [`parse_error_response`][crate::error::ProtocolError::ServerErrorResponse]
/// extracts severity / code / message / detail / hint into a typed
/// `ServerErrorResponse` classification.
pub const TAG_ERROR_RESPONSE: u8 = b'E';

/// The complete `Sync` frame on the wire.
///
/// PG `Sync` has a 5-byte body: tag (`'S'`) + 4-byte length-field
/// (value `4`, big-endian — the length includes itself but excludes
/// the tag).
///
/// This is a `&'static [u8]` because the message is parameter-free; we
/// ship it via [`crate::action::SendBuf`] with a 5-byte memcpy into
/// the bounded stack buffer (DEF-089 collapsed the Static/Owned enum
/// to a single-shape newtype; zero-copy recovery waits for the
/// lifetime-parametrised `Action<'buf>` redesign in Phase 1c).
pub const SYNC_WIRE_BYTES: [u8; 5] = [TAG_SYNC, 0, 0, 0, 4];

// ---------------------------------------------------------------
// Phase 1b tags
// ---------------------------------------------------------------

/// Backend `Authentication*` message tag (`'R'`).
///
/// Carries a 4-byte sub-code indicating the authentication method:
/// 0 = Ok, 10 = SASL, 11 = SASLContinue, 12 = SASLFinal.
pub const TAG_AUTHENTICATION: u8 = b'R';

/// Backend `ParameterStatus` message tag (`'S'`).
///
/// Carries a key=NUL + value=NUL pair for a session parameter.
/// Reused for both inbound ParameterStatus and outbound Sync
/// (the outbound Sync uses `TAG_SYNC` = `b'S'` = same byte;
/// disambiguation is by direction — we only parse inbound `S`
/// as ParameterStatus during connecting states).
pub const TAG_PARAMETER_STATUS: u8 = b'S';

/// Backend `BackendKeyData` message tag (`'K'`).
///
/// Carries 8 bytes: pid (i32 BE) + secret_key (i32 BE).
pub const TAG_BACKEND_KEY_DATA: u8 = b'K';

/// Backend `NegotiateProtocolVersion` message tag (`'v'`).
///
/// Sent when the server does not support a requested protocol option.
/// DEF-044.
pub const TAG_NEGOTIATE_PROTOCOL_VERSION: u8 = b'v';

/// Frontend `SASLInitialResponse` / `SASLResponse` message tag (`'p'`).
///
/// Used for both the initial SASL response (mechanism + client-first)
/// and the subsequent SASL response (client-final).
pub const TAG_SASL_RESPONSE: u8 = b'p';

// ---------------------------------------------------------------
// Authentication sub-codes (first 4 bytes of 'R' payload)
// ---------------------------------------------------------------

/// `AuthenticationOk` — sub-code 0.
pub const AUTH_OK: u32 = 0;

/// `AuthenticationSASL` — sub-code 10. Mechanism list follows.
pub const AUTH_SASL: u32 = 10;

/// `AuthenticationSASLContinue` — sub-code 11. Server-first-message follows.
pub const AUTH_SASL_CONTINUE: u32 = 11;

/// `AuthenticationSASLFinal` — sub-code 12. Server-final-message follows.
pub const AUTH_SASL_FINAL: u32 = 12;

/// The SCRAM-SHA-256 mechanism name as bytes.
pub const SCRAM_SHA_256_MECHANISM: &[u8] = b"SCRAM-SHA-256";

/// PG protocol version 3.0 = 196608 (0x00030000).
pub const PROTOCOL_VERSION_3_0: u32 = 196608;

/// Compile-time check: `Sync` body length matches the spec.
///
/// Tier-1 against typo-induced wire breaks. If this assert fires, the
/// crate does not build.
const _: () = assert!(SYNC_WIRE_BYTES.len() == 5);
const _: () = assert!(SYNC_WIRE_BYTES[0] == b'S');
const _: () = assert!(
    SYNC_WIRE_BYTES[1] == 0
        && SYNC_WIRE_BYTES[2] == 0
        && SYNC_WIRE_BYTES[3] == 0
        && SYNC_WIRE_BYTES[4] == 4,
    "Sync length-field must be 4 (length includes itself, no payload)",
);

// ---------------------------------------------------------------
// Tag collision defenses (§10 of DEF-094 audit round 2, 2026-04-20)
//
// PG wire semantics: each direction has its own tag-space. Inside
// a direction, two distinct messages MUST carry distinct tag
// bytes; cross-direction collisions are fine (the canonical
// example is `TAG_SYNC` = `b'S'` outbound vs
// `TAG_PARAMETER_STATUS` = `b'S'` inbound — same byte, different
// direction, disambiguated at the dispatcher by "who initiated
// this frame").
//
// Without these asserts, a copy-paste introducing
// `TAG_FOO: u8 = b'E'` duplicating `TAG_ERROR_RESPONSE` would
// compile silently and silently hijack one message arm. The
// exhaustive `match` in `dispatch.rs` would see both arms;
// whichever is listed first wins. Tier-3 audit hazard.
//
// Per-direction pairwise distinctness is cheap to assert in
// `const` (N = 6 inbound, 2 outbound — N² comparisons compiled
// away). Lifts the invariant from tier-3 (audit) to tier-1 (build
// failure on drift).
// ---------------------------------------------------------------

/// Pairwise `const _: () = assert!(A != B, …)` distinctness at
/// build time — recursive macro expansion generates one
/// anonymous const per pair, auto-scaling as the caller's ident
/// list grows. Tier-1 compile. DEF-111 / DEF-116.
///
/// **Why macro, not `const fn`.** MSRV 1.95 blocks the obvious
/// `const fn walk(arr: &[u8])` form: safe `<[T]>::get(i)` is not
/// yet const-stable (rust-lang/rust#143874), and `arr[i]` is
/// banned by the crate-root `forbid(clippy::indexing_slicing)`
/// (forbid cannot be downgraded by `#[expect]`). Macro expansion
/// runs at parse time — no slice indexing, so neither blocker
/// applies. Fold back to a `const fn` when `<[T]>::get`
/// stabilises in const.
macro_rules! assert_all_distinct {
    // Base cases: empty / single element — nothing to compare.
    ($scope:literal $(,)?) => {};
    ($scope:literal, $single:ident $(,)?) => {};
    // Recursive case: emit `$first != $rest` for every rest ident,
    // then recurse on the tail (which picks up the next `$first`).
    ($scope:literal, $first:ident, $($rest:ident),+ $(,)?) => {
        $(
            const _: () = assert!(
                $first != $rest,
                concat!(
                    $scope,
                    " collision: `",
                    stringify!($first),
                    "` and `",
                    stringify!($rest),
                    "` share a value — dispatcher arms will collide.",
                ),
            );
        )+
        assert_all_distinct!($scope, $($rest),+);
    };
}

// **Inbound** (backend → frontend) tag-distinctness. Adding a
// new inbound tag const above? Add it to this invocation — the
// macro generates every new pairwise assertion automatically.
assert_all_distinct!(
    "inbound PG wire tag",
    TAG_READY_FOR_QUERY,
    TAG_ERROR_RESPONSE,
    TAG_AUTHENTICATION,
    TAG_PARAMETER_STATUS,
    TAG_BACKEND_KEY_DATA,
    TAG_NEGOTIATE_PROTOCOL_VERSION,
);

// **Outbound** (frontend → backend) tag-distinctness.
assert_all_distinct!("outbound PG wire tag", TAG_SYNC, TAG_SASL_RESPONSE);

// **Authentication sub-codes** distinctness. The sub-code is
// the first four bytes of an `AUTHENTICATION` payload; a
// collision would make two auth methods indistinguishable at the
// dispatcher.
assert_all_distinct!(
    "SCRAM auth sub-code",
    AUTH_OK,
    AUTH_SASL,
    AUTH_SASL_CONTINUE,
    AUTH_SASL_FINAL,
);
