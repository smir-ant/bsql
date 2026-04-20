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

/// **Inbound** (backend → frontend) tag-distinctness assertions.
///
/// DEF-111 / DEF-116 history: the round-3 audit proposed replacing
/// the hand-unrolled N² form below with a `const fn` that iterates
/// a `&[u8]` array via `<[T]>::get(i)`. That would auto-scale to
/// every future tag: adding a const would just require extending
/// the input array. **Blocked on MSRV 1.95**:
/// [`<[T]>::get`](core::slice) is not yet const-stable
/// (rust-lang/rust#143874). `assert!(arr[i] != arr[j])` is banned
/// by `forbid(clippy::indexing_slicing)` and `forbid` cannot be
/// downgraded by `#[expect]`. When `<[T]>::get` stabilises in
/// const context, fold the N² form below into a
/// `const fn assert_distinct_pairwise` helper; until then, new
/// inbound tags MUST be added to both (a) this conjunction and
/// (b) the `INBOUND_TAGS` list — tracked as DEF-116.
///
/// Tier today: tier-1 compile for the tags currently listed;
/// tier-3 audit on "remember to extend the chain when a new tag
/// is added". The tier of the invariant itself is tier-1; the
/// maintenance discipline around adding tags is tier-3.
const _: () = assert!(
    TAG_READY_FOR_QUERY != TAG_ERROR_RESPONSE
        && TAG_READY_FOR_QUERY != TAG_AUTHENTICATION
        && TAG_READY_FOR_QUERY != TAG_PARAMETER_STATUS
        && TAG_READY_FOR_QUERY != TAG_BACKEND_KEY_DATA
        && TAG_READY_FOR_QUERY != TAG_NEGOTIATE_PROTOCOL_VERSION
        && TAG_ERROR_RESPONSE != TAG_AUTHENTICATION
        && TAG_ERROR_RESPONSE != TAG_PARAMETER_STATUS
        && TAG_ERROR_RESPONSE != TAG_BACKEND_KEY_DATA
        && TAG_ERROR_RESPONSE != TAG_NEGOTIATE_PROTOCOL_VERSION
        && TAG_AUTHENTICATION != TAG_PARAMETER_STATUS
        && TAG_AUTHENTICATION != TAG_BACKEND_KEY_DATA
        && TAG_AUTHENTICATION != TAG_NEGOTIATE_PROTOCOL_VERSION
        && TAG_PARAMETER_STATUS != TAG_BACKEND_KEY_DATA
        && TAG_PARAMETER_STATUS != TAG_NEGOTIATE_PROTOCOL_VERSION
        && TAG_BACKEND_KEY_DATA != TAG_NEGOTIATE_PROTOCOL_VERSION,
    "Two inbound PG wire tags share a byte — dispatcher arms will collide. \
     If this assert fires, someone duplicated a const in wire.rs.",
);

/// Parallel list used by the runtime drift-guard test at the
/// bottom of this file. If you add a new inbound tag const above,
/// add it here AND in the conjunction above.
#[cfg(test)]
const INBOUND_TAGS_FOR_RUNTIME_CHECK: &[u8] = &[
    TAG_READY_FOR_QUERY,
    TAG_ERROR_RESPONSE,
    TAG_AUTHENTICATION,
    TAG_PARAMETER_STATUS,
    TAG_BACKEND_KEY_DATA,
    TAG_NEGOTIATE_PROTOCOL_VERSION,
];

/// **Outbound** (frontend → backend) tag-distinctness assertions.
const _: () = assert!(
    TAG_SYNC != TAG_SASL_RESPONSE,
    "Two outbound PG wire tags share a byte — frame construction will \
     silently target the wrong message type.",
);

#[cfg(test)]
const OUTBOUND_TAGS_FOR_RUNTIME_CHECK: &[u8] = &[TAG_SYNC, TAG_SASL_RESPONSE];

/// **Authentication sub-codes** distinctness. The sub-code is the
/// first four bytes of an `AUTHENTICATION` payload; a collision
/// would make two auth methods indistinguishable at the dispatcher.
const _: () = assert!(
    AUTH_OK != AUTH_SASL
        && AUTH_OK != AUTH_SASL_CONTINUE
        && AUTH_OK != AUTH_SASL_FINAL
        && AUTH_SASL != AUTH_SASL_CONTINUE
        && AUTH_SASL != AUTH_SASL_FINAL
        && AUTH_SASL_CONTINUE != AUTH_SASL_FINAL,
    "Two SCRAM auth sub-codes share a value — dispatcher arms will \
     collide on AUTH_* matching.",
);

#[cfg(test)]
const AUTH_SUBCODES_FOR_RUNTIME_CHECK: &[u32] =
    &[AUTH_OK, AUTH_SASL, AUTH_SASL_CONTINUE, AUTH_SASL_FINAL];

#[cfg(test)]
mod collision_drift_guard {
    //! DEF-116 runtime drift-guard (Category 2 — tier-3 invariant).
    //!
    //! The const assertions above already catch collisions at
    //! build time for the tags currently listed. The risk the
    //! const form does NOT catch: adding a new tag to
    //! `wire.rs` **without** also widening the conjunction.
    //! That's a maintenance miss, not a build break.
    //!
    //! These runtime tests walk the `*_FOR_RUNTIME_CHECK`
    //! parallel arrays pairwise and assert distinctness. If a
    //! contributor adds a tag to the array but forgets the
    //! conjunction, this test catches it at CI time (one tier up
    //! from pure audit, one below compile-error).
    //!
    //! When `<[T]>::get` stabilises in const context, fold these
    //! runtime tests into a `const fn` helper and collapse the
    //! two-list form into one — see DEF-116 note.

    use super::*;

    fn assert_distinct_u8(arr: &[u8], scope: &str) {
        for (i, &a) in arr.iter().enumerate() {
            for &b in arr.iter().skip(i.saturating_add(1)) {
                assert_ne!(a, b, "tag collision in {scope}: 0x{a:02x}");
            }
        }
    }

    fn assert_distinct_u32(arr: &[u32], scope: &str) {
        for (i, &a) in arr.iter().enumerate() {
            for &b in arr.iter().skip(i.saturating_add(1)) {
                assert_ne!(a, b, "sub-code collision in {scope}: {a}");
            }
        }
    }

    #[test]
    fn inbound_tags_pairwise_distinct() {
        assert_distinct_u8(INBOUND_TAGS_FOR_RUNTIME_CHECK, "INBOUND_TAGS");
    }

    #[test]
    fn outbound_tags_pairwise_distinct() {
        assert_distinct_u8(OUTBOUND_TAGS_FOR_RUNTIME_CHECK, "OUTBOUND_TAGS");
    }

    #[test]
    fn auth_subcodes_pairwise_distinct() {
        assert_distinct_u32(AUTH_SUBCODES_FOR_RUNTIME_CHECK, "AUTH_SUBCODES");
    }
}
