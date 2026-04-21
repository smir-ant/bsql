//! PostgreSQL wire-protocol byte constants.
//!
//! These are compile-time `const` literals from the published PG wire
//! specification (PostgreSQL 17 §55.7, "Message Formats"). Any
//! modification is a protocol break and must be reviewed against the
//! upstream spec.
//!
//! # Tier-1 direction discipline
//!
//! Tags are typed by direction — [`InboundTag`] (backend → frontend)
//! and [`OutboundTag`] (frontend → backend) — via `#[repr(transparent)]`
//! newtypes around a raw byte. Cross-direction confusion is a
//! compile error: a dispatcher expecting `InboundTag` cannot receive
//! an `OutboundTag`, and vice-versa. PG's wire-tag space overlaps
//! between directions (e.g. `'E'` = `ErrorResponse` inbound vs
//! `Execute` outbound); the typed split eliminates that confusion
//! class at the type level rather than at audit time.

/// A PostgreSQL wire tag received from the server (backend → frontend).
///
/// `#[repr(transparent)]` over `u8` — zero runtime cost, same ABI
/// as a byte. Construction is crate-internal: external consumers
/// receive instances via [`crate::HeaderParse::Ok::tag`] from
/// [`crate::parse_header`] and can match against the named
/// constants ([`TAG_READY_FOR_QUERY`], [`TAG_DATA_ROW`], etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct InboundTag(u8);

impl InboundTag {
    /// Wrap a raw wire byte as an `InboundTag`. Crate-internal —
    /// the only legitimate source of an `InboundTag` is the frame
    /// parser, which consumes bytes received from the server.
    #[inline]
    pub(crate) const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    /// Extract the underlying wire byte.
    #[inline]
    #[must_use]
    pub const fn byte(self) -> u8 {
        self.0
    }
}

/// A PostgreSQL wire tag sent to the server (frontend → backend).
///
/// Mirror of [`InboundTag`] for the outbound direction. Used by the
/// protocol's frame builders ([`crate::PgProtocol::push_command`]
/// paths). Users never construct `OutboundTag` instances directly —
/// they reference the named constants ([`TAG_QUERY`], [`TAG_SYNC`],
/// etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct OutboundTag(u8);

impl OutboundTag {
    /// Crate-internal constructor — only the tag-declaration section
    /// below instantiates these. Private callers of
    /// [`OutboundTag::byte`] serialise the tag byte via
    /// [`crate::write_buf::WriteBuf::push_u8`].
    #[inline]
    pub(crate) const fn from_byte(b: u8) -> Self {
        Self(b)
    }

    /// Extract the underlying wire byte.
    #[inline]
    #[must_use]
    pub const fn byte(self) -> u8 {
        self.0
    }
}

/// Frontend `Sync` message tag (`'S'`).
///
/// Sent by the client to flush a pipelined batch. In Phase 1a we use it
/// as the Ping primitive: the only legal server response to a `Sync` in
/// `Idle` is a `ReadyForQuery`. PG protocol spec §55.2.4 (Extended Query).
pub const TAG_SYNC: OutboundTag = OutboundTag::from_byte(b'S');

/// Backend `ReadyForQuery` message tag (`'Z'`).
///
/// Carries one byte of payload — the transaction status indicator
/// (`'I'` idle, `'T'` in-transaction, `'E'` failed transaction). In
/// Phase 1a we accept any of the three (we are layer-below the
/// transaction state machine; it lands in 1c).
pub const TAG_READY_FOR_QUERY: InboundTag = InboundTag::from_byte(b'Z');

/// Backend `ErrorResponse` message tag (`'E'`).
///
/// A server-side error. Variable-length payload of typed fields. The
/// dispatcher's [`parse_error_response`][crate::error::ProtocolError::ServerErrorResponse]
/// extracts severity / code / message / detail / hint into a typed
/// `ServerErrorResponse` classification.
pub const TAG_ERROR_RESPONSE: InboundTag = InboundTag::from_byte(b'E');

/// The complete `Sync` frame on the wire.
///
/// PG `Sync` has a 5-byte body: tag (`'S'`) + 4-byte length-field
/// (value `4`, big-endian — the length includes itself but excludes
/// the tag).
///
/// This is a `&'static [u8]` because the message is parameter-free; we
/// ship it via a zero-copy static reference through [`crate::action::Action::SendBytes`].
///
/// # Visibility (F33 revision, 2026-04-21)
///
/// `pub(crate)` — NOT part of the user-facing API. Integration tests
/// used to reference this const to assert emitted bytes matched it,
/// but that was tautological (the emission IS this const, so the
/// test was essentially `SYNC_WIRE_BYTES == SYNC_WIRE_BYTES`). The
/// real drift-pin is the `const _: () = assert!(SYNC_WIRE_BYTES[0] == b'S')`
/// / `SYNC_WIRE_BYTES[1..=4] == [0,0,0,4]` assertions below, which
/// catch typo-induced wire breaks at BUILD time. Tests now assert
/// the LITERAL `[b'S', 0, 0, 0, 4]` instead — a stronger check that
/// fires if either the emission path OR the const drifts.
pub(crate) const SYNC_WIRE_BYTES: [u8; 5] = [TAG_SYNC.byte(), 0, 0, 0, 4];

// ---------------------------------------------------------------
// Phase 1b tags
// ---------------------------------------------------------------

/// Backend `Authentication*` message tag (`'R'`).
///
/// Carries a 4-byte sub-code indicating the authentication method:
/// 0 = Ok, 10 = SASL, 11 = SASLContinue, 12 = SASLFinal.
pub const TAG_AUTHENTICATION: InboundTag = InboundTag::from_byte(b'R');

/// Backend `ParameterStatus` message tag (`'S'`).
///
/// Carries a key=NUL + value=NUL pair for a session parameter.
/// Shares the byte with outbound `Sync` (`TAG_SYNC` = `b'S'`);
/// disambiguation is tier-1 compile now that [`InboundTag`] and
/// [`OutboundTag`] are distinct types — the dispatcher expecting
/// `InboundTag` cannot accidentally match against an `OutboundTag`.
pub const TAG_PARAMETER_STATUS: InboundTag = InboundTag::from_byte(b'S');

/// Backend `BackendKeyData` message tag (`'K'`).
///
/// Carries 8 bytes: pid (i32 BE) + secret_key (i32 BE).
pub const TAG_BACKEND_KEY_DATA: InboundTag = InboundTag::from_byte(b'K');

/// Backend `NegotiateProtocolVersion` message tag (`'v'`).
///
/// Sent when the server does not support a requested protocol option.
/// DEF-044.
pub const TAG_NEGOTIATE_PROTOCOL_VERSION: InboundTag = InboundTag::from_byte(b'v');

/// Backend `NoticeResponse` message tag (`'N'`).
///
/// PG emits `NoticeResponse` for advisory warnings (e.g. `NOTICE:
/// identifier will be truncated`). Any state can receive one at any
/// time — mid-query, during startup, in idle, etc. DEF-062 installs
/// a pre-dispatch filter in `feed_bytes` that silently consumes
/// notices and advances past, analogous to the `ParameterStatus`
/// filter (DEF-054). Future `Action::EmitNotice(...)` in Phase 1c+
/// will surface notices to the wrapper; Phase 1b drops them.
pub const TAG_NOTICE_RESPONSE: InboundTag = InboundTag::from_byte(b'N');

/// Frontend `SASLInitialResponse` / `SASLResponse` message tag (`'p'`).
///
/// Used for both the initial SASL response (mechanism + client-first)
/// and the subsequent SASL response (client-final).
pub const TAG_SASL_RESPONSE: OutboundTag = OutboundTag::from_byte(b'p');

// ---------------------------------------------------------------
// Phase 1c tags — Simple Query + Extended Query flow (PG §55.7)
// ---------------------------------------------------------------

// Inbound responses (backend → frontend):

/// Backend `RowDescription` message tag (`'T'`).
///
/// Describes the columns of a result set: name, type OID, size,
/// format. Precedes the run of `DataRow` frames for a query.
pub const TAG_ROW_DESCRIPTION: InboundTag = InboundTag::from_byte(b'T');

/// Backend `DataRow` message tag (`'D'`).
///
/// One row of a result set. Shares the byte with the frontend
/// `Describe` tag but is distinct at the type level ([`InboundTag`]
/// vs [`OutboundTag`]) — no runtime ambiguity possible.
pub const TAG_DATA_ROW: InboundTag = InboundTag::from_byte(b'D');

/// Backend `CommandComplete` message tag (`'C'`).
///
/// Signals end-of-result-set for the current command. Body is an
/// ASCII tag like `"SELECT 5"`, `"INSERT 0 3"`, `"UPDATE 7"`.
/// Shares the byte with the frontend `Close` tag — type-distinct.
pub const TAG_COMMAND_COMPLETE: InboundTag = InboundTag::from_byte(b'C');

/// Backend `EmptyQueryResponse` message tag (`'I'`).
///
/// Sent when the client submitted an empty / whitespace-only
/// simple-query string. Contains no body.
pub const TAG_EMPTY_QUERY_RESPONSE: InboundTag = InboundTag::from_byte(b'I');

/// Backend `NoData` message tag (`'n'`).
///
/// Sent after `Describe` when the described statement or portal
/// produces no result rows (e.g. `INSERT` without `RETURNING`).
/// Contains no body.
pub const TAG_NO_DATA: InboundTag = InboundTag::from_byte(b'n');

/// Backend `ParseComplete` message tag (`'1'`).
///
/// Sent after a successful `Parse` of a prepared statement.
/// Contains no body.
pub const TAG_PARSE_COMPLETE: InboundTag = InboundTag::from_byte(b'1');

/// Backend `BindComplete` message tag (`'2'`).
///
/// Sent after a successful `Bind` binding a portal to a prepared
/// statement's parameters. Contains no body.
pub const TAG_BIND_COMPLETE: InboundTag = InboundTag::from_byte(b'2');

/// Backend `CloseComplete` message tag (`'3'`).
///
/// Sent after a successful `Close` of a prepared statement or
/// portal. Contains no body.
pub const TAG_CLOSE_COMPLETE: InboundTag = InboundTag::from_byte(b'3');

/// Backend `ParameterDescription` message tag (`'t'`).
///
/// Sent in response to a `Describe` of a prepared statement:
/// lists the parameter type OIDs the statement expects.
pub const TAG_PARAMETER_DESCRIPTION: InboundTag = InboundTag::from_byte(b't');

// Outbound commands (frontend → backend):

/// Frontend `Query` message tag (`'Q'`) — simple-query string.
pub const TAG_QUERY: OutboundTag = OutboundTag::from_byte(b'Q');

/// Frontend `Parse` message tag (`'P'`) — prepare a statement.
pub const TAG_PARSE: OutboundTag = OutboundTag::from_byte(b'P');

/// Frontend `Bind` message tag (`'B'`) — bind parameters to a
/// prepared statement, producing a portal.
pub const TAG_BIND: OutboundTag = OutboundTag::from_byte(b'B');

/// Frontend `Describe` message tag (`'D'`) — request metadata for
/// a statement or portal. Shares the byte with backend `DataRow`
/// but is type-distinct ([`OutboundTag`] vs [`InboundTag`]).
pub const TAG_DESCRIBE: OutboundTag = OutboundTag::from_byte(b'D');

/// Frontend `Execute` message tag (`'E'`) — run a bound portal.
/// Shares the byte with backend `ErrorResponse` but is type-distinct.
pub const TAG_EXECUTE: OutboundTag = OutboundTag::from_byte(b'E');

/// Frontend `Close` message tag (`'C'`) — close a prepared
/// statement or portal. Shares the byte with backend
/// `CommandComplete` but is type-distinct.
pub const TAG_CLOSE: OutboundTag = OutboundTag::from_byte(b'C');

/// Frontend `Flush` message tag (`'H'`) — send buffered responses
/// without advancing the transaction state. (`Sync` commits the
/// implicit transaction and emits `ReadyForQuery`; `Flush` does
/// not.)
pub const TAG_FLUSH: OutboundTag = OutboundTag::from_byte(b'H');

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

/// Typed classification of PG `AuthenticationXxx` sub-codes.
///
/// PG's `AuthenticationXxx` frame carries a 4-byte BE `u32` code as
/// its first payload word. PG spec defines four values in our current
/// scope: `Ok` (0), `SASL` (10), `SASLContinue` (11), `SASLFinal` (12).
///
/// # Tier-1 compile benefits
///
/// Dispatch handlers previously matched on raw `u32`:
///
/// ```ignore
/// match code {
///     AUTH_SASL => ...,
///     _ => errored(..., UnsupportedAuthMethod { sub_code: code }),
/// }
/// ```
///
/// The `_` arm swallowed unknown codes alongside unhandled-but-known
/// codes. Adding a new legitimate sub-code (e.g., future
/// `AuthenticationMD5Password` support) would have no compile-time
/// indication at the handlers — they'd silently fall through to
/// `UnsupportedAuthMethod` even in states where the code is legal.
///
/// With [`AuthSubCode`] typed, handlers match on enum variants:
/// adding a new variant (e.g., `Md5` for AUTH_MD5_PASSWORD) forces
/// every handler to decide how to treat it — the compiler flags
/// any handler whose match is not exhaustive. Tier-3 audit on
/// "every sub-code is considered by every relevant handler" →
/// tier-1 compile.
///
/// # Unknown codes
///
/// The server may send any `u32`. [`AuthSubCode::try_from_u32`]
/// returns `None` for codes outside the 4 known values — callers
/// classify as `ProtocolError::UnsupportedAuthMethod` carrying the
/// raw u32. The enum itself stays closed (non-`#[non_exhaustive]`),
/// so exhaustive match works cleanly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum AuthSubCode {
    /// `AuthenticationOk` (0). Server accepted authentication.
    Ok = 0,
    /// `AuthenticationSASL` (10). Server offers SASL mechanisms.
    Sasl = 10,
    /// `AuthenticationSASLContinue` (11). Server-first-message follows.
    SaslContinue = 11,
    /// `AuthenticationSASLFinal` (12). Server-final-message follows.
    SaslFinal = 12,
}

impl AuthSubCode {
    /// Classify a raw wire sub-code. Returns `None` for codes outside
    /// the 4 PG-defined values.
    #[inline]
    #[must_use]
    pub const fn try_from_u32(code: u32) -> Option<Self> {
        match code {
            AUTH_OK => Some(Self::Ok),
            AUTH_SASL => Some(Self::Sasl),
            AUTH_SASL_CONTINUE => Some(Self::SaslContinue),
            AUTH_SASL_FINAL => Some(Self::SaslFinal),
            _ => None,
        }
    }

    /// The underlying wire u32 value. Used by handlers emitting
    /// `UnsupportedAuthMethod { sub_code }` diagnostics with the
    /// specific typed code.
    #[inline]
    #[must_use]
    pub const fn raw(self) -> u32 {
        match self {
            Self::Ok => AUTH_OK,
            Self::Sasl => AUTH_SASL,
            Self::SaslContinue => AUTH_SASL_CONTINUE,
            Self::SaslFinal => AUTH_SASL_FINAL,
        }
    }
}

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
    // Recursive case: emit `$first.byte() != $rest.byte()` for every
    // rest ident, then recurse on the tail. The `.byte()` const method
    // on `InboundTag`/`OutboundTag` newtypes unwraps to the underlying
    // `u8` for the const-time comparison — raw auth sub-codes (`u32`)
    // expose `.byte()` via a free-function path (see the auth-code
    // invocation below).
    ($scope:literal, $first:ident, $($rest:ident),+ $(,)?) => {
        $(
            const _: () = assert!(
                $first.byte() != $rest.byte(),
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

/// Sub-macro for the auth-code distinctness check. Auth codes are
/// raw `u32`s (not typed newtypes); this variant does the same
/// pairwise comparison without the `.byte()` method dispatch.
macro_rules! assert_all_distinct_raw {
    ($scope:literal $(,)?) => {};
    ($scope:literal, $single:ident $(,)?) => {};
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
        assert_all_distinct_raw!($scope, $($rest),+);
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
    TAG_NOTICE_RESPONSE,
    // Phase 1c additions:
    TAG_ROW_DESCRIPTION,
    TAG_DATA_ROW,
    TAG_COMMAND_COMPLETE,
    TAG_EMPTY_QUERY_RESPONSE,
    TAG_NO_DATA,
    TAG_PARSE_COMPLETE,
    TAG_BIND_COMPLETE,
    TAG_CLOSE_COMPLETE,
    TAG_PARAMETER_DESCRIPTION,
);

// **Outbound** (frontend → backend) tag-distinctness.
assert_all_distinct!(
    "outbound PG wire tag",
    TAG_SYNC,
    TAG_SASL_RESPONSE,
    // Phase 1c additions:
    TAG_QUERY,
    TAG_PARSE,
    TAG_BIND,
    TAG_DESCRIBE,
    TAG_EXECUTE,
    TAG_CLOSE,
    TAG_FLUSH,
);

// **Authentication sub-codes** distinctness. The sub-code is
// the first four bytes of an `AUTHENTICATION` payload; a
// collision would make two auth methods indistinguishable at the
// dispatcher.
assert_all_distinct_raw!(
    "SCRAM auth sub-code",
    AUTH_OK,
    AUTH_SASL,
    AUTH_SASL_CONTINUE,
    AUTH_SASL_FINAL,
);

// ---------------------------------------------------------------
// Tier-1 compile drift-pin: tag bytes and auth sub-codes match
// the PG wire spec (PG §55.7 "Message Formats"). A typo
// (`TAG_QUERY = b'q'` instead of `b'Q'`) or a deliberate rename
// would break the build here.
//
// This uplifts the "tag values match spec" invariant from
// tier-3 (documentation + audit) to tier-1 (compile-enforced).
// Complements `assert_all_distinct!` which only verified
// *distinctness* within a direction — silent drift of an
// individual tag to a wrong-but-distinct byte was possible.
// ---------------------------------------------------------------
const _: () = {
    // Inbound (backend → frontend). Compare `.byte()` of the newtype.
    assert!(TAG_READY_FOR_QUERY.byte() == b'Z', "TAG_READY_FOR_QUERY drift");
    assert!(TAG_ERROR_RESPONSE.byte() == b'E', "TAG_ERROR_RESPONSE drift");
    assert!(TAG_AUTHENTICATION.byte() == b'R', "TAG_AUTHENTICATION drift");
    assert!(TAG_PARAMETER_STATUS.byte() == b'S', "TAG_PARAMETER_STATUS drift");
    assert!(TAG_BACKEND_KEY_DATA.byte() == b'K', "TAG_BACKEND_KEY_DATA drift");
    assert!(TAG_NEGOTIATE_PROTOCOL_VERSION.byte() == b'v', "TAG_NEGOTIATE_PROTOCOL_VERSION drift");
    assert!(TAG_NOTICE_RESPONSE.byte() == b'N', "TAG_NOTICE_RESPONSE drift");
    assert!(TAG_ROW_DESCRIPTION.byte() == b'T', "TAG_ROW_DESCRIPTION drift");
    assert!(TAG_DATA_ROW.byte() == b'D', "TAG_DATA_ROW drift");
    assert!(TAG_COMMAND_COMPLETE.byte() == b'C', "TAG_COMMAND_COMPLETE drift");
    assert!(TAG_EMPTY_QUERY_RESPONSE.byte() == b'I', "TAG_EMPTY_QUERY_RESPONSE drift");
    assert!(TAG_NO_DATA.byte() == b'n', "TAG_NO_DATA drift");
    assert!(TAG_PARSE_COMPLETE.byte() == b'1', "TAG_PARSE_COMPLETE drift");
    assert!(TAG_BIND_COMPLETE.byte() == b'2', "TAG_BIND_COMPLETE drift");
    assert!(TAG_CLOSE_COMPLETE.byte() == b'3', "TAG_CLOSE_COMPLETE drift");
    assert!(TAG_PARAMETER_DESCRIPTION.byte() == b't', "TAG_PARAMETER_DESCRIPTION drift");

    // Outbound (frontend → backend).
    assert!(TAG_SYNC.byte() == b'S', "TAG_SYNC drift");
    assert!(TAG_SASL_RESPONSE.byte() == b'p', "TAG_SASL_RESPONSE drift");
    assert!(TAG_QUERY.byte() == b'Q', "TAG_QUERY drift");
    assert!(TAG_PARSE.byte() == b'P', "TAG_PARSE drift");
    assert!(TAG_BIND.byte() == b'B', "TAG_BIND drift");
    assert!(TAG_DESCRIBE.byte() == b'D', "TAG_DESCRIBE drift");
    assert!(TAG_EXECUTE.byte() == b'E', "TAG_EXECUTE drift");
    assert!(TAG_CLOSE.byte() == b'C', "TAG_CLOSE drift");
    assert!(TAG_FLUSH.byte() == b'H', "TAG_FLUSH drift");

    // Auth sub-codes (raw u32, no newtype).
    assert!(AUTH_OK == 0, "AUTH_OK drift");
    assert!(AUTH_SASL == 10, "AUTH_SASL drift");
    assert!(AUTH_SASL_CONTINUE == 11, "AUTH_SASL_CONTINUE drift");
    assert!(AUTH_SASL_FINAL == 12, "AUTH_SASL_FINAL drift");

    // Protocol version.
    assert!(PROTOCOL_VERSION_3_0 == 196608, "PROTOCOL_VERSION_3_0 drift from PG 3.0 (0x00030000)");
};
