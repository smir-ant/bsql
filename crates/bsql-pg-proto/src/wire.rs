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
/// Sent by the client to flush a pipelined batch. Used here as the
/// Ping primitive: the only legal server response to a `Sync` in
/// `Idle` is a `ReadyForQuery`. PG protocol spec §55.2.4
/// (Extended Query).
pub const TAG_SYNC: OutboundTag = OutboundTag::from_byte(b'S');

/// Backend `ReadyForQuery` message tag (`'Z'`).
///
/// Carries one byte of payload — the transaction status indicator
/// (`'I'` idle, `'T'` in-transaction, `'E'` failed transaction). All
/// three are accepted at this layer (the transaction state machine
/// is a higher-level concern).
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
/// # Visibility
///
/// `pub(crate)` — NOT part of the user-facing API. A test that
/// referenced this const to assert emitted bytes matched it would
/// be tautological (the emission IS this const, so the test
/// collapses to `SYNC_WIRE_BYTES == SYNC_WIRE_BYTES`). The real
/// drift-pin is the `const _: () = assert!(SYNC_WIRE_BYTES[0] ==
/// b'S')` / `SYNC_WIRE_BYTES[1..=4] == [0,0,0,4]` assertions
/// below, which catch typo-induced wire breaks at BUILD time.
/// Tests assert the LITERAL `[b'S', 0, 0, 0, 4]` instead — a
/// stronger check that fires if either the emission path OR the
/// const drifts.
pub(crate) const SYNC_WIRE_BYTES: [u8; 5] = [TAG_SYNC.byte(), 0, 0, 0, 4];

// ---------------------------------------------------------------
// Authentication-flow tags
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
pub const TAG_NEGOTIATE_PROTOCOL_VERSION: InboundTag = InboundTag::from_byte(b'v');

/// Backend `NoticeResponse` message tag (`'N'`).
///
/// PG emits `NoticeResponse` for advisory warnings (e.g. `NOTICE:
/// identifier will be truncated`). Any state can receive one at any
/// time — mid-query, during startup, in idle, etc. The pre-dispatch
/// filter in `feed_bytes` silently consumes notices and advances
/// past, analogous to the `ParameterStatus` filter; a future
/// `Action::EmitNotice(...)` would surface notices to the wrapper.
pub const TAG_NOTICE_RESPONSE: InboundTag = InboundTag::from_byte(b'N');

/// Frontend `SASLInitialResponse` / `SASLResponse` message tag (`'p'`).
///
/// Used for both the initial SASL response (mechanism + client-first)
/// and the subsequent SASL response (client-final).
pub const TAG_SASL_RESPONSE: OutboundTag = OutboundTag::from_byte(b'p');

// ---------------------------------------------------------------
// Simple Query + Extended Query flow tags (PG §55.7)
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
///
/// # Visibility
///
/// `pub(crate)` — `Close` frames are not currently sent by the
/// client, so the server should not emit `CloseComplete`. Any
/// server-sent `'3'` today falls through to the catch-all
/// `UnexpectedFrame` dispatch arm → teardown (correct defensive
/// behaviour: protocol desync → fatal). A `pub` visibility would
/// advertise a surface the dispatcher does not support; the
/// `pub(crate)` form keeps `InboundTag` introspection accurate
/// for downstream consumers.
pub(crate) const TAG_CLOSE_COMPLETE: InboundTag = InboundTag::from_byte(b'3');

/// Backend `ParameterDescription` message tag (`'t'`).
///
/// Sent in response to a `Describe` of a prepared statement:
/// lists the parameter type OIDs the statement expects.
pub const TAG_PARAMETER_DESCRIPTION: InboundTag = InboundTag::from_byte(b't');

/// Backend `PortalSuspended` message tag (`'s'`).
///
/// Sent mid-stream when an `Execute` with a non-zero `max_rows`
/// limit has produced its row quota. The portal is not closed —
/// a subsequent `Execute` continues from where this one paused.
///
/// **Current scope:** `max_rows = 0` (fetch all) is the only
/// supported shape; if a user-supplied `max_rows != 0` causes
/// the server to emit `PortalSuspended`, the dispatch path
/// classifies as `UnexpectedFrame` (tier-2 structural). Chunked
/// fetching with portal suspension is not yet implemented.
pub const TAG_PORTAL_SUSPENDED: InboundTag = InboundTag::from_byte(b's');

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

/// Target-byte discriminator carried inside a frontend `Describe`
/// frame (PG §55.2.2 field "S or P"). Typed enum with explicit
/// byte-valued discriminants — the only two legal values at this
/// wire slot are `'S'` (statement) and `'P'` (portal).
///
/// # Tier-1 vs raw `u8`
///
/// A `target: u8` parameter would compile cleanly for a call site
/// passing `b'X'` and produce a server `ErrorResponse` at runtime
/// (tier-3 audit seam). The typed enum moves discrimination to the
/// call site: `DescribeTargetByte::Statement` / `::Portal` are the
/// only paths to construct a value, and the `byte()` method folds
/// to a single literal at the monomorphic call site.
///
/// Const-asserts below pin the wire bytes to PG spec; an arm-body
/// edit swapping the two values would fail the build.
///
/// # NOT `#[non_exhaustive]`
///
/// PG §55.2.2 defines exactly two Describe targets (`'S'` statement,
/// `'P'` portal). Adding a third would be a major-protocol bump,
/// not a SemVer-compatible change. Closed-by-spec → exhaustive
/// `match` is the load-bearing tier-1 invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DescribeTargetByte {
    /// Describe a prepared statement previously created via
    /// `PgCommand::Parse`. Wire byte: `'S'`.
    ///
    /// Response shape: `ParameterDescription` (`'t'`) →
    /// `RowDescription` (`'T'`) or `NoData` (`'n'`) → `ReadyForQuery`.
    Statement = b'S',
    /// Describe a bound portal previously created via
    /// `push_bind_execute`. Wire byte: `'P'`.
    ///
    /// Response shape: `RowDescription` (`'T'`) or `NoData` (`'n'`)
    /// → `ReadyForQuery`. **No** `ParameterDescription` — the portal
    /// is already bound; its parameters are fixed at Bind time per
    /// PG §55.2.2.
    Portal = b'P',
}

impl DescribeTargetByte {
    /// The PG wire byte for this target. Explicit match (not `as u8`)
    /// — the crate forbids `clippy::as_conversions`. With the
    /// `#[repr(u8)]` discriminants above, each arm folds to a
    /// single literal load at the monomorphic call site.
    #[inline]
    #[must_use]
    pub const fn byte(self) -> u8 {
        match self {
            Self::Statement => b'S',
            Self::Portal => b'P',
        }
    }
}

// Drift-pin the wire bytes: an arm-body edit in `byte()` that
// swapped `b'S'` ↔ `b'P'` (or introduced any other value) would
// still compile, but the dispatcher's state machine would then
// route the wrong response shape — the statement-describe path
// expects a prior `ParameterDescription` (`'t'`) which the server
// does NOT emit for a portal-describe (per PG §55.2.2). A literal
// swap here would mean a statement-describe request is answered
// with a portal-shape response, which the dispatcher classifies
// as `UnexpectedFrame` on the missing `'t'` → tear-down.
//
// These const-asserts pin the invariant at build time. An edit
// that drifts `byte()` from the discriminant above fails
// immediately, with a pointer to PG §55.2.2.
const _: () = assert!(
    DescribeTargetByte::Statement.byte() == b'S',
    "DescribeTargetByte::Statement MUST wire-encode as b'S' per PG §55.2.2",
);
const _: () = assert!(
    DescribeTargetByte::Portal.byte() == b'P',
    "DescribeTargetByte::Portal MUST wire-encode as b'P' per PG §55.2.2",
);
// Belt-and-braces: if a future edit somehow lands both literals on
// the same byte (e.g. both `b'S'`), each per-variant assert above
// would still catch exactly one case, but this pairwise inequality
// is the tightest statement of "two distinct PG wire targets."
const _: () = assert!(
    DescribeTargetByte::Statement.byte() != DescribeTargetByte::Portal.byte(),
    "DescribeTargetByte variants MUST map to distinct wire bytes",
);

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

/// The complete `Flush` frame on the wire.
///
/// PG `Flush` has a 5-byte body: tag (`'H'`) + 4-byte length-field
/// (BE u32 = 4, length includes itself but excludes the tag).
///
/// Static byte literal. Same shape as the (`pub(crate)`)
/// `SYNC_WIRE_BYTES` and the public [`TERMINATE_WIRE_BYTES`] —
/// parameter-free, exclusive of any payload.
///
/// # Visibility
///
/// `pub` — part of the user-facing API (parallels
/// [`TERMINATE_WIRE_BYTES`]). Pipelining drivers write `Flush`
/// mid-batch to extract intermediate responses without committing
/// the implicit transaction (that's `Sync`'s job).
///
/// # Why a const, not a routed `Action::SendBytes`
///
/// `Flush` carries no parameters and no state-machine residue —
/// the entire wire form is these 5 bytes regardless of context.
/// Direct byte exposure lets driver code emit it without staging
/// through the protocol's WriteBuf (matches `TERMINATE_WIRE_BYTES`
/// convention).
///
/// # Usage
///
/// ```ignore
/// // Driver pseudocode — pipeline Bind+Execute then read partial
/// // results before committing.
/// proto.push_command(PgCommand::Bind { ... }, &mut wb)?;
/// proto.push_command(PgCommand::Execute { ... }, &mut wb)?;
/// socket.write_all(&bsql_pg_proto::FLUSH_WIRE_BYTES).await?;
/// // ... read intermediate frames ...
/// ```
pub const FLUSH_WIRE_BYTES: [u8; 5] = [TAG_FLUSH.byte(), 0, 0, 0, 4];

/// Frontend `Terminate` message tag (`'X'`) — graceful-close
/// primitive (PG §55.7 "Message Formats").
///
/// Sent immediately before TCP close. The server completes any
/// in-flight query (best effort), releases locks, and closes the
/// connection cleanly. Without `Terminate`, a TCP RST or FIN-only
/// teardown leaves the server in a transient confused state that
/// surfaces as connection-loss warnings in the server log and
/// momentarily-held locks.
///
/// `Terminate` carries no body — the entire wire form is the
/// 5-byte literal [`TERMINATE_WIRE_BYTES`] below. Same shape as
/// [`SYNC_WIRE_BYTES`] — tag + length-field of 4 (length includes
/// itself, no payload).
pub const TAG_TERMINATE: OutboundTag = OutboundTag::from_byte(b'X');

/// The complete `Terminate` frame on the wire.
///
/// Static byte literal. Mirrors [`SYNC_WIRE_BYTES`]: 5-byte body =
/// tag (`'X'`) + 4-byte length-field (BE u32 = 4, length includes
/// itself but excludes tag).
///
/// # Visibility
///
/// `pub` — part of the user-facing API. Wrapper drivers write these
/// bytes to the socket immediately before TCP close to signal
/// graceful shutdown. Direct byte exposure rather than a routed
/// `Action::SendBytes` because Terminate happens OUTSIDE the
/// state-machine envelope — it's the last frame, sent
/// unconditionally regardless of `ProtoState`.
///
/// # Usage
///
/// ```ignore
/// // Driver pseudocode:
/// async fn close(self) {
///     self.socket.write_all(&bsql_pg_proto::TERMINATE_WIRE_BYTES).await?;
///     self.socket.shutdown().await?;
/// }
/// ```
pub const TERMINATE_WIRE_BYTES: [u8; 5] = [TAG_TERMINATE.byte(), 0, 0, 0, 4];

/// The complete `SSLRequest` packet on the wire.
///
/// 8-byte StartupMessage-shaped probe sent BEFORE the real
/// StartupMessage on connections that want TLS. PG §55.10:
///
/// ```text
/// [length (BE u32 = 8)] [SSL_REQUEST_VERSION (BE u32 = 80877103)]
///   = [0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f]
/// ```
///
/// # Server response (out-of-band, NOT a tagged frame)
///
/// The server replies with a SINGLE byte (no length prefix, no
/// frame tag — it's a special pre-frame negotiation byte):
///
/// - `'S'` (0x53) — server accepts SSL; the driver immediately
///   performs the TLS handshake on the same socket, then sends
///   the real `StartupMessage` THROUGH TLS. All subsequent
///   protocol traffic is encrypted.
/// - `'N'` (0x4e) — server does NOT support SSL; the driver
///   decides per its `sslmode` policy whether to fall back to
///   plaintext (sending `StartupMessage` directly) or fail.
/// - Any other byte beginning an `ErrorResponse` ('E' = 0x45)
///   indicates a server-side error before SSL negotiation
///   completed; the driver reads the rest of the ErrorResponse
///   frame normally and surfaces it.
///
/// # Visibility
///
/// `pub` — the user-facing wire primitive. Wrapper drivers write
/// these bytes BEFORE constructing the `PgProtocol` state machine;
/// the byte handling for the 1-byte response is the driver's
/// concern (it lives outside this crate's frame parser, which
/// expects tagged + length-prefixed frames). Once TLS is
/// established, the driver constructs `PgProtocol::new()` and
/// pushes a normal `PgCommand::Startup`.
///
/// # State-machine integration
///
/// This primitive is wire-bytes-only, paralleling
/// [`TERMINATE_WIRE_BYTES`]. A future
/// `ProtoState::ConnectingPreSslAwaitingResponse` variant plus
/// response-byte feeder would lift the SSL probe into the state-
/// machine envelope; that integration is the driver wrapper's
/// concern, not this crate's.
///
/// # Usage
///
/// ```ignore
/// // Driver pseudocode:
/// async fn connect_tls(socket: &mut Socket) -> Result<TlsStream, Err> {
///     socket.write_all(&bsql_pg_proto::SSL_REQUEST_WIRE_BYTES).await?;
///     let mut response = [0u8; 1];
///     socket.read_exact(&mut response).await?;
///     match response[0] {
///         b'S' => perform_tls_handshake(socket).await,
///         b'N' => /* fallback per sslmode policy */,
///         b'E' => /* read ErrorResponse via frame parser */,
///         _    => /* protocol violation */,
///     }
/// }
/// ```
pub const SSL_REQUEST_WIRE_BYTES: [u8; 8] = [
    // Length: BE u32 = 8 (length includes itself; no separate body
    // beyond the 4-byte version code).
    0, 0, 0, 8,
    // SSL_REQUEST_VERSION = 80877103 in big-endian byte order.
    // Spelled as literal bytes here (not derived from the const)
    // to keep the array a pure byte literal — verification asserts
    // below pin it against `SSL_REQUEST_VERSION`'s `to_be_bytes`.
    0x04, 0xd2, 0x16, 0x2f,
];

// ---------------------------------------------------------------
// Authentication sub-codes (first 4 bytes of 'R' payload)
// ---------------------------------------------------------------

/// `AuthenticationOk` — sub-code 0.
pub const AUTH_OK: u32 = 0;

/// `AuthenticationCleartextPassword` — sub-code 3.
///
/// Server requests the user's password as a NUL-terminated
/// cleartext string in a `PasswordMessage` ('p') frame. Most
/// legacy on-prem PG configurations still default to this auth
/// method; SCRAM (sub-code 10) became the default only in PG 14.
pub const AUTH_CLEARTEXT_PASSWORD: u32 = 3;

/// `AuthenticationMD5Password` — sub-code 5.
///
/// Server requests the password salted and hashed via the legacy
/// MD5-based scheme. Client digest is `md5_hex(md5_hex(password
/// concat username) concat salt)`, prefixed with the literal
/// `"md5"`, sent in a `PasswordMessage`. Salt is 4 bytes from the
/// auth payload. Common in PG up to and including version 13 on
/// enterprise on-prem installs; PG 14 and newer default to SCRAM.
pub const AUTH_MD5_PASSWORD: u32 = 5;

/// `AuthenticationSASL` — sub-code 10. Mechanism list follows.
pub const AUTH_SASL: u32 = 10;

/// `AuthenticationSASLContinue` — sub-code 11. Server-first-message follows.
pub const AUTH_SASL_CONTINUE: u32 = 11;

/// `AuthenticationSASLFinal` — sub-code 12. Server-final-message follows.
pub const AUTH_SASL_FINAL: u32 = 12;

/// Typed classification of PG `AuthenticationXxx` sub-codes.
///
/// PG's `AuthenticationXxx` frame carries a 4-byte BE `u32` code as
/// its first payload word. PG spec defines six values in our current
/// scope: `Ok` (0), `CleartextPassword` (3), `Md5Password` (5),
/// `SASL` (10), `SASLContinue` (11), `SASLFinal` (12).
///
/// # Tier-1 compile benefits
///
/// Dispatch handlers previously matched on raw `u32`:
///
/// ```text
/// match code {
///     AUTH_SASL => ...,
///     _ => errored(..., UnsupportedAuthMethod { sub_code: code }),
/// }
/// ```
///
/// The snippet above is `text`-fenced (not `rust,ignore`) because
/// it is pre-`AuthSubCode` pseudo-code, not a runnable example —
/// `rust,ignore` would leave it compiler-unchecked and drift-prone.
///
/// The `_` arm in that shape swallows unknown codes alongside
/// unhandled-but-known codes. Adding a new legitimate sub-code
/// (e.g., future `AuthenticationMD5Password` support) would have
/// no compile-time indication at the handlers — they'd silently
/// fall through to `UnsupportedAuthMethod` even in states where
/// the code is legal.
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
/// returns `None` for codes outside the 6 known values — callers
/// classify as `ProtocolError::UnsupportedAuthMethod` carrying the
/// raw u32.
///
/// Pass #6 (MI10): marked `#[non_exhaustive]` — PG may add new
/// authentication methods (scram-sha-512, post-quantum, etc.) and
/// adding them here should not break downstream exhaustive matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
#[non_exhaustive]
pub enum AuthSubCode {
    /// `AuthenticationOk` (0). Server accepted authentication.
    Ok = 0,
    /// `AuthenticationCleartextPassword` (3).
    /// Server requests cleartext password in a `PasswordMessage`.
    /// Most legacy on-prem PG configs still default to this.
    CleartextPassword = 3,
    /// `AuthenticationMD5Password` (5).
    /// Server requests salted MD5 password digest in a
    /// `PasswordMessage`. Common in PG ≤ 13 enterprise installs.
    Md5Password = 5,
    /// `AuthenticationSASL` (10). Server offers SASL mechanisms.
    Sasl = 10,
    /// `AuthenticationSASLContinue` (11). Server-first-message follows.
    SaslContinue = 11,
    /// `AuthenticationSASLFinal` (12). Server-final-message follows.
    SaslFinal = 12,
}

impl AuthSubCode {
    /// Classify a raw wire sub-code.
    ///
    /// Returns `Err(code)` carrying the offending raw u32 for any
    /// code outside the 4 PG-defined values — matches the
    /// [`crate::decode::FormatCode::try_from_wire_i16`] and
    /// [`crate::action::TxStatus::try_from_byte`] shapes. Callers
    /// forward the raw value to
    /// `UnsupportedAuthMethod { sub_code: AuthSubCodeClass::Unknown(u) }`.
    #[inline]
    pub const fn try_from_u32(code: u32) -> Result<Self, u32> {
        match code {
            AUTH_OK => Ok(Self::Ok),
            AUTH_CLEARTEXT_PASSWORD => Ok(Self::CleartextPassword),
            AUTH_MD5_PASSWORD => Ok(Self::Md5Password),
            AUTH_SASL => Ok(Self::Sasl),
            AUTH_SASL_CONTINUE => Ok(Self::SaslContinue),
            AUTH_SASL_FINAL => Ok(Self::SaslFinal),
            other => Err(other),
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
            Self::CleartextPassword => AUTH_CLEARTEXT_PASSWORD,
            Self::Md5Password => AUTH_MD5_PASSWORD,
            Self::Sasl => AUTH_SASL,
            Self::SaslContinue => AUTH_SASL_CONTINUE,
            Self::SaslFinal => AUTH_SASL_FINAL,
        }
    }
}

// Round-trip compile pin for AuthSubCode.
const _: () = {
    assert!(
        matches!(AuthSubCode::try_from_u32(AuthSubCode::Ok.raw()), Ok(AuthSubCode::Ok)),
        "AuthSubCode round-trip broken: Ok",
    );
    assert!(
        matches!(
            AuthSubCode::try_from_u32(AuthSubCode::CleartextPassword.raw()),
            Ok(AuthSubCode::CleartextPassword),
        ),
        "AuthSubCode round-trip broken: CleartextPassword",
    );
    assert!(
        matches!(
            AuthSubCode::try_from_u32(AuthSubCode::Md5Password.raw()),
            Ok(AuthSubCode::Md5Password),
        ),
        "AuthSubCode round-trip broken: Md5Password",
    );
    assert!(
        matches!(AuthSubCode::try_from_u32(AuthSubCode::Sasl.raw()), Ok(AuthSubCode::Sasl)),
        "AuthSubCode round-trip broken: Sasl",
    );
    assert!(
        matches!(AuthSubCode::try_from_u32(AuthSubCode::SaslContinue.raw()), Ok(AuthSubCode::SaslContinue)),
        "AuthSubCode round-trip broken: SaslContinue",
    );
    assert!(
        matches!(AuthSubCode::try_from_u32(AuthSubCode::SaslFinal.raw()), Ok(AuthSubCode::SaslFinal)),
        "AuthSubCode round-trip broken: SaslFinal",
    );
};

/// The SCRAM-SHA-256 mechanism name as bytes.
pub const SCRAM_SHA_256_MECHANISM: &[u8] = b"SCRAM-SHA-256";

/// PG protocol version 3.0 = 196608 (0x00030000).
pub const PROTOCOL_VERSION_3_0: u32 = 196608;

/// PG `SSLRequest` magic version code = 80877103 (0x04d2162f).
///
/// This is NOT a real protocol version — it's a sentinel value in
/// the `version` field of the StartupMessage-shaped 8-byte
/// SSLRequest packet that tells PG "I want to negotiate SSL/TLS
/// before sending the actual StartupMessage". PG §55.10
/// "SSL Session Encryption". The companion code
/// [`GSSENC_REQUEST_VERSION`] (80877104) is the GSS encryption
/// counterpart (not yet supported in this crate).
///
/// Composed of (1234 << 16) | 5679 — PG's standard "magic-version"
/// shape (CancelRequest uses 1234 << 16 | 5678 = 80877102, etc.).
pub const SSL_REQUEST_VERSION: u32 = 80877103;

/// PG `CancelRequest` magic version code = 80877102 (0x04d2162e).
///
/// Sentinel value sent in the version field of the StartupMessage-
/// shaped 16-byte CancelRequest packet on a SEPARATE TCP connection
/// to cancel an in-flight query on the original connection. PG §55.4
/// "Canceling Requests in Progress".
///
/// Composed of (1234 << 16) | 5678 — same magic-version family as
/// [`SSL_REQUEST_VERSION`] (5679). The shared 1234 high half is the
/// family marker; the low half discriminates message type. See
/// [`MAGIC_VERSION_HIGH_HALF`] for the family-pin formula.
pub const CANCEL_REQUEST_VERSION: u32 = 80877102;

/// Wire-length constant for the CancelRequest packet per PG §55.2.7.
///
/// 16 bytes total: 4 B length-field + 4 B magic version + 4 B pid +
/// 4 B secret_key. The drift-pin below cross-checks against
/// [`cancel_request_bytes`]'s return-type `[u8; 16]` so a future
/// edit reshaping the encoded packet fails at build time.
///
/// `pub(crate)` because the constant is an internal composition
/// primitive used by [`crate::cancel`] for its own const-pins, plus
/// referenced directly inside [`cancel_request_bytes`] as the
/// length-field source-of-truth (single binding for all four
/// length-related sites).
pub(crate) const CANCEL_REQUEST_LEN: u32 = 16;

const _CANCEL_REQUEST_LEN_DRIFT_PIN: () = {
    assert!(
        CANCEL_REQUEST_LEN == 16,
        "CancelRequest length must be exactly 16 bytes per PG §55.2.7. \
         If this constant ever drifts from 16, the packet body composition \
         in `cancel_request_bytes` must update in lockstep.",
    );
    // Cross-pin against the wire-encoding function: the const and
    // the byte builder agree on packet size.
    assert!(
        cancel_request_bytes(0, 0).len() == 16,
        "cancel_request_bytes return-type slice length must equal \
         CANCEL_REQUEST_LEN — drift here is a wire-spec break.",
    );
    // Cross-pin the length field encoded inside the packet matches
    // the constant (the length field is itself the BE encoding of
    // CANCEL_REQUEST_LEN). Catches a typo where the builder's
    // hardcoded `16u32.to_be_bytes()` drifts from this constant.
    let bytes = cancel_request_bytes(0, 0);
    let len_be = [bytes[0], bytes[1], bytes[2], bytes[3]];
    let len_decoded = u32::from_be_bytes(len_be);
    assert!(
        len_decoded == CANCEL_REQUEST_LEN,
        "CancelRequest length field encoded in bytes[0..4] must equal \
         CANCEL_REQUEST_LEN constant — drift here breaks the wire \
         length-field invariant (length includes self per PG protocol).",
    );
};

/// Shared high half (1234 = 0x04d2) of every PG magic-version
/// sentinel.
///
/// PG's StartupMessage-shaped magic packets all encode their
/// "version" field as `(MAGIC_VERSION_HIGH_HALF << 16) | low`
/// where `low` discriminates the variant:
///
/// | low  | message        | const                                 |
/// |------|----------------|---------------------------------------|
/// | 5678 | CancelRequest  | [`CANCEL_REQUEST_VERSION`]            |
/// | 5679 | SSLRequest     | [`SSL_REQUEST_VERSION`]               |
/// | 5680 | GSSENCRequest  | not supported yet                     |
///
/// Real protocol version codes (e.g. [`PROTOCOL_VERSION_3_0`] =
/// `3 << 16 | 0`) use a different shape — the magic 1234 marker
/// is a deliberately-distinct sentinel band so a server can tell
/// "this is a magic packet, not a real StartupMessage" by
/// inspecting the high 16 bits alone.
///
/// Pinning the formula (not just the values) means: a future
/// GSSENCRequest const must satisfy
/// `(MAGIC_VERSION_HIGH_HALF << 16) | 5680`; if a contributor
/// types `1235 << 16` by mistake, the const-assert below fails at
/// build time.
pub const MAGIC_VERSION_HIGH_HALF: u32 = 1234;

/// Typed classification of the single byte the server sends in
/// response to an `SSLRequest` packet (PG §55.10).
///
/// The SSL response byte is **out-of-band**: it has no length
/// prefix and no tagged-frame envelope, so it cannot flow through
/// the normal `feed_bytes` path. Drivers read the single byte
/// directly from their socket and call
/// [`classify_ssl_response_byte`] to obtain a typed outcome
/// instead of carrying ad-hoc `match byte { b'S' => ... }` logic
/// at every dispatch site.
///
/// # Tier-1 closure
///
/// `#[non_exhaustive]` — future PG versions could add a 4th
/// response byte (e.g. for a new TLS extension); downstream
/// consumers MUST use a catch-all when matching, but inside this
/// crate the [`classify_ssl_response_byte`] match is exhaustive
/// over the four currently-defined outcomes. Adding a new variant
/// is a build error in the classifier until the new wire byte
/// is mapped explicitly.
///
/// Drivers exhaustively-match (modulo the catch-all required by
/// `#[non_exhaustive]`) to encode their `sslmode` policy:
///
/// ```ignore
/// match bsql_pg_proto::wire::classify_ssl_response_byte(byte) {
///     SslNegotiationOutcome::Accepted        => /* TLS handshake */,
///     SslNegotiationOutcome::Refused         => /* sslmode policy: fallback or fail */,
///     SslNegotiationOutcome::ErrorIncoming   => /* read ErrorResponse frame */,
///     SslNegotiationOutcome::InvalidByte(b)  => /* protocol violation, fatal */,
///     // catch-all required by #[non_exhaustive]
///     _                                      => /* future extension; treat as fatal until handled */,
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SslNegotiationOutcome {
    /// Server byte `'S'` (0x53) — server accepts SSL. The driver
    /// MUST immediately initiate the TLS handshake on the same
    /// socket; all subsequent protocol traffic flows through TLS.
    Accepted,
    /// Server byte `'N'` (0x4e) — server does NOT support SSL. The
    /// driver decides per its `sslmode` policy:
    /// - `disable`/`allow`/`prefer`: proceed with plaintext
    ///   `StartupMessage`.
    /// - `require`/`verify-ca`/`verify-full`: refuse the connection.
    ///
    /// `bsql-pg-proto` itself does NOT enforce policy — that is a
    /// driver-level concern (the protocol crate has no I/O knowledge
    /// of which mode the user requested).
    Refused,
    /// Server byte `'E'` (0x45) — an `ErrorResponse` frame follows.
    /// The byte itself is the `TAG_ERROR_RESPONSE` tag of the
    /// real frame; the remaining 4 bytes (length field) plus body
    /// follow on the wire. The driver should buffer the consumed
    /// `'E'` byte plus the rest, then route through the normal
    /// frame parser ([`crate::parse_header`] +
    /// [`crate::PgProtocol::feed_bytes`]) to surface the typed
    /// error.
    ///
    /// Pre-TLS errors are typically auth-config issues (the server
    /// was unable to honour the request before establishing TLS,
    /// e.g. SSL globally disabled in `pg_hba.conf`).
    ErrorIncoming,
    /// Server sent a byte that is none of the three defined
    /// responses. **Protocol violation** — the driver MUST treat
    /// this as a fatal connection error (do not attempt recovery,
    /// do not retry; the server's wire-state is unknowable).
    /// Carries the offending byte for forensic logging.
    InvalidByte(u8),
}

/// Classify the single byte a PG server sends in response to an
/// `SSLRequest` packet.
///
/// Pure mapping — no allocation, no panic, `const fn`. See
/// [`SslNegotiationOutcome`] for the response-byte semantics.
///
/// # Tier impact
///
/// A driver-side ad-hoc
/// `match byte { b'S' => ..., b'N' => ..., b'E' => ..., _ => ... }`
/// at every call site would be tier-3 by-discipline (forgetting a
/// branch silently mishandles the connection). The typed
/// [`SslNegotiationOutcome`] with `#[non_exhaustive]` lifts the
/// dispatch to tier-1 for the known-byte arms (compiler enforces
/// handling) and tier-3 for the future-extension arm (catch-all
/// required by `#[non_exhaustive]`).
#[inline]
#[must_use]
pub const fn classify_ssl_response_byte(byte: u8) -> SslNegotiationOutcome {
    match byte {
        b'S' => SslNegotiationOutcome::Accepted,
        b'N' => SslNegotiationOutcome::Refused,
        b'E' => SslNegotiationOutcome::ErrorIncoming,
        other => SslNegotiationOutcome::InvalidByte(other),
    }
}

// Tier-1 round-trip pin for `classify_ssl_response_byte`. If the
// classifier's mapping drifts (e.g. someone swaps the 'S' and 'N'
// arm bodies), these asserts fire at build time.
const _: () = {
    assert!(matches!(
        classify_ssl_response_byte(b'S'),
        SslNegotiationOutcome::Accepted,
    ));
    assert!(matches!(
        classify_ssl_response_byte(b'N'),
        SslNegotiationOutcome::Refused,
    ));
    assert!(matches!(
        classify_ssl_response_byte(b'E'),
        SslNegotiationOutcome::ErrorIncoming,
    ));
    assert!(matches!(
        classify_ssl_response_byte(0x00),
        SslNegotiationOutcome::InvalidByte(0x00),
    ));
    assert!(matches!(
        classify_ssl_response_byte(0xff),
        SslNegotiationOutcome::InvalidByte(0xff),
    ));
};

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

// Drift-pin for `TERMINATE_WIRE_BYTES`. Same shape as the
// `SYNC_WIRE_BYTES` block above. If a future edit changes either
// the tag literal or the length field, these asserts fail at
// build time. Tier-1 against typo-induced wire breaks.
const _: () = assert!(TERMINATE_WIRE_BYTES.len() == 5);
const _: () = assert!(TERMINATE_WIRE_BYTES[0] == b'X');
const _: () = assert!(
    TERMINATE_WIRE_BYTES[1] == 0
        && TERMINATE_WIRE_BYTES[2] == 0
        && TERMINATE_WIRE_BYTES[3] == 0
        && TERMINATE_WIRE_BYTES[4] == 4,
    "Terminate length-field must be 4 (length includes itself, no payload)",
);

// Drift-pin for `FLUSH_WIRE_BYTES`. Same shape as the
// `SYNC_WIRE_BYTES` / `TERMINATE_WIRE_BYTES` blocks above. If a
// future edit changes either the tag literal or the length field,
// these asserts fail at build time. Tier-1 against typo-induced
// wire breaks.
const _: () = assert!(FLUSH_WIRE_BYTES.len() == 5);
const _: () = assert!(FLUSH_WIRE_BYTES[0] == b'H');
const _: () = assert!(
    FLUSH_WIRE_BYTES[1] == 0
        && FLUSH_WIRE_BYTES[2] == 0
        && FLUSH_WIRE_BYTES[3] == 0
        && FLUSH_WIRE_BYTES[4] == 4,
    "Flush length-field must be 4 (length includes itself, no payload)",
);
// Family-disjointness — Flush, Sync, Terminate must not share bytes.
// All three are 5-byte parameterless frames with a 4-byte length-field
// — the ONLY distinguishing byte is the tag. A copy-paste error that
// duplicated a tag literal would otherwise pass the per-frame asserts
// above silently.
const _: () = assert!(
    FLUSH_WIRE_BYTES[0] != SYNC_WIRE_BYTES[0]
        && FLUSH_WIRE_BYTES[0] != TERMINATE_WIRE_BYTES[0]
        && SYNC_WIRE_BYTES[0] != TERMINATE_WIRE_BYTES[0],
    "Flush/Sync/Terminate tag bytes must be pairwise distinct \
     ('H' / 'S' / 'X' per PG §55.7) — copy-paste safety net",
);

// Drift-pin for `SSL_REQUEST_WIRE_BYTES` and the underlying
// `SSL_REQUEST_VERSION` const. Sentinel value 80877103 = 0x04d2162f
// per PG §55.10. The byte literal in the array MUST match
// `SSL_REQUEST_VERSION.to_be_bytes()`; the length field MUST be
// exactly 8 (length includes itself, version is the only payload).
// A bump of either operand without matching the other breaks the
// build here. Tier-1 against typo-induced wire breaks.
const _: () = assert!(SSL_REQUEST_WIRE_BYTES.len() == 8);
const _: () = assert!(SSL_REQUEST_VERSION == 80_877_103);
const _: () = assert!(
    SSL_REQUEST_WIRE_BYTES[0] == 0
        && SSL_REQUEST_WIRE_BYTES[1] == 0
        && SSL_REQUEST_WIRE_BYTES[2] == 0
        && SSL_REQUEST_WIRE_BYTES[3] == 8,
    "SSLRequest length-field must be exactly 8 (length includes itself + 4-byte version)",
);
// Pin the version bytes by comparing against
// `SSL_REQUEST_VERSION.to_be_bytes()`. If anyone bumps either
// the const or the literal without updating the other, the
// formula breaks here.
const _: () = {
    let v = SSL_REQUEST_VERSION.to_be_bytes();
    assert!(
        SSL_REQUEST_WIRE_BYTES[4] == v[0]
            && SSL_REQUEST_WIRE_BYTES[5] == v[1]
            && SSL_REQUEST_WIRE_BYTES[6] == v[2]
            && SSL_REQUEST_WIRE_BYTES[7] == v[3],
        "SSLRequest version bytes must equal SSL_REQUEST_VERSION.to_be_bytes() — \
         bump both the const and the literal in lockstep, or the formula drifts",
    );
};

// Magic-version family pin.
//
// Every magic-version sentinel in the PG protocol shares the shape
// `(MAGIC_VERSION_HIGH_HALF << 16) | low_half`. Pinning the FORMULA
// (not just the value) means: a future GSSENCRequest const must
// satisfy this same formula (with `low_half = 5680`); if a
// contributor types `1235 << 16` by mistake, the assert below
// fires at build time.
//
// The asserts also pin disjointness vs real protocol version codes
// — `PROTOCOL_VERSION_3_0 = 3 << 16 | 0` uses high half 3, NOT
// 1234 — so a copy-paste typo bumping the protocol version into
// the magic band is caught.
const _: () = assert!(MAGIC_VERSION_HIGH_HALF == 1234);
const _: () = assert!(
    SSL_REQUEST_VERSION == (MAGIC_VERSION_HIGH_HALF << 16) | 5679,
    "SSL_REQUEST_VERSION must equal (1234 << 16) | 5679 = 80877103 \
     per PG §55.10 magic-version family",
);
const _: () = assert!(
    CANCEL_REQUEST_VERSION == (MAGIC_VERSION_HIGH_HALF << 16) | 5678,
    "CANCEL_REQUEST_VERSION must equal (1234 << 16) | 5678 = 80877102 \
     per PG §55.4 magic-version family",
);
// Family-disjointness from real protocol versions: the high half
// of `PROTOCOL_VERSION_3_0` (= 3) MUST NOT collide with the magic
// 1234 marker. If a future bump makes major = 1234 (extremely
// unlikely; PG would never go past version 99-ish), the magic
// family loses its discriminator role.
const _: () = assert!(
    (PROTOCOL_VERSION_3_0 >> 16) != MAGIC_VERSION_HIGH_HALF,
    "PROTOCOL_VERSION_3_0 (3.0 = major.minor encoding) high half \
     must NOT collide with magic-version family marker (1234)",
);
// Cross-pin: SSL and CancelRequest version codes MUST be distinct
// (they live in the same family but discriminate via low half).
// A copy-paste of one into the other would silently break dispatch.
const _: () = assert!(
    SSL_REQUEST_VERSION != CANCEL_REQUEST_VERSION,
    "SSL and CancelRequest magic versions must be distinct",
);

/// Build a `CancelRequest` packet on the wire.
///
/// 16-byte StartupMessage-shaped packet sent on a SEPARATE TCP
/// connection to cancel an in-flight query on the ORIGINAL
/// connection. PG §55.4 "Canceling Requests in Progress":
///
/// ```text
/// [length (BE u32 = 16)] [CANCEL_REQUEST_VERSION (BE u32 = 80877102)]
/// [process_id (BE i32)]  [secret_key (BE i32)]
///   = [0x00, 0x00, 0x00, 0x10,
///      0x04, 0xd2, 0x16, 0x2e,
///      <pid 4B BE>,
///      <secret_key 4B BE>]
/// ```
///
/// `pid` and `secret_key` come from the `BackendKeyData` ('K')
/// frame the server emits during startup (captured in the
/// [`crate::ProtoState::ConnectingPostAuthHaveKey`] variant; a
/// future driver wrapper can surface them on a `Connection`
/// typestate).
///
/// # Driver protocol
///
/// 1. Open a NEW TCP socket to the same PG server (the cancel
///    cannot piggy-back on the connection running the query —
///    that connection is busy on the server side).
/// 2. Write these 16 bytes.
/// 3. Close the socket. PG processes the cancel asynchronously;
///    no reply comes back on this socket.
///
/// The original connection's behaviour after a successful cancel
/// is server-driven: depending on the operation, the server may
/// emit `ErrorResponse` with code `57014` (query_canceled) +
/// `ReadyForQuery`, or simply complete normally if the cancel
/// arrived too late. That handling lives in the regular
/// [`crate::PgProtocol::feed_bytes`] dispatch path on the
/// original connection.
///
/// # Tier impact
///
/// Pure function returning `[u8; 16]`. No allocation, no panic,
/// no `unsafe`, no I/O — `const fn`. Caller cannot misshape the
/// buffer (return size compile-fixed) and cannot misorder fields
/// (positional encoding hidden inside the function body); the
/// dynamic payload (`pid` + `secret_key`) is BE-encoded
/// internally. Tier-1 by-construction at the API surface.
///
/// # Why a function, not a static const
///
/// Unlike [`SSL_REQUEST_WIRE_BYTES`] / [`TERMINATE_WIRE_BYTES`] /
/// [`SYNC_WIRE_BYTES`] (parameterless and thus encodable as
/// `pub const` arrays), CancelRequest carries dynamic per-
/// connection payload. A const can't capture the runtime values;
/// the `const fn` form materialises the bytes at every call site,
/// with the layout drift-pinned by the const-asserts immediately
/// below this definition.
///
/// # Usage
///
/// ```ignore
/// // Driver pseudocode:
/// async fn cancel_inflight(server_addr: SocketAddr, pid: i32, secret: i32) -> io::Result<()> {
///     let mut socket = TcpStream::connect(server_addr).await?;
///     socket.write_all(&bsql_pg_proto::cancel_request_bytes(pid, secret)).await?;
///     socket.shutdown().await
/// }
/// ```
#[inline]
#[must_use]
pub const fn cancel_request_bytes(pid: i32, secret_key: i32) -> [u8; 16] {
    // Reference `CANCEL_REQUEST_LEN` (not a hardcoded `16u32`) so
    // the constant is the single source of truth for the length-
    // field byte composition. The drift-pin above asserts
    // `CANCEL_REQUEST_LEN == 16` and the post-builder assert block
    // below cross-checks the encoded length-field bytes — three
    // independent pins on the same invariant.
    let len = CANCEL_REQUEST_LEN.to_be_bytes();
    let ver = CANCEL_REQUEST_VERSION.to_be_bytes();
    let p = pid.to_be_bytes();
    let s = secret_key.to_be_bytes();
    [
        len[0], len[1], len[2], len[3],
        ver[0], ver[1], ver[2], ver[3],
        p[0], p[1], p[2], p[3],
        s[0], s[1], s[2], s[3],
    ]
}

// Tier-1 round-trip pins for `cancel_request_bytes` layout. Same
// shape as the SSLRequest drift-pins above. If a future edit
// reorders the fields, swaps length/version positions, or breaks
// BE encoding, these assertions fail at build time.
const _: () = {
    // Spec-canonical zero pid + zero secret_key — pins length
    // field + version field exactly; the dynamic-payload bytes
    // are zero by construction.
    let bytes = cancel_request_bytes(0, 0);
    // Length field = 16 BE u32 at bytes[0..4].
    assert!(bytes[0] == 0 && bytes[1] == 0);
    assert!(bytes[2] == 0 && bytes[3] == 16);
    // Version field = 80877102 BE u32 at bytes[4..8] = 04 d2 16 2e.
    assert!(bytes[4] == 0x04 && bytes[5] == 0xd2);
    assert!(bytes[6] == 0x16 && bytes[7] == 0x2e);
    // pid (zero) at bytes[8..12].
    assert!(bytes[8] == 0 && bytes[9] == 0);
    assert!(bytes[10] == 0 && bytes[11] == 0);
    // secret_key (zero) at bytes[12..16].
    assert!(bytes[12] == 0 && bytes[13] == 0);
    assert!(bytes[14] == 0 && bytes[15] == 0);
};
const _: () = {
    // Non-zero payload — pins dynamic positions. pid =
    // 0x12345678 = 305419896, secret_key = 0x09abcdef =
    // 162254319 (positive i32 to keep sign clean for the pin).
    let pid: i32 = 0x1234_5678;
    let key: i32 = 0x09ab_cdef;
    let bytes = cancel_request_bytes(pid, key);
    // Length + version unchanged from spec.
    assert!(bytes[0] == 0 && bytes[1] == 0);
    assert!(bytes[2] == 0 && bytes[3] == 16);
    assert!(bytes[4] == 0x04 && bytes[5] == 0xd2);
    assert!(bytes[6] == 0x16 && bytes[7] == 0x2e);
    // pid (BE i32) at bytes[8..12].
    assert!(bytes[8] == 0x12 && bytes[9] == 0x34);
    assert!(bytes[10] == 0x56 && bytes[11] == 0x78);
    // secret_key (BE i32) at bytes[12..16].
    assert!(bytes[12] == 0x09 && bytes[13] == 0xab);
    assert!(bytes[14] == 0xcd && bytes[15] == 0xef);
};
const _: () = {
    // Negative i32 pid — sign-extended in BE encoding via two's
    // complement. pid = -1 → 0xFFFFFFFF; secret_key = i32::MIN
    // → 0x80000000. Pins the BE-encoding-of-signed-int contract.
    let bytes = cancel_request_bytes(-1, i32::MIN);
    assert!(bytes[8] == 0xff && bytes[9] == 0xff);
    assert!(bytes[10] == 0xff && bytes[11] == 0xff);
    assert!(bytes[12] == 0x80 && bytes[13] == 0x00);
    assert!(bytes[14] == 0x00 && bytes[15] == 0x00);
};
// Total length sanity — the public API guarantees a 16-byte
// return; this also pins it from another angle.
const _: () = assert!(cancel_request_bytes(0, 0).len() == 16);
const _: () = {
    // Length field's declared value must equal the array's
    // physical length (PG protocol convention: length includes
    // self).
    let bytes = cancel_request_bytes(0, 0);
    let declared = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    assert!(
        declared == 16,
        "CancelRequest length field must equal physical packet size (16)"
    );
};

// ---------------------------------------------------------------
// Tag collision defenses
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
/// list grows. Tier-1 compile.
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
    // Query-flow inbound tags:
    TAG_ROW_DESCRIPTION,
    TAG_DATA_ROW,
    TAG_COMMAND_COMPLETE,
    TAG_EMPTY_QUERY_RESPONSE,
    TAG_NO_DATA,
    TAG_PARSE_COMPLETE,
    TAG_BIND_COMPLETE,
    TAG_CLOSE_COMPLETE,
    TAG_PARAMETER_DESCRIPTION,
    TAG_PORTAL_SUSPENDED,
);

// **Outbound** (frontend → backend) tag-distinctness.
assert_all_distinct!(
    "outbound PG wire tag",
    TAG_SYNC,
    TAG_SASL_RESPONSE,
    // Query-flow outbound tags:
    TAG_QUERY,
    TAG_PARSE,
    TAG_BIND,
    TAG_DESCRIBE,
    TAG_EXECUTE,
    TAG_CLOSE,
    TAG_FLUSH,
    // Connection-teardown:
    TAG_TERMINATE,
);

// **Authentication sub-codes** distinctness. The sub-code is
// the first four bytes of an `AUTHENTICATION` payload; a
// collision would make two auth methods indistinguishable at the
// dispatcher.
assert_all_distinct_raw!(
    "SCRAM auth sub-code",
    AUTH_OK,
    AUTH_CLEARTEXT_PASSWORD,
    AUTH_MD5_PASSWORD,
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
    assert!(TAG_TERMINATE.byte() == b'X', "TAG_TERMINATE drift");

    // Auth sub-codes (raw u32, no newtype).
    assert!(AUTH_OK == 0, "AUTH_OK drift");
    assert!(AUTH_CLEARTEXT_PASSWORD == 3, "AUTH_CLEARTEXT_PASSWORD drift");
    assert!(AUTH_MD5_PASSWORD == 5, "AUTH_MD5_PASSWORD drift");
    assert!(AUTH_SASL == 10, "AUTH_SASL drift");
    assert!(AUTH_SASL_CONTINUE == 11, "AUTH_SASL_CONTINUE drift");
    assert!(AUTH_SASL_FINAL == 12, "AUTH_SASL_FINAL drift");

    // Protocol version.
    assert!(PROTOCOL_VERSION_3_0 == 196608, "PROTOCOL_VERSION_3_0 drift from PG 3.0 (0x00030000)");
};
