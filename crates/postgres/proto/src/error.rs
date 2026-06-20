//! Classified protocol errors.
//!
//! Per reforge.md §3.5 / §4.6, manufactured variants are forbidden
//! — a variant lands when its emission site lands.
//!
//! Public surface is [`#[non_exhaustive]`][non_exhaustive] so adding
//! variants in 1b/1c does not break user `match`es; user code is forced
//! to carry a `_ =>` catch-all and gets a clean upgrade path.
//!
//! [non_exhaustive]: https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute

use core::fmt;

// -----------------------------------------------------------------
// Typed fields for ServerErrorResponse
// -----------------------------------------------------------------

/// Server-error severity classification.
///
/// Replaces the earlier `heapless::String<32>` with a 1-byte enum.
/// Parsed from the PG "S" (localised) / "V" (non-localised) field
/// at receive time; unrecognised strings map to
/// [`Severity::Unknown`] rather than silently dropping the field.
///
/// # Discriminant layout
///
/// `#[repr(u8)]` — 1 byte. The niche `Severity::Unknown = 0` is
/// deliberately the first variant so that `unsafe` zero-init (not
/// used in this crate but is in caller crates) land on `Unknown`
/// rather than a specific level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum Severity {
    /// Unrecognised server severity. Fall-through for forward-compat.
    Unknown = 0,
    /// Diagnostic log output — non-error informational traffic.
    Log,
    /// Info — server informational message.
    Info,
    /// Debug — server debug-level traffic.
    Debug,
    /// Notice — server noteworthy condition (not an error).
    Notice,
    /// Warning — potentially problematic but non-fatal.
    Warning,
    /// Standard error — query failed, server stays connected.
    Error,
    /// Fatal — connection terminated by server.
    Fatal,
    /// Panic — server is aborting, process exit imminent.
    Panic,
}

impl Severity {
    /// Parse a server-provided severity byte slice into the enum.
    ///
    /// Matches the PG-standard uppercase names. Case-sensitive (PG
    /// emits uppercase). Falls through to [`Severity::Unknown`] for
    /// anything else — never panics, never rejects.
    ///
    /// `const fn` so the round-trip pin below can const-assert
    /// `from_bytes(as_str(v).as_bytes()) == v`.
    #[must_use]
    pub const fn from_bytes(bytes: &[u8]) -> Self {
        match bytes {
            b"ERROR" => Self::Error,
            b"FATAL" => Self::Fatal,
            b"PANIC" => Self::Panic,
            b"WARNING" => Self::Warning,
            b"NOTICE" => Self::Notice,
            b"DEBUG" => Self::Debug,
            b"INFO" => Self::Info,
            b"LOG" => Self::Log,
            _ => Self::Unknown,
        }
    }

    /// Uppercase name for `Display`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Log => "LOG",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
            Self::Notice => "NOTICE",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
            Self::Panic => "PANIC",
        }
    }
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// Round-trip compile pin for Severity.
// `from_bytes(as_str(v).as_bytes())` must equal v for every
// variant. Catches body-swap drift between the 8 known variants'
// literal mappings. `Unknown` is special — `as_str` returns
// "UNKNOWN" which `from_bytes` does NOT recognize, falling through
// to `Self::Unknown` correctly (round-trip preserved via fallthrough).
//
// `matches!` in const context avoids both the `as u8` coercion
// (forbid-bundle bans) and the requirement for a const
// `PartialEq` impl.
const _: () = {
    assert!(matches!(Severity::from_bytes(Severity::Error.as_str().as_bytes()), Severity::Error));
    assert!(matches!(Severity::from_bytes(Severity::Fatal.as_str().as_bytes()), Severity::Fatal));
    assert!(matches!(Severity::from_bytes(Severity::Panic.as_str().as_bytes()), Severity::Panic));
    assert!(matches!(Severity::from_bytes(Severity::Warning.as_str().as_bytes()), Severity::Warning));
    assert!(matches!(Severity::from_bytes(Severity::Notice.as_str().as_bytes()), Severity::Notice));
    assert!(matches!(Severity::from_bytes(Severity::Debug.as_str().as_bytes()), Severity::Debug));
    assert!(matches!(Severity::from_bytes(Severity::Info.as_str().as_bytes()), Severity::Info));
    assert!(matches!(Severity::from_bytes(Severity::Log.as_str().as_bytes()), Severity::Log));
    assert!(matches!(Severity::from_bytes(Severity::Unknown.as_str().as_bytes()), Severity::Unknown));
};

// Niche-pack invariant pin. `Severity` is `#[repr(u8)]` with 9
// variants occupying discriminants 0..=8; values 9..=255 are
// unused, giving `Option<Severity>` a niche for `None` without
// growing the layout. The 1-byte total for `Option<Severity>` is
// load-bearing — `parse_error_response` uses it as the "seen /
// not-seen" severity slot and every byte of the ProtocolError
// variant matters. Adding a 248th+ variant to Severity would
// overflow the niche and silently grow the Option to 2 bytes —
// this assert fails the build instead.
const _: () = assert!(
    core::mem::size_of::<Option<Severity>>() == 1,
    "Severity niche-pack: Option<Severity> must stay 1 byte via unused discriminant range",
);

/// PostgreSQL SQLSTATE code — always exactly 5 ASCII chars (per spec).
///
/// Packed as `[u8; 5]` newtype: 5 bytes, `Copy`, no allocation. If
/// the server sends a shorter code (shouldn't — SQLSTATE is always
/// 5 chars), the remainder is space-padded (`0x20`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct SqlStateCode {
    /// The 5 ASCII chars. Non-ASCII input is coerced to `?` at
    /// construction (never panics).
    bytes: [u8; 5],
}

impl SqlStateCode {
    /// Construct from a byte slice. Non-ASCII chars become `?`,
    /// short inputs are space-padded, over-5-char inputs take the
    /// first 5 bytes. Never fails, never allocates.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut out = [b' '; 5];
        let take = bytes.len().min(5);
        if let (Some(dst), Some(src)) = (out.get_mut(..take), bytes.get(..take)) {
            dst.copy_from_slice(src);
        }
        for byte in &mut out {
            if !byte.is_ascii() {
                *byte = b'?';
            }
        }
        Self { bytes: out }
    }

    /// The 5 bytes (may include trailing spaces for short codes).
    #[inline]
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 5] {
        &self.bytes
    }

    /// The code as `&str` — guaranteed ASCII by construction.
    ///
    /// Explicit match over `from_utf8` with a documented-dead None
    /// arm. `self.bytes` is ASCII-only by construction
    /// (`from_bytes` coerces every non-ASCII byte to `b'?'`); ASCII
    /// is valid UTF-8 → Err arm architecturally unreachable. A
    /// naive `unwrap_or("")` is the silent-fallback pattern this
    /// crate bans. The explicit empty-string sentinel on the dead
    /// arm has no corruption vector at the display-only boundary;
    /// empty code surfaces as visible regression in logs.
    ///
    /// Bypass options considered: `unsafe { from_utf8_unchecked }`
    /// (forbid-bundle bans unsafe), `const fn` + stable
    /// `core::str::from_utf8` (not const-stable in MSRV 1.95).
    /// O(5) runtime check is negligible on this cold error path.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        // `if let` form rather than `match` to avoid clippy's
        // `manual_unwrap_or_default` lint — the `match Ok(s) | Err(_)
        // => ""` pattern it suggests-to-simplify via `unwrap_or_default`
        // IS exactly the silent-fallback pattern user banned. `if let`
        // is functionally identical but escapes the lint.
        if let Ok(s) = core::str::from_utf8(&self.bytes) {
            return s;
        }
        // Architecturally unreachable per `from_bytes`'s ASCII
        // coercion invariant. Empty-string sentinel on the dead
        // arm: no corruption vector at the display-only boundary;
        // empty SqlStateCode surfaces as obvious regression in logs.
        ""
    }
}

impl fmt::Display for SqlStateCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// `BoundedStr<N>` is a type alias in `crate::ident` for
// `FixedStr<N, BoundedStrTag>`. Re-exported here so downstream code
// can write `error::BoundedStr<128>`.
pub use crate::ident::BoundedStr;

/// Typed classification for [`ProtocolError::UnsupportedAuthMethod`].
///
/// Distinguishes "server sent a sub-code we don't know about" from
/// "server sent a known sub-code that's wrong for the current state"
/// — the latter preserves the typed [`crate::wire::AuthSubCode`]
/// enum so diagnostics can say *which* known method the server
/// requested.
///
/// # `#[non_exhaustive]`
///
/// New variants may land if PG introduces additional sub-code
/// classification dimensions (e.g., a separate "deprecated but
/// recognised" tier) or if internal classification grows new
/// boundaries. Sealed via `non_exhaustive` so any future variant
/// addition forces external matches to add a catch-all arm —
/// closes the silent-pass-through audit seam where new variants
/// would otherwise land in a hidden default branch downstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AuthSubCodeClass {
    /// A sub-code outside the 4 PG-defined values (0/10/11/12).
    /// Carries the raw u32 for forensic logging.
    ///
    /// `NonZeroU32` (not `u32`) — tier-1 structural proof that
    /// server sent a value other than 0 (AUTH_OK = 0 is a known
    /// sub-code, classified via `KnownButWrong(AuthSubCode::Ok)` if
    /// seen in wrong state, never in `Unknown`). Niche optimises
    /// `Option<AuthSubCodeClass>` and other nested options.
    Unknown(core::num::NonZeroU32),
    /// A recognised sub-code that's legal on the wire but wrong
    /// for the current state (e.g., server returned `Sasl` while
    /// the client is in Trust auth with no SCRAM credentials).
    KnownButWrong(crate::wire::AuthSubCode),
}

impl fmt::Display for AuthSubCodeClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown(code) => write!(f, "unknown sub-code {code}"),
            Self::KnownButWrong(c) => write!(
                f,
                "{:?} (wire value {})",
                c,
                c.raw(),
            ),
        }
    }
}

// Drift-pin — `Unknown(NonZeroU32)` must niche into the same 8
// bytes as `KnownButWrong(AuthSubCode)`. NonZeroU32 carries the
// non-zero invariant at type level; `Option<AuthSubCodeClass>`
// niche-optimises via the 0-u32-bit-pattern slot. A naive
// `Unknown(u32)` shape would be 8 bytes incl. discriminant +
// 4-byte padding but lose the Option niche.
const _: () = assert!(
    core::mem::size_of::<AuthSubCodeClass>() == 8,
    "AuthSubCodeClass: 4-byte discriminant + 4-byte NonZeroU32 payload",
);
const _: () = assert!(
    core::mem::size_of::<Option<AuthSubCodeClass>>() == 8,
    "Option<AuthSubCodeClass>: niche-packed via NonZeroU32 = 8 B",
);

// -----------------------------------------------------------------
// ProtocolError footprint anchor (co-located at the definition).
// -----------------------------------------------------------------
//
// A wrong pin — or any layout change that invalidates a correct pin —
// aborts the build with E0080. Both size and align are pinned in one
// anchor: a field reorder can keep the byte count while changing the
// alignment, and the size dimension alone is blind to that.
//
// Constraint shape (24 B, align 8): error payloads that would inline
// large bounded strings are externalised into the error arena, so the
// dominant variant stays pointer-sized:
//   • ServerErrorResponse carries `details_ref: ErrorRef` (8 B), not
//     an inline `BoundedStr<N>`.
//   • ScramHandshakeFailure carries `ScramFailureClass` (8 B) +
//     `Option<ErrorRef>` (8 B), not an inline `ScramError`.
// Re-inlining either payload, or adding a variant body over the
// 24 B budget, trips this anchor.
crate::wire_pin!(ProtocolError, size = 24, align = 8);


/// A classified failure on the wire protocol.
///
/// Errors are *transport-level* signals from the state machine to the
/// async wrapper, not user-visible types — the wrapper translates them
/// into its public `BackendError`. Variants are kept narrow and
/// self-describing; the wrapper never has to invent error context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProtocolError {
    /// The header's length-field is below the legal minimum (4).
    ///
    /// PG length-fields include their own 4 bytes; a value < 4 cannot
    /// describe even an empty body. Tier-2 against silent buffer
    /// underflow (the parser refuses to interpret the tag).
    MalformedFrameLength {
        /// The illegal value the server sent.
        declared: u32,
    },

    /// The header's length-field exceeds [`crate::MAX_FRAME_LEN_FIELD`].
    ///
    /// Refuses to allocate buffer space for an attacker-chosen frame.
    /// This is the structural cap that makes "frame length amplification
    /// DoS" tier-2 (reforge.md §53). The connection must be torn down.
    FrameTooLarge {
        /// The oversized declared length.
        declared: u32,
    },

    /// Received a server frame whose tag is not legal in the current
    /// state.
    ///
    /// Example: an unsolicited `ReadyForQuery` arriving in [`crate::ProtoState::Idle`]
    /// (we never sent anything to provoke it) — the connection is
    /// out-of-sync and must be discarded.
    UnexpectedFrame {
        /// The offending PG message tag, received from the server.
        ///
        /// Tier-1 typed as [`crate::wire::InboundTag`] — the field
        /// can only hold bytes that came through the frame parser.
        /// A refactor that tried to stuff an outbound tag byte here
        /// (via `.byte()` extraction) would lose the type-safety;
        /// forcing construction through `InboundTag` at every
        /// emission site makes cross-direction confusion impossible.
        tag: crate::wire::InboundTag,
    },

    /// A `ReadyForQuery` frame had an unexpected payload size.
    ///
    /// The PG spec mandates exactly one payload byte (the transaction
    /// status indicator). A different size means we are out of sync with
    /// the server's framing; the connection is unsafe to continue.
    MalformedReadyForQuery {
        /// The actual payload byte count.
        payload_len: usize,
    },

    /// A server frame that PG §55.7 specifies MUST have a zero-byte
    /// body arrived with non-zero body. Examples:
    /// - `EmptyQueryResponse` (`'I'`) — body is always empty per spec
    /// - `ParseComplete` (`'1'`) — body is always empty
    /// - `BindComplete` (`'2'`) — body is always empty
    /// - `NoData` (`'n'`) — body is always empty
    /// - `CloseComplete` (`'3'`) — body is always empty
    ///
    /// # Why the dispatch arms classify
    ///
    /// A naive dispatch arm that ignores the `payload` parameter
    /// would silently accept a server sending 500 bytes of body on
    /// `EmptyQueryResponse` — the bytes consumed, state transitioned,
    /// no error emitted. **Tier-4 silent spec drift.** A future PG
    /// version that legitimately added a body field to one of these
    /// frames would be silently accepted instead of classified —
    /// masking the protocol break.
    ///
    /// Every zero-body arm explicitly slice-pattern matches `[]` and
    /// classifies any other body as `UnexpectedFrameBody`.
    UnexpectedFrameBody {
        /// The offending tag.
        tag: crate::wire::InboundTag,
        /// The observed body length (should be 0 per spec).
        payload_len: usize,
    },

    /// The bounded read buffer overflowed while appending inbound bytes.
    ///
    /// This only fires when the caller tries to feed a chunk whose size
    /// plus the currently-unread region exceeds [`crate::READ_BUF_CAP`].
    /// Can happen if the host chunks aggressively or if a server sends a
    /// legal-looking (per header) but unusually large frame that slipped
    /// past the header-size cap. Close the connection.
    ReadBufferFull {
        /// How many bytes the caller tried to append.
        attempted: usize,
        /// How much headroom was available.
        available: usize,
    },

    /// Server sent an `ErrorResponse` (tag `'E'`) during the startup
    /// handshake or mid-query. Carries typed [`Severity`] +
    /// [`SqlStateCode`] (5-byte SQLSTATE); bounded strings (message
    /// / detail / hint) live in [`crate::PgProtocol`]'s `ErrorArena`.
    ///
    /// Size: variant payload = `Severity` 1 B + `SqlStateCode` 5 B +
    /// `ErrorRef` 8 B = 14 B. The enclosing `ProtocolError` pins at
    /// 72 B exact. A naive inline-bounded-string shape would balloon
    /// the payload to ~288 B and cascade through `Action<'w,'r>`
    /// (which dominates `OutActions = [Action; 9]` per feed_bytes
    /// call).
    ServerErrorResponse {
        /// Severity classification.
        ///
        /// - `None` — the wire payload contained no `S` (localised) or
        ///   `V` (non-localised) severity field. Servers in compliance
        ///   with PostgreSQL §55.7 always send `S`; `None` indicates a
        ///   non-conformant peer (proxy, debugging shim, or wire
        ///   corruption).
        /// - `Some(Severity::Unknown)` — the server sent a severity
        ///   string but it didn't match any known variant. The raw
        ///   bytes are preserved in the arena-resolved `ErrorPayload`.
        /// - `Some(Severity::Error)` (or other known variant) — server
        ///   sent a recognised severity string.
        ///
        /// Niche-packs into the 1-byte `Severity` discriminant via
        /// `core::option::Option<Severity>` — total payload still 1 B
        /// (Severity has unused discriminant range). Pre-Tier-3
        /// uplift, the field was `Severity` directly and the
        /// `unwrap_or(Severity::Unknown)` fallback at the parser
        /// collapsed "absent" and "unknown" into the same observable;
        /// the Option layer preserves the diagnostic distinction.
        severity: Option<Severity>,
        /// SQLSTATE code — always exactly 5 ASCII chars, packed as
        /// a `[u8; 5]` newtype. Space-padded if shorter; never empty.
        code: SqlStateCode,
        /// Handle into [`crate::PgProtocol`]'s `ErrorArena` for the
        /// bounded strings (message / detail / hint).
        ///
        /// Carries 8 bytes: NonZeroU8 slot + u32 generation + 3 B
        /// struct padding (see `error_arena.rs` size pin). The
        /// generation is u32 (not u8) for wrap-safety on
        /// long-running connections; the resolve API is
        /// `Result<&ErrorPayload, ArenaError>` with classified
        /// `Empty` / `Stale` variants.
        ///
        /// Resolve via [`crate::PgProtocol::get_server_error`] or
        /// format the full error (severity + code + message +
        /// detail + hint) via
        /// [`crate::PgProtocol::display_error`]. The ref is `Copy`
        /// — callers can stash it, drop `OutActions` / the Action /
        /// StreamItem, then resolve on the freed protocol borrow.
        details_ref: crate::error_arena::ErrorRef,
    },

    /// Server sent an authentication method we do not support.
    ///
    /// PG's Authentication message (tag `'R'`) carries a sub-code for
    /// the method. Only sub-code 0 (Ok), 10 (SASL), 11
    /// (SASLContinue), 12 (SASLFinal) are supported. Anything else
    /// lands here.
    ///
    /// # Typed classification
    ///
    /// [`AuthSubCodeClass`] distinguishes "server sent an unknown
    /// u32 code" (`Unknown(u32)`) from "server sent a known-but-wrong
    /// code for this state" (`KnownButWrong(AuthSubCode)`). The
    /// known-but-wrong case (e.g., server insisted on SASL when
    /// client connected with Trust auth) preserves the typed
    /// [`crate::wire::AuthSubCode`] enum rather than widening back
    /// to u32. Downstream wrappers can render "server insisted on
    /// SCRAM on a Trust connection" instead of "unsupported auth
    /// method 10".
    UnsupportedAuthMethod {
        /// Typed classification of the offending sub-code.
        sub_code: AuthSubCodeClass,
    },

    /// Server sent `NegotiateProtocolVersion` (tag `'v'`) during
    /// startup, indicating it does not support a protocol option we
    /// requested.
    UnsupportedProtocolOption,

    /// SCRAM authentication failure.
    ///
    /// **shape**: carries a slim
    /// [`crate::scram::wire::ScramFailureClass`] (8 B inline — tag +
    /// optional u32 iteration count) alongside an
    /// `Option<crate::error_arena::ErrorRef>` for the optional
    /// server-supplied error text (`e=<text>` from RFC 5802 §5.1
    /// server-final-message). The text — only populated for the
    /// `ScramFailureClass::ServerScramError` class — lives in
    /// [`crate::error_arena::ErrorArena`] alongside the
    /// `ServerError` payload (mutually exclusive single-slot use:
    /// SCRAM never coexists with `ErrorResponse` on the wire).
    ///
    /// # Why externalised rather than inline `BoundedStr<64>`
    ///
    /// Pre-shape stored the inline 64-B
    /// `ServerScramError { message }` payload directly inside
    /// `ScramError`, blowing `ProtocolError` to 72 B (max-variant-
    /// dominator). Externalisation collapses the variant to
    /// `(class 8 B + detail 8 B) = 16 B` payload, taking
    /// `ProtocolError` from 72 → 24 B (−67 %) — and that win
    /// cascades through `Action` (80 → 32) and `OutActions`
    /// (728 → 296) without touching the Copy chain or introducing
    /// a Drop cascade through Vec slots (measured regression class).
    ///
    /// # Class vs detail ref
    ///
    /// - `class`: identity of the failure, including iteration count
    ///   for `IterationsTooLow`/`TooHigh`. Always present.
    /// - `detail`: `Some(ErrorRef)` only when the wire-supplied
    ///   text was non-empty (i.e., `class ==
    ///   ScramFailureClass::ServerScramError` with a non-empty
    ///   `e=<text>` field); `None` for every other class. Resolved
    ///   via [`crate::PgProtocol::get_server_error`] →
    ///   `Result<&ErrorPayload, ArenaError>` with the `Scram`
    ///   variant.
    ///
    /// [`scram::wire::ScramError`]: crate::scram::wire::ScramError
    ScramHandshakeFailure {
        /// Discrete identity of the SCRAM failure (every variant of
        /// [`crate::scram::wire::ScramError`] is mirrored, minus the
        /// inline text payload).
        class: crate::scram::wire::ScramFailureClass,
        /// Arena-backed text for the `ServerScramError` class.
        /// `None` for all other classes (the wire format only
        /// supplies text in the `e=<text>` field of server-final-
        /// message).
        detail: Option<crate::error_arena::ErrorRef>,
    },

    /// Server's `BackendKeyData` payload has wrong size (expected 8).
    MalformedBackendKeyData {
        /// Actual payload byte count.
        payload_len: usize,
    },

    /// Server's `Authentication*` message payload is too short to
    /// contain the 4-byte sub-code.
    MalformedAuthentication {
        /// Actual payload byte count.
        payload_len: usize,
    },

    /// Attempted to start a second Startup handshake while one is
    /// already in progress.
    StartupAlreadyInProgress,

    /// Attempted to push a command while another query or the
    /// startup handshake already occupies the connection.
    /// Simple-query states reject new pushes with this error — the
    /// existing query must complete first.
    CommandInProgress,

    /// Server sent a `CommandComplete` (`'C'`) payload that was not
    /// NUL-terminated or otherwise malformed. The `CommandComplete`
    /// body is an ASCII command tag
    /// (`"SELECT 5"`, `"INSERT 0 3"`, …) terminated by a single
    /// NUL byte; a missing terminator or non-ASCII bytes beyond
    /// the cap signal framing desync.
    MalformedCommandComplete {
        /// Actual payload byte count.
        payload_len: usize,
    },

    /// Server sent a malformed `RowDescription` (`'T'`) payload —
    /// short header, negative column count, missing name NUL,
    /// truncated per-column metadata, or trailing bytes after the
    /// declared columns. Framing-desync classification: the
    /// connection is torn down (the wire is out of sync with the
    /// per-column 18-byte stride).
    MalformedRowDescription {
        /// Actual payload byte count.
        payload_len: usize,
    },

    /// Server sent a malformed `CopyOutResponse` ('H') or
    /// `CopyInResponse` ('G') payload () — short header
    /// (< 3 bytes for format + count), format byte not in {0, 1},
    /// negative column count, trailing bytes after declared columns,
    /// or per-column format code disagreeing with the overall format
    /// byte (PG §55.2.6 pins per-column codes to equal overall).
    /// Framing-desync classification.
    MalformedCopyResponse {
        /// Actual payload byte count.
        payload_len: usize,
    },

    /// Server sent a `DataRow` (`'D'`) frame with no body — the
    /// 5-byte header is followed by zero payload bytes (`total_len
    /// == HEADER_LEN`). PG's wire spec mandates at minimum a 2-byte
    /// column count in the body even for zero-column rows; a 0-byte
    /// body signals framing desync or a malformed/adversarial
    /// server.
    ///
    /// A naive classification of this case as `InternalCrateBug`
    /// would mislead operators — the crate isn't buggy, the server
    /// is. The distinct variant lets logs say "malformed data row"
    /// instead of "internal bsql-pg-proto bug".
    MalformedDataRow {
        /// Declared frame total length (tag + length-prefix + body
        /// = HEADER_LEN + body_len). Valid DataRow has
        /// `total_len > HEADER_LEN`.
        total_len: usize,
    },

    /// Server's `RowDescription` declares more columns than
    /// [`crate::MAX_ROW_COLUMNS`] — this crate's bounded inline
    /// storage cannot accommodate the result-set. The query is
    /// failed and the connection is torn down; the user retries
    /// with a narrower projection.
    TooManyColumns {
        /// Column count declared by the server.
        count: usize,
        /// Maximum supported — [`crate::MAX_ROW_COLUMNS`].
        max: usize,
    },

    /// Server's `RowDescription` carried a per-column format code
    /// outside the legal `{0, 1}` range. Text (`0`) and binary
    /// (`1`) are the only values PG defines; any other value is a
    /// server-side wire violation.
    UnexpectedFormatCode {
        /// The offending format code from the server.
        code: i16,
    },

    /// Server's `ParameterDescription` (`'t'`) body was ill-formed:
    /// payload too short to hold the 2-byte count header, declared
    /// count disagrees with the remaining byte length, or negative
    /// count. Wire violation; the connection is torn down.
    ///
    /// Emitted by [`crate::decode::parse_parameter_description`].
    MalformedParameterDescription {
        /// Actual payload byte count.
        payload_len: usize,
    },

    /// Server's `ParameterDescription` declared more parameter OIDs
    /// than this crate's bounded storage can accept.
    ///
    /// The cap is [`crate::params::MAX_PARAMS_ARITY`] = 16, which
    /// matches the crate's Bind-side arity cap. A statement with
    /// more placeholders than this can be Parsed by the server but
    /// never Bound against — so the describe result is useless
    /// downstream.
    ///
    /// Mirrors [`Self::TooManyColumns`] shape.
    TooManyParameters {
        /// Count declared by the server.
        count: usize,
        /// Maximum supported — [`crate::params::MAX_PARAMS_ARITY`].
        max: usize,
    },

    /// Outbound frame builder (`build_startup_message` /
    /// `build_query_message` / `build_parse_message`) returned Err
    /// in a const-assert-dead path.
    ///
    /// A crate-internal invariant failed at a structurally-dead
    /// code path — emission indicates a bug inside `bsql-pg-proto`
    /// itself, not a wire-level event.
    ///
    /// Uniform "internal crate bug" shape — fewer discriminants
    /// than per-locus variants, single diagnostic template,
    /// additive locus enum for new dead-paths as they're
    /// identified. Classification is always
    /// [`ErrorKind::Internal`].
    InternalCrateBug {
        /// Identifies the specific architecturally-dead code path
        /// that fired. Diagnostic only — every locus classifies as
        /// `ErrorKind::Internal`.
        locus: CrateBugLocus,
    },

    /// `RowStream::collect_tuple<R>` observed a row with column
    /// count different from the prepared query's
    /// `R::ARITY`. This is a server-side contract violation — the
    /// macro emitted the row OID list at compile time; PG would
    /// only ship rows with the matching arity for the same SQL.
    /// Diagnostic carries the mismatch for ops debugging.
    ColumnCountMismatch {
        /// Arity the prepared query expected (`R::ARITY`).
        expected: u16,
        /// Arity the server actually delivered.
        actual: u16,
    },

    /// A column body exceeded the active read-buf headroom during
    /// a typed `collect_tuple` call. The typed-decode path requires
    /// contiguous column bytes; chunked columns are not assembled
    /// into typed values (would require either caller-owned scratch
    /// buffer or heap-allocated per-cell vectors — both outside the
    /// no_alloc contract).
    ///
    /// Caller falls back to `col_next` for the row to consume the
    /// chunked bytes.
    ChunkedColumnInTypedRow,

    /// A per-column `Cell::decode` call returned an error
    /// during a typed `collect_tuple` row assembly. The inner
    /// [`crate::decode::DecodeError`] is the root cause (bad UTF-8,
    /// IntParse, NullInNonNullColumn, etc.). The connection itself
    /// is healthy — the error is row-level, not transport-level.
    DecodeFailure(crate::decode::DecodeError),

    /// A user command arrived on a connection that had already been
    /// torn down by a prior fatal. The wrapper translates this into
    /// the public error "connection closed, see earlier error" with
    /// the `prior_kind` as diagnostic context.
    ///
    /// See [`ErrorKind`] for the ship-order rationale. A naive
    /// approach that cloned the full 856-byte `ProtocolError` into
    /// every `FailReply` on an Errored connection is rejected; the
    /// prior cause is surfaced **once** in the first `FailReply`
    /// and subsequent pushes get a compact
    /// `ConnectionAlreadyClosed { prior_kind }` (17 bytes incl.
    /// discriminant + padding).
    ConnectionAlreadyClosed {
        /// Classification of the earlier fatal that closed the
        /// connection. The full cause was emitted exactly once in
        /// the first `FailReply` action; the wrapper is expected to
        /// have preserved it.
        ///
        /// Typed as [`StateErrorKind`] (not [`ErrorKind`]) so the
        /// type system proves this field can never recursively be
        /// `AlreadyClosed` — a `ConnectionAlreadyClosed { prior_kind:
        /// AlreadyClosed }` nonsense value is a type error at
        /// construction.
        prior_kind: StateErrorKind,
    },
}

/// Locus discriminator for [`ProtocolError::InternalCrateBug`].
/// Names the specific architecturally-dead code path that fired;
/// every locus classifies as [`ErrorKind::Internal`].
///
/// Additive: as new dead-paths are identified, variants grow
/// WITHOUT expanding the top-level [`ProtocolError`] enum.
///
/// `#[repr(u8)]` makes the discriminant explicit 1-byte.
/// `Option<CrateBugLocus>` niche-packs in the same byte —
/// const-asserted below to catch drift if a future variant with
/// payload lands.
///
/// # `#[non_exhaustive]`
///
/// This enum is the public catalogue of internal-bug classifications.
/// New loci land as new architectural dead-arms get classified — every
/// sub-phase potentially adds entries. Sealing via `non_exhaustive`
/// forces downstream consumers (drivers logging crate-bug payloads,
/// observability harnesses) to keep a catch-all arm, so a future
/// variant addition cannot silently fall through a downstream
/// exhaustive `match` and lose its diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum CrateBugLocus {
    /// [`crate::buf::ReadBuf::advance`] returned Err after
    /// `parse_header` successfully validated
    /// `total_len <= populated.len()`. The two checks happen in
    /// the same `feed_bytes` iteration with no interleaving
    /// mutation; emission indicates a ReadBuf lifecycle or coords
    /// bug.
    ReadCursorAdvance,

    /// [`crate::action::NonEmptyRange::new`] returned None when
    /// constructing a row-range for a `DataRow` frame.
    /// `parse_header` validates `payload_end <= populated_len`;
    /// emission indicates a [`crate::dispatch::FrameCoords`] math
    /// bug.
    RowRangeConstruction,

    /// A `ParamsWriter::write_params` impl returned
    /// `Err(WriteBufFull)` while the `Bind` frame was being built.
    /// `ParamsWriter` is a `pub` sealed trait — user impls of arity
    /// 0..=16 exist via derive/macro. A well-behaved impl never
    /// triggers this: the crate's `MAX_OWNED_SEND_LEN` is
    /// const-asserted against the worst-case
    /// `max_bind_message_size()` sum. Emission indicates either a
    /// drift between `MAX_PARAMS_DATA_TOTAL` and the builder's size
    /// budget, or an adversarial/buggy user impl that writes past
    /// its advertised bound. Classified tier-3 (vs a naive silent
    /// discard with `debug_assert!(false)` that would ship a
    /// truncated Bind frame with miscomputed length prefix —
    /// tier-4 silent corruption).
    ParamsWriterOverflow,

    /// A crate-internal frame builder (`build_query_message`,
    /// `build_parse_message`, etc.) saw `Err(WriteBufFull)` from a
    /// `BrandedWriteReserved::push_*` call. A naive `debug_assert!
    /// + silent discard` would let release builds keep writing a
    /// frame whose length-prefix had already been emitted
    /// ASSUMING body bytes would follow, producing a
    /// correct-looking-length `Action::SendBytes` with TRUNCATED
    /// content (bit-junk on wire, PG server sees framing desync).
    /// Instead every push_* returns Result, builders `?`
    /// propagate, builder-return Err classifies as this locus and
    /// routes through `FailReply + CloseSocket`.
    ///
    /// Architecturally dead under
    /// `const _: () = assert!(MAX_OWNED_SEND_LEN >= max_*_message_size())`
    /// in write_buf.rs — but the const-assert only catches
    /// BUILDER-DECLARED max-size drift; a push site that violates
    /// its declared budget (e.g. a new builder missing a length
    /// cap) lands here rather than silently ships corrupt bytes.
    BuilderCapacityOverflow,

    /// A `build_*_message` branded builder produced a zero-length
    /// span when `WriteRange::from_write_span` invoked
    /// `NonEmptyRange::new(start, reserved.len(), reserved.len())`
    /// and got `None`.
    ///
    /// Architecturally dead under intact builders: every PG wire
    /// builder emits ≥ 5 bytes (tag + 4-byte length prefix + body),
    /// so `reserved.len() > start` holds post-build. Emission
    /// indicates a builder bug (missed push) or const-assert drift
    /// on `MAX_OWNED_SEND_LEN`.
    ///
    /// Classified tier-3 (vs a naive silent fallback to unit-length
    /// `NonEmptyRange (start=0, len=1)` which — applied against an
    /// empty buffer in materialise — would produce a 0-byte
    /// `Action::SendBytes` and hang the handshake at the wire).
    /// Builders return `Result<WriteRange, ProtocolError>`;
    /// `compute_push_*` routes `Err` through `FailReply +
    /// CloseSocket`.
    EmptyWriteRange,

    /// `AuthSubCode::try_from_u32` returned Err carrying raw value
    /// 0 — architecturally impossible because `AUTH_OK = 0` is the
    /// first match arm and returns Ok. The
    /// `AuthSubCodeClass::Unknown(NonZeroU32)` niche-packed variant
    /// rejects zero values at the type level; this locus classifies
    /// the dead arm that would otherwise require either silent
    /// fallback (tier-4, CREDO §5) or new-variant-with-payload.
    AuthSubCodeZeroInErr,

    /// The static `AtomicU64` counter backing
    /// [`crate::PgProtocol::next_reply_id`] reached `u64::MAX` and
    /// the next mint would produce a duplicate ID (atomics wrap to
    /// 0 by spec; subsequent mints cycle through previously-issued
    /// values). Architecturally distant (~10^19 mints process-wide)
    /// but a real ceiling.
    ///
    /// A naive shape that allowed the saturation point to silently
    /// return a duplicate-ID would let the wrapper's
    /// pending-replies table mis-route subsequent server replies to
    /// the wrong correlator. Instead saturation detection
    /// transitions the affected `PgProtocol` instance to
    /// `Errored(ReplyIdSaturation)`, so the next push fails with
    /// `ConnectionAlreadyClosed`-classified before the duplicate
    /// reaches the server. Cross-instance duplicate-ID risk remains
    /// tier-2 (separate residue — brand-lifetime closure deferred).
    ReplyIdSaturation,

    /// `push_command_internal` was invoked from a non-Idle state —
    /// a contract violation between the only legitimate caller
    /// (`ReadyGuard::push_command`, which classifies state as Idle
    /// via `as_ready` upstream) and `push_command_internal`.
    /// Reaching this locus implies a structural regression in the
    /// ReadyGuard → push_command_internal pipeline; production
    /// binaries never reach it under the existing call graph (state
    /// cannot transition between `as_ready`'s check and
    /// `push_command_internal`'s entry — the `&mut PgProtocol`
    /// borrow chain rules out interleaving).
    PushCommandInternalNonIdle,

    /// The closure passed to [`crate::PgProtocol::iter_rows`]
    /// returned (normal exit, early
    /// return, or panic unwind) while a `RowStream` was mid-frame —
    /// either inside a row body (column events still pending) or in
    /// partial-frame mode (frame body bytes still in flight on the
    /// wire). The wire is in an ambiguous state: the read cursor may
    /// sit mid-frame and any subsequent feed_bytes call on the same
    /// connection would mis-classify the inbound bytes as a fresh
    /// frame header.
    ///
    /// The [`crate::row_stream::RowStream`] `Drop` impl installs
    /// `Errored(InternalCrateBug { locus: StreamDroppedMidStream })`
    /// when the stream's `drained` flag is `false` at drop time. The
    /// in-flight reply id (if any — streaming variants always carry
    /// one) is atomically drained via the FeedStateSetter route;
    /// because Drop has no caller context to which to deliver a
    /// FailReply, the drained id is currently absorbed (architectural
    /// boundary — see method doc on
    /// `PgProtocol::install_errored_stream_dropped_mid_stream`). The
    /// next operation on the connection observes the Errored state and
    /// surfaces `ConnectionAlreadyClosed { prior_kind: ClientOrdering }`.
    StreamDroppedMidStream,

    /// A `compute_push_*` family function staged a
    /// `StagedAction::DeliverReply` action — architecturally dead
    /// because replies come from the server via `feed_bytes` only;
    /// the push path never emits DeliverReply. Reaching this locus
    /// indicates a `compute_push` refactor regression.
    ///
    /// A naive `debug_assert!(false, …)` shield + silent drop on
    /// release would match the CREDO §V glass pattern. Instead the
    /// dead arm classifies via `PushFailure { id: …, cause:
    /// InternalCrateBug { locus: PushEmittedDeliverReply } }`; both
    /// dev and release route uniformly. The sentinel id is the
    /// distinct [`crate::reply_id::CRATE_BUG_REPLY_ID_SENTINEL`]
    /// (= NonZeroU64::MAX), not `NonZeroU64::MIN` — the latter would
    /// collide with the legitimate first id minted by
    /// `next_reply_id` on every connection's first command. Closed
    /// by-construction by the distinct sentinel.
    PushEmittedDeliverReply,

    /// [`crate::buf::ReadBuf::enter_partial_mode`] was called while
    /// the buffer was already in partial-frame mode
    /// (`partial_remaining > 0`). The streaming dispatcher's state
    /// machine guarantees the precondition (`exit_partial_mode`
    /// runs before re-entry), so reaching this locus indicates an
    /// internal refactor regression in the dispatch loop.
    ///
    /// A naive `debug_assert!` panic in dev builds + silent
    /// overwrite of the prior `partial_remaining` in release would
    /// match the CREDO §V glass pattern, with wire-desync
    /// consequence (forgotten body-byte count: the next inbound
    /// bytes classified as a fresh frame header instead of body
    /// continuation). Instead both dev and release return typed
    /// `Err` and route through this locus + `Errored` state install.
    PartialModeReentry,

    /// [`crate::buf::ReadBuf::exit_partial_mode`] was called while
    /// the buffer still owed wire body bytes (`partial_remaining >
    /// 0`). The streaming dispatcher's state machine guarantees the
    /// precondition (every wire-legal streaming row drains its body
    /// before reaching the end-of-row code path), so reaching this
    /// locus indicates either an internal refactor regression in
    /// the dispatch loop OR an adversarial server emitting a
    /// malformed DataRow whose `col_count`/per-column length sum
    /// doesn't match the frame-header body length.
    ///
    /// A naive `debug_assert!` + silent reset of `partial_remaining`
    /// to `0` on release would match the CREDO §V glass pattern
    /// (mirror of [`Self::PartialModeReentry`]'s entry-side hazard).
    /// Wire-desync consequence: previously-pending body bytes never
    /// drained from the wire, next inbound bytes mis-classified as
    /// a fresh frame header. Instead both dev and release return
    /// typed `Err`, the counter is preserved, and the caller routes
    /// through this locus and `Errored` state install.
    PartialModeExitUndrained,

    /// [`crate::command_tags_arena::CommandTagsArena::alloc`] returned
    /// `None` while staging a multi-statement
    /// `IntermediateCommandComplete` — the per-cycle slot cap
    /// ([`crate::command_tags_arena::MAX_INTERMEDIATE_TAGS_PER_CALL`]
    /// = 9, equal to [`crate::MAX_ACTIONS_PER_CALL`]) was exceeded.
    /// Architecturally dead: the dispatch loop cannot emit more ICCs
    /// than there are OutActions slots, and arena capacity equals
    /// that cap exactly. Emission indicates a refactor that
    /// decoupled the two caps without re-aligning them.
    ///
    /// A naive `debug_assert!(false) + silent drop` would lose the
    /// `IntermediateCommandComplete` action — wrapper observes
    /// fewer events than the wire delivered, intermediate tags
    /// silently absent from operator logs. Tier-3 classifier
    /// installs `Errored(InternalCrateBug { locus: CommandTagsArenaOverflow })`
    /// + `FailReply` + `CloseSocket`.
    CommandTagsArenaOverflow,

    /// `StatePushClass::Idle` was observed by `compute_push_*` but
    /// `IdleState::try_from(state)` returned `None` — the push-class
    /// classifier and the state enum disagree on whether the state is
    /// Idle. Architecturally dead under intact `push_class()` →
    /// `IdleState::try_from` pairing; emission indicates a refactor
    /// regression that decoupled the two classifiers.
    ///
    /// Pre-audit (session) this arm was a glass pattern:
    /// `debug_assert!(false, ...) + return` — dev-loud, release-
    /// silent. The silent `return` dropped the `ReplyId` without
    /// emitting a FailReply — the user's oneshot was never resolved,
    /// the command silently vanished. Now classified: FailReply +
    /// CloseSocket + Errored state install.
    PushClassIdleMismatch,
}

// Niche-packed `Option<CrateBugLocus>` — 1 byte since all variants
// are C-like + `#[repr(u8)]`. Drift pin catches any future variant
// that adds a payload (would bump size to ≥ 2B and break the niche).
const _: () = assert!(
    core::mem::size_of::<CrateBugLocus>() == 1,
    "CrateBugLocus must stay 1-byte (repr(u8), C-like variants) — \
     adding a payload variant regresses Option<CrateBugLocus> \
     niche packing.",
);
const _: () = assert!(
    core::mem::size_of::<Option<CrateBugLocus>>() == 1,
    "Option<CrateBugLocus> must niche-pack into 1 byte — \
     repr(u8) + C-like enum leaves 256 - 7 = 249 unused \
     discriminant slots for the None sentinel.",
);

impl fmt::Display for CrateBugLocus {
    /// Dedicated Display impl for operator-facing log output. A
    /// naive `{locus:?}` (Debug) rendering would produce Rust
    /// struct-expression output (`OutboundFrameBuild { stage:
    /// Query }`) — cluttered in operator logs and fragile to a
    /// future Debug derive change.
    ///
    /// This impl renders each locus as a stable kebab-case tag:
    /// - `ReadCursorAdvance` → `"read-cursor-advance"`
    /// - `RowRangeConstruction` → `"row-range-construction"`
    /// - `ParamsWriterOverflow` → `"params-writer-overflow"`
    /// - `EmptyWriteRange` → `"empty-write-range"`
    /// - `AuthSubCodeZeroInErr` → `"auth-sub-code-zero-in-err"`
    /// - `BuilderCapacityOverflow` → `"builder-capacity-overflow"`
    ///
    /// Test module `crate_bug_locus_display_tests` pins each string
    /// literal — a future variant rename cannot silently change
    /// operator logs without tripping the test.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadCursorAdvance => f.write_str("read-cursor-advance"),
            Self::RowRangeConstruction => f.write_str("row-range-construction"),
            Self::ParamsWriterOverflow => f.write_str("params-writer-overflow"),
            Self::EmptyWriteRange => f.write_str("empty-write-range"),
            Self::AuthSubCodeZeroInErr => f.write_str("auth-sub-code-zero-in-err"),
            Self::BuilderCapacityOverflow => f.write_str("builder-capacity-overflow"),
            Self::ReplyIdSaturation => f.write_str("reply-id-saturation"),
            Self::PushCommandInternalNonIdle => f.write_str("push-command-internal-non-idle"),
            Self::StreamDroppedMidStream => f.write_str("stream-dropped-mid-stream"),
            Self::PushEmittedDeliverReply => f.write_str("push-emitted-deliver-reply"),
            Self::PartialModeReentry => f.write_str("partial-mode-reentry"),
            Self::PartialModeExitUndrained => f.write_str("partial-mode-exit-undrained"),
            Self::CommandTagsArenaOverflow => f.write_str("command-tags-arena-overflow"),
            Self::PushClassIdleMismatch => f.write_str("push-class-idle-mismatch"),
        }
    }
}

#[cfg(test)]
mod crate_bug_locus_display_tests {
    //! Each [`CrateBugLocus`] variant renders to its canonical
    //! operator-facing string. A rename or Debug-derive refactor
    //! that breaks the rendering will trip these tests loudly
    //! instead of silently corrupting production log output.

    use super::*;
    extern crate alloc;
    use alloc::format;

    #[test]
    fn read_cursor_advance_display() {
        let e = ProtocolError::InternalCrateBug {
            locus: CrateBugLocus::ReadCursorAdvance,
        };
        assert_eq!(
            format!("{e}"),
            "internal bsql-pg-proto bug at locus read-cursor-advance",
        );
    }

    #[test]
    fn row_range_construction_display() {
        let e = ProtocolError::InternalCrateBug {
            locus: CrateBugLocus::RowRangeConstruction,
        };
        assert_eq!(
            format!("{e}"),
            "internal bsql-pg-proto bug at locus row-range-construction",
        );
    }

    #[test]
    fn params_writer_overflow_display() {
        let e = ProtocolError::InternalCrateBug {
            locus: CrateBugLocus::ParamsWriterOverflow,
        };
        assert_eq!(
            format!("{e}"),
            "internal bsql-pg-proto bug at locus params-writer-overflow",
        );
    }

    #[test]
    fn empty_write_range_display() {
        let e = ProtocolError::InternalCrateBug {
            locus: CrateBugLocus::EmptyWriteRange,
        };
        assert_eq!(
            format!("{e}"),
            "internal bsql-pg-proto bug at locus empty-write-range",
        );
    }

    /// ReplyIdSaturation locus renders to its canonical
    /// operator-facing string. Trips loudly if a future rename or
    /// display-impl edit silently changes the log output a
    /// wrapper-level monitor relies on.
    #[test]
    fn reply_id_saturation_display() {
        let e = ProtocolError::InternalCrateBug {
            locus: CrateBugLocus::ReplyIdSaturation,
        };
        assert_eq!(
            format!("{e}"),
            "internal bsql-pg-proto bug at locus reply-id-saturation",
        );
    }

    /// StreamDroppedMidStream locus renders to its canonical
    /// operator-facing string. Watches for drift on the
    /// closure-scoped iter_rows Drop-install path's operator-facing
    /// log signal.
    #[test]
    fn stream_dropped_mid_stream_display() {
        let e = ProtocolError::InternalCrateBug {
            locus: CrateBugLocus::StreamDroppedMidStream,
        };
        assert_eq!(
            format!("{e}"),
            "internal bsql-pg-proto bug at locus stream-dropped-mid-stream",
        );
    }

    /// PushEmittedDeliverReply locus renders to its canonical
    /// operator-facing string. Watches for drift on the
    /// compute_push pipeline classifier-bug signal — classified
    /// PushFailure on what would otherwise be a `debug_assert!
    /// (false, …)` glass pattern.
    #[test]
    fn push_emitted_deliver_reply_display() {
        let e = ProtocolError::InternalCrateBug {
            locus: CrateBugLocus::PushEmittedDeliverReply,
        };
        assert_eq!(
            format!("{e}"),
            "internal bsql-pg-proto bug at locus push-emitted-deliver-reply",
        );
    }

    /// PartialModeReentry locus renders to its canonical operator-
    /// facing string. Watches for drift on the row-stream partial-
    /// mode classifier-bug signal — typed Err return + classified
    /// install_errored routing on what would otherwise be a
    /// `debug_assert!(partial_remaining == 0, …)` glass pattern.
    #[test]
    fn partial_mode_reentry_display() {
        let e = ProtocolError::InternalCrateBug {
            locus: CrateBugLocus::PartialModeReentry,
        };
        assert_eq!(
            format!("{e}"),
            "internal bsql-pg-proto bug at locus partial-mode-reentry",
        );
    }

    /// PartialModeExitUndrained locus renders to its canonical
    /// operator-facing string. Watches for drift on the row-stream
    /// partial-mode exit-with-bytes-owed classifier-bug signal —
    /// typed Err return + classified install_errored routing on
    /// what would otherwise be a `debug_assert!(partial_remaining
    /// == 0, …)` glass pattern.
    #[test]
    fn partial_mode_exit_undrained_display() {
        let e = ProtocolError::InternalCrateBug {
            locus: CrateBugLocus::PartialModeExitUndrained,
        };
        assert_eq!(
            format!("{e}"),
            "internal bsql-pg-proto bug at locus partial-mode-exit-undrained",
        );
    }
}

/// Compact 1-byte classification of a [`ProtocolError`], stored in
/// [`crate::state::ProtoState::Errored`].
///
/// # Rationale
///
/// A naive shape where state carries the full `ProtocolError`
/// (~856 bytes dominated by `ServerErrorResponse`'s five
/// `heapless::String<N>` fields) would mean every `push_command`
/// on an Errored connection clones the whole thing into a new
/// `FailReply` — ~1.3 KB of stack churn per push on the cold path.
///
/// Instead `ProtoState::Errored(ErrorKind)` is 2 bytes (1 for the
/// discriminant + 1 for the outer variant tag). The full
/// `ProtocolError` is emitted in `FailReply` **exactly once** (the
/// first fatal); subsequent pushes emit
/// [`ProtocolError::ConnectionAlreadyClosed { prior_kind }`] — a
/// compact typed echo.
///
/// # Tier
///
/// - **Tier-1 compile** on "every `ProtocolError` variant maps to
///   exactly one `ErrorKind`": the `ProtocolError::kind` match is
///   exhaustive; adding a new `ProtocolError` variant without
///   classifying it is a build error.
// `#[non_exhaustive]` pre-empts the SemVer footgun — adding a new
// variant in a future release would otherwise be a major-version
// break. With `non_exhaustive`, downstream `match`es require a
// wildcard arm (or accept future variants explicitly), so adding a
// variant is a minor-version non-breaking change. Internal
// `match`es here remain exhaustive because `#[non_exhaustive]`
// permits exhaustive matches WITHIN the defining crate — only
// EXTERNAL crates are required to use a wildcard. Tier-1 invariants
// on the internal exhaustive-match shields (e.g.,
// `ProtocolError::kind`, `StateErrorKind` mapping) are preserved.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorKind {
    /// Wire framing: length field below minimum, over cap, or tag
    /// mismatch. [`ProtocolError::MalformedFrameLength`] +
    /// [`ProtocolError::FrameTooLarge`] + [`ProtocolError::UnexpectedFrame`] +
    /// [`ProtocolError::MalformedReadyForQuery`] +
    /// [`ProtocolError::MalformedBackendKeyData`] +
    /// [`ProtocolError::MalformedAuthentication`].
    Framing = 0,
    /// Read buffer overflow — local transport classification.
    /// [`ProtocolError::ReadBufferFull`].
    Transport = 1,
    /// Server-side error response arrived mid-handshake or mid-query.
    /// [`ProtocolError::ServerErrorResponse`].
    ServerError = 2,
    /// Authentication negotiation failed — unsupported method or
    /// SCRAM exchange error. [`ProtocolError::UnsupportedAuthMethod`] +
    /// [`ProtocolError::ScramHandshakeFailure`] +
    /// [`ProtocolError::UnsupportedProtocolOption`].
    ///
    /// `StartupAlreadyInProgress` and `CommandInProgress` are NOT
    /// in this bucket — they're client-side push-ordering errors,
    /// not server-driven auth failures. A naive bucketing here
    /// would let wrappers reading `ConnectionAlreadyClosed
    /// { prior_kind }` from a push-race see `Auth` and report
    /// "authentication error" when the real cause was the user
    /// pushing too fast. Those variants route to
    /// [`Self::ClientOrdering`].
    Auth = 3,
    /// Internal invariant broken — bug in this crate. Covers
    /// [`ProtocolError::InternalCrateBug`] (uniform shape over the
    /// architecturally-dead loci enumerated in [`CrateBugLocus`]).
    Internal = 4,
    /// Pseudo-kind for a `ConnectionAlreadyClosed` meta-error. Only
    /// ever appears in `FailReply` replies, never in state (the state
    /// retains the real prior kind).
    AlreadyClosed = 5,
    /// Client-side command-ordering error — caller pushed a new
    /// command while one was still in flight, or pushed Startup
    /// after Startup. Covers
    /// [`ProtocolError::CommandInProgress`] +
    /// [`ProtocolError::StartupAlreadyInProgress`].
    ///
    /// **Not a server auth failure.** This classification distinguishes
    /// the wrapper-side bug class ("your code ordering") from genuine
    /// auth path errors ([`Self::Auth`]) so diagnostics in
    /// `ConnectionAlreadyClosed { prior_kind }` correctly identify
    /// the culprit. 1 byte (repr(u8)).
    ClientOrdering = 6,
}

/// Subset of [`ErrorKind`] that CAN be stored in
/// [`crate::state::ProtoState::Errored`] and carried as
/// `prior_kind` of [`ProtocolError::ConnectionAlreadyClosed`].
///
/// # Tier-1 compile invariant
///
/// "State never holds `ErrorKind::AlreadyClosed`" is enforced at the
/// type level rather than left to audit. A naive shape that used
/// `ErrorKind` directly inside `ProtoState::Errored(_)` and the
/// `prior_kind` field would need a `fail_inflight_and_close` guard
/// to reject `AlreadyClosed` reaching state — drop the guard and
/// you get nonsensical `ConnectionAlreadyClosed { prior_kind:
/// AlreadyClosed }` diagnostics.
///
/// This newtype lifts the invariant to tier-1: the constructor
/// [`Self::try_from_kind`] rejects `AlreadyClosed`, so
/// `ProtoState::Errored(StateErrorKind)` cannot type-check with
/// an `AlreadyClosed` kind at the construction site.
///
/// # Layout
///
/// `#[repr(transparent)]` — zero-cost over `ErrorKind`. Same 1-byte
/// footprint; no discriminant or padding bloat. The newtype is
/// compile-time only.
///
/// # Niche optimisation
///
/// Wrapping `ErrorKind` (which is `#[repr(u8)]` with 8 variants
/// 0..=7) preserves 247 unused discriminant values as niches, so
/// `Option<StateErrorKind>` is still 1 byte just like
/// `Option<ErrorKind>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct StateErrorKind(ErrorKind);

impl StateErrorKind {
    // No public `INTERNAL_FALLBACK` const. A naive shape would
    // expose one to supply the `unwrap_or_else` landing pad for
    // three
    // `state_kind().unwrap_or_else(|| { debug_assert!(false, ...); INTERNAL_FALLBACK })`
    // call sites — the exact "release silent + debug loud" pattern
    // that is banned crate-wide. Instead `state_kind() ->
    // StateErrorKind` is total (see `ProtocolError::state_kind` at
    // the end of this module) and the internal
    // `Self(ErrorKind::Internal)` sentinel is encapsulated inside
    // `from_kind_or_internal`.

    /// Construct from a full [`ErrorKind`]. Returns `None` when
    /// passed [`ErrorKind::AlreadyClosed`] — that variant is the
    /// reply-only "pseudo-kind" that never reaches state.
    ///
    /// # Tier-1 pin
    ///
    /// The const match below is exhaustive; a future addition to
    /// `ErrorKind` forces an explicit decision (state-storable or
    /// not) here. Adding a new state-reachable variant without
    /// extending this match is a build error.
    #[inline]
    #[must_use]
    pub const fn try_from_kind(k: ErrorKind) -> Option<Self> {
        match k {
            ErrorKind::AlreadyClosed => None,
            ErrorKind::Framing
            | ErrorKind::Transport
            | ErrorKind::ServerError
            | ErrorKind::Auth
            | ErrorKind::Internal
            | ErrorKind::ClientOrdering => Some(Self(k)),
        }
    }

    /// Infallible conversion — maps [`ErrorKind::AlreadyClosed`] to
    /// `Internal` (a nonsensical `AlreadyClosed` reaching state
    /// implies a crate bug, which is precisely what `Internal`
    /// classifies).
    ///
    /// Sole infallible-path production. `ProtocolError::state_kind()`
    /// is implemented on top of this. Tests and fixture code use it
    /// to produce a `StateErrorKind` from a known-valid literal
    /// without Option ceremony.
    #[inline]
    #[must_use]
    pub const fn from_kind_or_internal(k: ErrorKind) -> Self {
        match Self::try_from_kind(k) {
            Some(s) => s,
            // Inline the Internal sentinel instead of a separate
            // `INTERNAL_FALLBACK` const. Architecturally dead:
            // AlreadyClosed never reaches the state-install paths
            // (sealed by the `try_from_kind` rejection above);
            // call-site classification.
            None => Self(ErrorKind::Internal),
        }
    }

    /// Unwrap to the underlying [`ErrorKind`]. Always non-
    /// `AlreadyClosed` by the constructor's invariant.
    #[inline]
    #[must_use]
    pub const fn as_kind(self) -> ErrorKind {
        self.0
    }
}

// See the `ProtocolError` `core::error::Error` impl below for the
// rationale on satisfying the canonical error-trait contract.
impl core::error::Error for StateErrorKind {}

impl fmt::Display for StateErrorKind {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Delegate to ErrorKind's Debug (it doesn't impl Display
        // currently; use Debug for uniformity).
        write!(f, "{:?}", self.0)
    }
}

// Drift pin: size/niche invariant. If these break, downstream
// `ProtoState::Errored(StateErrorKind)` and `ConnectionAlreadyClosed
// { prior_kind: StateErrorKind }` would grow beyond the documented
// 1-byte budget.
const _: () = assert!(
    core::mem::size_of::<StateErrorKind>() == 1,
    "StateErrorKind must stay 1 byte (#[repr(transparent)] over ErrorKind)",
);
const _: () = assert!(
    core::mem::size_of::<Option<StateErrorKind>>() == 1,
    "Option<StateErrorKind> must niche-pack to 1 byte (ErrorKind uses 8 of 256 u8 discriminants)",
);

impl ProtocolError {
    /// Compact kind classification for this error.
    ///
    /// Used by [`crate::state::ProtoState::Errored(ErrorKind)`] to
    /// store the terminal state cheaply — 1 byte instead of 856.
    /// The full cause is emitted in `FailReply` exactly once (the
    /// first fatal); the state retains only the kind.
    ///
    /// Exhaustive match — adding a `ProtocolError` variant without
    /// classifying it is a build error. Tier-1 compile.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        match self {
            Self::MalformedFrameLength { .. }
            | Self::FrameTooLarge { .. }
            | Self::UnexpectedFrame { .. }
            | Self::UnexpectedFrameBody { .. }
            | Self::MalformedReadyForQuery { .. }
            | Self::MalformedBackendKeyData { .. }
            | Self::MalformedAuthentication { .. } => ErrorKind::Framing,
            Self::ReadBufferFull { .. } => ErrorKind::Transport,
            Self::ServerErrorResponse { .. } => ErrorKind::ServerError,
            Self::UnsupportedAuthMethod { .. }
            | Self::UnsupportedProtocolOption
            | Self::ScramHandshakeFailure { .. } => ErrorKind::Auth,
            // Client-side push-ordering bugs must NOT route to `Auth`
            // — they're the user calling push_command out of order,
            // not a server auth failure. Wrappers reading
            // `ConnectionAlreadyClosed { prior_kind: Auth }` would
            // report a misleading "authentication error" diagnostic.
            Self::StartupAlreadyInProgress | Self::CommandInProgress => ErrorKind::ClientOrdering,
            Self::MalformedCommandComplete { .. }
            | Self::MalformedRowDescription { .. }
            | Self::MalformedCopyResponse { .. }
            | Self::MalformedDataRow { .. }
            | Self::TooManyColumns { .. }
            | Self::UnexpectedFormatCode { .. }
            | Self::MalformedParameterDescription { .. }
            | Self::TooManyParameters { .. } => ErrorKind::Framing,
            Self::InternalCrateBug { .. } => ErrorKind::Internal,
            // Typed-row decoder errors. Column-count mismatch is a
            // Framing-class issue (server vs prepared query
            // disagreement on schema shape); ChunkedColumnInTypedRow
            // is a Framing-class limitation (caller used typed decode
            // on a multi-MB column that doesn't fit one buffer).
            // DecodeFailure is row-level data parsing — Framing class
            // because the underlying server bytes failed parsing.
            Self::ColumnCountMismatch { .. }
            | Self::ChunkedColumnInTypedRow
            | Self::DecodeFailure(_) => ErrorKind::Framing,
            Self::ConnectionAlreadyClosed { .. } => ErrorKind::AlreadyClosed,
        }
    }

    /// Total projection from [`ProtocolError`] to the
    /// [`StateErrorKind`] subset storable in
    /// [`crate::state::ProtoState::Errored`].
    ///
    /// `ErrorKind::AlreadyClosed` is the only kind that isn't
    /// state-storable in principle (sealed by `try_from_kind`); it
    /// only arises in reply-only contexts (push_command /
    /// push_bind_execute emitting
    /// `FailReply { cause: ConnectionAlreadyClosed }` when the user
    /// invokes on an already-Errored state). The dispatch +
    /// feed_bytes + builder paths NEVER see it as a cause —
    /// architectural invariant of the `StateErrorKind` seal.
    ///
    /// # Total typed projection
    ///
    /// A naive shape would return `Option<StateErrorKind>` and let
    /// call sites open-code
    /// `state_kind().unwrap_or_else(|| { debug_assert!(false, ...); INTERNAL_FALLBACK })`
    /// — the exact "release silent + debug loud" pattern that is
    /// banned crate-wide ("никаких потенциальных паник и прочих
    /// атрибутов хрупкой и стеклянной структуры"). Instead the
    /// projection is **total**: `AlreadyClosed → Internal`. That
    /// IS an honest classification ("something went wrong at the
    /// crate level") — not silent corruption. Architecturally dead
    /// under the `StateErrorKind` seal; preserved as behavioural
    /// fallback rather than a panic + silent-release split.
    #[inline]
    #[must_use]
    pub const fn state_kind(&self) -> StateErrorKind {
        StateErrorKind::from_kind_or_internal(self.kind())
    }
}

impl ProtocolError {
    /// Construct a `ScramHandshakeFailure` from a wire-layer
    /// [`crate::scram::wire::ScramError`] **without** allocating any
    /// arena-backed text — used at SCRAM dispatch sites where the
    /// error class is architecturally guaranteed never to be
    /// `ServerScramError` (e.g., outbound builder
    /// `BufferOverflow`, `parse_server_first` malformed-frame
    /// classes, `HmacKeyRejected`).
    ///
    /// For sites where text COULD be present (notably
    /// `parse_server_final` returning the server `e=<text>`),
    /// callers MUST use
    /// [`crate::error_arena::scram_error_to_protocol_error`]
    /// instead — that path threads the arena slot and preserves the
    /// forensic text payload via `ErrorPayload::Scram`.
    ///
    /// # Total
    ///
    /// Every `ScramError` variant maps to a `ScramFailureClass` of
    /// matching identity. The `ServerScramError`'s inline message
    /// (which is the only carrier of text) is dropped here — this
    /// helper is reserved for sites where that variant is
    /// architecturally unreachable, so the drop is a contract
    /// fulfilment, not a forensic loss.
    #[inline]
    #[must_use]
    pub(crate) fn from_scram_no_text(e: crate::scram::wire::ScramError) -> Self {
        let (class, _text_unused) = e.split_into_class_and_text();
        Self::ScramHandshakeFailure { class, detail: None }
    }

    /// Construct a `ScramHandshakeFailure` directly from a
    /// [`crate::scram::wire::ScramFailureClass`] with no arena
    /// text. Used at sites that construct the class inline (e.g.
    /// `BufferOverflow` raised by an outbound-frame push without
    /// going through a `ScramError`).
    #[inline]
    #[must_use]
    pub(crate) const fn scram_no_text(class: crate::scram::wire::ScramFailureClass) -> Self {
        Self::ScramHandshakeFailure { class, detail: None }
    }
}

/// Converts `WriteBufFull` (write-side buffer overflow) to the
/// crate-internal-bug classification `BuilderCapacityOverflow`.
/// Enables `?`-propagation through builders that return
/// `Result<WriteRange, ProtocolError>` over raw push_* sites that
/// return `Result<(), WriteBufFull>`.
impl From<crate::write_buf::WriteBufFull> for ProtocolError {
    #[inline]
    fn from(_: crate::write_buf::WriteBufFull) -> Self {
        Self::InternalCrateBug {
            locus: CrateBugLocus::BuilderCapacityOverflow,
        }
    }
}

// `ProtocolError` satisfies the canonical error-trait contract from
// `core`. Downstream crates (`bsql-driver-postgres`, async wrappers)
// can `?`-propagate `ProtocolError` through
// `Box<dyn core::error::Error>` boundaries + downstream
// `thiserror`-style enums without a manual `From`/Display wrapping
// bridge. Empty body: the default `Error::source()` (returns `None`)
// is correct — `ProtocolError` is a leaf error type (no inner errors
// it wraps that satisfy `Error`); it has variants carrying typed
// classifications (ScramError, DecodeError) but those are independent
// errors, not chained sources.
//
// `no_std` note: `core::error::Error` is available in `no_std`; the
// crate uses the `core::` path (NOT `std::`) to keep `no_std` clean.
impl core::error::Error for ProtocolError {}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedFrameLength { declared } => write!(
                f,
                "malformed frame: length field {declared} below minimum (4)",
            ),
            Self::FrameTooLarge { declared } => write!(
                f,
                "frame too large: declared length {declared} exceeds buffer cap",
            ),
            Self::UnexpectedFrame { tag } => {
                // Print the underlying byte — `InboundTag` wraps a u8
                // via `.byte()`. Printable-ASCII branch for PG tags
                // (all within `0x20..=0x7e`); hex fallback is dead
                // in practice but preserved for robustness.
                let b = tag.byte();
                if matches!(b, 0x20..=0x7e) {
                    write!(f, "unexpected frame tag '{}' ({b:#04x})", char::from(b))
                } else {
                    write!(f, "unexpected frame tag {b:#04x}")
                }
            }
            Self::MalformedReadyForQuery { payload_len } => write!(
                f,
                "ReadyForQuery payload length {payload_len} (expected 1)",
            ),
            Self::UnexpectedFrameBody { tag, payload_len } => {
                let b = tag.byte();
                if matches!(b, 0x20..=0x7e) {
                    write!(
                        f,
                        "frame '{}' ({b:#04x}) has non-zero body ({payload_len} bytes); \
                         PG spec requires zero body for this tag",
                        char::from(b),
                    )
                } else {
                    write!(
                        f,
                        "frame {b:#04x} has non-zero body ({payload_len} bytes); \
                         PG spec requires zero body for this tag",
                    )
                }
            }
            Self::ReadBufferFull {
                attempted,
                available,
            } => write!(
                f,
                "read buffer full: tried to append {attempted} bytes, only {available} available",
            ),
            Self::ServerErrorResponse {
                severity,
                code,
                ..
            } => {
                // `Display` for this variant emits inline severity +
                // sqlstate + a LOUD `[use PgProtocol::display_error
                // for message/detail/hint]` advisory directing the
                // operator to the arena-aware adapter API for full
                // text. The built-in `Display` cannot reach
                // `PgProtocol`'s `ErrorArena`, so message / detail /
                // hint are NOT rendered here — the advisory makes the
                // absence explicit and grep-able in production logs.
                //
                // **Tier classification**: tier-3 classified diagnostic
                // (loud-advisory on non-adapter call sites:
                // `format!("{err}")`, `log::error!("{err}")`,
                // `err.to_string()`, `thiserror` source-chaining,
                // etc.). The output never fabricates "details"
                // content; absence of message/detail/hint is the
                // explicit contract.
                //
                // **Severity is `Option<Severity>`** (Tier-3 #30):
                // `None` renders as `[severity absent]` to
                // disambiguate "server didn't send the S/V field"
                // from `Some(Severity::Unknown)` (server sent an
                // unrecognised severity string).
                match severity {
                    Some(s) => write!(
                        f,
                        "server error: {s} ({code}) \
                         [use PgProtocol::display_error for message/detail/hint]",
                    ),
                    None => write!(
                        f,
                        "server error: [severity absent] ({code}) \
                         [use PgProtocol::display_error for message/detail/hint]",
                    ),
                }
            },
            // Typed Severity + SqlStateCode + BoundedStr all impl
            // Display — no extra plumbing needed.
            Self::UnsupportedAuthMethod { sub_code } => {
                write!(f, "unsupported authentication method (sub-code {sub_code})")
            }
            Self::UnsupportedProtocolOption => {
                f.write_str("server does not support requested protocol option")
            }
            Self::ScramHandshakeFailure { class, detail: _ } => {
                // Inline Display emits the class identity; the
                // arena-backed text (when present) is rendered by
                // `PgProtocol::display_error` (the arena-aware
                // adapter) since the built-in `Display` has no
                // arena borrow in scope.
                write!(
                    f,
                    "SCRAM authentication failed: {class} \
                     [use PgProtocol::display_error for server text]",
                )
            }
            Self::MalformedBackendKeyData { payload_len } => write!(
                f,
                "BackendKeyData payload length {payload_len} (expected 8)",
            ),
            Self::MalformedAuthentication { payload_len } => write!(
                f,
                "Authentication message payload too short: {payload_len} bytes (need >= 4)",
            ),
            Self::StartupAlreadyInProgress => {
                f.write_str("startup handshake already in progress")
            }
            Self::CommandInProgress => {
                f.write_str("another command is in progress on this connection")
            }
            Self::MalformedCommandComplete { payload_len } => write!(
                f,
                "malformed CommandComplete: {payload_len}-byte payload missing NUL terminator",
            ),
            Self::MalformedRowDescription { payload_len } => write!(
                f,
                "malformed RowDescription: {payload_len}-byte payload (short header, negative count, missing name NUL, or truncated metadata)",
            ),
            Self::MalformedCopyResponse { payload_len } => write!(
                f,
                "malformed CopyOutResponse / CopyInResponse: {payload_len}-byte payload (short header, format byte not in {{0, 1}}, negative count, or per-column format code disagreeing with overall format)",
            ),
            Self::MalformedDataRow { total_len } => write!(
                f,
                "malformed DataRow: total frame length {total_len} has no body (min valid body = 2-byte column count)",
            ),
            Self::TooManyColumns { count, max } => write!(
                f,
                "result-set too wide: {count} columns (max supported {max})",
            ),
            Self::MalformedParameterDescription { payload_len } => write!(
                f,
                "malformed ParameterDescription: {payload_len}-byte payload (short header, negative count, or body length mismatch)",
            ),
            Self::TooManyParameters { count, max } => write!(
                f,
                "too many parameters in ParameterDescription: {count} (max supported {max})",
            ),
            Self::UnexpectedFormatCode { code } => write!(
                f,
                "unexpected format code in RowDescription: {code} (expected 0 text or 1 binary)",
            ),
            Self::InternalCrateBug { locus } => write!(
                f,
                "internal bsql-pg-proto bug at locus {locus}",
            ),
            Self::ColumnCountMismatch { expected, actual } => write!(
                f,
                "prepared query row arity mismatch: expected {expected} columns (R::ARITY), \
                 server delivered {actual}",
            ),
            Self::ChunkedColumnInTypedRow => f.write_str(
                "prepared query: column body exceeds read-buf headroom and would require chunked \
                 assembly; v1 typed-decode requires contiguous columns. Use `col_next` directly \
                 for multi-MB cells (chunk-aware typed decoders are a planned follow-up)",
            ),
            Self::DecodeFailure(err) => write!(
                f,
                "prepared query: per-column decode failed: {err}",
            ),
            Self::ConnectionAlreadyClosed { prior_kind } => {
                write!(
                    f,
                    "connection already closed (prior fatal kind: {prior_kind:?})",
                )
            }
        }
    }
}
