//! Classified protocol errors.
//!
//! Phase 1a ships only the variants the Ping flow can produce. Per
//! reforge.md §3.5 / §4.6, manufactured variants are forbidden — a
//! variant lands when its emission site lands.
//!
//! Public surface is [`#[non_exhaustive]`][non_exhaustive] so adding
//! variants in 1b/1c does not break user `match`es; user code is forced
//! to carry a `_ =>` catch-all and gets a clean upgrade path.
//!
//! [non_exhaustive]: https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute

use core::fmt;

// -----------------------------------------------------------------
// DEF-060 part 2 — typed fields for ServerErrorResponse
// -----------------------------------------------------------------

/// Server-error severity classification. DEF-060 part 2.
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
    /// DEF-154 (V): `const fn` so the round-trip pin below can
    /// const-assert `from_bytes(as_str(v).as_bytes()) == v`.
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

// DEF-154 (V) P2-6: round-trip compile pin for Severity.
// `from_bytes(as_str(v).as_bytes())` must equal v for every
// variant. Catches body-swap drift between the 8 known variants'
// literal mappings. `Unknown` is special — `as_str` returns
// "UNKNOWN" which `from_bytes` does NOT recognize, falling through
// to `Self::Unknown` correctly (round-trip preserved via fallthrough).
//
// `matches!` in const context (stable since Rust 1.46) avoids both
// the `as u8` coercion (forbid-bundle bans) and the requirement for
// a const `PartialEq` impl.
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

// F-054 (pass-#8): niche-pack invariant pin. `Severity` is `#[repr(u8)]`
// with 9 variants occupying discriminants 0..=8; values 9..=255 are
// unused, giving `Option<Severity>` a niche for `None` without
// growing the layout. The 1-byte total for `Option<Severity>` is
// load-bearing — `parse_error_response` uses it as the "seen /
// not-seen" severity slot (DEF-060 pattern) and every byte of the
// 280-byte ProtocolError variant matters. Adding a 248th+ variant
// to Severity would overflow the niche and silently grow the
// Option to 2 bytes — this assert fails the build instead.
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
    /// DEF-154 (U) P2/P3: explicit match over `from_utf8` with a
    /// documented-dead None arm. `self.bytes` is ASCII-only by
    /// construction (`from_bytes` coerces every non-ASCII byte to
    /// `b'?'`); ASCII is valid UTF-8 → Err arm architecturally
    /// unreachable. Pre-(U) was `unwrap_or("")` — silent fallback
    /// pattern user banned. Post-(U): empty-string sentinel on
    /// the dead arm is explicit (no corruption vector at the
    /// display-only boundary; empty code surfaces as visible
    /// regression in logs).
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

// `BoundedStr<N>` moved to `crate::ident` as a type alias for
// `FixedStr<N, BoundedStrTag>` (DEF-096). Re-exported here so
// downstream code continues to write `error::BoundedStr<128>`.
pub use crate::ident::BoundedStr;

/// Typed classification for [`ProtocolError::UnsupportedAuthMethod`].
/// Architect finding #1 (2026-04-21).
///
/// Distinguishes "server sent a sub-code we don't know about" from
/// "server sent a known sub-code that's wrong for the current state"
/// — the latter preserves the typed [`crate::wire::AuthSubCode`]
/// enum so diagnostics can say *which* known method the server
/// requested.
///
/// # `#[non_exhaustive]` (DEF-256, audit 2026-05-08)
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
    /// DEF-184 (B9): `NonZeroU32` (not `u32`) — tier-1 structural
    /// proof that server sent a value other than 0 (AUTH_OK = 0 is
    /// a known sub-code, classified via `KnownButWrong(AuthSubCode::Ok)`
    /// if seen in wrong state, never in `Unknown`). Niche optimises
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

// DEF-184 (B9): drift-pin — `Unknown(NonZeroU32)` must niche into
// the same 8 bytes as `KnownButWrong(AuthSubCode)`. Pre-(184) was
// `Unknown(u32)` — 8 bytes incl. discriminant + 4-byte padding.
// Post-(184) NonZeroU32 carries the non-zero invariant at type
// level; `Option<AuthSubCodeClass>` niche-optimises via the
// 0-u32-bit-pattern slot.
const _: () = assert!(
    core::mem::size_of::<AuthSubCodeClass>() == 8,
    "AuthSubCodeClass: 4-byte discriminant + 4-byte NonZeroU32 payload",
);
const _: () = assert!(
    core::mem::size_of::<Option<AuthSubCodeClass>>() == 8,
    "Option<AuthSubCodeClass>: niche-packed via NonZeroU32 = 8 B",
);

// -----------------------------------------------------------------
// ProtocolError (below)
// -----------------------------------------------------------------

// DEF-184 (audit #3 A-12): colocated drift-pin.
//
// ProtocolError exact size must stay 72 B post-(A1+A13) ErrorArena
// externalisation — a variant growth here cascades into
// `Action<'w,'r>` (88 B), `OutActions = [Action; 9] + len` (800 B),
// and `StreamItem<'a>` (~80 B). The cascade costs 1-2 KB of per-call
// stack frame, so the pin catches:
//
//   • New payload field on any variant that exceeds the 72 B budget.
//   • Refactor that re-inlines the 288 B bounded strings into
//     `ServerErrorResponse` (defeating the A1+A13 goal).
//   • Alignment-driven padding bumps from field ordering changes.
//
// The complementary `Action` / `OutActions` pins live in lib.rs
// alongside the full cascade measurements; pin-in-error.rs catches
// drift AT the variant-definition site (Fail-Fast locality) rather
// than at first use.
const _: () = assert!(
    core::mem::size_of::<ProtocolError>() == 72,
    "ProtocolError exact size — 72 B post-(A1+A13). \
     Variant shape change detected. Run `cargo expand --test` and \
     audit each variant payload: ServerErrorResponse should carry \
     ErrorRef (8 B), not inline BoundedStr<N>. See lib.rs cascade \
     pins (Action / OutActions) for downstream impact.",
);


/// A classified failure on the wire protocol.
///
/// Errors are *transport-level* signals from the state machine to the
/// async wrapper, not user-visible types — the wrapper translates them
/// into its public `BackendError` (Phase 1e). Variants are kept narrow
/// and self-describing; the wrapper never has to invent error context.
// DEF-184 (A1+A13): ProtocolError shrunk 312 B → ~72 B post-
// ServerErrorResponse arena externalisation; no longer triggers
// `large_enum_variant` lint.
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
    /// # DEF-185 P0-F (audit 2026-04-24)
    ///
    /// Pre-fix: the dispatch arms for these frames ignored the
    /// `payload` parameter entirely — a server sending 500 bytes of
    /// body on `EmptyQueryResponse` was silently accepted, the bytes
    /// consumed, state transitioned, no error emitted. **Tier-4 silent
    /// spec drift.** A future PG version that legitimately added a
    /// body field to one of these frames would be silently accepted
    /// instead of classified — masking the protocol break.
    ///
    /// Post-fix: every zero-body arm explicitly slice-pattern matches
    /// `[]` and classifies any other body as `UnexpectedFrameBody`.
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
    /// handshake or mid-query. DEF-060 part 2: typed
    /// [`Severity`] + [`SqlStateCode`] (5-byte SQLSTATE); bounded
    /// strings (message / detail / hint) live in
    /// [`crate::PgProtocol`]'s `ErrorArena` post-(DEF-184).
    ///
    /// Size: variant payload = `Severity` 1 B + `SqlStateCode` 5 B +
    /// `ErrorRef` 8 B = 14 B. The enclosing `ProtocolError` pins at
    /// 72 B exact (A-12 const-assert in error.rs) — down from 312 B
    /// pre-DEF-184 where the three bounded strings (288 B) were
    /// inline. That shrink cascades: `Action<'w,'r>` 312 → 88 B,
    /// `OutActions = [Action; 9]` 2808 → 800 B per feed_bytes call.
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
        /// DEF-184 (A1+A13): handle into [`crate::PgProtocol`]'s
        /// `ErrorArena` for the bounded strings (message / detail /
        /// hint). Pre-(184) these were inline `BoundedStr<N>` fields
        /// (~288 B), cascading through `Action::FailReply.cause` →
        /// `OutActions = [Action; 9]` (2808 B stack frame) →
        /// `StreamItem::FailReply.cause` (320 B per-pull).
        ///
        /// Post-(184) carries 8 bytes: NonZeroU8 slot + u32
        /// generation + 3 B struct padding (see `error_arena.rs`
        /// size pin). A-04 widened gen u8→u32 for wrap-safety on
        /// long-running connections; A-06 elevated the resolve API
        /// to `Result<&ErrorPayload, ArenaError>` with classified
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
    /// the method. Phase 1b supports only sub-code 0 (Ok), 10 (SASL),
    /// 11 (SASLContinue), 12 (SASLFinal). Anything else lands here.
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
    /// method 10". Architect finding #1 (2026-04-21).
    UnsupportedAuthMethod {
        /// Typed classification of the offending sub-code.
        sub_code: AuthSubCodeClass,
    },

    /// Server sent `NegotiateProtocolVersion` (tag `'v'`) during
    /// startup, indicating it does not support a protocol option we
    /// requested. DEF-044.
    UnsupportedProtocolOption,

    /// SCRAM authentication failure.
    ///
    /// DEF-060: typed variant carrying the discrete [`scram::wire::ScramError`]
    /// classification directly. The previous shape
    /// `ScramError { detail: heapless::String<128> }` was a tier-3
    /// silent-truncation seam (`.unwrap_or_default()` on
    /// `heapless::String::try_from`) — formatted strings larger than
    /// 128 bytes silently collapsed to empty. Now the cause is a
    /// discrete enum; `Display` is computed from the variant, no
    /// intermediate buffer, no truncation class.
    ///
    /// [`scram::wire::ScramError`]: crate::scram::wire::ScramError
    Scram(crate::scram::wire::ScramError),

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
    /// startup handshake already occupies the connection. 1c-1b:
    /// simple-query states reject new pushes with this error — the
    /// existing query must complete first.
    CommandInProgress,

    /// Server sent a `CommandComplete` (`'C'`) payload that was not
    /// NUL-terminated or otherwise malformed. 1c-1b: the
    /// `CommandComplete` body is an ASCII command tag
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
    /// declared columns. 1c-2a framing-desync classification. The
    /// connection is torn down (the wire is out of sync with the
    /// per-column 18-byte stride).
    MalformedRowDescription {
        /// Actual payload byte count.
        payload_len: usize,
    },

    /// DEF-154 (F): server sent a `DataRow` (`'D'`) frame with no
    /// body — the 5-byte header is followed by zero payload bytes
    /// (`total_len == HEADER_LEN`). PG's wire spec mandates at
    /// minimum a 2-byte column count in the body even for zero-
    /// column rows; a 0-byte body signals framing desync or a
    /// malformed/adversarial server.
    ///
    /// Pre-DEF-154 (F) this case routed to
    /// `InternalCrateBug { locus: EmptyReadRange }` via the
    /// `NonEmptyRange::new` None branch in `ReadRange::new` — a
    /// misclassification: the crate isn't buggy, the server is.
    /// Operators reading a log that says "internal bsql-pg-proto
    /// bug" would chase the wrong target.
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
    /// with a narrower projection. 1c-2a.
    TooManyColumns {
        /// Column count declared by the server.
        count: usize,
        /// Maximum supported — [`crate::MAX_ROW_COLUMNS`].
        max: usize,
    },

    /// Server's `RowDescription` carried a per-column format code
    /// outside the legal `{0, 1}` range. Round-4 finding #5: text
    /// (`0`) and binary (`1`) are the only values PG defines; any
    /// other value is a server-side wire violation. 1c-2a.
    UnexpectedFormatCode {
        /// The offending format code from the server.
        code: i16,
    },

    /// Server's `ParameterDescription` (`'t'`) body was ill-formed:
    /// payload too short to hold the 2-byte count header, declared
    /// count disagrees with the remaining byte length, or negative
    /// count. Wire violation; the connection is torn down.
    ///
    /// 1c-3c: emitted by [`crate::decode::parse_parameter_description`].
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
    /// 1c-3c. Mirrors [`Self::TooManyColumns`] shape.
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
    /// DEF-150: consolidates three pre-merge variants
    /// (OutboundFrameBuildUnreachable / ReadCursorAdvanceUnreachable
    /// / RowRangeConstructionUnreachable). DEF-188: the
    /// `StaleSchemaRef` and `SchemaArenaAllocFull` loci were
    /// DELETED with the schema arena — without a `SchemaRef`
    /// handle, generation drift cannot occur (architecturally
    /// impossible). Classification is always
    /// [`ErrorKind::Internal`].
    ///
    /// F6 / DEF-150: uniform "internal crate bug" shape replaces
    /// three separate variants — fewer discriminants, single
    /// diagnostic template, additive locus enum for new dead-paths
    /// as they're identified.
    InternalCrateBug {
        /// Identifies the specific architecturally-dead code path
        /// that fired. Diagnostic only — every locus classifies as
        /// `ErrorKind::Internal`.
        locus: CrateBugLocus,
    },

    /// DEF-244 (2026-05-13): `RowStream::collect_tuple<R>` observed
    /// a row with column count different from the prepared query's
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

    /// DEF-244 (2026-05-13): a column body exceeded the active
    /// read-buf headroom during a typed `collect_tuple` call. The
    /// v1 typed-decode path requires contiguous column bytes;
    /// chunked columns are not assembled into typed values (would
    /// require either caller-owned scratch buffer or heap-allocated
    /// per-cell vectors — both outside the no_alloc contract).
    ///
    /// Caller falls back to `col_next` for the row to consume the
    /// chunked bytes. Wider coverage tracks DEF-244 follow-up.
    ChunkedColumnInTypedRow,

    /// DEF-244 (2026-05-13): a per-column `DecodeFormat::decode`
    /// call returned an error during a typed `collect_tuple` row
    /// assembly. The inner [`crate::decode::DecodeError`] is the
    /// root cause (bad UTF-8, IntParse, NullInNonNullColumn, etc.).
    /// The connection itself is healthy — the error is row-level,
    /// not transport-level.
    DecodeFailure(crate::decode::DecodeError),

    /// A user command arrived on a connection that had already been
    /// torn down by a prior fatal. The wrapper translates this into
    /// the public error "connection closed, see earlier error" with
    /// the `prior_kind` as diagnostic context.
    ///
    /// Introduced by DEF-061 — see [`ErrorKind`] for the ship-order
    /// rationale. Before DEF-061, the full 856-byte `ProtocolError`
    /// was cloned into every `FailReply` on an Errored connection;
    /// now the prior cause is surfaced **once** in the first
    /// `FailReply`, and subsequent pushes get a compact
    /// `ConnectionAlreadyClosed { prior_kind }` (17 bytes incl.
    /// discriminant + padding).
    ConnectionAlreadyClosed {
        /// Classification of the earlier fatal that closed the
        /// connection. The full cause was emitted exactly once in
        /// the first `FailReply` action; the wrapper is expected to
        /// have preserved it.
        ///
        /// DEF-142 (pass-#8 F-056): typed as [`StateErrorKind`] (not
        /// [`ErrorKind`]) so the type system proves this field can
        /// never recursively be `AlreadyClosed` — a
        /// `ConnectionAlreadyClosed { prior_kind: AlreadyClosed }`
        /// nonsense value is a type error at construction.
        prior_kind: StateErrorKind,
    },
}

/// DEF-150 locus discriminator for
/// [`ProtocolError::InternalCrateBug`]. Names the specific
/// architecturally-dead code path that fired; every locus
/// classifies as [`ErrorKind::Internal`].
///
/// Additive: as new dead-paths are identified (e.g. DEF-154's
/// buffer-witness stale-ref detection), variants grow WITHOUT
/// expanding the top-level [`ProtocolError`] enum.
///
/// DEF-184 (B23): `#[repr(u8)]` makes the discriminant explicit
/// 1-byte. `Option<CrateBugLocus>` niche-packs in the same byte —
/// const-asserted below to catch drift if a future variant with
/// payload lands.
///
/// # `#[non_exhaustive]` (DEF-256, audit 2026-05-08)
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
    /// bug. Pre-DEF-150: `ReadCursorAdvanceUnreachable`.
    ReadCursorAdvance,

    /// [`crate::action::NonEmptyRange::new`] returned None when
    /// constructing a row-range for a `DataRow` frame.
    /// `parse_header` validates `payload_end <= populated_len`;
    /// emission indicates a [`crate::dispatch::FrameCoords`] math
    /// bug. Pre-DEF-150: `RowRangeConstructionUnreachable`.
    RowRangeConstruction,

    /// DEF-154 (B) Phase B4-W P0-3: a `ParamsWriter::write_params`
    /// impl returned `Err(WriteBufFull)` while the `Bind` frame was
    /// being built. `ParamsWriter` is a `pub` sealed trait —
    /// user impls of arity 0..=16 exist via derive/macro. A
    /// well-behaved impl never triggers this: the crate's
    /// `MAX_OWNED_SEND_LEN` is const-asserted against the worst-
    /// case `max_bind_message_size()` sum. Emission indicates
    /// either a drift between `MAX_PARAMS_DATA_TOTAL` and the
    /// builder's size budget, or an adversarial/buggy user impl
    /// that writes past its advertised bound. Pre-B4-W P0-3 fix,
    /// this Err was silently discarded with a `debug_assert!(false)`,
    /// shipping a truncated Bind frame with miscomputed length
    /// prefix — tier-4 silent corruption. Tier-3 classified now.
    ParamsWriterOverflow,

    /// DEF-154 (M) P0-3: a crate-internal frame builder
    /// (`build_query_message`, `build_parse_message`, etc.) saw
    /// `Err(WriteBufFull)` from a `BrandedWriteReserved::push_*`
    /// call. Pre-(M), the 7 push_* methods accepted `WriteBufFull`
    /// with `debug_assert! + silent discard` — release builds kept
    /// writing a frame whose length-prefix had already been emitted
    /// ASSUMING body bytes would follow, producing a
    /// correct-looking-length `Action::SendBytes` with TRUNCATED
    /// content (bit-junk on wire, PG server sees framing desync).
    /// Post-(M): every push_* returns Result, builders `?`
    /// propagate, builder-return Err classified as this locus and
    /// routed through `FailReply + CloseSocket`.
    ///
    /// Architecturally dead under
    /// `const _: () = assert!(MAX_OWNED_SEND_LEN >= max_*_message_size())`
    /// in write_buf.rs — but the const-assert only catches
    /// BUILDER-DECLARED max-size drift; a push site that violates
    /// its declared budget (e.g. a new builder missing a length
    /// cap) lands here rather than silently ships corrupt bytes.
    BuilderCapacityOverflow,

    /// DEF-154 (B) Phase B4-W P0-2: a `build_*_message` branded
    /// builder produced a zero-length span when
    /// `WriteRange::from_write_span` invoked
    /// `NonEmptyRange::new(start, reserved.len(), reserved.len())`
    /// and got `None`.
    ///
    /// Architecturally dead under intact builders: every PG wire
    /// builder emits ≥ 5 bytes (tag + 4-byte length prefix + body),
    /// so `reserved.len() > start` holds post-build. Emission
    /// indicates a builder bug (missed push) or const-assert drift
    /// on `MAX_OWNED_SEND_LEN`.
    ///
    /// Pre-P0-2 fix, the None case silently fell back to a
    /// unit-length `NonEmptyRange (start=0, len=1)` — applied against
    /// an empty buffer in materialise, produced a 0-byte
    /// `Action::SendBytes`, handshake hangs at the wire (tier-4
    /// silent corruption). Tier-3 classified now: builders return
    /// `Result<WriteRange, ProtocolError>`; `compute_push_*`
    /// routes `Err` through `FailReply + CloseSocket`.
    EmptyWriteRange,

    /// DEF-184 (B9): `AuthSubCode::try_from_u32` returned Err
    /// carrying raw value 0 — architecturally impossible because
    /// `AUTH_OK = 0` is the first match arm and returns Ok. The
    /// `AuthSubCodeClass::Unknown(NonZeroU32)` niche-packed variant
    /// rejects zero values at the type level; this locus classifies
    /// the dead arm that would otherwise require either silent
    /// fallback (tier-4, CREDO §5) or new-variant-with-payload.
    AuthSubCodeZeroInErr,

    /// DEF-271 cluster D (2026-05-10): the static `AtomicU64` counter
    /// backing [`crate::PgProtocol::next_reply_id`] reached `u64::MAX`
    /// and the next mint would produce a duplicate ID (atomics wrap
    /// to 0 by spec; subsequent mints cycle through previously-issued
    /// values). Architecturally distant (~10^19 mints process-wide)
    /// but a real ceiling.
    ///
    /// Pre-DEF-271 the saturation point silently allowed the
    /// duplicate-ID return; the wrapper's pending-replies table would
    /// mis-route subsequent server replies to the wrong correlator.
    /// Post-DEF-271 saturation detection transitions the affected
    /// `PgProtocol` instance to `Errored(ReplyIdSaturation)`, so the
    /// next push fails with `ConnectionAlreadyClosed`-classified the
    /// duplicate never reaches the server in a usable state. Cross-
    /// instance duplicate-ID risk remains tier-2 (separate residue —
    /// architect's #1B brand-lifetime closure, deferred to Phase 4+).
    ReplyIdSaturation,

    /// DEF-272 cluster γ (2026-05-10): `push_command_internal` was
    /// invoked from a non-Idle state — a contract violation between
    /// the only legitimate caller (`ReadyGuard::push_command`, which
    /// classifies state as Idle via `as_ready` upstream) and
    /// `push_command_internal`. Reaching this locus implies a
    /// structural regression in the ReadyGuard → push_command_internal
    /// pipeline; production-built binaries never reach it under the
    /// existing call graph (state cannot transition between
    /// `as_ready`'s check and `push_command_internal`'s entry — the
    /// `&mut PgProtocol` borrow chain rules out interleaving).
    PushCommandInternalNonIdle,

    /// DEF-248 Sub-A (2026-05-12): the closure passed to
    /// [`crate::PgProtocol::iter_rows`] returned (normal exit, early
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

    /// DEF-280 Bundle G (2026-05-18): a `compute_push_*` family function
    /// staged a `StagedAction::DeliverReply` action — architecturally
    /// dead because replies come from the server via `feed_bytes` only;
    /// the push path never emits DeliverReply. Reaching this locus
    /// indicates a `compute_push` refactor regression (or pipelining
    /// work that didn't update DEF-160 Z2 invariant).
    ///
    /// Pre-Bundle G the dead arm in the materialise closure for the
    /// push path used `debug_assert!(false, …)` plus a silent drop on
    /// release — the CREDO §V glass pattern. Post-Bundle G the dead
    /// arm classifies via `PushFailure { id: …, cause: InternalCrateBug
    /// { locus: PushEmittedDeliverReply } }` (same sentinel-id shape
    /// as `PushCommandInternalNonIdle`); both modes return Err
    /// uniformly. Post-DEF-280 Bundle J the sentinel id is the distinct
    /// [`crate::reply_id::CRATE_BUG_REPLY_ID_SENTINEL`] (= NonZeroU64::MAX),
    /// not `NonZeroU64::MIN` — the latter collided with the legitimate
    /// first id minted by `next_reply_id` on every connection's first
    /// command. Closed by-construction by the distinct sentinel.
    PushEmittedDeliverReply,

    /// DEF-280 Bundle K (2026-05-18): [`crate::buf::ReadBuf::enter_partial_mode`]
    /// was called while the buffer was already in partial-frame mode
    /// (`partial_remaining > 0`). The streaming dispatcher's state
    /// machine guarantees the precondition (`exit_partial_mode` runs
    /// before re-entry), so reaching this locus indicates an internal
    /// refactor regression in the dispatch loop.
    ///
    /// Pre-Bundle K the same condition was a `debug_assert!` panic in
    /// dev builds + silent overwrite of the prior `partial_remaining`
    /// in release — the CREDO §V glass pattern, with wire-desync
    /// consequence (forgotten body-byte count: the next inbound bytes
    /// classified as a fresh frame header instead of body
    /// continuation). Post-Bundle K both modes return typed `Err` and
    /// route through this locus + `Errored` state install.
    PartialModeReentry,

    /// DEF-280 Bundle K-mirror (2026-05-18): [`crate::buf::ReadBuf::exit_partial_mode`]
    /// was called while the buffer still owed wire body bytes
    /// (`partial_remaining > 0`). The streaming dispatcher's
    /// state machine guarantees the precondition (every wire-legal
    /// streaming row drains its body before reaching the
    /// end-of-row code path), so reaching this locus indicates
    /// either an internal refactor regression in the dispatch loop
    /// OR an adversarial server emitting a malformed DataRow whose
    /// `col_count`/per-column length sum doesn't match the
    /// frame-header body length.
    ///
    /// Pre-Bundle-K-mirror the same condition was a `debug_assert!`
    /// plus silent reset of `partial_remaining` to `0` on release —
    /// the CREDO §V glass pattern, mirror of [`Self::PartialModeReentry`]'s
    /// entry-side hazard. Wire-desync consequence: previously-pending
    /// body bytes never drained from the wire, next inbound bytes
    /// mis-classified as a fresh frame header. Post-Bundle-K-mirror
    /// both modes return typed `Err`, the counter is preserved, and
    /// the caller routes through this locus and `Errored` state install.
    PartialModeExitUndrained,
}

// DEF-184 (B23): niche-packed `Option<CrateBugLocus>` — 1 byte
// since all variants are C-like + `#[repr(u8)]`. Drift pin catches
// any future variant that adds a payload (would bump size to ≥ 2B
// and break the niche).
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
    /// DEF-173 (audit2 A006 + A031): dedicated Display impl for
    /// operator-facing log output. Pre-DEF-173,
    /// [`ProtocolError::InternalCrateBug`]'s Display used `{locus:?}`
    /// (Debug) which renders `OutboundFrameBuild { stage: Query }`
    /// as a Rust struct-expression — cluttered in operator logs and
    /// fragile to a future Debug derive change.
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
    ///
    /// DEF-188: `SchemaArenaAllocFull` and `StaleSchemaRef` loci
    /// removed alongside the schema arena's deletion.
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
        }
    }
}

#[cfg(test)]
mod crate_bug_locus_display_tests {
    //! DEF-173 pin: each [`CrateBugLocus`] variant renders to its
    //! canonical operator-facing string. A rename or Debug-derive
    //! refactor that breaks the rendering will trip these tests
    //! loudly instead of silently corrupting production log output.

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

    // DEF-188: `schema_arena_alloc_full_display` and
    // `stale_schema_ref_display` tests DELETED — `CrateBugLocus`
    // variants `SchemaArenaAllocFull` and `StaleSchemaRef` removed
    // alongside the schema arena. The ref-handle is gone; arena
    // allocation no longer exists. No corresponding production
    // emission site remains.

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

    // DEF-154 (A): `outbound_frame_build_display_per_stage` test and
    // the `FrameBuildStage` enum it exercised were DELETED alongside
    // `CrateBugLocus::OutboundFrameBuild` variant — builders are now
    // infallible via the `WriteReserved` capacity witness, so the
    // locus variant + its stage-discriminator + the pin test all
    // became dead code. See `crate::protocol::build_*_message`.

    /// DEF-271 cluster D (2026-05-10) pin: ReplyIdSaturation locus
    /// renders to its canonical operator-facing string. Trips loudly
    /// if a future rename or display-impl edit silently changes the
    /// log output a wrapper-level monitor relies on.
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

    /// DEF-248 Sub-A (2026-05-12) pin: StreamDroppedMidStream locus
    /// renders to its canonical operator-facing string. Watches for
    /// drift on the closure-scoped iter_rows Drop-install path's
    /// operator-facing log signal.
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

    /// DEF-280 Bundle G (2026-05-18) pin: PushEmittedDeliverReply locus
    /// renders to its canonical operator-facing string. Watches for
    /// drift on the compute_push pipeline classifier-bug signal —
    /// replaces the pre-Bundle G `debug_assert!(false, …)` glass
    /// pattern at protocol.rs:2881 with classified PushFailure.
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

    /// DEF-280 Bundle K (2026-05-18) pin: PartialModeReentry locus
    /// renders to its canonical operator-facing string. Watches for
    /// drift on the row-stream partial-mode classifier-bug signal —
    /// replaces the pre-Bundle K `debug_assert!(partial_remaining
    /// == 0, …)` glass pattern at buf.rs:855 with typed Err return
    /// + classified install_errored routing.
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

    /// DEF-280 Bundle K-mirror (2026-05-18) pin: PartialModeExitUndrained
    /// locus renders to its canonical operator-facing string. Watches
    /// for drift on the row-stream partial-mode exit-with-bytes-owed
    /// classifier-bug signal — replaces the pre-Bundle-K-mirror
    /// `debug_assert!(partial_remaining == 0, …)` glass pattern at
    /// buf.rs:894 with typed Err return + classified install_errored
    /// routing.
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
/// # DEF-061 rationale
///
/// Before DEF-061, the state carried the full `ProtocolError`
/// (~856 bytes dominated by `ServerErrorResponse`'s five
/// `heapless::String<N>` fields). Every `push_command` on an Errored
/// connection cloned the whole thing into a new `FailReply` —
/// ~1.3 KB of stack churn per push on the cold path.
///
/// Now `ProtoState::Errored(ErrorKind)` is 2 bytes (1 for the
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
// DEF-211 SAFE-07 (audit 2026-05-04): `#[non_exhaustive]` pre-empts
// the SemVer footgun — adding a new variant in a future release
// would otherwise be a major-version break. With `non_exhaustive`,
// downstream `match`es require a wildcard arm (or accept future
// variants explicitly), so adding a variant is a minor-version
// non-breaking change. Internal `match`es here remain exhaustive
// because `#[non_exhaustive]` permits exhaustive matches WITHIN
// the defining crate — only EXTERNAL crates are required to use
// a wildcard. Tier-1 invariants on the internal exhaustive-match
// shields (e.g., `ProtocolError::kind`, `StateErrorKind` mapping)
// are preserved.
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
    /// [`ProtocolError::ScramError`] +
    /// [`ProtocolError::UnsupportedProtocolOption`].
    ///
    /// **Pass-#8 F-052 (2026-04-21).** Prior to pass-#8 this bucket
    /// also included `StartupAlreadyInProgress` and `CommandInProgress`
    /// — both client-side push-ordering errors, NOT server-driven auth
    /// failures. Wrappers reading `ConnectionAlreadyClosed { prior_kind }`
    /// from a push-race would see `Auth` and report "authentication
    /// error" when the real cause was the user pushing too fast. Those
    /// variants now route to [`Self::ClientOrdering`].
    Auth = 3,
    /// Internal invariant broken — bug in this crate. Covers
    /// [`ProtocolError::InternalCrateBug`] (DEF-150 merge of the
    /// former three `*Unreachable` variants plus the
    /// builder/writer dead-arm loci. DEF-188 removed
    /// `SchemaArenaAllocFull` and `StaleSchemaRef` along with the
    /// schema arena — those classes are now structurally
    /// impossible).
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
    /// the culprit. Pass-#8 F-052. 1 byte (repr(u8)).
    ClientOrdering = 6,
    /// DEF-189 Q8-C4 — counter-storm classifier.
    ///
    /// Lifts the `malformed_frame_count` saturation event from tier-4
    /// silent (the counter just clamps at u32::MAX) to tier-3 explicit:
    /// when the per-connection malformed-frame counter crosses the
    /// `MALFORMED_STORM_THRESHOLD` (10_000) inside
    /// `fail_inflight_no_readbuf`, the resulting `Errored` transition
    /// classifies as `MalformedStorm` instead of the underlying
    /// per-frame cause.
    ///
    /// # Today: defensive classifier, architecturally hard-to-reach
    ///
    /// Under the current single-event-then-Errored semantics
    /// (`fail_inflight_no_readbuf` early-returns when state is already
    /// Errored, dispatch loop breaks on first malformed frame, post-
    /// Errored feed_bytes short-circuits without dispatching), the
    /// counter caps at 1 in practice. The threshold (10_000) is
    /// unreachable with today's flow.
    ///
    /// The variant is retained as **defensive tier-3 classification**:
    /// if a future flow change (e.g., soft-recovery, batched dispatch
    /// with continue-on-error) lets the counter accumulate, the
    /// classifier activates at the documented threshold without a
    /// silent saturation surprise. Without the variant, an adversary
    /// flooding a partially-tolerant flow with malformed frames would
    /// see the counter pin at u32::MAX with **no diagnostic signal** —
    /// pure tier-4.
    MalformedStorm = 7,
}

/// Subset of [`ErrorKind`] that CAN be stored in
/// [`crate::state::ProtoState::Errored`] and carried as
/// `prior_kind` of [`ProtocolError::ConnectionAlreadyClosed`].
///
/// # DEF-142 (pass-#8 F-056): tier-1 compile invariant
///
/// The invariant "state never holds `ErrorKind::AlreadyClosed`" was
/// previously tier-3 audit — maintained by the `fail_inflight_and_close`
/// early-return guard on already-Errored state. A future refactor
/// that dropped the guard could route `AlreadyClosed` into state
/// and the `prior_kind` field, producing nonsensical
/// `ConnectionAlreadyClosed { prior_kind: AlreadyClosed }` diagnostics.
///
/// This newtype makes the invariant tier-1: the constructor
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
/// 0..=7 post-DEF-189) preserves 247 unused discriminant values
/// as niches, so `Option<StateErrorKind>` is still 1 byte just
/// like `Option<ErrorKind>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct StateErrorKind(ErrorKind);

impl StateErrorKind {
    // DEF-154 (I) — HISTORICAL: `INTERNAL_FALLBACK` const DELETED
    // (Tier-3 audit #53, 2026-05-19, verified). It was exposed
    // publicly only to supply the `unwrap_or_else` landing pad for
    // three `state_kind().unwrap_or_else(|| { debug_assert!(false,
    // ...); INTERNAL_FALLBACK })` call sites, which are now replaced
    // by a total `state_kind() -> StateErrorKind` projection (see
    // `ProtocolError::state_kind` at the end of this module). The
    // internal `Self(ErrorKind::Internal)` sentinel lives on inside
    // `from_kind_or_internal`. The "tier-4 debug-loud-release-silent"
    // pattern the audit flagged is closed.

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
            | ErrorKind::ClientOrdering
            | ErrorKind::MalformedStorm => Some(Self(k)),
        }
    }

    /// Infallible conversion — maps [`ErrorKind::AlreadyClosed`] to
    /// `Internal` (a nonsensical `AlreadyClosed` reaching state
    /// implies a crate bug, which is precisely what `Internal`
    /// classifies).
    ///
    /// DEF-154 (I): this is the sole infallible path production now.
    /// `ProtocolError::state_kind()` is implemented on top of this.
    /// Tests and fixture code use it to produce a `StateErrorKind`
    /// from a known-valid literal without Option ceremony.
    #[inline]
    #[must_use]
    pub const fn from_kind_or_internal(k: ErrorKind) -> Self {
        match Self::try_from_kind(k) {
            Some(s) => s,
            // DEF-154 (I): inline the Internal sentinel instead of a
            // separate `INTERNAL_FALLBACK` const. Architecturally
            // dead: AlreadyClosed never reaches the state-install
            // paths (DEF-142 seal); call-site classification.
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

// DEF-244 modernisation audit (rust-version 1.81): see ProtocolError
// `core::error::Error` impl below for the rationale.
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
    "Option<StateErrorKind> must niche-pack to 1 byte (ErrorKind uses 8 of 256 u8 discriminants post-DEF-189)",
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
            | Self::Scram(_) => ErrorKind::Auth,
            // F-052 (pass-#8): client-side push-ordering bugs must
            // NOT route to `Auth` — they're the user calling push_command
            // out of order, not a server auth failure. Wrappers reading
            // `ConnectionAlreadyClosed { prior_kind: Auth }` would
            // report a misleading "authentication error" diagnostic.
            Self::StartupAlreadyInProgress | Self::CommandInProgress => ErrorKind::ClientOrdering,
            Self::MalformedCommandComplete { .. }
            | Self::MalformedRowDescription { .. }
            | Self::MalformedDataRow { .. }
            | Self::TooManyColumns { .. }
            | Self::UnexpectedFormatCode { .. }
            | Self::MalformedParameterDescription { .. }
            | Self::TooManyParameters { .. } => ErrorKind::Framing,
            Self::InternalCrateBug { .. } => ErrorKind::Internal,
            // DEF-244: typed-row decoder errors. Column-count mismatch
            // is a Framing-class issue (server vs prepared query
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

    /// DEF-176 (audit2 A016) + DEF-154 (I): total projection from
    /// [`ProtocolError`] to the [`StateErrorKind`] subset storable
    /// in [`crate::state::ProtoState::Errored`].
    ///
    /// `ErrorKind::AlreadyClosed` is the only kind that isn't
    /// state-storable in principle (per DEF-142's seal); it only
    /// arises in reply-only contexts (push_command /
    /// push_bind_execute emitting `FailReply { cause: ConnectionAlreadyClosed }`
    /// when the user invokes on an already-Errored state). The
    /// dispatch + feed_bytes + builder paths NEVER see it as a
    /// cause — architectural invariant per DEF-142.
    ///
    /// Pre-(I) (HISTORICAL — closed by DEF-154 (I); Tier-3 audit
    /// #53, 2026-05-19, verified): this method returned
    /// `Option<StateErrorKind>` and three call sites open-coded
    /// `state_kind().unwrap_or_else(|| { debug_assert!(false, ...); INTERNAL_FALLBACK })`
    /// — the exact "release silent + debug loud" pattern the user
    /// has banned (
    /// "никаких потенциальных паник и прочих атрибутов хрупкой и
    /// стеклянной структуры").
    ///
    /// Post-(I), the projection is **total**: `AlreadyClosed →
    /// Internal`. That IS an honest classification ("something went
    /// wrong at the crate level") — not silent corruption.
    /// Architecturally dead under DEF-142 seal; preserved as
    /// behavioural fallback rather than a panic + silent-release
    /// split. Tier-2 typed-total per audit #53 plan.
    #[inline]
    #[must_use]
    pub const fn state_kind(&self) -> StateErrorKind {
        StateErrorKind::from_kind_or_internal(self.kind())
    }
}

/// DEF-154 (M) P0-3: convert `WriteBufFull` (write-side buffer
/// overflow) to the crate-internal-bug classification
/// `BuilderCapacityOverflow`. Enables `?`-propagation through
/// builders that returned `Result<WriteRange, ProtocolError>`
/// pre-(M) and whose push_* sites returned the raw
/// `Result<(), WriteBufFull>` post-(M).
impl From<crate::write_buf::WriteBufFull> for ProtocolError {
    #[inline]
    fn from(_: crate::write_buf::WriteBufFull) -> Self {
        Self::InternalCrateBug {
            locus: CrateBugLocus::BuilderCapacityOverflow,
        }
    }
}

// DEF-244 modernisation audit (rust-version 1.81 — `core::error::Error`
// stabilised). Additive impl: `ProtocolError` now satisfies the
// canonical error-trait contract from `core`. Downstream crates
// (`bsql-driver-postgres`, async wrappers) can `?`-propagate
// `ProtocolError` through `Box<dyn core::error::Error>` boundaries +
// downstream `thiserror`-style enums without a manual `From`/Display
// wrapping bridge. Empty body: the default `Error::source()` (returns
// `None`) is correct — `ProtocolError` is a leaf error type (no inner
// errors it wraps that satisfy `Error`); it has variants carrying typed
// classifications (ScramError, DecodeError) but those are independent
// errors, not chained sources.
//
// `no_std` note: `core::error::Error` is available in `no_std` since
// Rust 1.81; we use the `core::` path (NOT `std::`) to keep the crate
// `no_std`-clean.
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
            Self::Scram(failure) => write!(f, "SCRAM authentication failed: {failure}"),
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
                 for multi-MB cells (DEF-244 follow-up will add chunk-aware typed decoders)",
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
