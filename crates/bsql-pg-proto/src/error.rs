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
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
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
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        // SAFETY note — we would need unsafe to avoid the utf8 check,
        // but the forbid-bundle bans unsafe. The check is O(5) —
        // negligible on a cold error path.
        core::str::from_utf8(&self.bytes).unwrap_or("?????")
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

// -----------------------------------------------------------------
// ProtocolError (below)
// -----------------------------------------------------------------


/// A classified failure on the wire protocol.
///
/// Errors are *transport-level* signals from the state machine to the
/// async wrapper, not user-visible types — the wrapper translates them
/// into its public `BackendError` (Phase 1e). Variants are kept narrow
/// and self-describing; the wrapper never has to invent error context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[expect(clippy::large_enum_variant, reason = "no_alloc crate: Box unavailable; ProtocolError is constructed on error paths only, never hot path. Now Copy-and-POD (DEF-060 part 2 + POD BoundedStr) — making Action<'buf> and OutActions<'buf> Drop-free so NLL releases borrows at last use without explicit drop().")]
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
        /// The offending PG message tag.
        tag: u8,
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
    /// [`Severity`] + [`SqlStateCode`] (5-byte SQLSTATE) + bounded
    /// strings with explicit truncation via `BoundedStr`.
    ///
    /// Size: ~280 bytes (down from ~848 in the pre-DEF-060 form with
    /// 5 × `heapless::String<256>`). The shrink landed alongside the
    /// silent-truncation fix — previous code used
    /// `heapless::String::try_from(s).unwrap_or_default()` which
    /// silently collapsed oversized server messages to empty
    /// (tier-4); now overflow appends an explicit `"…"` marker
    /// (tier-2 structural — bounded, explicit).
    ServerErrorResponse {
        /// Severity classification (enum, 1 byte). Unknown server
        /// severities map to [`Severity::Unknown`] rather than silently
        /// dropping the field.
        severity: Severity,
        /// SQLSTATE code — always exactly 5 ASCII chars, packed as
        /// a `[u8; 5]` newtype. Space-padded if shorter; never empty.
        code: SqlStateCode,
        /// Primary human-readable error message (up to 128 bytes,
        /// explicit truncation marker on overflow).
        message: BoundedStr<128>,
        /// Optional detail string (up to 96 bytes).
        detail: BoundedStr<96>,
        /// Optional hint string (up to 64 bytes).
        hint: BoundedStr<64>,
    },

    /// Server sent an authentication method we do not support.
    ///
    /// PG's Authentication message (tag `'R'`) carries a sub-code for
    /// the method. Phase 1b supports only sub-code 0 (Ok), 10 (SASL),
    /// 11 (SASLContinue), 12 (SASLFinal). Anything else lands here.
    UnsupportedAuthMethod {
        /// The sub-code the server requested.
        sub_code: u32,
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

    /// A local protocol-crate invariant was violated.
    ///
    /// Classified rather than silent: in Phase 1a the only emission site
    /// is the "advance-past-unread" branch in [`crate::PgProtocol::feed_bytes`]
    /// that a future refactor could reach if someone broke the local
    /// `unread().len() >= total_len` check that precedes the advance. The
    /// branch is currently unreachable by audit of the single function
    /// that guards it; surfacing it as a classified error rather than
    /// leaving it mislabelled as `MalformedFrameLength` makes any future
    /// regression loud (the wrapper sees a distinct error code and the
    /// connection is torn down cleanly).
    ///
    /// If this error ever appears at runtime it is a logic bug in
    /// `bsql-pg-proto` itself, not wire-level input — triage accordingly.
    ProtocolInvariantBroken,

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
        prior_kind: ErrorKind,
    },
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
    /// [`ProtocolError::UnsupportedProtocolOption`] +
    /// [`ProtocolError::StartupAlreadyInProgress`].
    Auth = 3,
    /// Internal invariant broken — bug in this crate.
    /// [`ProtocolError::ProtocolInvariantBroken`].
    Internal = 4,
    /// Pseudo-kind for a `ConnectionAlreadyClosed` meta-error. Only
    /// ever appears in `FailReply` replies, never in state (the state
    /// retains the real prior kind).
    AlreadyClosed = 5,
}

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
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::MalformedFrameLength { .. }
            | Self::FrameTooLarge { .. }
            | Self::UnexpectedFrame { .. }
            | Self::MalformedReadyForQuery { .. }
            | Self::MalformedBackendKeyData { .. }
            | Self::MalformedAuthentication { .. } => ErrorKind::Framing,
            Self::ReadBufferFull { .. } => ErrorKind::Transport,
            Self::ServerErrorResponse { .. } => ErrorKind::ServerError,
            Self::UnsupportedAuthMethod { .. }
            | Self::UnsupportedProtocolOption
            | Self::Scram(_)
            | Self::StartupAlreadyInProgress => ErrorKind::Auth,
            Self::ProtocolInvariantBroken => ErrorKind::Internal,
            Self::ConnectionAlreadyClosed { .. } => ErrorKind::AlreadyClosed,
        }
    }
}

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
                // Print the tag as a character if it is in the printable
                // ASCII range; otherwise hex. PG message tags are all in
                // `0x20..=0x7e`, so the printable branch is the norm.
                if matches!(*tag, 0x20..=0x7e) {
                    write!(f, "unexpected frame tag '{}' ({tag:#04x})", char::from(*tag))
                } else {
                    write!(f, "unexpected frame tag {tag:#04x}")
                }
            }
            Self::MalformedReadyForQuery { payload_len } => write!(
                f,
                "ReadyForQuery payload length {payload_len} (expected 1)",
            ),
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
                message,
                ..
            } => write!(f, "server error: {severity} ({code}): {message}"),
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
            Self::ProtocolInvariantBroken => {
                f.write_str("protocol invariant violated — internal bsql-pg-proto logic bug")
            }
            Self::ConnectionAlreadyClosed { prior_kind } => {
                write!(
                    f,
                    "connection already closed (prior fatal kind: {prior_kind:?})",
                )
            }
        }
    }
}
