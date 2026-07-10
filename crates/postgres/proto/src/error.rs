//! Classified protocol errors.
//!
//! [`ProtocolError`] is the error type the wire-decode carve-outs
//! ([`crate::decode`] and [`crate::command_tag`]) return when a server frame is
//! malformed. The sans-IO engine consumes it opaquely — any `Err` tears the
//! connection down via the engine's own [`crate::engine::EngineError`]; the
//! engine never inspects the variant. Public surface is
//! [`#[non_exhaustive]`][non_exhaustive] so adding a variant when its emission
//! site lands does not break user `match`es.
//!
//! [non_exhaustive]: https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute

use core::fmt;

// -----------------------------------------------------------------
// ProtocolError footprint anchor (co-located at the definition).
// -----------------------------------------------------------------
//
// A wrong pin — or any layout change that invalidates a correct pin — aborts
// the build with E0080. Both size and align are pinned in one anchor.
crate::wire_pin!(ProtocolError, size = 24, align = 8);

/// A classified failure decoding a server wire frame.
///
/// Every variant is constructed by a wire-decode carve-out: the `'C'`
/// CommandComplete parser ([`crate::command_tag`]) and the `'T'`/`'H'`/`'G'`
/// RowDescription / CopyResponse parsers ([`crate::decode`]). The engine
/// receives these as the `Err` arm of those parsers and tears the connection
/// down; it never matches a specific variant.
///
/// Errors are *transport-level* signals, not user-visible types — the driver
/// translates them into its public error enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProtocolError {
    /// Server sent a `CommandComplete` (`'C'`) payload that was not
    /// NUL-terminated or otherwise malformed. The `CommandComplete` body is
    /// an ASCII command tag terminated by a NUL byte; a missing terminator or
    /// non-ASCII bytes beyond the cap signal framing desync.
    MalformedCommandComplete {
        /// Actual payload byte count.
        payload_len: usize,
    },

    /// Server sent a malformed `RowDescription` (`'T'`) payload — short
    /// header, negative column count, missing name NUL, truncated per-column
    /// metadata, or trailing bytes after the declared columns. Framing-desync
    /// classification: the connection is torn down.
    MalformedRowDescription {
        /// Actual payload byte count.
        payload_len: usize,
    },

    /// Server sent a malformed `CopyOutResponse` (`'H'`) or `CopyInResponse`
    /// (`'G'`) payload — short header (< 3 bytes for format + count), format
    /// byte not in `{0, 1}`, negative column count, trailing bytes, or a
    /// per-column format code disagreeing with the overall format byte
    /// (PG §55.2.6 pins per-column codes to equal overall). Framing-desync.
    MalformedCopyResponse {
        /// Actual payload byte count.
        payload_len: usize,
    },

    /// Server's `RowDescription` declares more columns than
    /// [`crate::MAX_ROW_COLUMNS`] (1664 — PostgreSQL's own
    /// `MaxTupleAttributeNumber`). Unlike the malformed-frame variants above, the
    /// frame is WELL-FORMED — only too wide — so the stream position is known and
    /// this is RECOVERABLE: the driver drains the in-flight result to the trailing
    /// `ReadyForQuery` and leaves the connection alive + pooled, then the caller
    /// retries with a narrower projection. A conforming server never exceeds 1664
    /// (it errors at 1665 first, a server error), so this classifies a
    /// nonconforming peer.
    TooManyColumns {
        /// Column count declared by the server.
        count: usize,
        /// Maximum supported — [`crate::MAX_ROW_COLUMNS`].
        max: usize,
    },

    /// Server's `RowDescription` carried a per-column format code outside the
    /// legal `{0, 1}` range. Text (`0`) and binary (`1`) are the only values
    /// PG defines; any other value is a server-side wire violation.
    UnexpectedFormatCode {
        /// The offending format code from the server.
        code: i16,
    },
}

impl ProtocolError {
    /// Compact kind classification for this error. Every current variant is a
    /// framing-class violation (a server wire-format error in a decoded
    /// frame). Exhaustive match — adding a `ProtocolError` variant without
    /// classifying it is a build error.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        match self {
            Self::MalformedCommandComplete { .. }
            | Self::MalformedRowDescription { .. }
            | Self::MalformedCopyResponse { .. }
            | Self::TooManyColumns { .. }
            | Self::UnexpectedFormatCode { .. } => ErrorKind::Framing,
        }
    }

    /// Total projection to the state-storable [`StateErrorKind`] (the
    /// connecting-phase error classification carried by
    /// [`crate::state::ConnectingState::Errored`]).
    #[inline]
    #[must_use]
    pub const fn state_kind(&self) -> StateErrorKind {
        StateErrorKind::from_kind_or_internal(self.kind())
    }
}

// `ProtocolError` satisfies the canonical `core::error::Error` contract so
// downstream crates can `?`-propagate it through `Box<dyn Error>` boundaries.
// Empty body: it is a leaf error type (no inner error it wraps).
impl core::error::Error for ProtocolError {}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            Self::TooManyColumns { count, max } => write!(
                f,
                "result-set too wide: {count} columns (max supported {max})",
            ),
            Self::UnexpectedFormatCode { code } => write!(
                f,
                "unexpected format code in RowDescription: {code} (expected 0 text or 1 binary)",
            ),
        }
    }
}

/// Compact error classification — a 1-byte category for an error.
///
/// `#[repr(u8)]`; explicit discriminants are stable wire-adjacent identities
/// (a gap at `2` is the retired `ServerError` kind — the engine classifies
/// server `ErrorResponse` frames via the driver's error type, not here).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorKind {
    /// Wire framing: malformed length / tag / row-or-copy descriptor.
    Framing = 0,
    /// Read buffer overflow — local transport classification.
    Transport = 1,
    /// Authentication negotiation failed.
    Auth = 3,
    /// Internal invariant broken — a bug in this crate.
    Internal = 4,
    /// Pseudo-kind for an "already closed" meta-error. Only ever appears in a
    /// reply, never stored in state.
    AlreadyClosed = 5,
    /// Client-side command-ordering error (caller drove the connection out of
    /// order). Not a server auth failure.
    ClientOrdering = 6,
}

/// Subset of [`ErrorKind`] that can be stored in a connecting-phase
/// [`crate::state::ConnectingState::Errored`] terminal.
///
/// # Tier-1 compile invariant
///
/// "State never holds `ErrorKind::AlreadyClosed`" is enforced at the type
/// level: the constructor [`Self::try_from_kind`] rejects `AlreadyClosed`, so
/// the state variant cannot type-check with that kind at the construction site.
///
/// # Layout
///
/// `#[repr(transparent)]` over `ErrorKind` — 1 byte; `Option<StateErrorKind>`
/// niche-packs to 1 byte (`ErrorKind` uses few of 256 u8 discriminants).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct StateErrorKind(ErrorKind);

impl StateErrorKind {
    /// Construct from a full [`ErrorKind`]. Returns `None` for
    /// [`ErrorKind::AlreadyClosed`] — the reply-only "pseudo-kind" that never
    /// reaches state. The match is exhaustive; a future `ErrorKind` addition
    /// forces an explicit state-storable decision here.
    #[inline]
    #[must_use]
    pub const fn try_from_kind(k: ErrorKind) -> Option<Self> {
        match k {
            ErrorKind::AlreadyClosed => None,
            ErrorKind::Framing
            | ErrorKind::Transport
            | ErrorKind::Auth
            | ErrorKind::Internal
            | ErrorKind::ClientOrdering => Some(Self(k)),
        }
    }

    /// Infallible conversion — maps [`ErrorKind::AlreadyClosed`] to `Internal`
    /// (a nonsensical `AlreadyClosed` reaching state implies a crate bug,
    /// which is what `Internal` classifies). Sole infallible-path production.
    #[inline]
    #[must_use]
    pub const fn from_kind_or_internal(k: ErrorKind) -> Self {
        match Self::try_from_kind(k) {
            Some(s) => s,
            None => Self(ErrorKind::Internal),
        }
    }

    /// Unwrap to the underlying [`ErrorKind`]. Always non-`AlreadyClosed` by
    /// the constructor's invariant.
    #[inline]
    #[must_use]
    pub const fn as_kind(self) -> ErrorKind {
        self.0
    }
}

impl core::error::Error for StateErrorKind {}

impl fmt::Display for StateErrorKind {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

// Drift pin: size/niche invariant.
const _: () = assert!(
    core::mem::size_of::<StateErrorKind>() == 1,
    "StateErrorKind must stay 1 byte (#[repr(transparent)] over ErrorKind)",
);
const _: () = assert!(
    core::mem::size_of::<Option<StateErrorKind>>() == 1,
    "Option<StateErrorKind> must niche-pack to 1 byte (ErrorKind uses few of 256 u8 discriminants)",
);
