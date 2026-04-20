//! Fixed-struct session parameters from `ParameterStatus` messages.
//!
//! [`SessionParams`] stores the known-useful parameters PostgreSQL
//! sends during the post-authentication handshake.
//!
//! Unknown keys are parsed and dropped — there is no growable map, so
//! the "overflow" error class does not exist. DEF-042: tier-1 by
//! absence of a growable container.
//!
//! # DEF-114 — typed fields where semantics are structural
//!
//! Four of the nine fields carry values whose semantics are
//! enumerable (encoding name) or binary (boolean). Storing them as
//! opaque bounded strings was tier-2 ("some bounded string body
//! from the wire") — a consumer reading `params.is_superuser`
//! had to re-parse `"on"`/`"off"` at every site, audit-enforced.
//!
//! DEF-114 elevates these four fields:
//!
//! - [`is_superuser`](SessionParams::is_superuser): `Option<bool>` —
//!   parsed from `"on"`/`"off"` at ingest; unrecognised values drop
//!   to `None`. Tier-1 on "value is bool or absent".
//! - [`integer_datetimes`](SessionParams::integer_datetimes): same.
//! - [`server_encoding`](SessionParams::server_encoding):
//!   `Option<Encoding>` — parsed to a typed enum with `Other`
//!   fallback carrying the raw bytes. Tier-1 on the known-variant
//!   set.
//! - [`client_encoding`](SessionParams::client_encoding): same.
//!
//! The remaining five fields stay as bounded strings
//! (`server_version`, `application_name`, `session_authorization`,
//! `date_style`, `time_zone`). Each is either freeform text
//! (`application_name`, `session_authorization`, `time_zone`) or a
//! composite grammar (`server_version` = "major.minor[.patch]…",
//! `date_style` = "ISO, MDY") that would need its own parser for
//! no clear consumer benefit at Phase 1c.
//!
//! # Capacity
//!
//! # DEF-106 — per-field POD capacity + `BoundedStr`
//!
//! Each string field was historically `Option<heapless::String<128>>`
//! — uniform 128-byte capacity across all five freeform fields, 5 ×
//! ~144 bytes ≈ 720 bytes plus a blanket `heapless::Vec::drop`
//! propagation through `SessionParams` → `PgProtocol`.
//!
//! DEF-106 right-sizes per-field capacity via
//! [`crate::ident::BoundedStr<N>`] (POD, `Copy`, Drop-free from
//! DEF-096 / DEF-099):
//!
//! - `server_version`: 32 bytes (e.g. `"17.2 (Debian 17.2-1.pgdg120+1)"` fits).
//! - `application_name`: 64 bytes.
//! - `session_authorization`: 64 bytes (role names, bounded like `Ident` at 63).
//! - `date_style`: 32 bytes (`"ISO, MDY"` + variants).
//! - `time_zone`: 64 bytes (longest IANA `"America/Argentina/Buenos_Aires"` = 33).
//!
//! Total string footprint: 32 + 64 + 64 + 32 + 64 = 256 bytes of
//! `buf` + `u16 len` per field + Option-discriminant + padding.
//! About 400 bytes saved in `SessionParams` (and therefore in
//! `PgProtocol`).
//!
//! An over-length value from the server is **not silently dropped**
//! anymore — `BoundedStr::from_str_truncating` appends a `"…"`
//! marker, preserving information that the value was oversized.
//! This is a slight behaviour upgrade from pre-DEF-106 (which used
//! `heapless::String::try_from(s).ok()` — Err → `None`, i.e. the
//! parameter appeared absent even though the server sent it).

use core::fmt;
use crate::ident::BoundedStr;

/// Maximum byte length for the raw bytes stored in
/// [`Encoding::Other`]. Longest PG encoding name is
/// `MULE_INTERNAL` (13 bytes); 32 is comfortably above every
/// documented value while still small.
const MAX_ENCODING_NAME_LEN: usize = 32;

/// Typed parsed form of PG server / client encoding.
///
/// PG supports ~42 encodings; this enum lists the ones common in
/// practice with an `Other(..)` fallback carrying the raw bytes
/// for faithful round-trip. Unlike [`crate::error::Severity`]
/// whose unknown-variant discards bytes, [`Encoding::Other`]
/// preserves them — so a consumer can still introspect an
/// unexpected server encoding without information loss.
///
/// DEF-114: elevates `server_encoding` / `client_encoding` from
/// tier-3 audit (freeform string) to tier-2 typed variants. The
/// consumer's "is this UTF-8?" check becomes a pattern match
/// instead of a byte compare; typos in comparisons (e.g. `"UTF-8"`
/// vs `"UTF8"`) become impossible at the call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Encoding {
    /// `UTF8` — the PG default, and the only UTF-8 encoding name
    /// PG emits (no hyphen).
    Utf8,
    /// `SQL_ASCII` — pass-through ASCII, no validation server-side.
    SqlAscii,
    /// `LATIN1` (ISO-8859-1).
    Latin1,
    /// `LATIN9` (ISO-8859-15).
    Latin9,
    /// `WIN1252` (Windows-1252, Western European).
    Win1252,
    /// `EUC_JP` (Japanese).
    EucJp,
    /// `EUC_KR` (Korean).
    EucKr,
    /// `BIG5` (Traditional Chinese).
    Big5,
    /// `GB18030` (Simplified Chinese, full coverage).
    Gb18030,
    /// Any other PG encoding name the server sends. The raw bytes
    /// are preserved for introspection / logging. No information
    /// loss.
    Other(OtherEncoding),
}

impl Encoding {
    /// Parse a PG encoding name from raw bytes.
    ///
    /// Comparison is case-sensitive — PG's `ParameterStatus`
    /// conventionally uppercases encoding names. An over-length
    /// or malformed name maps to `Other(OtherEncoding::empty())`.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        match bytes {
            b"UTF8" => Self::Utf8,
            b"SQL_ASCII" => Self::SqlAscii,
            b"LATIN1" => Self::Latin1,
            b"LATIN9" => Self::Latin9,
            b"WIN1252" => Self::Win1252,
            b"EUC_JP" => Self::EucJp,
            b"EUC_KR" => Self::EucKr,
            b"BIG5" => Self::Big5,
            b"GB18030" => Self::Gb18030,
            other => Self::Other(OtherEncoding::try_from_bytes(other).unwrap_or_default()),
        }
    }
}

/// Raw bytes of an unrecognised PG encoding name, bounded at
/// [`MAX_ENCODING_NAME_LEN`]. Preserves the byte-exact spelling
/// the server sent. DEF-114.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OtherEncoding {
    buf: [u8; MAX_ENCODING_NAME_LEN],
    len: u16,
}

impl OtherEncoding {
    /// Empty fallback — used when the server sends a name longer
    /// than [`MAX_ENCODING_NAME_LEN`].
    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            buf: [0u8; MAX_ENCODING_NAME_LEN],
            len: 0,
        }
    }

    /// Construct from a byte slice. Returns `None` if `src`
    /// exceeds the capacity bound.
    pub fn try_from_bytes(src: &[u8]) -> Option<Self> {
        if src.len() > MAX_ENCODING_NAME_LEN {
            return None;
        }
        let len = u16::try_from(src.len()).ok()?;
        let mut out = Self::empty();
        if let Some(dst) = out.buf.get_mut(..src.len()) {
            dst.copy_from_slice(src);
        }
        out.len = len;
        Some(out)
    }

    /// Borrow the raw bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.buf.get(..usize::from(self.len)).unwrap_or(&[])
    }
}

impl Default for OtherEncoding {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

/// Parse PG's conventional `"on"` / `"off"` boolean parameter
/// string. Any other value returns `None`.
#[inline]
fn parse_pg_bool(value: &[u8]) -> Option<bool> {
    match value {
        b"on" => Some(true),
        b"off" => Some(false),
        _ => None,
    }
}

/// Known session parameters received from the PostgreSQL server.
///
/// Populated during the post-authentication handshake from
/// `ParameterStatus` messages. Read-only after handshake
/// completes. Accessible via [`crate::PgProtocol::session_params`].
///
/// Per DEF-042: fixed struct, no map, no overflow class.
///
/// Per DEF-114: four fields are parsed to typed form at ingest:
/// `is_superuser` / `integer_datetimes` → `Option<bool>`;
/// `server_encoding` / `client_encoding` → `Option<Encoding>`.
///
/// Per DEF-106: each string field uses a right-sized
/// `BoundedStr<N>` (POD, Drop-free) instead of a uniform 128-byte
/// `heapless::String`. Reduces `SessionParams` footprint by ~400
/// bytes and breaks the `heapless::Vec::drop` chain through
/// `PgProtocol`.
#[derive(Default)]
pub struct SessionParams {
    /// PostgreSQL server version string (e.g. `"17.2"`,
    /// `"17.2 (Debian 17.2-1.pgdg120+1)"`). BoundedStr<32> — PG's
    /// version string is occasionally embellished with build
    /// provenance but stays under 32 bytes. DEF-106.
    pub server_version: Option<BoundedStr<32>>,
    /// Server-side encoding, parsed to a typed enum. DEF-114.
    pub server_encoding: Option<Encoding>,
    /// Client-side encoding, parsed to a typed enum. DEF-114.
    pub client_encoding: Option<Encoding>,
    /// Application name echoed back by the server. BoundedStr<64>
    /// — deployment-tagged names (`myapp-worker-pod-abc123`) fit
    /// comfortably. DEF-106.
    pub application_name: Option<BoundedStr<64>>,
    /// Whether the connected role is a superuser. DEF-114.
    /// `Some(true)` / `Some(false)` / `None` (server sent neither
    /// `"on"` nor `"off"`).
    pub is_superuser: Option<bool>,
    /// The authorised session user. BoundedStr<64> — role names
    /// are bounded like PG's `NAMEDATALEN` (63 usable chars).
    /// DEF-106.
    pub session_authorization: Option<BoundedStr<64>>,
    /// DateStyle setting (e.g. `"ISO, MDY"`). BoundedStr<32> —
    /// the grammar is `"<format>, <order>"` with short
    /// components. DEF-106.
    pub date_style: Option<BoundedStr<32>>,
    /// Whether integer datetimes are used. DEF-114.
    pub integer_datetimes: Option<bool>,
    /// Server timezone (e.g. `"UTC"`, `"America/New_York"`).
    /// BoundedStr<64> — longest documented IANA zone
    /// `"America/Argentina/Buenos_Aires"` = 33 bytes. DEF-106.
    pub time_zone: Option<BoundedStr<64>>,
}

impl SessionParams {
    /// Create empty params — all fields `None`.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            server_version: None,
            server_encoding: None,
            client_encoding: None,
            application_name: None,
            is_superuser: None,
            session_authorization: None,
            date_style: None,
            integer_datetimes: None,
            time_zone: None,
        }
    }

    /// Record a parameter from the server.
    ///
    /// Known keys dispatch to typed parsers (DEF-114: booleans,
    /// encodings) or bounded-string storage (freeform fields).
    /// Unknown keys are ignored. Values that fail parsing / exceed
    /// capacity bounds are silently dropped — the parameter is
    /// treated as if the server never sent it (DEF-042).
    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        match key {
            // ═══ DEF-114 typed fields ═══
            b"server_encoding" => {
                self.server_encoding = Some(Encoding::from_bytes(value));
            }
            b"client_encoding" => {
                self.client_encoding = Some(Encoding::from_bytes(value));
            }
            b"is_superuser" => {
                if let Some(b) = parse_pg_bool(value) {
                    self.is_superuser = Some(b);
                }
                // Unrecognised bool value: leave as None. Tier-2
                // structural — the consumer sees the absence,
                // not a silently-wrong `false`.
            }
            b"integer_datetimes" => {
                if let Some(b) = parse_pg_bool(value) {
                    self.integer_datetimes = Some(b);
                }
            }

            // ═══ Remaining freeform / composite fields (DEF-106) ═══
            //
            // BoundedStr::from_str_truncating right-sizes per-field:
            // oversized input trims to N-3 bytes + "…" marker
            // (no silent value-drop, unlike the prior
            // heapless::String::try_from → None path).
            b"server_version" => {
                let Ok(s) = core::str::from_utf8(value) else { return };
                self.server_version = Some(BoundedStr::<32>::from_str_truncating(s));
            }
            b"application_name" => {
                let Ok(s) = core::str::from_utf8(value) else { return };
                self.application_name = Some(BoundedStr::<64>::from_str_truncating(s));
            }
            b"session_authorization" => {
                let Ok(s) = core::str::from_utf8(value) else { return };
                self.session_authorization = Some(BoundedStr::<64>::from_str_truncating(s));
            }
            b"DateStyle" => {
                let Ok(s) = core::str::from_utf8(value) else { return };
                self.date_style = Some(BoundedStr::<32>::from_str_truncating(s));
            }
            b"TimeZone" => {
                let Ok(s) = core::str::from_utf8(value) else { return };
                self.time_zone = Some(BoundedStr::<64>::from_str_truncating(s));
            }
            _ => {
                // Unknown key — silently dropped (DEF-042).
            }
        }
    }
}

impl fmt::Debug for SessionParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionParams")
            .field("server_version", &self.server_version)
            .field("server_encoding", &self.server_encoding)
            .field("client_encoding", &self.client_encoding)
            .field("application_name", &self.application_name)
            .field("is_superuser", &self.is_superuser)
            .field("session_authorization", &self.session_authorization)
            .field("date_style", &self.date_style)
            .field("integer_datetimes", &self.integer_datetimes)
            .field("time_zone", &self.time_zone)
            .finish()
    }
}
