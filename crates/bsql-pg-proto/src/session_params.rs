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
use crate::ident::SecretBoundedStr;

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
    /// conventionally uppercases encoding names.
    ///
    /// # Over-length names
    ///
    /// Names longer than [`MAX_ENCODING_NAME_LEN`] are preserved via
    /// [`OtherEncoding::from_truncated_bytes`] — the visible prefix
    /// plus a `"…"` truncation marker. Previously (pre-2026-04-21)
    /// such names silently became `Other(empty)` — a tier-4 silent
    /// drop that lost forensic information. Now the name is present
    /// as "{visible-prefix}…" so downstream logging/diagnostics can
    /// see what the server actually sent. Tier-4 → tier-2 structural.
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
            other => Self::Other(OtherEncoding::from_truncated_bytes(other)),
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

    /// Construct from a byte slice, truncating with a `"…"` marker
    /// on overflow — mirror of [`crate::ident::BoundedStr::from_str_truncating`]
    /// but byte-oriented (encoding names can contain any PG-legal
    /// bytes). Over-length input preserves the longest prefix that
    /// fits in `MAX_ENCODING_NAME_LEN - marker_len` followed by
    /// the 3-byte UTF-8 ellipsis.
    ///
    /// Replaces the previous `unwrap_or_default` silent-drop on
    /// over-length server-sent encoding names — forensic
    /// information is preserved.
    #[must_use]
    pub fn from_truncated_bytes(src: &[u8]) -> Self {
        const MARKER: &[u8] = "…".as_bytes(); // 3 bytes
        if src.len() <= MAX_ENCODING_NAME_LEN {
            // Fast path — fits verbatim.
            return Self::try_from_bytes(src).unwrap_or_else(Self::empty);
        }
        let budget = MAX_ENCODING_NAME_LEN.saturating_sub(MARKER.len());
        let mut out = Self::empty();
        if let (Some(dst_prefix), Some(src_prefix)) =
            (out.buf.get_mut(..budget), src.get(..budget))
        {
            dst_prefix.copy_from_slice(src_prefix);
        }
        let marker_end = budget.saturating_add(MARKER.len());
        if let Some(dst_marker) = out.buf.get_mut(budget..marker_end) {
            dst_marker.copy_from_slice(MARKER);
        }
        // DEF-154 (T) P1-2: see `crate::ident::narrow_len_u16`
        // docstring. `marker_end ≤ MAX_ENCODING_NAME_LEN ≤ u16::MAX`
        // by construction; Err arms documented-dead.
        out.len = crate::ident::narrow_len_u16(marker_end, MAX_ENCODING_NAME_LEN);
        out
    }

    /// Borrow the raw bytes.
    ///
    /// DEF-154 (S) P1-1: explicit `split_at_checked` match with a
    /// documented-dead None arm. `self.len ≤ self.buf.len()` by
    /// construction in `try_from_bytes` / `from_bytes_truncating`,
    /// so None is architecturally unreachable. The Some-arm has no
    /// silent `unwrap_or(&[])`; the None-arm is a no-silent-op
    /// sentinel (empty slice is semantically "no bytes to expose",
    /// same as an empty encoding — no corruption vector). Pre-(S)
    /// was `self.buf.get(..len).unwrap_or(&[])` — silent fallback
    /// the user banned.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        let n = usize::from(self.len);
        match self.buf.split_at_checked(n) {
            Some((head, _)) => head,
            None => &[],
        }
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
/// # F-073 (pass-#8) — public fields are intentionally readable
///
/// Every field below is `pub` so external code (tests today,
/// `bsql-driver-postgres` wrapper in Phase 1e, user diagnostic code)
/// can read server-reported session state without a 9-accessor
/// boilerplate layer. Audit noted the risk of internal writes
/// bypassing `set()` — acknowledged; internal writers are limited
/// to `set()` by convention, pinned by the `session_params_set_key_routing_table`
/// test which exercises every known key.
///
/// A future refactor that adds a second writer path must either
/// call through `set()` (preserving validation) or explicitly
/// document why a raw assignment is correct.
#[derive(Default)]
pub struct SessionParams {
    /// PostgreSQL server version string (e.g. `"17.2"`,
    /// `"17.2 (Debian 17.2-1.pgdg120+1)"`). BoundedStr<32> — PG's
    /// version string is occasionally embellished with build
    /// provenance but stays under 32 bytes. DEF-106.
    pub server_version: Option<SecretBoundedStr<32>>,
    /// Server-side encoding, parsed to a typed enum. DEF-114.
    pub server_encoding: Option<Encoding>,
    /// Client-side encoding, parsed to a typed enum. DEF-114.
    pub client_encoding: Option<Encoding>,
    /// Application name echoed back by the server. Capacity 128
    /// bytes — matches the client-side [`crate::ident::MAX_APP_NAME_LEN`]
    /// so the server-echoed value is byte-faithful for any name the
    /// client legitimately sent. Pre-uplift capacity was 64, which
    /// would truncate client-sent names in the 64..128 range with a
    /// `"…"` marker — a fidelity gap for long deployment-tagged
    /// names. DEF-106 + architect finding #66 (2026-04-21).
    pub application_name: Option<SecretBoundedStr<128>>,
    /// Whether the connected role is a superuser. DEF-114.
    /// `Some(true)` / `Some(false)` / `None` (server sent neither
    /// `"on"` nor `"off"`).
    pub is_superuser: Option<bool>,
    /// The authorised session user. BoundedStr<64> — role names
    /// are bounded like PG's `NAMEDATALEN` (63 usable chars).
    /// DEF-106.
    pub session_authorization: Option<SecretBoundedStr<64>>,
    /// DateStyle setting (e.g. `"ISO, MDY"`). BoundedStr<32> —
    /// the grammar is `"<format>, <order>"` with short
    /// components. DEF-106.
    pub date_style: Option<SecretBoundedStr<32>>,
    /// Whether integer datetimes are used. DEF-114.
    pub integer_datetimes: Option<bool>,
    /// Server timezone (e.g. `"UTC"`, `"America/New_York"`).
    /// BoundedStr<64> — longest documented IANA zone
    /// `"America/Argentina/Buenos_Aires"` = 33 bytes. DEF-106.
    pub time_zone: Option<SecretBoundedStr<64>>,
    /// Number of unknown `ParameterStatus` keys the server sent that
    /// we couldn't classify.
    ///
    /// F-074 (pass-#8): prior to this, unknown keys were silently
    /// dropped (DEF-042 forward-compat policy). That's still the
    /// right behaviour — PG may add new keys in future versions —
    /// but operator visibility was zero. Counting lets diagnostics
    /// surface "we dropped N keys; upgrade the client or report".
    /// Saturating `u16` — overflows stay pinned at `u16::MAX` rather
    /// than wrapping.
    pub n_unknown_dropped: u16,
    /// Number of `ParameterStatus` bool-valued fields (`is_superuser`,
    /// `integer_datetimes`) whose value failed PG's `on`/`off` form.
    ///
    /// DEF-153 (audit A003): prior to this, non-standard bool values
    /// (e.g. `is_superuser=yes` — a common human-error or legacy
    /// proxy variant vs PG's canonical `on` / `off`) left the field
    /// as `None`, indistinguishable from "server never sent the
    /// parameter." Operators investigating "why does the client not
    /// see superuser state" had no diagnostic signal. Mirrors the
    /// F-074 `n_unknown_dropped` pattern.
    ///
    /// Saturating `u16` — overflows stay pinned at `u16::MAX` rather
    /// than wrapping.
    pub n_malformed_bool_dropped: u16,
    /// Number of `ParameterStatus` frames with malformed payload
    /// (e.g. missing NUL separator between key and value, or missing
    /// trailing NUL) the protocol layer consumed without surfacing.
    ///
    /// # DEF-185 P2-B (audit 2026-04-24)
    ///
    /// Pre-fix: [`crate::record_param_status`]'s
    /// `ParamStatusRecordOutcome::MalformedPayload` outcome was
    /// silently collapsed into `{}` at the dispatch filter site
    /// (`protocol.rs` pre-dispatch filter). No visibility for
    /// operators investigating proxy-injection / wire-corruption
    /// incidents. Post-fix: this counter increments on each malformed
    /// payload — mirrors the existing `n_unknown_dropped` /
    /// `n_malformed_bool_dropped` pattern for ops diagnostic parity.
    ///
    /// Saturating `u16` — overflows stay pinned at `u16::MAX`.
    pub n_malformed_param_status_dropped: u32,
    /// Number of `NoticeResponse` frames the protocol silently consumed.
    ///
    /// # DEF-185 P2-3 (audit 2026-04-24)
    ///
    /// Pre-fix: `NoticeResponse` was unconditionally skipped by the
    /// pre-dispatch filter with no visibility. A server flooding the
    /// client with notices (adversarial or mis-configured) burned
    /// bandwidth silently. Post-DEF-185 P1-E the filter gates by state,
    /// and now this counter surfaces the count for operator
    /// diagnostics — parallel to `n_unknown_dropped` /
    /// `n_malformed_bool_dropped` / `n_malformed_param_status_dropped`.
    ///
    /// A non-zero value signals the server is emitting notices that
    /// bsql-pg-proto is discarding at the protocol layer (Phase 1d
    /// will route these to an `Action::EmitNotice` stream; for now
    /// the counter lets ops detect the pattern).
    ///
    /// Saturating `u32` (DEF-186 P1-5 widened from u16) — overflows
    /// stay pinned at `u32::MAX` rather than collapsing diagnostic
    /// fidelity at 65k events on long-lived adversarial-flood paths.
    pub n_notice_response_dropped: u32,
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
            n_unknown_dropped: 0,
            n_malformed_bool_dropped: 0,
            n_malformed_param_status_dropped: 0,
            n_notice_response_dropped: 0,
        }
    }

    /// DEF-189 Q8-C3: reset all session parameters to their `new()`
    /// state. Called from `clear_session_residue_if_idle_or_errored`
    /// when state transitions to `Errored` — a tear-down forfeits all
    /// session state.
    ///
    /// # Why a method, not `*self = Self::new()`
    ///
    /// Same observable effect, but a method keeps the discipline
    /// explicit at the call site (grep `session_params.clear()` finds
    /// every reset point) and avoids accidentally creating a new
    /// `SessionParams` value with different defaults if `new()`
    /// signature changes.
    #[inline]
    pub fn clear(&mut self) {
        *self = Self::new();
    }

    /// DEF-185 P2-B: bump the `n_malformed_param_status_dropped`
    /// counter. Called from `protocol.rs`'s pre-dispatch filter when
    /// `record_param_status` returns `MalformedPayload` outcome.
    #[inline]
    pub fn bump_malformed_param_status(&mut self) {
        self.n_malformed_param_status_dropped =
            self.n_malformed_param_status_dropped.saturating_add(1);
    }

    /// DEF-185 P2-3: bump the `n_notice_response_dropped` counter.
    /// Called from `protocol.rs`'s pre-dispatch filter when a
    /// NoticeResponse is silently consumed.
    #[inline]
    pub fn bump_notice_response(&mut self) {
        self.n_notice_response_dropped =
            self.n_notice_response_dropped.saturating_add(1);
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
                } else {
                    // DEF-153: bump diagnostic counter on malformed
                    // bool (e.g. `is_superuser=yes` vs PG's `on`/`off`).
                    // Field stays None; the counter surfaces the drop.
                    self.n_malformed_bool_dropped =
                        self.n_malformed_bool_dropped.saturating_add(1);
                }
            }
            b"integer_datetimes" => {
                if let Some(b) = parse_pg_bool(value) {
                    self.integer_datetimes = Some(b);
                } else {
                    // DEF-153: same treatment as is_superuser above.
                    self.n_malformed_bool_dropped =
                        self.n_malformed_bool_dropped.saturating_add(1);
                }
            }

            // ═══ Remaining freeform / composite fields (DEF-106) ═══
            //
            // BoundedStr::from_str_truncating right-sizes per-field:
            // oversized input trims to N-3 bytes + "…" marker
            // (no silent value-drop, unlike the prior
            // heapless::String::try_from → None path).
            // F55 (pass #6 audit): non-UTF-8 bytes no longer silently
            // drop the whole field. A PG server configured with
            // LATIN1 / legacy client_encoding may emit non-UTF-8
            // bytes in freeform string fields (application_name
            // echo with non-UTF-8 user input, etc.). `from_bytes_lossy`
            // preserves the ASCII subset, coerces non-ASCII bytes to
            // `?` placeholders, and guarantees valid UTF-8 output —
            // same F22 treatment used for ErrorResponse M/D/H fields.
            b"server_version" => {
                self.server_version = Some(SecretBoundedStr::<32>::from_bytes_lossy(value));
            }
            b"application_name" => {
                self.application_name = Some(SecretBoundedStr::<128>::from_bytes_lossy(value));
            }
            b"session_authorization" => {
                self.session_authorization = Some(SecretBoundedStr::<64>::from_bytes_lossy(value));
            }
            b"DateStyle" => {
                self.date_style = Some(SecretBoundedStr::<32>::from_bytes_lossy(value));
            }
            b"TimeZone" => {
                self.time_zone = Some(SecretBoundedStr::<64>::from_bytes_lossy(value));
            }
            _ => {
                // Unknown key — silently dropped (DEF-042).
                // F-074 (pass-#8): count the drop so operators can
                // detect PG-version mismatches via `n_unknown_dropped`.
                self.n_unknown_dropped = self.n_unknown_dropped.saturating_add(1);
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
            .field("n_unknown_dropped", &self.n_unknown_dropped)
            .field("n_malformed_bool_dropped", &self.n_malformed_bool_dropped)
            .finish()
    }
}
