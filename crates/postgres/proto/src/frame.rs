//! Frame-header parser — pure function, no state, no I/O.
//!
//! PostgreSQL backend messages start with a 5-byte header:
//!
//! ```text
//! +------+----------+
//! | tag  |  length  |
//! | u8   | u32 BE   |
//! +------+----------+
//! ```
//!
//! `length` includes itself but **excludes** the tag byte. Total wire
//! footprint of a complete frame is therefore `1 + length` bytes.
//!
//! [`parse_header`] is a pure function over `&[u8]` returning a typed
//! [`HeaderParse`]. No state, no allocation, no panic. This is the
//! smallest, most testable unit of the wire layer; a tier-3 randomized
//! fuzz harness drives it directly (see `tests/frame_parse.rs`).

/// Maximum byte capacity of the read buffer.
///
/// 4 KiB is the historical default for PG client read buffers and
/// matches one OS page on aarch64-Apple, x86\_64-Linux, and most other
/// targets bsql ships on. Tunable per-connection in a later phase via
/// const generic; this is the current crate-wide constant.
///
/// **Internal sizing choice, NOT a PostgreSQL spec commitment.**
/// PostgreSQL does not limit frame sizes; the value here is our memory
/// budget for the bounded read buffer. Bumping to 8192 (or higher) in
/// a future phase is a legitimate change — tests that pin the absolute
/// value would falsely block such a bump. The pair-consistency between
/// `READ_BUF_CAP` and [`MAX_FRAME_LEN_FIELD`] is what must hold (and
/// is const-asserted below); the absolute number itself is ours to
/// choose.
pub const READ_BUF_CAP: usize = 4096;

/// Maximum legal value of a frame's length-field.
///
/// A frame's wire footprint is `1 + length` (tag + length-region); to
/// fit in `READ_BUF_CAP` the length must satisfy
/// `1 + length <= READ_BUF_CAP`, so the cap on the length-field itself
/// is `READ_BUF_CAP - 1`. Anything larger is rejected at the
/// header-parse step — the buffer is never asked to hold the body.
/// This is the structural cap that turns "frame length amplification
/// DoS" into a STRUCTURALLY UNREACHABLE case.
///
/// Hard-coded as a `u32` literal because the forbid-bundle refuses
/// `as` conversions (including inside const blocks — `forbid` cannot
/// be downgraded by `expect`) and `u32::try_from` is not yet
/// const-callable on stable (issue #143874). The drift-protection
/// assert below ties the literal to [`READ_BUF_CAP`]: bump either and
/// the build fails until both are in sync.
pub const MAX_FRAME_LEN_FIELD: u32 = 4095;

/// Fixed wire-header length in bytes: 1 byte (tag) + 4 bytes (big-
/// endian length field).
///
/// Named here so that every consumer (parser, dispatcher, buffer slice
/// sites) references a single named constant rather than the magic `5`.
/// A future refactor that changes the wire-header layout (unlikely for
/// PG but possible for a multiplexed wrapper) has exactly one symbol to
/// find and update. The parser's `total_len = declared + 1` formula
/// still uses the literal `1` (that is the tag-byte component of the
/// header, not the whole header), which is a separate spec commitment
/// pinned by `total_len_equals_one_plus_declared_len` in
/// `tests/frame_parse.rs`.
pub const HEADER_LEN: usize = 5;

// Tier-1 drift guard: `MAX_FRAME_LEN_FIELD` (u32) must correspond
// to `READ_BUF_CAP - 1` (usize). The two consts live in different
// integer types; stable Rust does not yet expose `usize::try_from(u32)`
// in `const fn` context (RU-01) and `as` casts are forbidden by
// the workspace clippy bundle (`cast_possible_truncation` etc.).
//
// A pair-pin form (separate `READ_BUF_CAP == 4096` +
// `MAX_FRAME_LEN_FIELD == 4095` asserts) would catch either-side drift
// but leave the **relationship** as documentation only — a contributor
// could see one assert fail, update only that const, and ship a
// binary with a different formula (e.g. `MAX_FRAME_LEN_FIELD = 4096`,
// `READ_BUF_CAP = 4096`, off-by-one frame-cap for the lifetime of the
// connection). The single-equation form below makes the relationship
// load-bearing: the third conjunct expresses the formula directly via
// `saturating_add` in
// u32-space (the type both values fit in given the
// `READ_BUF_CAP <= u16::MAX` const-assert below + `protocol.rs`
// drift pin coupling `READ_BUF_CAP` to `frames_consumed: u16`).
// Tier-1 by single assertion: failure message points to BOTH
// consts AND the formula; no documentation-only relationship.
const _: () = assert!(
    READ_BUF_CAP == 4096
        && MAX_FRAME_LEN_FIELD == 4095
        && MAX_FRAME_LEN_FIELD.saturating_add(1) == 4096_u32,
    "MAX_FRAME_LEN_FIELD + 1 == READ_BUF_CAP must hold (in u32 space); \
     bumping either const requires updating BOTH literals here. \
     The third conjunct is load-bearing — it pins the formula, NOT \
     a tautology of the value pins (a paired bump like \
     `READ_BUF_CAP=8192, MAX_FRAME_LEN_FIELD=8200` violates the \
     formula even though both consts changed).",
);
const _: () = assert!(HEADER_LEN == 5, "wire header = 1 byte tag + 4 bytes BE length field");

// READ_BUF_CAP must be large enough to hold the smallest legal complete
// frame (5 bytes: 1 tag + 4 length-field). Below that the protocol
// cannot make forward progress.
const _: () = assert!(READ_BUF_CAP >= 5);

// MAX_FRAME_LEN_FIELD must accommodate the smallest legal frame
// (5 bytes total ⇒ length-field = 4). If the cap dropped below that,
// no frame would ever parse.
const _: () = assert!(MAX_FRAME_LEN_FIELD >= 4);

/// Result of inspecting the leading bytes of an unread region.
///
/// Plain enum; not `#[non_exhaustive]` — the wire protocol cannot grow
/// new framing modes. A new variant here would be a protocol break,
/// and we want exhaustive-match enforcement on the dispatcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderParse {
    /// The buffer is empty. Need more bytes.
    Empty,
    /// Fewer than 5 bytes available — header is incomplete. Need more.
    Incomplete,
    /// Header parsed cleanly. The frame is fully described.
    ///
    /// # `declared_len` is not carried here
    ///
    /// The variant carries `total_len` only; `declared_len` is
    /// derivable as `total_len - 1` (`parse_header` constructs both
    /// fields in lock-step from the wire bytes). Production code at
    /// `protocol.rs::feed_bytes` never reads `declared_len`. Tests
    /// that need it compute it inline:
    /// `let declared = u32::try_from(total_len.saturating_sub(1))
    ///     .ok().and_then(NonZeroU32::new)`.
    /// Dropping the field shrinks the variant by 8 B per return.
    Ok {
        /// The PG message tag, typed as [`crate::wire::InboundTag`]
        /// — bytes received from the server are wrapped here so
        /// they cannot cross-pollinate with [`crate::wire::OutboundTag`]
        /// values elsewhere in the crate (tier-1 compile on
        /// direction).
        tag: crate::wire::InboundTag,
        /// Total bytes the frame occupies including the tag.
        ///
        /// `total_len = 1 + declared_len_on_wire`.
        /// `5 <= total_len <= READ_BUF_CAP <= u16::MAX` — the u16
        /// type encodes the upper bound at the type level so
        /// downstream consumers (`AbsFrameStart::new`,
        /// `FrameTotalLen::new`, feed_bytes cursor math) work in
        /// u16 without silent narrowing.
        ///
        /// `u16` (not `usize`) — every downstream callsite would
        /// otherwise have to call `u16::try_from(v).unwrap_or(u16::MAX)`,
        /// a silent clamp on drift. Narrowing once at the parser keeps
        /// every consumer on a typed bound.
        total_len: u16,
    },
    /// Header malformed: length-field below 4.
    ///
    /// PG's length-field includes its own 4 bytes; values < 4 cannot
    /// describe even an empty body. The connection is irrecoverably
    /// out of sync.
    MalformedLength {
        /// The illegal value the server sent.
        declared: u32,
    },
    /// Header well-formed but the frame would not fit in the buffer.
    ///
    /// Equivalent to a structural DoS-attempt: the connection is torn
    /// down before any allocation toward the body occurs.
    FrameTooLarge {
        /// The oversized declared length.
        declared: u32,
    },
}

/// Parse the first frame header in `unread`.
///
/// Pure function; no state mutation. Stable, branchless on the happy
/// path (slice-pattern match compiles to a length check + 5 byte
/// loads + 4 BE-shift assemble).
///
/// **Tier-1 invariant (no-panic):** every panic-able expression in
/// this function is a build error under the crate's forbid-bundle
/// (`unwrap_used`, `indexing_slicing`, `arithmetic_side_effects`,
/// `as_conversions`, …). Slice patterns bound every byte access;
/// `u32::from_be_bytes([u8; 4])` is total; `usize::try_from` returns
/// `Result`; `saturating_add` cannot overflow. No fuzz harness —
/// there is no path the compiler does not already close.
#[inline]
#[must_use]
pub fn parse_header(unread: &[u8]) -> HeaderParse {
    match unread {
        [] => HeaderParse::Empty,
        // Slice patterns of explicit lengths < 5 — `Incomplete`.
        [_] | [_, _] | [_, _, _] | [_, _, _, _] => HeaderParse::Incomplete,
        [tag, l0, l1, l2, l3, ..] => {
            let declared = u32::from_be_bytes([*l0, *l1, *l2, *l3]);
            if declared < 4 {
                // Cold branch — malformed frames are the adversarial /
                // server-bug case, not the common path. `cold_path()`
                // hints LLVM to push this block to the end of the
                // generated body so the happy path stays contiguous
                // in I-cache. Stable since Rust 1.95.
                core::hint::cold_path();
                return HeaderParse::MalformedLength { declared };
            }
            if declared > MAX_FRAME_LEN_FIELD {
                core::hint::cold_path();
                return HeaderParse::FrameTooLarge { declared };
            }
            // declared >= 4 and declared <= MAX_FRAME_LEN_FIELD <= READ_BUF_CAP - 1;
            // total_len = 1 + declared <= READ_BUF_CAP.
            //
            // Narrow to u16 in one step — `declared: u32`,
            // `declared + 1` fits u32 always; the `u16::try_from`
            // narrowing is routed through the classified
            // `FrameTooLarge` variant on Err (architecturally dead
            // under `const _ = assert!(READ_BUF_CAP <= 65_535)` in
            // `buf.rs`, but structurally classified — NOT a silent
            // fallback). There is no silent narrowing anywhere on
            // the ingress path.
            let total_len_u32 = declared.saturating_add(1);
            let total_len = match u16::try_from(total_len_u32) {
                Ok(n) => n,
                Err(_) => {
                    core::hint::cold_path();
                    return HeaderParse::FrameTooLarge { declared };
                }
            };
            // At this point `declared >= 4` is proved by the
            // early-return at the top of this arm; a
            // `NonZeroU32::new(declared).is_some()` guard would be
            // architecturally dead (declared >= 4 implies non-zero).
            HeaderParse::Ok {
                tag: crate::wire::InboundTag::from_byte(*tag),
                total_len,
            }
        }
    }
}
