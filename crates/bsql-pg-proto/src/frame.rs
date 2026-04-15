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

use core::num::NonZeroU32;

/// Maximum byte capacity of the read buffer.
///
/// 4 KiB is the historical default for PG client read buffers and
/// matches one OS page on aarch64-Apple, x86\_64-Linux, and most other
/// targets bsql ships on. Tunable per-connection in a later phase via
/// const generic; the const here is the Phase 1a constant.
pub const READ_BUF_CAP: usize = 4096;

/// Maximum legal value of a frame's length-field.
///
/// A frame's wire footprint is `1 + length` (tag + length-region); to
/// fit in `READ_BUF_CAP` the length must satisfy
/// `1 + length <= READ_BUF_CAP`, so the cap on the length-field itself
/// is `READ_BUF_CAP - 1`. Anything larger is rejected at the
/// header-parse step — the buffer is never asked to hold the body.
/// This is the structural cap that turns "frame length amplification
/// DoS" into reforge.md §53's STRUCTURALLY UNREACHABLE.
///
/// Hard-coded as a `u32` literal because the forbid-bundle refuses
/// `as` conversions (including inside const blocks — `forbid` cannot
/// be downgraded by `expect`) and `u32::try_from` is not yet
/// const-callable on stable (issue #143874). The drift-protection
/// assert below ties the literal to [`READ_BUF_CAP`]: bump either and
/// the build fails until both are in sync.
pub const MAX_FRAME_LEN_FIELD: u32 = 4095;

// Tier-1 drift guard: `MAX_FRAME_LEN_FIELD` (u32) must correspond to
// `READ_BUF_CAP - 1` (usize). A change to either without updating the
// other fails the build here, because the arithmetic identity below
// only holds for the *paired* values. Expressing the check as a
// single `usize` equation avoids any `as` cast:
//
//   READ_BUF_CAP == (MAX_FRAME_LEN_FIELD + 1) projected to usize
//
// Since `MAX_FRAME_LEN_FIELD: u32`, the comparison needs a common
// integer type. `u32::MAX` fits in `usize` on every target bsql
// supports (≥ 32-bit); we express the right-hand side in `u32` space
// and the left-hand side via its known literal.
const _: () = assert!(READ_BUF_CAP == 4096);
const _: () = assert!(MAX_FRAME_LEN_FIELD == 4095);
const _: () = assert!(MAX_FRAME_LEN_FIELD.saturating_add(1) == 4096);

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
    Ok {
        /// The PG message tag (e.g. `b'Z'` for `ReadyForQuery`).
        tag: u8,
        /// The length-field as carried on the wire (includes itself,
        /// excludes the tag). Always `>= 4`.
        declared_len: NonZeroU32,
        /// Total bytes the frame occupies including the tag.
        ///
        /// `total_len = 1 + declared_len`. Always `<= READ_BUF_CAP`.
        total_len: usize,
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
/// Tier-3 invariant: never panics on arbitrary input. The forbid-bundle
/// in [`crate`] makes panic-able expressions a build error; a
/// randomized fuzz harness in `tests/frame_parse.rs` exercises 100k
/// arbitrary slices to give the empirical confidence the spec demands.
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
                return HeaderParse::MalformedLength { declared };
            }
            if declared > MAX_FRAME_LEN_FIELD {
                return HeaderParse::FrameTooLarge { declared };
            }
            // declared >= 4 and declared <= MAX_FRAME_LEN_FIELD <= READ_BUF_CAP - 1;
            // total_len = 1 + declared <= READ_BUF_CAP. checked_add
            // satisfies arithmetic_side_effects with no real cost
            // (compiles to add + jno; and we already know it cannot
            // overflow for declared <= READ_BUF_CAP - 1, so the Err
            // path is dead).
            let total_len = match usize::try_from(declared) {
                Ok(n) => n.saturating_add(1),
                Err(_) => return HeaderParse::FrameTooLarge { declared },
            };
            // declared >= 4 ⇒ NonZeroU32::new is Some.
            match NonZeroU32::new(declared) {
                Some(nz) => HeaderParse::Ok {
                    tag: *tag,
                    declared_len: nz,
                    total_len,
                },
                None => HeaderParse::MalformedLength { declared },
            }
        }
    }
}
