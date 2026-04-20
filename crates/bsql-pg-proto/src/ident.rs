//! Bounded POD string types for the PostgreSQL wire — one generic
//! `FixedStr<N, Tag>` parameterised by a phantom-tag for nominal
//! typing. DEF-096.
//!
//! # Why one generic type
//!
//! Before DEF-096 this module defined three near-identical newtypes
//! (`Ident`, `DatabaseName`, `ApplicationName`) each wrapping a
//! `heapless::Vec<u8, N>`, plus [`crate::error::BoundedStr<N>`] —
//! a fourth, slightly different wrapper carrying a `[u8; N] + u16`
//! form. The four shared ~300 LoC of validation, accessors, and
//! `Debug`/`Display` impls.
//!
//! [`FixedStr<const N: usize, Tag>`] consolidates all four behind a
//! single POD layout (`[u8; N] + u16 len + PhantomData<Tag>`). The
//! phantom tag gives each aliased concrete type its own nominal
//! identity at compile time: `FixedStr<63, IdentTag>` and
//! `FixedStr<63, DatabaseNameTag>` are distinct types despite
//! identical runtime layout, so a function taking `&Ident` rejects
//! `&DatabaseName` at the type system level — the call-site safety
//! property that justified having three types in the first place.
//!
//! # POD — Copy, no Drop
//!
//! Prior form (`heapless::Vec<u8, N>`-backed) carried a blanket
//! `Drop` impl inherited from `heapless::Vec`. Even though the
//! `u8` element type has an empty `Drop` body, `needs_drop::<Vec<u8, _>>()`
//! returns `true`, which tripped Drop propagation all the way up
//! into `ProtoState`. POD form (`[u8; N] + u16`) makes
//! `needs_drop::<FixedStr<_, _>>()` = `false`, giving `Ident`,
//! `DatabaseName`, `ApplicationName`, and `BoundedStr<N>` all
//! `Copy` in one stroke.
//!
//! # Capacity
//!
//! PostgreSQL's `NAMEDATALEN` is 64 bytes (63 chars + NUL terminator).
//! Identifiers (user, database) are capped at 63. Application name is
//! conventionally capped at 64 but has no hard server limit; we use
//! 128 to accommodate deployment-tagged names like
//! `myapp-worker-pod-abc123`.
//!
//! Over-length inputs are rejected by validated-tag constructors with
//! a typed error — no silent truncation (Part V ban). The separate
//! `BoundedStrTag` constructor is explicitly truncating with a `"…"`
//! marker, used only on cold error-reporting paths.

use core::fmt;
use core::marker::PhantomData;

/// Maximum byte length for a PostgreSQL identifier (user / database).
///
/// PostgreSQL `NAMEDATALEN = 64`; usable chars = 63.
pub const MAX_IDENT_LEN: usize = 63;

/// Maximum byte length for an application name parameter.
///
/// No hard PG limit; 128 bytes accommodates deployment-tagged names
/// like `myapp-worker-pod-abc123def456`.
pub const MAX_APP_NAME_LEN: usize = 128;

/// Tag trait supplying the per-kind debug name. Every
/// `FixedStr<_, Tag>` uses this to render its own type name in
/// `Debug`.
///
/// `ALLOW_EMPTY` is consulted by validated-constructor impls.
pub trait FixedStrKind {
    /// Human-readable type name used by `Debug`.
    const DEBUG_NAME: &'static str;
    /// Whether construction accepts empty input.
    ///
    /// Unused by `BoundedStrTag` (whose constructor is truncating,
    /// not validating) — the const is declared anyway so the trait
    /// stays uniform and the dead-code lint cannot flag it.
    const ALLOW_EMPTY: bool;
}

/// Marker trait opting a tag into the validated
/// `try_from_str` constructor (rejects NUL, rejects over-length,
/// rejects empty iff `ALLOW_EMPTY = false`). `BoundedStrTag` does
/// *not* implement this trait — its truncating constructor lives on
/// a separate impl block.
pub trait Validated: FixedStrKind {}

/// Tag for [`Ident`] — non-empty, no NUL, max 63 bytes.
///
/// `enum`-with-no-variants → uninstantiable; the type parameter
/// alone carries the nominal distinction without runtime cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentTag {}

impl FixedStrKind for IdentTag {
    const DEBUG_NAME: &'static str = "Ident";
    const ALLOW_EMPTY: bool = false;
}
impl Validated for IdentTag {}

/// Tag for [`DatabaseName`] — same invariants as [`IdentTag`] but a
/// distinct compile-time type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseNameTag {}

impl FixedStrKind for DatabaseNameTag {
    const DEBUG_NAME: &'static str = "DatabaseName";
    const ALLOW_EMPTY: bool = false;
}
impl Validated for DatabaseNameTag {}

/// Tag for [`ApplicationName`] — may be empty; no NUL; max 128 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationNameTag {}

impl FixedStrKind for ApplicationNameTag {
    const DEBUG_NAME: &'static str = "ApplicationName";
    const ALLOW_EMPTY: bool = true;
}
impl Validated for ApplicationNameTag {}

/// Tag for [`BoundedStr<N>`] — truncating constructor with `"…"`
/// marker, no validation. Used exclusively on error-reporting paths
/// where silent truncation would otherwise occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundedStrTag {}

impl FixedStrKind for BoundedStrTag {
    const DEBUG_NAME: &'static str = "BoundedStr";
    const ALLOW_EMPTY: bool = true;
}
// Deliberately *not* `impl Validated for BoundedStrTag` — its
// constructor is `from_str_truncating`, not `try_from_str`.

/// POD fixed-capacity byte string with a phantom `Tag` for nominal
/// typing.
///
/// Layout: `[u8; N]` buffer + `u16` populated-length + ZST tag.
/// `u16` requires `N ≤ 65_535`, checked at monomorphisation via an
/// inline `const { assert!(…) }` in every constructor.
///
/// # Phantom variance
///
/// The phantom field is `PhantomData<fn() -> Tag>` (covariant fn
/// pointer), not `PhantomData<Tag>`. The reason is Rust's derive
/// macros: `#[derive(Copy)]` expands to `impl<Tag: Copy> Copy`,
/// synthesising a `Tag: Copy` bound that would propagate upward
/// ("FixedStr is Copy only if Tag is Copy"). Since our tags are
/// uninhabited `enum`s with no autotraits by default, this would
/// reject `impl Copy`. Using `PhantomData<fn() -> Tag>` makes the
/// field a fn pointer, which is unconditionally `Copy + Send + Sync`
/// regardless of `Tag`, so all derived traits succeed without
/// leaking bounds onto the tag.
///
/// # Traits
///
/// `Copy`, `Clone`, `PartialEq`, `Eq` are implemented **manually**
/// rather than derived. Derive macros synthesise bounds on every
/// type parameter (e.g. `#[derive(Copy)]` expands to
/// `impl<Tag: Copy> Copy`), which would require the tag to implement
/// each trait — but the tags are uninhabited `enum`s with no trait
/// impls. Manual impls bypass the synthesised bounds and produce
/// unconditional implementations that depend only on the concrete
/// fields (`[u8; N]` and `u16`, both of which are `Copy + Eq`).
///
/// `Eq`/`PartialEq` use full `[u8; N]` comparison including the tail
/// past `len`. Since tail bytes past `len` are constructor-zeroed
/// and never mutated post-construction, this is equivalent to
/// comparing `as_bytes()` slices.
#[repr(C)]
pub struct FixedStr<const N: usize, Tag> {
    buf: [u8; N],
    len: u16,
    _tag: PhantomData<fn() -> Tag>,
}

impl<const N: usize, Tag> Clone for FixedStr<N, Tag> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}
impl<const N: usize, Tag> Copy for FixedStr<N, Tag> {}

impl<const N: usize, Tag> PartialEq for FixedStr<N, Tag> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // `len == len` short-circuits the buffer comparison in the
        // common "same length" case. Buffers compare byte-wise over
        // the full `[u8; N]` — tails past `len` are zero-init by
        // every constructor and never mutated post-hoc, so byte
        // equality implies string equality.
        self.len == other.len && self.buf == other.buf
    }
}
impl<const N: usize, Tag> Eq for FixedStr<N, Tag> {}

/// A PostgreSQL user identifier (63-byte cap, non-empty, no NUL).
pub type Ident = FixedStr<MAX_IDENT_LEN, IdentTag>;

/// A PostgreSQL database name (63-byte cap, non-empty, no NUL).
pub type DatabaseName = FixedStr<MAX_IDENT_LEN, DatabaseNameTag>;

/// A PostgreSQL `application_name` parameter (128-byte cap, no NUL,
/// may be empty).
pub type ApplicationName = FixedStr<MAX_APP_NAME_LEN, ApplicationNameTag>;

/// A bounded string with explicit `"…"`-marked truncation on overflow.
///
/// Used in [`crate::error::ServerErrorResponse`] to hold server-sent
/// error message fields with a hard byte cap and no silent truncation.
pub type BoundedStr<const N: usize> = FixedStr<N, BoundedStrTag>;

/// Errors from validated-tag [`FixedStr`] construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentError {
    /// The input was empty and the tag requires non-empty input.
    Empty,
    /// The input contains a NUL byte, which PG uses as a field
    /// terminator in the wire protocol. Tier-1 rejection.
    ContainsNul,
    /// The input exceeds the capacity bound.
    TooLong {
        /// Actual byte length of the rejected input.
        len: usize,
        /// Maximum allowed.
        max: usize,
    },
}

impl fmt::Display for IdentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("identifier must not be empty"),
            Self::ContainsNul => f.write_str("identifier must not contain NUL bytes"),
            Self::TooLong { len, max } => {
                write!(f, "identifier too long: {len} bytes (max {max})")
            }
        }
    }
}

// ───────────────────────── Shared impl block ──────────────────────────

impl<const N: usize, Tag> FixedStr<N, Tag> {
    /// Empty value. Compile-time asserts `N ≤ u16::MAX` via an
    /// inline `const { … }` block that fires at monomorph time.
    ///
    /// `65_535` is hard-coded instead of `u16::MAX as usize` because
    /// `as` casts are banned by the crate forbid-bundle.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(
                N <= 65_535,
                "FixedStr<N, _>: N must fit u16 length prefix (≤ 65_535)",
            );
        }
        Self {
            buf: [0u8; N],
            len: 0,
            _tag: PhantomData,
        }
    }

    /// Populated byte length.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        // `u16 → usize` via `From` impl (infallible, widening).
        usize::from(self.len)
    }

    /// Whether no bytes have been stored.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Borrow the populated bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        // `self.len` is clamped to `≤ N` by every constructor.
        self.buf.get(..self.len()).unwrap_or(&[])
    }

    /// Borrow the populated bytes as `&str`.
    ///
    /// **Validity:** every constructor on this crate accepts only
    /// UTF-8 input (`&str`, or a static UTF-8 marker), so the bytes
    /// are UTF-8 by construction. `core::str::from_utf8` runs an O(N)
    /// validation pass anyway because the crate's `#![forbid(unsafe_code)]`
    /// rules out `from_utf8_unchecked`. The `.unwrap_or("")` fallback
    /// is architecturally unreachable — it surfaces a future
    /// construction bug rather than panicking.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        core::str::from_utf8(self.as_bytes()).unwrap_or("")
    }
}

impl<const N: usize, Tag> Default for FixedStr<N, Tag> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize, Tag: FixedStrKind> fmt::Debug for FixedStr<N, Tag> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(\"{}\")", Tag::DEBUG_NAME, self.as_str())
    }
}

impl<const N: usize, Tag> fmt::Display for FixedStr<N, Tag> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ───────────────── Validated-tag constructor (Ident / DatabaseName /
// ApplicationName) ────────────────

impl<const N: usize, Tag: Validated> FixedStr<N, Tag> {
    /// Construct from a UTF-8 string with full validation.
    ///
    /// Rejects NUL-containing and over-length inputs. Empty input is
    /// rejected iff `Tag::ALLOW_EMPTY` is `false`.
    pub fn try_from_str(s: &str) -> Result<Self, IdentError> {
        let bytes = s.as_bytes();
        if !Tag::ALLOW_EMPTY && bytes.is_empty() {
            return Err(IdentError::Empty);
        }
        if bytes.contains(&0) {
            return Err(IdentError::ContainsNul);
        }
        if bytes.len() > N {
            return Err(IdentError::TooLong {
                len: bytes.len(),
                max: N,
            });
        }
        // `bytes.len() <= N <= 65_535` (const-asserted in `new`),
        // so the narrowing below is infallible.
        let len = u16::try_from(bytes.len()).map_err(|_| IdentError::TooLong {
            len: bytes.len(),
            max: N,
        })?;
        let mut out = Self::new();
        if let Some(dst) = out.buf.get_mut(..bytes.len()) {
            dst.copy_from_slice(bytes);
        }
        out.len = len;
        Ok(out)
    }
}

// ───────────────── BoundedStr-specific truncating constructor ─────────

impl<const N: usize> FixedStr<N, BoundedStrTag> {
    /// UTF-8 ellipsis marker appended on overflow. 3 bytes.
    const OVERFLOW_MARKER: &[u8] = "…".as_bytes();

    /// Construct from a `&str`, truncating at a UTF-8-safe boundary
    /// and appending `"…"` on overflow. Never panics, never silently
    /// drops content.
    ///
    /// Happy path (source fits): one `copy_from_slice` memcpy.
    /// Overflow path: `str::is_char_boundary` walks up to 3 bytes
    /// backward to find the nearest UTF-8 boundary — O(1), not O(N).
    #[must_use]
    pub fn from_str_truncating(source: &str) -> Self {
        let mut out = Self::new(); // also runs the N ≤ u16::MAX assert.
        let src = source.as_bytes();

        // Fast path: source fits verbatim.
        if src.len() <= N {
            if let Some(dst) = out.buf.get_mut(..src.len()) {
                dst.copy_from_slice(src);
            }
            out.len = u16::try_from(src.len()).unwrap_or(0);
            return out;
        }

        // Slow path: truncate to the largest UTF-8 prefix that fits
        // in `N - MARKER.len()` bytes, then append the marker.
        let budget = N.saturating_sub(Self::OVERFLOW_MARKER.len());
        let mut fit_end = budget.min(src.len());
        // A UTF-8 char is 1..=4 bytes; this loop converges in ≤ 3 steps.
        while fit_end > 0 && !source.is_char_boundary(fit_end) {
            fit_end = fit_end.saturating_sub(1);
        }

        if let (Some(dst), Some(slice)) = (out.buf.get_mut(..fit_end), src.get(..fit_end)) {
            dst.copy_from_slice(slice);
        }
        let marker_end = fit_end.saturating_add(Self::OVERFLOW_MARKER.len());
        if let Some(dst) = out.buf.get_mut(fit_end..marker_end) {
            dst.copy_from_slice(Self::OVERFLOW_MARKER);
        }
        out.len = u16::try_from(marker_end).unwrap_or(0);
        out
    }
}
