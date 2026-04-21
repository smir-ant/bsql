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

/// Maximum byte length for a SQL query text (Phase 1c). 2 KiB
/// covers typical statements; anything longer is either a pathological
/// generated query or a COPY command that uses a different path.
/// Extension-statements (`CREATE EXTENSION …`) and multi-statement
/// batch strings fit. If this cap is tightened, the `Sql` type's
/// truncation semantics (via [`BoundedStr::from_str_truncating`])
/// preserve up-to-cap prefix with `"…"` marker — not silent drop.
pub const MAX_SQL_LEN: usize = 2048;

/// Maximum byte length for a PG statement / portal name. PG's
/// `NAMEDATALEN = 64` applies here too (same as [`MAX_IDENT_LEN`]);
/// duplicated const name gives the typed wrapper a distinct
/// compile-time identity.
pub const MAX_PG_NAME_LEN: usize = 63;

/// Sealing module for [`FixedStrKind`] and [`Validated`]. Private
/// to this module so external crates cannot impl the sealed
/// supertrait — and thus cannot impl the public traits either.
/// DEF-115 (escalation of DEF-096).
///
/// Without sealing, a downstream crate could introduce its own tag:
///
/// ```rust,ignore
/// pub enum MyTag {}
/// impl bsql_pg_proto::ident::FixedStrKind for MyTag { … }
/// impl bsql_pg_proto::ident::Validated for MyTag {}
/// ```
///
/// and call the generic `try_from_str` with it. The set of tags was
/// tier-4 in practice ("users happen not to") rather than tier-1
/// compile. The sealed supertrait closes this hole: only types
/// defined inside `bsql-pg-proto` can ever be valid tags.
mod sealed {
    /// Supertrait seal for [`super::FixedStrKind`]. Can only be
    /// impl'd from within the `ident` module.
    pub trait FixedStrKindSealed {}
    /// Supertrait seal for [`super::Validated`].
    pub trait ValidatedSealed {}
    /// Supertrait seal for [`super::Truncating`].
    pub trait TruncatingSealed {}
}

/// Tag trait supplying the per-kind debug name. Every
/// `FixedStr<_, Tag>` uses this to render its own type name in
/// `Debug`.
///
/// **Sealed** (DEF-115): external crates cannot introduce new tags.
/// The sealed supertrait [`sealed::FixedStrKindSealed`] is
/// module-private, so no downstream impl compiles.
///
/// `ALLOW_EMPTY` is consulted by validated-constructor impls.
pub trait FixedStrKind: sealed::FixedStrKindSealed {
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
///
/// **Sealed** (DEF-115): only the crate's own tags can be
/// `Validated`.
pub trait Validated: FixedStrKind + sealed::ValidatedSealed {}

/// Marker trait opting a tag into the truncating
/// `from_str_truncating` constructor — source over the cap is
/// truncated at a UTF-8-safe boundary and an explicit `"…"` marker
/// appended. Used by tags that accept arbitrary user-supplied text
/// (SQL, server error messages) where strict rejection would be
/// hostile.
///
/// **Sealed** (DEF-115).
pub trait Truncating: FixedStrKind + sealed::TruncatingSealed {}

/// Tag for [`Ident`] — non-empty, no NUL, max 63 bytes.
///
/// `enum`-with-no-variants → uninstantiable; the type parameter
/// alone carries the nominal distinction without runtime cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentTag {}

impl sealed::FixedStrKindSealed for IdentTag {}
impl sealed::ValidatedSealed for IdentTag {}
impl FixedStrKind for IdentTag {
    const DEBUG_NAME: &'static str = "Ident";
    const ALLOW_EMPTY: bool = false;
}
impl Validated for IdentTag {}

/// Tag for [`DatabaseName`] — same invariants as [`IdentTag`] but a
/// distinct compile-time type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseNameTag {}

impl sealed::FixedStrKindSealed for DatabaseNameTag {}
impl sealed::ValidatedSealed for DatabaseNameTag {}
impl FixedStrKind for DatabaseNameTag {
    const DEBUG_NAME: &'static str = "DatabaseName";
    const ALLOW_EMPTY: bool = false;
}
impl Validated for DatabaseNameTag {}

/// Tag for [`ApplicationName`] — may be empty; no NUL; max 128 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationNameTag {}

impl sealed::FixedStrKindSealed for ApplicationNameTag {}
impl sealed::ValidatedSealed for ApplicationNameTag {}
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

impl sealed::FixedStrKindSealed for BoundedStrTag {}
impl sealed::TruncatingSealed for BoundedStrTag {}
impl FixedStrKind for BoundedStrTag {
    const DEBUG_NAME: &'static str = "BoundedStr";
    const ALLOW_EMPTY: bool = true;
}
impl Truncating for BoundedStrTag {}
// Deliberately *not* `impl Validated for BoundedStrTag` — its
// constructor is `from_str_truncating`, not `try_from_str`. Also
// deliberately *not* `impl sealed::ValidatedSealed` — the sealed
// supertrait makes this impossible externally anyway, but the
// explicit omission documents the intent.

// ───────────────── Phase 1c typed newtypes ────────────────────
//
// Each PG-level identifier concept gets its own tag so the type
// system rejects cross-use. A `fn execute(stmt: StmtName, portal:
// PortalName)` with arguments swapped is a compile error. Parallels
// the DEF-096 Ident/DatabaseName pattern.
//
// Round-4 finding #2 (2026-04-20). Sealed via DEF-115 seal.

/// Tag for [`Sql`] — SQL query text, truncating on overflow.
///
/// Uses the `BoundedStr`-like truncating constructor (not
/// `Validated`) because arbitrary SQL may contain any UTF-8
/// characters — NUL, empty string, binary-seeming bytes — so the
/// strict `try_from_str` path doesn't fit. Over-length SQL
/// truncates with the `"…"` marker; the caller sees a visibly
/// truncated statement instead of a silent drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlTag {}
impl sealed::FixedStrKindSealed for SqlTag {}
impl sealed::TruncatingSealed for SqlTag {}
impl FixedStrKind for SqlTag {
    const DEBUG_NAME: &'static str = "Sql";
    const ALLOW_EMPTY: bool = true;
}
impl Truncating for SqlTag {}
// Not Validated — truncating constructor only.

/// Tag for [`StmtName`] — a PG prepared-statement name. Validated:
/// no NUL, max [`MAX_PG_NAME_LEN`] bytes. **Empty allowed** — PG
/// treats the empty statement name as the "unnamed statement",
/// a legitimate wire value (§55.2.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtNameTag {}
impl sealed::FixedStrKindSealed for StmtNameTag {}
impl sealed::ValidatedSealed for StmtNameTag {}
impl FixedStrKind for StmtNameTag {
    const DEBUG_NAME: &'static str = "StmtName";
    const ALLOW_EMPTY: bool = true;
}
impl Validated for StmtNameTag {}

/// Tag for [`PortalName`] — a PG portal name (bound statement
/// instance). Same validation shape as [`StmtNameTag`] (NUL-free,
/// capped, **empty allowed** for the unnamed portal) but a
/// distinct compile-time type: passing a `PortalName` where a
/// `StmtName` is expected is a build failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortalNameTag {}
impl sealed::FixedStrKindSealed for PortalNameTag {}
impl sealed::ValidatedSealed for PortalNameTag {}
impl FixedStrKind for PortalNameTag {
    const DEBUG_NAME: &'static str = "PortalName";
    const ALLOW_EMPTY: bool = true;
}
impl Validated for PortalNameTag {}

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

/// A bounded SQL query text. Capacity [`MAX_SQL_LEN`] = 2048 bytes.
/// Overflow truncates at UTF-8 boundary with `"…"` marker (no silent
/// drop — user sees a visibly-truncated statement).
///
/// Round-4 finding #2 — Phase 1c typed newtype.
pub type Sql = FixedStr<MAX_SQL_LEN, SqlTag>;

/// A PG prepared-statement name. Capacity [`MAX_PG_NAME_LEN`] = 63
/// (PG's `NAMEDATALEN - 1`). Validated: non-empty, no NUL.
///
/// Round-4 finding #2 — Phase 1c typed newtype.
pub type StmtName = FixedStr<MAX_PG_NAME_LEN, StmtNameTag>;

/// A PG portal name (bound statement instance). Capacity and
/// validation match [`StmtName`], but distinct compile-time type —
/// a function expecting `StmtName` rejects `PortalName` at type-check.
///
/// Round-4 finding #2 — Phase 1c typed newtype.
pub type PortalName = FixedStr<MAX_PG_NAME_LEN, PortalNameTag>;

/// POD raw-byte buffer — the `FixedStr` cousin without UTF-8
/// semantics. Used for wire-protocol byte slices that aren't strings
/// (e.g. SCRAM `client-first-message-bare`, base64-encoded nonces).
///
/// # Why a separate type
///
/// [`FixedStr<N, _>`] carries UTF-8 invariants (its `as_str` assumes
/// the bytes decode). Raw SCRAM wire bytes can be arbitrary, and a
/// silent `as_str() → ""` on malformed input would mask bugs. Keeping
/// the two types separate makes the "this is bytes, not a string"
/// promise load-bearing at the type level.
///
/// # Layout
///
/// Identical to `FixedStr` minus the phantom tag: `{ buf: [u8; N],
/// len: u16 }`. `Copy`, `Clone`, `PartialEq`, `Eq`, no `Drop`,
/// `Default`. Replaces `heapless::Vec<u8, N>` in state fields where
/// the blanket `Vec::drop` impl (empty body for `u8`, but
/// `needs_drop = true`) propagated up into [`crate::state::ProtoState`]
/// — DEF-099.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PodBytes<const N: usize> {
    buf: [u8; N],
    len: u16,
}

/// Error from [`PodBytes::try_from_slice`] when input exceeds `N`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodBytesOverflow {
    /// Actual byte length of the rejected input.
    pub len: usize,
    /// Maximum capacity `N`.
    pub max: usize,
}

impl fmt::Display for PodBytesOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PodBytes<{}> overflow: {} bytes (max {})",
            self.max, self.len, self.max,
        )
    }
}

impl<const N: usize> PodBytes<N> {
    /// Empty value. Compile-asserts `N ≤ u16::MAX` at monomorph time.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(
                N <= 65_535,
                "PodBytes<N>: N must fit u16 length prefix (≤ 65_535)",
            );
        }
        Self {
            buf: [0u8; N],
            len: 0,
        }
    }

    /// Construct from a byte slice. Rejects over-length inputs with
    /// [`PodBytesOverflow`].
    pub fn try_from_slice(src: &[u8]) -> Result<Self, PodBytesOverflow> {
        if src.len() > N {
            return Err(PodBytesOverflow {
                len: src.len(),
                max: N,
            });
        }
        let len = u16::try_from(src.len()).map_err(|_| PodBytesOverflow {
            len: src.len(),
            max: N,
        })?;
        let mut out = Self::new();
        if let Some(dst) = out.buf.get_mut(..src.len()) {
            dst.copy_from_slice(src);
        }
        out.len = len;
        Ok(out)
    }

    /// Populated byte length.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        usize::from(self.len)
    }

    /// Whether no bytes have been stored.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Borrow the populated bytes.
    ///
    /// See [`FixedStr::as_bytes`] for the rationale behind the
    /// `.get(..n).unwrap_or(&[])` idiom — same forbid-bundle
    /// constraints apply here.
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.buf.get(..self.len()).unwrap_or(&[])
    }
}

impl<const N: usize> Default for PodBytes<N> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> fmt::Debug for PodBytes<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Prints as a normal byte slice. Strings-pretending-to-be-bytes
        // print as their ASCII equivalent where possible (the
        // stdlib's Debug for &[u8] does this).
        f.debug_tuple("PodBytes").field(&self.as_slice()).finish()
    }
}

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
    ///
    /// # Pattern commentary
    ///
    /// The body is `self.buf.get(..self.len()).unwrap_or(&[])`. This
    /// is not a defensive-programming kludge — it is the
    /// minimum-overhead stable-library form that satisfies the crate's
    /// forbid-bundle simultaneously:
    ///
    /// - `&self.buf[..self.len()]` is rejected by
    ///   `clippy::indexing_slicing`.
    /// - `self.buf.get_unchecked(..self.len())` is rejected by
    ///   `#![forbid(unsafe_code)]`.
    /// - `self.buf.get(..self.len()).unwrap()` / `.expect(..)` are
    ///   rejected by the panic / unwrap bans.
    ///
    /// `self.len ≤ N` by constructor invariant, so `get(..n)` always
    /// returns `Some`; the `unwrap_or(&[])` branch is
    /// architecturally unreachable and LLVM eliminates it under any
    /// non-zero optimisation level. The generated machine code is
    /// identical to `&self.buf[..self.len()]`.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
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

// ───────────────── Truncating-tag constructor (BoundedStr / Sql) ──────

impl<const N: usize, Tag: Truncating> FixedStr<N, Tag> {
    /// UTF-8 ellipsis marker appended on overflow. 3 bytes.
    ///
    /// # Why `"…"` and not `"~"` or `"..."` (DEF-126 investigation, 2026-04-21)
    ///
    /// A periodic audit suggestion is "replace `…` with ASCII `~`
    /// to save 2 bytes per truncated buffer and relax the F1
    /// `N >= 3` bound to `N >= 1`". **Rejected** after frequency +
    /// convention analysis — recorded here so future audits don't
    /// re-litigate:
    ///
    /// - **Truncation is error-path only.** Happy paths (CommandComplete
    ///   tag, Sql, EncodingName) essentially never truncate. Only
    ///   long-form ErrorResponse M/D/H fields trigger this marker.
    ///   On a 1M-QPS pool at 0.1% error rate with half producing
    ///   long detail text: ~500 truncations/sec × 2 bytes = 1 KB/sec
    ///   vs MB/sec of wire traffic. Noise.
    /// - **`~` is not a recognised truncation convention.**
    ///   Chrome DevTools / VS Code / modern UIs all use `…`; PG
    ///   internal logs / nginx / Python textwrap use `...`. No
    ///   production system uses `~` — it's semantically loaded
    ///   with home-dir, bitwise-NOT, "approximately", regex-negation.
    ///   A reader seeing `"error: column \"foo\" does not exist~"`
    ///   would not instantly parse "truncated"; they'd wonder
    ///   what the tilde means.
    /// - **F1's `N >= 3` bound is defensive, not constraining.**
    ///   No `BoundedStr<2>` exists in the crate or is planned.
    ///   Relaxing to `N >= 1` is theoretical tidy-up, not practical.
    /// - **ASCII `"..."` alternative:** same 3 bytes, universal
    ///   convention, but the crate is fully UTF-8-aware so there's
    ///   no portability reason to pick ASCII over UTF-8.
    ///
    /// Net: `"…"` is the Pareto-optimal choice. Full analysis
    /// preserved in `deferred.md` DEF-126.
    const OVERFLOW_MARKER: &[u8] = "…".as_bytes();

    /// Compile-time floor for `N` on any `Truncating` tag.
    ///
    /// If `N < OVERFLOW_MARKER.len()`, the truncation path silently
    /// drops the marker (`out.buf.get_mut(fit_end..marker_end)`
    /// returns `None` when `marker_end > N`) while still setting
    /// `out.len = marker_end`. The resulting `FixedStr` claims
    /// length 3 but holds at most `N < 3` valid bytes — corrupt
    /// state. Closing this as tier-1: `BoundedStr<2>` is now a
    /// build failure instead of a latent silent-corruption path.
    ///
    /// All crate-side usages are far above this floor (`BoundedStr<32>`,
    /// `<64>`, `<96>`, `<128>`, `Sql<2048>`); the bound is purely
    /// defensive against future `Truncating` tags with tiny `N`.
    const _TRUNCATING_N_MIN: () = assert!(
        N >= Self::OVERFLOW_MARKER.len(),
        "FixedStr<N, Truncating>: N must be >= OVERFLOW_MARKER.len() (3 bytes for UTF-8 ellipsis). Use a larger N or pick a 1-byte marker.",
    );

    /// Construct from a `&str`, truncating at a UTF-8-safe boundary
    /// and appending `"…"` on overflow. Never panics, never silently
    /// drops content.
    ///
    /// Happy path (source fits): one `copy_from_slice` memcpy.
    /// Overflow path: `str::is_char_boundary` walks up to 3 bytes
    /// backward to find the nearest UTF-8 boundary — O(1), not O(N).
    #[must_use]
    pub fn from_str_truncating(source: &str) -> Self {
        // Force monomorphisation of the floor assert — associated
        // `const` items are lazy; without this reference the assert
        // never triggers for bad `N`.
        let () = Self::_TRUNCATING_N_MIN;
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

    /// Construct from possibly-non-UTF-8 bytes without silent drop.
    ///
    /// Fast path: if `source` is valid UTF-8, delegates to
    /// [`Self::from_str_truncating`] — preserves multibyte characters.
    ///
    /// Slow path: scans byte-by-byte. Bytes outside `0x20..=0x7e`
    /// (ASCII printable) and outside `{b'\t', b'\n', b'\r'}` become
    /// `b'?'`. Output is ASCII-only → always valid UTF-8. Applies the
    /// same truncation + marker-append policy as the fast path.
    ///
    /// # When to use
    ///
    /// PG ErrorResponse field values are encoded in the server's
    /// `client_encoding` setting, which is UTF-8 by default but CAN
    /// be Latin-1 or a legacy encoding on mis-configured servers.
    /// Pre-F22 code used `core::str::from_utf8(..).unwrap_or("")`,
    /// which silently collapsed the WHOLE field to empty on any
    /// invalid byte — destroying forensic diagnostic info. This
    /// method preserves the ASCII subset (most of every error
    /// message) and visibly marks the rest.
    ///
    /// # Tier elevation (F22)
    ///
    /// - Old: silent-empty-on-invalid-UTF-8 (tier-3 audit — nothing
    ///   in the type system prevented the diagnostic loss).
    /// - New: byte-by-byte ASCII coercion is a structural
    ///   always-valid-UTF-8 guarantee. No information is silently
    ///   dropped; over-length or invalid bytes are visibly marked.
    #[must_use]
    pub fn from_bytes_lossy(source: &[u8]) -> Self {
        let () = Self::_TRUNCATING_N_MIN;
        // Fast path: preserve multibyte UTF-8 when the bytes are valid.
        if let Ok(s) = core::str::from_utf8(source) {
            return Self::from_str_truncating(s);
        }
        // Slow path: coerce every non-ASCII byte to `?`.
        let mut out = Self::new();
        let budget = N.saturating_sub(Self::OVERFLOW_MARKER.len());
        let mut written = 0usize;
        for &b in source.iter() {
            if written >= budget {
                break;
            }
            // Accept ASCII printable + common whitespace; everything
            // else (non-ASCII, control chars, NUL) → `?`.
            let out_byte = if matches!(b, 0x20..=0x7e | b'\t' | b'\n' | b'\r') {
                b
            } else {
                b'?'
            };
            if let Some(dst) = out.buf.get_mut(written) {
                *dst = out_byte;
            }
            written = written.saturating_add(1);
        }
        if source.len() > budget {
            let marker_end = written.saturating_add(Self::OVERFLOW_MARKER.len());
            if let Some(dst) = out.buf.get_mut(written..marker_end) {
                dst.copy_from_slice(Self::OVERFLOW_MARKER);
            }
            out.len = u16::try_from(marker_end).unwrap_or(0);
        } else {
            out.len = u16::try_from(written).unwrap_or(0);
        }
        out
    }
}
