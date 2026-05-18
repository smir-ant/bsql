//! Bounded POD string types for the PostgreSQL wire — one generic
//! `FixedStr<N, Tag>` parameterised by a phantom-tag for nominal
//! typing. DEF-096.
//!
//! # Trait hierarchy overview (DEF-163 G016)
//!
//! ```text
//!                    ┌─────────────────────────┐
//!                    │     FixedStrKind        │  sealed — supertrait
//!                    │   (sealed base trait)   │  for phantom tags
//!                    └─────────────────────────┘
//!                         ▲          ▲
//!                         │          │
//!               ┌─────────┤          ├───────────┐
//!               │         │          │           │
//!               │         │          │           │
//!          ┌────┴───┐ ┌───┴────┐ ┌───┴─────┐ ┌───┴──────┐
//!          │Validated│ │Truncat.│ │ValidUtf8│ │ ... (future) │
//!          │(sealed) │ │(sealed)│ │(sealed) │ │              │
//!          └────┬────┘ └───┬────┘ └────┬────┘ └──────────────┘
//!               │          │           │
//!         (`try_*`   (`from_str_      (`as_str()` — runtime
//!         constructor   truncating`    UTF-8 check dead-arm
//!         returns      constructor    per §4.12 `unwrap_or("")`
//!         Result)      infallible)    type-safe sink)
//!               │          │           │
//!               └──────┬───┴───────────┘
//!                      │
//!                      │  (tags opt INTO the capability markers
//!                      │   they satisfy; no bypass is possible
//!                      │   via sealed-seal invariant)
//!                      │
//!          ┌───────────┴──────────────┬────────────────┬──────────────┐
//!          ▼                          ▼                ▼              ▼
//!     IdentTag                DatabaseNameTag  ApplicationNameTag  BoundedStrTag
//!     (impls Validated+       (impls          (impls             (impls
//!      Truncating+            Validated+       Validated+          Truncating+
//!      ValidUtf8)             Truncating+      Truncating+         ValidUtf8)
//!                             ValidUtf8)       ValidUtf8)
//! ```
//!
//! - `FixedStrKind` — supertrait sealed by `sealed::FixedStrKindSealed`.
//!   The only legitimate tag types are the four crate-internal ones
//!   above; external crates cannot define new tags.
//! - `Validated` — tags whose constructor validates input (e.g. PG
//!   ident / database-name rules). Exposes `FixedStr::try_from_*`.
//! - `Truncating` — tags whose constructor accepts any input and
//!   truncates to N bytes (with `"…"` marker on overflow). Exposes
//!   `FixedStr::from_*_truncating`.
//! - `ValidUtf8` — tags whose stored bytes are guaranteed valid UTF-8.
//!   Exposes `FixedStr::as_str()` (runtime `from_utf8` check is a
//!   type-safe sink per §4.12 — dead-arm `unwrap_or("")` documented).
//!
//! Concrete public aliases live at the module bottom:
//! `pub type Ident = FixedStr<63, IdentTag>;` etc.
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
/// ```text
/// pub enum MyTag {}
/// impl bsql_pg_proto::ident::FixedStrKind for MyTag { … }
/// impl bsql_pg_proto::ident::Validated for MyTag {}
/// ```
///
/// DEF-154 (R) P1-3: reclassified from `rust,ignore` to `text` —
/// this is a NEGATIVE example (what the seal PREVENTS from
/// compiling). A `compile_fail` trybuild harness would verify
/// the seal is load-bearing; the docstring example is
/// illustrative prose.
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
    /// Supertrait seal for [`super::ValidUtf8`].
    pub trait ValidUtf8Sealed {}
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
//
// DEF-115 follow-up (rust-version 1.78 modernisation): structural
// diagnostic. Sealed-bound failures need the candidate set in plain
// text.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a `FixedStrKind` tag",
    label = "valid tags are the uninhabited enums `IdentTag`, `DatabaseNameTag`, `ApplicationNameTag`, `BoundedStrTag`, `SqlTag`, `StmtNameTag`, `PortalNameTag`",
    note = "`FixedStrKind` is sealed (DEF-115) — the tag set is fixed at the crate boundary; downstream `impl FixedStrKind for ...` is forbidden by construction"
)]
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
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not opt into the validated constructor path",
    label = "tags that implement `Validated`: `IdentTag`, `DatabaseNameTag`, `ApplicationNameTag`, `StmtNameTag`, `PortalNameTag`",
    note = "`Validated` is sealed — `BoundedStrTag` and `SqlTag` deliberately do NOT implement it because their construction is truncating (silent truncation with `…` marker), not validating; choose the matching tag or use `from_str_truncating` for those"
)]
pub trait Validated: FixedStrKind + sealed::ValidatedSealed {}

/// Marker trait opting a tag into the truncating
/// `from_str_truncating` constructor — source over the cap is
/// truncated at a UTF-8-safe boundary and an explicit `"…"` marker
/// appended. Used by tags that accept arbitrary user-supplied text
/// (SQL, server error messages) where strict rejection would be
/// hostile.
///
/// **Sealed** (DEF-115).
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not opt into the truncating constructor path",
    label = "tags that implement `Truncating`: `BoundedStrTag`, `SqlTag`",
    note = "`Truncating` is sealed — the validated tags (`IdentTag`, `DatabaseNameTag`, `ApplicationNameTag`, `StmtNameTag`, `PortalNameTag`) deliberately reject silent truncation; choose the matching tag or use `try_from_str` for those"
)]
pub trait Truncating: FixedStrKind + sealed::TruncatingSealed {}

/// Marker trait asserting that a tag's constructors guarantee the
/// stored bytes are valid UTF-8.
///
/// [`FixedStr::as_str`] is only available on `FixedStr<N, Tag>` where
/// `Tag: ValidUtf8` — tags whose constructors don't guarantee UTF-8
/// (none exist today — all crate tags take `&str` or coerce to ASCII
/// via `from_bytes_lossy`) would be statically prevented from
/// exposing their bytes as `&str`. F3: tier-3 audit pairing (the
/// `as_str` fallback `""` is safe only because every current tag
/// happens to produce UTF-8) → tier-2 structural (tag must opt into
/// `ValidUtf8` to earn `as_str`).
///
/// **Sealed** (DEF-115): only the crate's own tags can be
/// `ValidUtf8`. A downstream tag type cannot bypass the check.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not assert UTF-8-validity of its stored bytes",
    label = "all current crate tags implement `ValidUtf8` (DEF-115); only `as_str()` requires this bound",
    note = "`ValidUtf8` is sealed — a downstream tag type cannot bypass the UTF-8 check; if you reach this error you're likely working in test code with a non-crate tag, which the `FixedStr` machinery deliberately rejects"
)]
pub trait ValidUtf8: FixedStrKind + sealed::ValidUtf8Sealed {}

/// Tag for [`Ident`] — non-empty, no NUL, max 63 bytes.
///
/// `enum`-with-no-variants → uninstantiable; the type parameter
/// alone carries the nominal distinction without runtime cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentTag {}

impl sealed::FixedStrKindSealed for IdentTag {}
impl sealed::ValidatedSealed for IdentTag {}
impl sealed::ValidUtf8Sealed for IdentTag {}
impl FixedStrKind for IdentTag {
    const DEBUG_NAME: &'static str = "Ident";
    const ALLOW_EMPTY: bool = false;
}
impl Validated for IdentTag {}
impl ValidUtf8 for IdentTag {}

/// Tag for [`DatabaseName`] — same invariants as [`IdentTag`] but a
/// distinct compile-time type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseNameTag {}

impl sealed::FixedStrKindSealed for DatabaseNameTag {}
impl sealed::ValidatedSealed for DatabaseNameTag {}
impl sealed::ValidUtf8Sealed for DatabaseNameTag {}
impl FixedStrKind for DatabaseNameTag {
    const DEBUG_NAME: &'static str = "DatabaseName";
    const ALLOW_EMPTY: bool = false;
}
impl Validated for DatabaseNameTag {}
impl ValidUtf8 for DatabaseNameTag {}

/// Tag for [`ApplicationName`] — may be empty; no NUL; max 128 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationNameTag {}

impl sealed::FixedStrKindSealed for ApplicationNameTag {}
impl sealed::ValidatedSealed for ApplicationNameTag {}
impl sealed::ValidUtf8Sealed for ApplicationNameTag {}
impl FixedStrKind for ApplicationNameTag {
    const DEBUG_NAME: &'static str = "ApplicationName";
    const ALLOW_EMPTY: bool = true;
}
impl Validated for ApplicationNameTag {}
impl ValidUtf8 for ApplicationNameTag {}

/// Tag for [`BoundedStr<N>`] — truncating constructor with `"…"`
/// marker, no validation. Used exclusively on error-reporting paths
/// where silent truncation would otherwise occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundedStrTag {}

impl sealed::FixedStrKindSealed for BoundedStrTag {}
impl sealed::TruncatingSealed for BoundedStrTag {}
impl sealed::ValidUtf8Sealed for BoundedStrTag {}
impl FixedStrKind for BoundedStrTag {
    const DEBUG_NAME: &'static str = "BoundedStr";
    const ALLOW_EMPTY: bool = true;
}
impl Truncating for BoundedStrTag {}
impl ValidUtf8 for BoundedStrTag {}
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
impl sealed::ValidUtf8Sealed for SqlTag {}
impl FixedStrKind for SqlTag {
    const DEBUG_NAME: &'static str = "Sql";
    const ALLOW_EMPTY: bool = true;
}
impl Truncating for SqlTag {}
impl ValidUtf8 for SqlTag {}
// Not Validated — truncating constructor only.

/// Tag for [`StmtName`] — a PG prepared-statement name. Validated:
/// no NUL, max [`MAX_PG_NAME_LEN`] bytes. **Empty allowed** — PG
/// treats the empty statement name as the "unnamed statement",
/// a legitimate wire value (§55.2.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtNameTag {}
impl sealed::FixedStrKindSealed for StmtNameTag {}
impl sealed::ValidatedSealed for StmtNameTag {}
impl sealed::ValidUtf8Sealed for StmtNameTag {}
impl FixedStrKind for StmtNameTag {
    const DEBUG_NAME: &'static str = "StmtName";
    const ALLOW_EMPTY: bool = true;
}
impl Validated for StmtNameTag {}
impl ValidUtf8 for StmtNameTag {}

/// Tag for [`PortalName`] — a PG portal name (bound statement
/// instance). Same validation shape as [`StmtNameTag`] (NUL-free,
/// capped, **empty allowed** for the unnamed portal) but a
/// distinct compile-time type: passing a `PortalName` where a
/// `StmtName` is expected is a build failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortalNameTag {}
impl sealed::FixedStrKindSealed for PortalNameTag {}
impl sealed::ValidatedSealed for PortalNameTag {}
impl sealed::ValidUtf8Sealed for PortalNameTag {}
impl FixedStrKind for PortalNameTag {
    const DEBUG_NAME: &'static str = "PortalName";
    const ALLOW_EMPTY: bool = true;
}
impl Validated for PortalNameTag {}
impl ValidUtf8 for PortalNameTag {}

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
/// `Eq`/`PartialEq` (F46) compare only the populated prefix
/// `[..self.len()]` — not the full `[u8; N]` buffer. Tail bytes are
/// constructor-zeroed and never mutated, so logically the two forms
/// are equivalent, but comparing only the populated prefix saves
/// up to `N - len` bytes per equality check. For `Sql<2048>` with a
/// typical 64-byte query, the prefix compare is 64 bytes vs 2048
/// for the full-buffer compare — 32x reduction on every `==`.
#[repr(C)]
pub struct FixedStr<const N: usize, Tag, LenT = crate::bounded::BoundedU16<N>>
where
    LenT: crate::bounded::BoundedLen<N>,
{
    buf: [u8; N],
    /// DEF-203 ext: typed length-storage parameter. Defaults to
    /// `BoundedU16<N>` (2 B, NonZeroU16 niche). Type aliases for
    /// small-N (≤ 254) types pick `BoundedU8<N>` (1 B + niche).
    /// **Tier-2 by-construct** — out-of-range len values cannot
    /// exist via `BoundedLen::try_new_usize`.
    len: LenT,
    /// DEF-185 P2-D (audit 2026-04-24): flag indicating that
    /// `from_bytes_lossy` coerced at least one non-ASCII-printable
    /// byte to `b'?'`. Callers can query via [`Self::was_lossy`]
    /// to distinguish legitimate `?` characters in server text
    /// from our lossy fallback. False on every non-lossy
    /// constructor (`new`, `from_str_truncating`, `Default`).
    ///
    /// Stored as `u8` (1 byte) rather than `bool` to keep the
    /// `#[repr(C)]` layout portable across platforms — Rust's
    /// `bool` ABI is stable on all tier-1 targets but the zeroize
    /// derive macros use `u8`-based reflection on repr(C) structs,
    /// matching here keeps the Zeroize bounds if a future refactor
    /// makes FixedStr `Zeroize`-aware.
    was_lossy_flag: u8,
    _tag: PhantomData<fn() -> Tag>,
}

impl<const N: usize, Tag, LenT: crate::bounded::BoundedLen<N>> Clone for FixedStr<N, Tag, LenT> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}
impl<const N: usize, Tag, LenT: crate::bounded::BoundedLen<N>> Copy for FixedStr<N, Tag, LenT> {}

impl<const N: usize, Tag, LenT: crate::bounded::BoundedLen<N>> PartialEq for FixedStr<N, Tag, LenT> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // F46: compare `len` first (short-circuit on different
        // length), then compare only the populated prefix.
        self.len == other.len && self.as_bytes() == other.as_bytes()
    }
}
impl<const N: usize, Tag, LenT: crate::bounded::BoundedLen<N>> Eq for FixedStr<N, Tag, LenT> {}

/// A PostgreSQL user identifier (63-byte cap, non-empty, no NUL).
/// **DEF-203 ext**: uses `BoundedU8<63>` LenT for the niche win on
/// `Option<Ident>` (saves 3 B per Option vs the default BoundedU16).
pub type Ident = FixedStr<MAX_IDENT_LEN, IdentTag, crate::bounded::BoundedU8<MAX_IDENT_LEN>>;

/// A PostgreSQL database name (63-byte cap, non-empty, no NUL).
pub type DatabaseName =
    FixedStr<MAX_IDENT_LEN, DatabaseNameTag, crate::bounded::BoundedU8<MAX_IDENT_LEN>>;

/// A PostgreSQL `application_name` parameter (128-byte cap, no NUL,
/// may be empty).
pub type ApplicationName =
    FixedStr<MAX_APP_NAME_LEN, ApplicationNameTag, crate::bounded::BoundedU8<MAX_APP_NAME_LEN>>;

/// A bounded string with explicit `"…"`-marked truncation on overflow.
///
/// Used in [`crate::error::ServerErrorResponse`] to hold server-sent
/// error message fields with a hard byte cap and no silent truncation.
///
/// Uses the default `BoundedU16<N>` LenT — covers any N up to 65_534.
pub type BoundedStr<const N: usize> = FixedStr<N, BoundedStrTag>;

/// A bounded SQL query text. Capacity [`MAX_SQL_LEN`] = 2048 bytes.
/// Overflow truncates at UTF-8 boundary with `"…"` marker (no silent
/// drop — user sees a visibly-truncated statement).
///
/// Uses the default `BoundedU16<N>` LenT (2048 > 254).
pub type Sql = FixedStr<MAX_SQL_LEN, SqlTag>;

/// A PG prepared-statement name. Capacity [`MAX_PG_NAME_LEN`] = 63
/// (PG's `NAMEDATALEN - 1`). Validated: non-empty, no NUL.
pub type StmtName =
    FixedStr<MAX_PG_NAME_LEN, StmtNameTag, crate::bounded::BoundedU8<MAX_PG_NAME_LEN>>;

/// A PG portal name (bound statement instance). Capacity and
/// validation match [`StmtName`], but distinct compile-time type —
/// a function expecting `StmtName` rejects `PortalName` at type-check.
///
/// Round-4 finding #2 — Phase 1c typed newtype.
pub type PortalName =
    FixedStr<MAX_PG_NAME_LEN, PortalNameTag, crate::bounded::BoundedU8<MAX_PG_NAME_LEN>>;

// ─── DEF-203 ext: Option<T> niche size pins ─────────────────────────
//
// Migrating `len: u16` → `len: BoundedU8<63>` (or `BoundedU16<N>`)
// shrinks the type AND lets `Option<T>` absorb the discriminant via
// the underlying `NonZero` niche. These const-asserts pin the win
// exactly so a future regression that loses the niche fails the build.
//
// Pre-DEF-203-ext layout for `FixedStr<63, _>` (all small-N validated
// types): `buf: [u8; 63] (63 B) + pad (1 B) + len: u16 (2 B) +
// was_lossy_flag: u8 (1 B) + tag (0 B) = 67 B aligned to 2 = 68 B`.
// `Option<Self> = 68 + 1 disc + 1 pad = 70 B`.
//
// Post-DEF-203-ext: `buf (63 B) + len: BoundedU8<63> (1 B) +
// was_lossy_flag (1 B) = 65 B aligned to 1 = 65 B`. `Option<Self> =
// 65 B (NonZeroU8 niche absorbs the discriminant).` **Saving: 5 B
// per type, 5 B per Option per site.**

const _: () = assert!(
    core::mem::size_of::<Ident>() == 65,
    "Ident must be 65 B post-DEF-203 ext (was 68 B with u16 len). \
     If this trips, BoundedU8 niche may have been lost.",
);
const _: () = assert!(
    core::mem::size_of::<Option<Ident>>() == 65,
    "Option<Ident> must be 65 B post-DEF-203 ext (NonZeroU8 niche). \
     Pre-DEF-203-ext was 70 B. Saving: 5 B per Option<Ident>.",
);
const _: () = assert!(
    core::mem::size_of::<DatabaseName>() == 65,
    "DatabaseName must be 65 B post-DEF-203 ext (BoundedU8<63> len)",
);
const _: () = assert!(
    core::mem::size_of::<Option<DatabaseName>>() == 65,
    "Option<DatabaseName> must be 65 B post-DEF-203 ext (niche)",
);
const _: () = assert!(
    core::mem::size_of::<ApplicationName>() == 130,
    "ApplicationName must be 130 B post-DEF-203 ext (= 128 buf + 1 BoundedU8<128> len + 1 was_lossy)",
);
const _: () = assert!(
    core::mem::size_of::<Option<ApplicationName>>() == 130,
    "Option<ApplicationName> must be 130 B post-DEF-203 ext (niche)",
);
const _: () = assert!(
    core::mem::size_of::<StmtName>() == 65,
    "StmtName must be 65 B post-DEF-203 ext (BoundedU8<63> len)",
);
const _: () = assert!(
    core::mem::size_of::<PortalName>() == 65,
    "PortalName must be 65 B post-DEF-203 ext (BoundedU8<63> len)",
);

// ═════════════════════════════════════════════════════════════════
// DEF-205 (2026-04-27): SecretBoundedStr — sensitive bounded string.
//
// Closes the staleness leak class for `Option<T> = None` and
// `*self = Self::new()` patterns where `T` contains potentially
// sensitive bytes (server error messages, server-echoed parameters
// that include username / application name / deployment info).
//
// **Tier-1 by Drop chain**: Rust language semantics guarantee that
// assignment `field = new_value` drops the old value before moving
// the new one in. Combined with `ZeroizeOnDrop`, this scrubs the
// previous bytes by compiler-enforced construction — no programmer
// action required at the assignment site, no audit dependency.
//
// **Why a separate type vs `BoundedStr<N>`**: `BoundedStr<N>` (=
// `FixedStr<N, BoundedStrTag>`) is `Copy`. Adding `Drop` to a `Copy`
// type is forbidden by Rust. Splitting sensitive vs non-sensitive
// bounded strings keeps the hot-path types (`Ident`, `StmtName`,
// `Sql`, non-secret `BoundedStr`) Copy-fast while sensitive types
// (used in `ErrorPayload` / `SessionParams` sensitive fields) get
// the Drop-chain guarantee.
//
// **Implementation**: thin wrapper around `BoundedStr<N>` (storage
// reuse, no duplicated truncation/UTF-8 logic). The wrapper is
// non-Copy (Drop forbids Copy), so cloning is explicit via
// `Clone`-derive — caller decides when to duplicate.
// ═════════════════════════════════════════════════════════════════

/// Bounded UTF-8 string for sensitive bytes — server error messages,
/// session-parameter values that may include credentials/usernames.
///
/// Mirrors [`BoundedStr<N>`]'s shape and constructors but is
/// **non-Copy** with a `Drop` impl that scrubs the buffer. By Rust
/// language semantics, every overwrite (`field = new`, `*self = X`,
/// `option.replace(new)`) fires `Drop` on the previous value before
/// the new one is moved in — closing the staleness leak class
/// described in DEF-205.
///
/// # Tier-1 by Drop chain
///
/// Replacement → old's Drop → `inner.zeroize_in_place()` → buffer
/// scrubbed → new value installed. The compiler enforces step 1
/// (Drop firing) — programmer cannot accidentally skip it. This is
/// stronger than tier-2 "explicit scrub call before reassignment"
/// because there's no callsite to forget.
///
/// # API mirror of `BoundedStr<N>`
///
/// Same constructor names ([`Self::new`], [`Self::from_str_truncating`],
/// [`Self::from_bytes_lossy`]) and accessors ([`Self::as_bytes`],
/// [`Self::as_str`], [`Self::len`], [`Self::is_empty`],
/// [`Self::was_lossy`]) so migrations from `BoundedStr<N>` to
/// `SecretBoundedStr<N>` are mechanical.
///
/// # Debug redaction
///
/// `Debug` prints `SecretBoundedStr<N>(<REDACTED, len=K>)` —
/// content is hidden to defend against accidental log-leak when
/// a containing struct is debug-printed (e.g., `eprintln!("{params:?}",
/// params)`). Same precedent as [`crate::sensitive::Sensitive<T>`]
/// (DEF-185 P1-C).
#[repr(transparent)]
pub struct SecretBoundedStr<const N: usize> {
    inner: BoundedStr<N>,
}

impl<const N: usize> SecretBoundedStr<N> {
    /// Empty `SecretBoundedStr<N>`. Buffer all-zero, `len = 0`.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: BoundedStr::<N>::new(),
        }
    }

    /// Construct from `&str`, truncating with `"…"` marker on overflow.
    /// Mirrors [`BoundedStr<N>::from_str_truncating`].
    #[inline]
    #[must_use]
    pub fn from_str_truncating(source: &str) -> Self {
        Self {
            inner: BoundedStr::<N>::from_str_truncating(source),
        }
    }

    /// Construct from raw `&[u8]`, coercing non-UTF-8 bytes to `b'?'`
    /// and setting the lossy flag (queryable via [`Self::was_lossy`]).
    /// Mirrors [`BoundedStr<N>::from_bytes_lossy`].
    #[inline]
    #[must_use]
    pub fn from_bytes_lossy(source: &[u8]) -> Self {
        Self {
            inner: BoundedStr::<N>::from_bytes_lossy(source),
        }
    }

    /// Borrow the populated bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    /// Borrow as `&str` (UTF-8 invariant from `BoundedStr<N>`'s
    /// `ValidUtf8` constructor).
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.inner.as_str()
    }

    /// Populated byte length.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the buffer is empty (`len == 0`).
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Whether the constructor coerced any non-UTF-8 bytes during
    /// `from_bytes_lossy`. Mirrors [`BoundedStr<N>::was_lossy`].
    #[inline]
    #[must_use]
    pub const fn was_lossy(&self) -> bool {
        self.inner.was_lossy()
    }
}

impl<const N: usize> Clone for SecretBoundedStr<N> {
    /// Explicit clone — `SecretBoundedStr<N>` is non-Copy by design
    /// (Drop conflicts with Copy). Caller picks the duplication
    /// point; each clone gets its own Drop / scrub.
    #[inline]
    fn clone(&self) -> Self {
        Self { inner: self.inner }
    }
}

impl<const N: usize> PartialEq for SecretBoundedStr<N> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<const N: usize> Eq for SecretBoundedStr<N> {}

impl<const N: usize> Default for SecretBoundedStr<N> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> fmt::Debug for SecretBoundedStr<N> {
    /// Redacted Debug — buffer content hidden, only type, capacity,
    /// and populated length printed. Defends against accidental
    /// log-leak via debug-printing a containing struct.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SecretBoundedStr<{N}>(<REDACTED, len={}>)",
            self.inner.len(),
        )
    }
}

impl<const N: usize> zeroize::Zeroize for SecretBoundedStr<N> {
    /// Scrub the buffer in place (same effect as Drop, but explicit
    /// and non-consuming).
    #[inline]
    fn zeroize(&mut self) {
        self.inner.zeroize_in_place();
    }
}

impl<const N: usize> Drop for SecretBoundedStr<N> {
    /// DEF-205 tier-1 closure: scrub the buffer when the value is
    /// dropped. By Rust language semantics this fires on every
    /// overwrite (`field = new`, `*self = X`, `option.replace(new)`,
    /// `mem::take`, `mem::replace`) before the new value is moved in
    /// — the previous bytes can never silently persist past the
    /// assignment.
    fn drop(&mut self) {
        self.inner.zeroize_in_place();
    }
}

// Tier-1 size pin — `repr(transparent)` ensures `SecretBoundedStr<N>`
// is layout-identical to `BoundedStr<N>`. A future field addition
// would change the size and trip this assertion. Stable across
// targets (BoundedStr/FixedStr is `repr(C)` with deterministic
// layout under MAX_PG_NAME_LEN-class N values).
const _: () = {
    // Concrete N for the assertion (any reasonable N works; we pick
    // the smallest Sql-class to keep the assertion compile cost low).
    assert!(
        core::mem::size_of::<SecretBoundedStr<32>>()
            == core::mem::size_of::<BoundedStr<32>>(),
        "SecretBoundedStr<N> must be layout-identical to BoundedStr<N> \
         (repr(transparent) wrapper). A field addition or repr change \
         broke this — re-audit before shipping.",
    );
};

// `Send` is automatically derived (no non-Send fields). Pin it
// explicitly so a future field addition that introduces non-Send
// state (e.g., `Rc<...>`) becomes a build error here rather than a
// silent regression in user-facing code.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<SecretBoundedStr<32>>();
};

// ═════════════════════════════════════════════════════════════════
// 1c-3c F12 (pass-#7 audit): sealed `DescribeName` trait
//
// Narrows the `build_describe_message` builder's `name` parameter
// from a raw `&[u8]` (tier-3 "caller promises to pass StmtName or
// PortalName .as_bytes()") to a typed `&impl DescribeName` (tier-1
// "builder accepts only these two types, sealed against downstream
// impls"). Catches the bug class where a refactor accidentally
// passes a raw `&[u8]` containing an embedded NUL — the type system
// now rejects it at compile time.
//
// The trait is sealed against downstream implementation via the
// private `sealed::Sealed` supertrait — external crates cannot add
// impls, so the set `{StmtName, PortalName}` is closed at crate
// boundary.
// ═════════════════════════════════════════════════════════════════

mod describe_name_sealed {
    /// Seal: prevents external crates from implementing
    /// `DescribeName`. The trait's whole job is to enumerate exactly
    /// the two types PG's Describe frame accepts.
    pub trait Sealed {}
    impl Sealed for super::StmtName {}
    impl Sealed for super::PortalName {}
}

/// Typed name argument for the PG Extended Query `Describe` builder.
/// Sealed (`StmtName` + `PortalName` only).
///
/// # Why sealed
///
/// PG's Describe frame (`'D'`) takes exactly one target-name field:
/// a prepared-statement name or a portal name. Both are ≤ 63 bytes
/// (PG's `NAMEDATALEN - 1`) with no embedded NULs. Binding the
/// builder's `name` parameter to this trait makes "caller passes
/// the right name type" a tier-1 compile guarantee; a caller who
/// passes raw `&[u8]` fails to type-check.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a valid target for the PG `Describe` frame",
    label = "valid targets are `StmtName` (prepared statement) and `PortalName` (bound statement instance)",
    note = "`DescribeName` is sealed — the PG wire `Describe` frame ('D') takes exactly one of `S` (statement) or `P` (portal); raw `&[u8]` is rejected at compile time because it cannot guarantee absence of embedded NUL"
)]
pub trait DescribeName: describe_name_sealed::Sealed {
    /// The raw NUL-free bytes to embed into the `'D'` frame body,
    /// followed by the NUL terminator the builder appends. Every
    /// `FixedStr<N, _>` satisfies "no embedded NUL" via its
    /// validating constructors.
    #[must_use]
    fn as_describe_name_bytes(&self) -> &[u8];
}

impl DescribeName for StmtName {
    #[inline]
    fn as_describe_name_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl DescribeName for PortalName {
    #[inline]
    fn as_describe_name_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

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
// Clone/Copy/PartialEq/Eq are impl'd manually for the LenT-generic
// form below — derives don't mix well with generic-over-LenT bounds
// when the trait bound itself constrains the field type.
#[repr(C)]
pub struct PodBytes<const N: usize, LenT = crate::bounded::BoundedU16<N>>
where
    LenT: crate::bounded::BoundedLen<N>,
{
    buf: [u8; N],
    /// DEF-203 ext: typed length-storage (parallel to FixedStr's
    /// LenT migration). Default `BoundedU16<N>` covers any N up to
    /// 65_534. **Tier-2 by-construct** — out-of-range len cannot
    /// exist via `BoundedLen::try_new_usize`.
    len: LenT,
}

/// Error from [`PodBytes::try_from_slice`] when input exceeds `N`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodBytesOverflow {
    /// Actual byte length of the rejected input.
    pub len: usize,
    /// Maximum capacity `N`.
    pub max: usize,
}

// DEF-244 modernisation audit (rust-version 1.81): additive
// `core::error::Error` impl on the PodBytes overflow sentinel.
impl core::error::Error for PodBytesOverflow {}

impl fmt::Display for PodBytesOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PodBytes<{}> overflow: {} bytes (max {})",
            self.max, self.len, self.max,
        )
    }
}

// ─── Concrete `const fn new` for default LenT (DEF-203 ext) ─────────

impl<const N: usize> PodBytes<N, crate::bounded::BoundedU16<N>> {
    /// Empty value. Compile-asserts `N ≤ 65_534` (BoundedU16 niche).
    /// Const-fn provided for the default LenT only; non-default LenT
    /// users can use `Self::default()`.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(N <= 65_534, "PodBytes with BoundedU16 LenT requires N ≤ 65_534");
        }
        Self {
            buf: [0u8; N],
            len: crate::bounded::BoundedU16::<N>::ZERO,
        }
    }
}

// ─── Generic methods over LenT ──────────────────────────────────────

impl<const N: usize, LenT: crate::bounded::BoundedLen<N>> PodBytes<N, LenT> {
    /// Construct from a byte slice. Rejects over-length inputs with
    /// [`PodBytesOverflow`].
    pub fn try_from_slice(src: &[u8]) -> Result<Self, PodBytesOverflow> {
        if src.len() > N {
            return Err(PodBytesOverflow {
                len: src.len(),
                max: N,
            });
        }
        // DEF-203 ext: tier-2 by-construct via BoundedLen::try_new_usize.
        // Earlier `src.len() > N` guard makes this Err branch dead.
        let len = LenT::try_new_usize(src.len()).ok_or(PodBytesOverflow {
            len: src.len(),
            max: N,
        })?;
        let mut out = Self::default();
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
        self.len.get_usize()
    }

    /// Whether no bytes have been stored.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len.get_usize() == 0
    }

    /// Borrow the populated bytes.
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        let n = self.len();
        match self.buf.split_at_checked(n) {
            Some((head, _)) => head,
            None => &[],
        }
    }
}

impl<const N: usize, LenT: crate::bounded::BoundedLen<N>> Default for PodBytes<N, LenT> {
    #[inline]
    fn default() -> Self {
        Self {
            buf: [0u8; N],
            len: LenT::default(),
        }
    }
}

impl<const N: usize, LenT: crate::bounded::BoundedLen<N>> fmt::Debug for PodBytes<N, LenT> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PodBytes").field(&self.as_slice()).finish()
    }
}

impl<const N: usize, LenT: crate::bounded::BoundedLen<N>> Clone for PodBytes<N, LenT> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<const N: usize, LenT: crate::bounded::BoundedLen<N>> Copy for PodBytes<N, LenT> {}

impl<const N: usize, LenT: crate::bounded::BoundedLen<N>> PartialEq for PodBytes<N, LenT> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.as_slice() == other.as_slice()
    }
}

impl<const N: usize, LenT: crate::bounded::BoundedLen<N>> Eq for PodBytes<N, LenT> {}

/// Errors from validated-tag [`FixedStr`] construction.
///
/// # `#[non_exhaustive]` (DEF-256, audit 2026-05-08)
///
/// New rejection classes may land as additional [`FixedStr`] tags
/// introduce new validation rules (e.g. UTF-8 normalisation
/// requirements, Unicode scalar restrictions). Sealing via
/// `non_exhaustive` forces downstream `match` callers to retain
/// a catch-all arm — closes the silent-misclassification audit
/// seam where a new variant could otherwise fall through a
/// downstream exhaustive match and lose its diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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

// DEF-244 modernisation audit (rust-version 1.81): additive
// `core::error::Error` impl on the public ident-validation error.
impl core::error::Error for IdentError {}

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

// ─── Concrete `const fn new` for default LenT (DEF-203 ext) ─────────
//
// Generic `Self::new` would need `LenT::ZERO` (trait associated const)
// in const context, which requires `const_trait_impl` (unstable).
// Workaround: provide a const fn `new()` ONLY for the default
// `BoundedU16<N>` LenT — `Self::new()` then resolves cleanly when
// caller relies on the default. Type aliases that pick `BoundedU8<N>`
// LenT (Ident, DatabaseName, etc.) use `Self::default()` (non-const,
// trait method) — none of those types are needed in `static` context
// in this crate.

impl<const N: usize, Tag> FixedStr<N, Tag, crate::bounded::BoundedU16<N>> {
    /// Empty value. Compile-time asserts `N ≤ 65_534` (BoundedU16 niche
    /// requirement) via const-block.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(N <= 65_534, "FixedStr with BoundedU16 LenT requires N ≤ 65_534");
        }
        Self {
            buf: [0u8; N],
            len: crate::bounded::BoundedU16::<N>::ZERO,
            was_lossy_flag: 0,
            _tag: PhantomData,
        }
    }
}

// ─── Generic methods over LenT (DEF-203 ext) ─────────────────────────

impl<const N: usize, Tag, LenT: crate::bounded::BoundedLen<N>> FixedStr<N, Tag, LenT> {
    /// Populated byte length.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.len.get_usize()
    }

    /// Whether no bytes have been stored.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len.get_usize() == 0
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
        // F-061 (pass-#8): debug-builds assert the invariant
        // `self.len ≤ N` so a constructor that violates the cap
        // fails tests loudly instead of the dead `unwrap_or(&[])`
        // masking it to an empty slice.
        debug_assert!(
            self.len() <= N,
            "FixedStr invariant: len ({}) must not exceed N ({N})",
            self.len(),
        );
        self.buf.get(..self.len()).unwrap_or(&[])
    }

}

/// `as_str` is available ONLY for tags that opt into [`ValidUtf8`] —
/// F3 tier-2 structural: a future tag type that doesn't guarantee
/// UTF-8 won't be able to call this method. All current crate tags
/// opt in because every constructor takes `&str` or produces ASCII
/// via `from_bytes_lossy`.
impl<const N: usize, Tag: ValidUtf8, LenT: crate::bounded::BoundedLen<N>> FixedStr<N, Tag, LenT> {
    /// Borrow the populated bytes as `&str`.
    ///
    /// **Validity:** every `ValidUtf8` tag's constructor guarantees
    /// the stored bytes are valid UTF-8. `core::str::from_utf8`
    /// runs an O(N) validation pass anyway because the crate's
    /// `#![forbid(unsafe_code)]` rules out `from_utf8_unchecked`.
    ///
    /// # `unwrap_or("")` classification (DEF-184 audit-2 item-2)
    ///
    /// The `.unwrap_or("")` fallback is NOT a tier-4 silent
    /// fallback — the logical error (non-UTF-8 bytes) is rejected
    /// at **construction** by every `ValidUtf8` tag's constructor
    /// (`try_from_str` / `try_from_bytes` etc.) which returns
    /// `Err` on invalid input. By the time `as_str` runs, the
    /// bytes are guaranteed-valid-UTF-8 as a consequence of the
    /// `ValidUtf8` trait-bound's contract.
    ///
    /// **Tier classification:**
    /// - Tier-1 compile of "bytes are UTF-8" requires
    ///   `unsafe { from_utf8_unchecked }` (banned).
    /// - Tier-2 structural via type-system: `ValidUtf8` trait is
    ///   sealed + constructor-validated — only valid bytes reach
    ///   the stored slot.
    /// - The runtime `from_utf8` re-check is the stable-Rust
    ///   price of forbidding `unsafe`. Its Err branch is a
    ///   **type-safe sink**: architecturally unreachable, zero
    ///   corruption vector (empty `&str` surfaces as visible
    ///   regression in any user-facing rendering rather than
    ///   masquerading as truncated data).
    /// - `debug_assert!` makes the invariant break LOUD in test
    ///   builds — a future constructor bug that forgets UTF-8
    ///   validation trips here immediately.
    ///
    /// **Alternatives rejected:**
    /// - `Result<&str, Infallible>` — would force every hot-path
    ///   caller through error-handling ceremony for an
    ///   architecturally-dead arm.
    /// - Store as `str` instead of `[u8; N]` — inflates POD size
    ///   (fixed-len UTF-8 needs `str` slice metadata) and breaks
    ///   the `FixedStr` zero-alloc Copy discipline.
    ///
    /// Net: empty `&str` on the dead arm is the minimum-overhead
    /// way to satisfy the forbid-bundle without introducing any
    /// silent-masquerade surface.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        let bytes = self.as_bytes();
        // F-062 (pass-#8): debug-builds assert the `ValidUtf8` tag
        // invariant — stored bytes must actually decode as UTF-8.
        // The dead `unwrap_or("")` branch below would mask a future
        // constructor that forgot to enforce UTF-8; this shield
        // fails tests loudly instead.
        debug_assert!(
            core::str::from_utf8(bytes).is_ok(),
            "ValidUtf8 tag invariant broken: stored bytes are not UTF-8",
        );
        core::str::from_utf8(bytes).unwrap_or("")
    }
}

impl<const N: usize, Tag, LenT: crate::bounded::BoundedLen<N>> Default for FixedStr<N, Tag, LenT> {
    #[inline]
    fn default() -> Self {
        // DEF-203 ext: generic Default uses LenT::default() (non-const,
        // trait method). Construct directly without calling Self::new
        // (which is per-concrete-LenT const fn).
        Self {
            buf: [0u8; N],
            len: LenT::default(),
            was_lossy_flag: 0,
            _tag: PhantomData,
        }
    }
}

// F3: Debug + Display are gated on `ValidUtf8` — both render via
// `as_str()` which requires UTF-8 validity. This is in practice
// unchanged from pre-F3 (every current tag is ValidUtf8), but
// future non-UTF-8 tags would need separate byte-based impls.
impl<const N: usize, Tag, LenT> fmt::Debug for FixedStr<N, Tag, LenT>
where
    Tag: FixedStrKind + ValidUtf8,
    LenT: crate::bounded::BoundedLen<N>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(\"{}\")", Tag::DEBUG_NAME, self.as_str())
    }
}

impl<const N: usize, Tag, LenT> fmt::Display for FixedStr<N, Tag, LenT>
where
    Tag: ValidUtf8,
    LenT: crate::bounded::BoundedLen<N>,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ───────────────── Validated-tag constructor (Ident / DatabaseName /
// ApplicationName) ────────────────

impl<const N: usize, Tag: Validated, LenT: crate::bounded::BoundedLen<N>> FixedStr<N, Tag, LenT> {
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
        // DEF-203 ext: tier-2 by-construct via BoundedLen::try_new_usize.
        // The earlier `bytes.len() > N` guard rejects oversize inputs, so
        // this Err arm is architecturally dead; classified to match the
        // original IdentError::TooLong signature for API stability.
        let len = LenT::try_new_usize(bytes.len()).ok_or(IdentError::TooLong {
            len: bytes.len(),
            max: N,
        })?;
        let mut out = Self::default();
        if let Some(dst) = out.buf.get_mut(..bytes.len()) {
            dst.copy_from_slice(bytes);
        }
        out.len = len;
        Ok(out)
    }
}

// ───────────────── Truncating-tag constructor (BoundedStr / Sql) ──────

impl<const N: usize, Tag: Truncating, LenT: crate::bounded::BoundedLen<N>> FixedStr<N, Tag, LenT> {
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
        let mut out = Self::default();
        let src = source.as_bytes();

        // Fast path: source fits verbatim.
        if src.len() <= N {
            if let Some(dst) = out.buf.get_mut(..src.len()) {
                dst.copy_from_slice(src);
            }
            // DEF-154 (T) P1-2: narrow via `narrow_len_u16` helper.
            // Invariants: `src.len() ≤ N` (gate above); `N ≤ u16::MAX`
            // (const-asserted at struct decl). Pre-(T) was
            // `.unwrap_or(0)` — silent "zero-length string" on
            // invariant break. Post-(T) Err-arm fallback is N (cap)
            // not 0, surfacing "full buffer" rather than silently
            // empty if both invariants somehow broke simultaneously.
            out.len = LenT::try_new_usize(src.len()).unwrap_or_default();
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
        // DEF-154 (T): narrow via helper; see `narrow_len_u16` docstring.
        out.len = LenT::try_new_usize(marker_end).unwrap_or_default();
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
        let mut out = Self::default();
        let budget = N.saturating_sub(Self::OVERFLOW_MARKER.len());
        let mut written = 0usize;
        // DEF-185 P2-D (audit 2026-04-24): track whether any lossy
        // coercion actually happened. Entering the slow path means
        // input had non-UTF-8 bytes SOMEWHERE, but individual byte-
        // level ASCII-acceptability may preserve much of the content
        // verbatim. `any_coerced` is true iff at least one byte was
        // replaced with `b'?'`.
        let mut any_coerced = false;
        for &b in source.iter() {
            if written >= budget {
                break;
            }
            // Accept ASCII printable + common whitespace; everything
            // else (non-ASCII, control chars, NUL) → `?`.
            let out_byte = if matches!(b, 0x20..=0x7e | b'\t' | b'\n' | b'\r') {
                b
            } else {
                any_coerced = true;
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
            // DEF-154 (T): see `narrow_len_u16` docstring.
            out.len = LenT::try_new_usize(marker_end).unwrap_or_default();
        } else {
            out.len = LenT::try_new_usize(written).unwrap_or_default();
        }
        // DEF-185 P2-D: surface the lossy flag.
        if any_coerced {
            out.was_lossy_flag = 1;
        }
        out
    }

    /// DEF-185 P2-D (audit 2026-04-24): `true` iff this value was
    /// constructed via [`Self::from_bytes_lossy`] AND at least one
    /// byte was coerced to `b'?'` (non-ASCII-printable, non-whitespace).
    ///
    /// Lets operators distinguish legitimate `?` characters in server
    /// text from our lossy coercion. Useful when investigating
    /// mis-encoded server error messages, proxy corruption, or
    /// client_encoding mismatch.
    ///
    /// Returns `false` for values constructed via any other path
    /// (`new`, `Default`, `from_str_truncating`, `try_from_*`).
    #[inline]
    #[must_use]
    pub const fn was_lossy(&self) -> bool {
        self.was_lossy_flag != 0
    }

    /// DEF-205 (2026-04-27): in-place zeroize hook for the
    /// `SecretBoundedStr<N>` Drop chain. Crate-private — `FixedStr` itself
    /// stays POD/Copy for non-sensitive uses (`Ident`, `StmtName`, `Sql`,
    /// non-secret `BoundedStr` fields). The wrapper-type
    /// `SecretBoundedStr<N>` calls this from its `Drop` to scrub bytes
    /// before the inner value is moved/overwritten.
    ///
    /// # Tier-1 enabler
    ///
    /// Without this hook, `SecretBoundedStr<N>` couldn't reach
    /// `FixedStr`'s private fields and would have to re-implement all
    /// constructors / accessors. With the hook, `SecretBoundedStr<N>`
    /// is a thin wrapper that delegates everything except Drop — the
    /// Drop fires the zeroize chain that closes the staleness leak
    /// class (see DEF-205 in `deferred.md`).
    ///
    /// # Why `pub(crate)` and not `pub`
    ///
    /// `FixedStr<N, Tag>` is `Copy`. Exposing `zeroize_in_place` as
    /// `pub` would invite callers to scrub Copy types — but Copy
    /// semantics mean a copy could remain unscrubbed (`let x = src; ...`
    /// makes a duplicate that the scrub on `src` doesn't reach). The
    /// scrub is sound only inside `SecretBoundedStr<N>`'s Drop, where
    /// the wrapper's non-Copy invariant guarantees no aliasing copies.
    #[inline]
    pub(crate) fn zeroize_in_place(&mut self) {
        use zeroize::Zeroize;
        self.buf.zeroize();
        self.len = LenT::default();
        self.was_lossy_flag = 0;
    }
}

#[cfg(test)]
mod drop_witness_tests {
    //! DEF-259 (2026-05-08): tier-1-by-construction Drop-fire witness
    //! for [`SecretBoundedStr<N>`] via [`crate::drop_witness::DropCounter`].
    //!
    //! Pre-DEF-259: `SecretBoundedStr<N>` Drop was verified by the
    //! `#[ignore]`-gated memory-probe test
    //! `tests/secret_bounded_str_spec.rs`. Run only via
    //! `cargo test -- --ignored` or `cargo miri test`.
    //!
    //! Post-DEF-259: this test runs deterministically on every
    //! `cargo test` invocation. The `DropCounter<SecretBoundedStr<N>>`
    //! wrapper observes that the manual `Drop` impl
    //! (`ident.rs:711`) reaches its body via the counter increment;
    //! the body calls `self.inner.zeroize_in_place()` which scrubs
    //! the underlying `BoundedStr<N>::buf` array.

    use super::SecretBoundedStr;
    use crate::drop_witness::{DropCounter, DropProbe};

    /// `SecretBoundedStr<N>::drop` fires its manual Drop body. The
    /// counter increments iff the body was reached.
    #[test]
    fn secret_bounded_str_drop_fires_zeroize_chain() {
        let probe = DropProbe::new();
        let s = SecretBoundedStr::<32>::from_str_truncating("witness-XYZ");
        DropCounter::scoped(s, probe.clone(), || {
            assert_eq!(probe.fired(), 0);
        });
        assert_eq!(
            probe.fired(),
            1,
            "SecretBoundedStr<32> drop must fire exactly once",
        );
    }

    /// Drop fires for every const-generic `N` instantiation we use
    /// in production: 32, 64, 96, 128.
    #[test]
    fn secret_bounded_str_drop_fires_for_each_used_capacity() {
        let probe = DropProbe::new();
        DropCounter::scoped(
            SecretBoundedStr::<32>::from_str_truncating("a"),
            probe.clone(),
            || {},
        );
        DropCounter::scoped(
            SecretBoundedStr::<64>::from_str_truncating("b"),
            probe.clone(),
            || {},
        );
        DropCounter::scoped(
            SecretBoundedStr::<96>::from_str_truncating("c"),
            probe.clone(),
            || {},
        );
        DropCounter::scoped(
            SecretBoundedStr::<128>::from_str_truncating("d"),
            probe.clone(),
            || {},
        );
        assert_eq!(
            probe.fired(),
            4,
            "every const-N capacity used in production (32/64/96/128) must fire Drop",
        );
    }

    /// Empty `SecretBoundedStr<N>` still drops with counter increment
    /// — pins that the manual Drop body runs unconditionally.
    #[test]
    fn empty_secret_bounded_str_drop_fires() {
        let probe = DropProbe::new();
        DropCounter::scoped(SecretBoundedStr::<32>::new(), probe.clone(), || {});
        assert_eq!(
            probe.fired(),
            1,
            "empty SecretBoundedStr drop must still fire",
        );
    }
}
