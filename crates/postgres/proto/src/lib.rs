//! PostgreSQL wire-protocol primitives — pure sync, `no_std`, no allocator.
//!
//! `bsql-pg-proto` is the **sans-I/O** core of bsql's PostgreSQL backend.
//! It contains zero I/O, zero async runtime. The same engine drives the
//! production async wrapper (`bsql-postgres-async`), the blocking wrapper
//! (`bsql-postgres-sync`), and test harnesses that feed bytes directly.
//!
//! Architectural promises (CREDO §0):
//!
//! - **Cancellation-safety by construction.** A dropped user future cannot
//!   leave the wire dirty, because the wire-state lives in the engine,
//!   separate from the user-visible future.
//! - **No panics.** The forbid-bundle below rejects every panic-able
//!   expression at compile time; bounded buffers replace `Vec` / `String`;
//!   `checked_*` arithmetic everywhere.
//! - **No data races.** `#![forbid(unsafe_code)]` plus the borrow checker.
//!
//! # Module layout
//!
//! - [`engine`] — the sans-I/O session engine: [`engine::Engine`],
//!   [`engine::Live`], the verbs, [`engine::Transport`], [`engine::Surface`].
//! - [`frame`] — pure-function frame-header parser. Never panics on
//!   arbitrary bytes — tier-1 by forbid-bundle + slice patterns +
//!   checked arithmetic.
//! - [`wire`] — protocol byte constants + the precomputed wire literals.
//! - [`decode`] — column / row decoding from wire bytes.
//! - [`scram`] / [`md5`] / [`password`] / [`sensitive`] — authentication +
//!   secret-bearing types.
//! - [`ident`] / [`write_buf`] / [`params`] / [`command_tag`] — bounded wire
//!   builders + typed command/tag parsing.
//! - [`mod@prepared`] — runtime support for the compile-checked `query!` macro.

#![no_std]
#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::mem_forget,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::integer_division,
    // Even with `as` cast banned, infallible `From`/`try_from` can
    // be subtly wrong if a narrowing happens at the type level
    // (e.g. `i32 → u32` sign loss, `u64 → usize` on 32-bit targets).
    // Tier-1 compile guard catches these.
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    // `clippy::let_underscore_must_use` catches `let _ = fn()`
    // where the result carries a `#[must_use]` contract. The
    // sibling `let_underscore_drop` was renamed/moved to a rustc
    // lint — see the `#![deny(let_underscore_drop)]` line below.
    // Tier-1 closes the silent-discard class at build time.
    clippy::let_underscore_must_use
)]
// Rustc-namespace `let_underscore_drop` moved out of `clippy::*`
// after Rust 1.69. Catches the explicit `let _ = drop_chain_value`
// form where the value's `Drop::drop` still fires (so it is NOT a
// "leak" of secrets — `ZeroizeOnDrop` chains still run) but the
// immediate discard is structurally suspicious. Distinct from
// `unused_must_use` (which fires on the call expression, not the
// let-pattern).
#![deny(let_underscore_drop)]
#![deny(
    unused_must_use,
    unused_lifetimes,
    unused_variables,
    missing_docs,
    rust_2024_incompatible_pat
)]
#![warn(missing_debug_implementations, missing_copy_implementations)]
// Opt out of the workspace tier-4 `disallowed_methods` ledger. This
// crate enforces a STRICTER, whole-crate forbid-bundle (above: it forbids
// the entire unwrap/expect/panic/as/arithmetic class) and centralises the
// `try_from(..).unwrap_or(SATURATION)` / `slice.get(..).unwrap_or(&[])`
// dead-arm shape behind the single-audit-point `narrow` module. In this
// crate the `.unwrap_or*` shape is the sanctioned forbid-bundle-compliant
// dead arm (the saturation is loud-fail-closed, the `&[]` arm is provably
// dead under the preceding bounds check), not a silent data fallback — so
// the per-site ledger that fits the driver crates would be noise here.
#![allow(
    clippy::disallowed_methods,
    reason = "stricter whole-crate forbid-bundle + single-audit-point narrowing; .unwrap_or* is the sanctioned dead-arm shape, not a silent data fallback"
)]

// bsql's `wire_pin!` / `footprint_pin!` guards assert exact `size_of` / `align_of`
// values computed for 64-bit pointers; on a non-64-bit target they fail as a wall
// of confusing `E0080` "WIRE FOOTPRINT DRIFT" panics. This one honest line replaces
// that wall. 64-bit is the only supported width (i686 / wasm32 / 32-bit ARM are
// unrequested and unsupported); 64-bit builds are unaffected.
#[cfg(not(target_pointer_width = "64"))]
compile_error!("bsql requires a 64-bit target; the footprint pins assume 64-bit pointers");

// `bsql-pg-proto` is `no_std + alloc`. The engine and the connecting-phase
// state box large secret-bearing handshake payloads (SCRAM / MD5 / cleartext
// password material) to keep the state enum compact; embedded targets
// without an allocator should use Trust-auth (no Box allocated).
extern crate alloc;

// **Transitive-`unsafe` audit-trust chain**.
//
// `bsql-pg-proto` itself uses `#![forbid(unsafe_code)]` (above). Every line
// of crate-internal Rust is `unsafe`-free by build-time rejection — the
// crate's own surface contributes ZERO unsafe boundaries. The runtime
// dependencies that contain `unsafe` blocks, ranked by audit-trust risk:
//
// 1. `simdutf8` — SIMD-accelerated UTF-8 validation, used by the text-cell
//    decode path (decode.rs). Audit-trust class: **ecosystem-tested**
//    (1M+ downloads/month). Failure mode: misclassified text →
//    `DecodeError::NonUtf8` (classified), never UB on attacker bytes.
//
// 2. `heapless` — bounded-capacity inline `Vec`/`String`. Used by the
//    engine send/ingest buffers, the SCRAM wire builders, the `ident`
//    `FixedStr`, and `write_buf`. Audit-trust class: **ecosystem-tested**
//    (embedded-Rust standard). Scope of trust: a bounded-cap `Vec` never
//    writes past its declared `N`; we never construct one from raw pointers.
//
// 3. RustCrypto: `sha2` + `md-5` + `hmac` + `pbkdf2` — SCRAM-SHA-256 / MD5
//    primitives. Audit-trust class: **expert-domain crypto** (CREDO §11 —
//    never hand-rolled). Behaviour parity per NIST FIPS / RFC.
//
// 4. `base64ct` — constant-time base64 for the SCRAM proof channel.
//
// 5. `subtle` — constant-time comparison/select for SCRAM verification.
//
// 6. `zeroize` — secret-bearing-type drop scrub. The `unsafe` is a single
//    `compiler_fence` to prevent dead-store elimination.
//
// **Audit-trust posture**: every transitive `unsafe` source is either
// ecosystem-standard (1M+ downloads), expert-domain (crypto), or
// deliberately-tiny (zeroize fence). Replacement with hand-rolled
// equivalents would CREATE a new `unsafe` audit boundary inside the crate
// — net worse per CREDO §11. Per-PR requirement: when bumping any of these
// deps, audit the changelog for `unsafe` boundary changes.

#[cfg(test)]
extern crate std;

pub mod action;
pub mod bounded;
pub mod decode;
// The sans-I/O session engine — `Engine`, `Live`, the verbs, `Transport`,
// `Surface`, `Outcome`, `EngineError`. The whole connect + active flow.
pub mod engine;
pub mod error;
pub mod frame;
pub mod ident;
// MD5-password authentication (`AuthenticationMD5Password`). Behind the
// default-on `md5-auth` feature: with it OFF the `md-5` crate (and its private
// crypto stack) leaves the runtime graph, `Credentials::Md5Password` cannot be
// built, and an MD5-demanding server is answered with
// `ConnFail::UnsupportedAuthMethod` (fail-loud) by the always-present dispatch.
#[cfg(feature = "md5-auth")]
pub(crate) mod md5;
// Typed numeric narrowing/widening helpers with single-audit-point
// encapsulation of the `try_from(...).unwrap_or(SATURATION)` pattern.
// Crate-internal module; no public re-exports.
pub(crate) mod narrow;
pub mod params;
pub mod password;
// Dep-free bsql-native semantic types (`Uuid`, `Timestamptz`, `Timestamp`,
// `Json`, `Jsonb`) the compile-checked `query!` path decodes without any
// external crate. Their `Cell`/`EncodeBinary` impls live in `decode.rs` and
// their `ColCellAt` row-tuple markers in `prepared.rs`, beside their peers.
pub mod pgtypes;
// Compile-time budgets for the `query!` macro's dynamic forms (toggled
// filters / runtime ORDER BY orderings). The generated code asserts
// against these; an over-budget query is `error[E0080]`.
pub mod query_budget;
// Runtime support for the compile-checked `query!` macro. Hosts
// `PreparedQuery<P, R>`, the `RowDecode` sealed trait, and the
// `new_prepared_query` validating constructor the macro routes through.
pub mod prepared;

// Typed `CommandTag` enum + wire parser for the `CommandComplete` ('C')
// frame the active engine reads.
pub mod command_tag;

// Typed binary `COPY … FROM STDIN`: the PGCOPY binary file header/trailer
// constants + the `TypedCopyIn` carrier trait. The `copy!` macro emits a
// `TypedCopyIn` impl in the consumer crate; the drivers' `copy_in_typed` verb
// streams each typed row through the shared `ParamsWriter` encoders.
pub mod copy_binary;

// SCRAM-SHA-256 authentication (RFC 5802 / 7677), composed over RustCrypto.
// Behind the default-on `scram` feature: with it off the SCRAM-exclusive crypto
// crates leave the build, `Credentials::ScramPassword` and the connecting-state
// SCRAM variants do not exist, and a Trust connection is the only password-free
// path (a password with SCRAM off fails loud at the driver).
#[cfg(feature = "scram")]
pub mod scram;
// Test-only `DropCounter` machinery + sealed `CrateZeroizeSecret`
// manifest. The exhaustiveness gate fails build-time if the
// manifest drifts from src; per-type DropCounter witnesses run on
// every `cargo test`. Module is `#[cfg(test)]`-only.
#[cfg(test)]
pub(crate) mod drop_witness;
// Shared test-fixture narrowing helpers — loud-fail `usize → i16/i32`
// conversion for hand-built wire-frame fixtures across the crate's
// test modules. Test-only; zero production surface.
#[cfg(test)]
pub(crate) mod test_fixtures;
pub mod sensitive;
pub mod startup;
pub mod state;
// The bridge trait from a compile-checked `query!` carrier to its prepared
// query + typed-record decoders. Consumed by the drivers' typed `query`
// method; implemented by the `query!` macro in the consumer crate.
pub mod typed_query;
pub mod wire;
pub mod write_buf;

pub use action::TxStatus;
pub use bounded::{BoundedLen, BoundedU8, BoundedU16};
pub use decode::{
    BinaryFmt, Cell, ColumnDesc, ColumnsIter, CompositeReader, CopyFormat, CopyHeader, DataRowRef,
    DecodeError, EnumLabel, Fmt, FormatCode, MAX_ROW_COLUMNS, PgComposite, PgEnum,
    RowDesc, RowDescColumnsIter, TextFmt, decode_with_format, oids,
    parse_long_uint_swar, parse_pg_bool_swar, parse_short_uint_swar, validate_utf8_swar,
};
pub use pgtypes::{
    Date, DateParseError, Interval, Json, Jsonb, Numeric, NumericParseError, Time, TimeParseError,
    Timestamp, Timestamptz, Uuid, UuidParseError,
};
pub use error::{ErrorKind, ProtocolError, StateErrorKind};
pub use frame::{HeaderParse, MAX_FRAME_LEN_FIELD, READ_BUF_CAP, parse_header};
pub use ident::{
    ApplicationName, DatabaseName, GucName, GucValue, Ident, IdentError, LossyDisplay, LossyText,
    PortalName, SecretBoundedStr, Sql, StmtName,
};
pub use startup::{StartupParam, StartupParamError, RESERVED_NAMES};
pub use password::{Credentials, Password, PasswordError};
pub use sensitive::Sensitive;
pub use prepared::{ColCellAt, PreparedQuery, QueryFingerprint, RowDecode, oids_equal};
pub use typed_query::TypedQuery;
// Typed binary COPY: the carrier trait + the PGCOPY binary header/trailer the
// drivers' `copy_in_typed` streams around the per-row frames.
pub use copy_binary::{PGCOPY_BINARY_HEADER, PGCOPY_BINARY_TRAILER, TypedCopyIn};
// The sealed public bound on the runtime-parameterised verbs
// (`query_params` / `execute_params` / `query_prepared` / …). Re-exported at
// the crate root — alongside its sibling sealed traits `RowDecode` /
// `TypedQuery` — so a downstream facade can surface it under one umbrella name
// and a consumer can name the bound (`fn insert<P: ParamsWriter>(…)`) without a
// direct dependency on this crate. Name-only: sealed via `ParamsWriterSealed`,
// so re-export cannot widen the impl set.
pub use params::ParamsWriter;
// The connecting-phase engine state enum.
pub use state::ConnectingState;
// Top-level re-export of the user-facing `Terminate` wire literal.
// Drivers write these bytes immediately before TCP close to signal
// graceful shutdown.
pub use wire::TERMINATE_WIRE_BYTES;
// Top-level re-export of the user-facing `Flush` wire literal.
pub use wire::FLUSH_WIRE_BYTES;
// Top-level re-export of the user-facing `SSLRequest` wire literal.
// Wrapper drivers write these bytes BEFORE the engine handshake to
// negotiate TLS; the 1-byte server response is OOB.
pub use wire::SSL_REQUEST_WIRE_BYTES;
// Typed classification of the 1-byte SSL response. Pairs with
// SSL_REQUEST_WIRE_BYTES.
pub use wire::{SslNegotiationOutcome, classify_ssl_response_byte};
// Top-level re-export of the user-facing `CancelRequest` builder.
// Drivers call `cancel_request_bytes(pid, secret_key)` to materialise the
// 16-byte cancel packet, open a parallel TCP connection, write it, close.
pub use wire::cancel_request_bytes;
pub use write_buf::{FrameSink, MAX_OWNED_SEND_LEN, WriteBuf, WriteBufFull};

// ---------------------------------------------------------------------
// Tier-1 compile gates on Send — every type that crosses a task
// boundary in the wrappers must be `Send`. A future refactor that
// introduces a non-Send field (`Rc<T>`, raw pointer, `MutexGuard`) into
// any of these becomes a build error here rather than a silent
// regression downstream.
// ---------------------------------------------------------------------
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<action::TxStatus>();
    assert_send::<error::ProtocolError>();
    assert_send::<state::ConnectingState>();
    // Bounded string types.
    assert_send::<ident::Ident>();
    assert_send::<ident::DatabaseName>();
    assert_send::<ident::ApplicationName>();
    assert_send::<password::Password>();
    assert_send::<password::Credentials>();
    assert_send::<write_buf::WriteBuf>();
    #[cfg(feature = "scram")]
    assert_send::<scram::types::SecretDigest>();
    #[cfg(feature = "scram")]
    assert_send::<scram::types::CappedServerNonce>();
    // Typestate wrappers.
    #[cfg(feature = "scram")]
    assert_send::<scram::session::ScramSession>();
    assert_send::<sensitive::Sensitive<password::Password>>();
    // Error sentinels — small Copy-like types that must stay Send so
    // that Result<T, E> returned across a task boundary compiles.
    assert_send::<write_buf::WriteBufFull>();
    #[cfg(feature = "scram")]
    assert_send::<scram::types::ServerNonceTooLong>();
    assert_send::<ident::IdentError>();
    assert_send::<password::PasswordError>();
    assert_send::<frame::HeaderParse>();
};

/// Pin the **footprint** (`size_of` AND `align_of`) of a wire type at
/// build time — a layout drift becomes an `E0080` const-eval failure,
/// not a silent regression and not a `cargo test`-only catch.
///
/// # Why both dimensions
///
/// A type can keep its byte count while its alignment changes (e.g. a
/// field reorder that raises `align` from 4 to 8 without growing `size`,
/// or a niche being lost). `align_of` is the dimension a size-only anchor
/// misses. This macro pins both in one place so the two cannot drift apart.
///
/// # Tier
///
/// **Tier-1, build-time (CREDO §0).** The emitted item is a free-standing
/// `const _: () = { … }` — it is **not** behind any `#[cfg(test)]`, so the
/// compiler evaluates it during `cargo check` / `cargo build`, **including
/// for types that are constructed nowhere in the binary**. Runtime cost is
/// **zero**: the `const _` item is fully asm-erased.
///
/// # Lifetime-generic wire types
///
/// `size_of` / `align_of` require a concrete type. For a wire type with a
/// borrow parameter, pin a `'static` instantiation:
///
/// ```
/// # use bsql_postgres_proto::wire_pin;
/// # struct Frame<'a> { body: &'a [u8] }
/// wire_pin!(Frame<'static>, size = 16, align = 8);
/// ```
///
/// # Drift is a build failure
///
/// A wrong pin (or a layout change that invalidates a correct pin) aborts
/// the build with `E0080`:
///
/// ```compile_fail,E0080
/// use bsql_postgres_proto::wire_pin;
/// #[repr(C)]
/// struct Drifted { a: u32, b: u32, c: u32 } // 12 B actual
/// wire_pin!(Drifted, size = 8, align = 4);   // pinned 8 → E0080
/// fn main() {}
/// ```
///
/// …and a size-preserving *alignment* drift, which a size-only anchor
/// cannot see, also aborts:
///
/// ```compile_fail,E0080
/// use bsql_postgres_proto::wire_pin;
/// #[repr(C, align(8))]
/// struct AlignDrifted { a: u32, b: u32 } // 8 B, align 8
/// wire_pin!(AlignDrifted, size = 8, align = 4); // pinned align 4 → E0080
/// fn main() {}
/// ```
#[macro_export]
macro_rules! wire_pin {
    ($t:ty, size = $n:expr, align = $a:expr $(,)?) => {
        // The pinned `size`/`align` are computed for 64-bit pointers, so the assert
        // is scoped to 64-bit targets — the only width bsql supports. On any other
        // width the crate-root `compile_error!` (which forbids non-64-bit) is the
        // single honest diagnostic, not a wall of misleading per-pin drift panics.
        #[cfg(target_pointer_width = "64")]
        const _: () = {
            assert!(
                core::mem::size_of::<$t>() == $n,
                concat!("WIRE FOOTPRINT DRIFT (size) for ", stringify!($t))
            );
            assert!(
                core::mem::align_of::<$t>() == $a,
                concat!("WIRE FOOTPRINT DRIFT (align) for ", stringify!($t))
            );
        };
    };
}

// Target architecture support bound: the crate uses `u32` body counters
// and assumes `usize::BITS >= 32` for infallible `u32 → usize` widening.
// 16-bit targets are unsupported.
const _: () = assert!(
    usize::BITS >= 32,
    "bsql-pg-proto requires a target with usize >= 32 bits. \
     16-bit targets are unsupported; the wire-protocol body counters \
     are u32 and several call sites infallibly widen u32 → usize.",
);

// `PreparedQuery<P, R>` is a struct of 6 × `&'static`-fat-pointers
// + `PhantomData<fn(P) -> R>` = 6 × 16 B + 0 = 96 B. The pin's upper
// bound is 128 B with cushion for alignment / cross-target portability.
const _: () = assert!(
    core::mem::size_of::<prepared::PreparedQuery<(i32,), (i32, &'static str)>>() <= 128,
    "PreparedQuery<(i32,), (i32, &'static str)> must stay ≤ 128 B \
     (6 × 16 B fat pointers + PhantomData = 96 B + padding cushion). \
     Larger sizes regress consumer crate .rodata footprint and \
     LLVM whole-crate codegen heuristics.",
);

// Alignment pins for the size-pinned engine-facing types. 64-bit-scoped (the
// pinned align of 8 is a 64-bit-pointer figure) — on any other width the crate-root
// `compile_error!` is the single honest diagnostic.
#[cfg(target_pointer_width = "64")]
const _: () = {
    use core::mem::align_of;
    assert!(
        align_of::<prepared::PreparedQuery<(i32,), (i32, &'static str)>>() == 8,
        "PreparedQuery align drift",
    );
};

// ---------------------------------------------------------------------
// Tier-1 compile gates on Drop semantics.
//
// `core::mem::needs_drop::<T>()` is a const fn that returns true iff T
// (or any of its fields transitively) has a non-trivial Drop impl.
//
// - Types that carry secrets MUST have Drop (for zeroize-on-drop).
// - Small value types SHOULD NOT have Drop (Copy-able / move-friendly).
//
// A regression that removed Zeroize impls from Password would fail the
// build here. Zero runtime cost.
// ---------------------------------------------------------------------
const _: () = assert!(
    core::mem::needs_drop::<password::Password>(),
    "Password must have Drop for zeroize-on-drop (secret scrub)",
);
#[cfg(feature = "scram")]
const _: () = assert!(
    core::mem::needs_drop::<scram::types::SecretDigest>(),
    "SecretDigest must have Drop for zeroize-on-drop",
);
#[cfg(feature = "scram")]
const _: () = assert!(
    core::mem::needs_drop::<scram::session::ScramSession>(),
    "ScramSession owns Sensitive<Password> — must Drop so the inner zeroize fires",
);
const _: () = assert!(
    core::mem::needs_drop::<sensitive::Sensitive<password::Password>>(),
    "Sensitive<Password> must Drop to trigger ZeroizeOnDrop on the inner",
);
const _: () = assert!(
    !core::mem::needs_drop::<frame::HeaderParse>(),
    "HeaderParse must stay drop-free — pure value type",
);
const _: () = assert!(
    !core::mem::needs_drop::<ident::IdentError>(),
    "IdentError must stay drop-free — enum of Copy variants",
);
const _: () = assert!(
    !core::mem::needs_drop::<password::PasswordError>(),
    "PasswordError must stay drop-free — enum of Copy variants",
);
const _: () = assert!(
    !core::mem::needs_drop::<write_buf::WriteBufFull>(),
    "WriteBufFull must stay drop-free — error sentinel",
);
#[cfg(feature = "scram")]
const _: () = assert!(
    !core::mem::needs_drop::<scram::types::ServerNonceTooLong>(),
    "ServerNonceTooLong must stay drop-free — error sentinel",
);
const _: () = assert!(
    !core::mem::needs_drop::<error::ProtocolError>(),
    "ProtocolError must stay drop-free — all variants' fields are Copy (POD BoundedStr)",
);
