//! Sealed `ParamsWriter` trait — zero-alloc serialisation of PG
//! Bind-frame parameter blocks from Rust tuples.
//!
//! # Phase 1c-3b
//!
//! Extended Query (PG §55.2.2) sends a `Bind` frame containing:
//! 1. Portal name (NUL-terminated)
//! 2. Statement name (NUL-terminated)
//! 3. `n_param_formats: i16` + array of format codes
//! 4. `n_params: i16` + array of `{len: i32, bytes: [u8; len]}` entries
//! 5. `n_result_formats: i16` + array of format codes
//!
//! The user supplies parameters as a tuple, e.g., `(42i32, "hi", true)`.
//! `ParamsWriter` threads that tuple through the wire format WITHOUT
//! any intermediate buffer — each element's [`crate::decode::EncodeBinary`]
//! impl writes directly into the caller's [`crate::write_buf::WriteBuf`].
//!
//! # Tier invariants
//!
//! - **Tier-1 compile**: the `COUNT` const is derived from tuple arity
//!   via the generating macro. A mismatch between the declared COUNT
//!   and the actual number of elements the impl writes is a build
//!   failure (the macro emits `const _: () = assert!(N == COUNT)`
//!   per impl).
//! - **Tier-1 compile**: `FORMATS.len() == COUNT`, `OIDS.len() == COUNT`.
//!   Built into the macro expansion.
//! - **Tier-1 compile**: sealed via [`sealed::ParamsWriterSealed`].
//!   Downstream crates cannot impl `ParamsWriter` for their own types.
//! - **Tier-2 structural**: all `write_params` impls return
//!   `Result<(), WriteBufFull>` — buffer overflow is classified,
//!   not silent.
//! - **Zero alloc**: no heap, no stack fixture buffer. Direct stream
//!   into the output `WriteBuf`.
//!
//! # Scope (1c-3b) — why arity 16
//!
//! Tuple arity 0..=16 covered. Each arity monomorphises into a
//! distinct machine-code body (~30 LOC post-codegen); 16 arities
//! × ~30 LOC ≈ 480 LOC of generated code per target build.
//! `[ParamEncoder; 16]` inline array fits a single AVX2 register
//! for the `FORMATS_WIRE` const (DEF-135, 1c-5 planned) — breaking
//! the ≤64-byte bound would force a branch-on-length eq path and
//! lose branch-free compare. The 16-cap also matches `ParamOids`'s
//! MAX_PARAMS_ARITY (action.rs) so the describe-reply shape
//! mirrors the bind-send shape.
//!
//! Tradeoff: callers wanting > 16 parameters must refactor into
//! smaller statements. Cross-database universality (MySQL /
//! MariaDB / SQLite) inherits this cap via `bsql-macros` Phase 2 —
//! all three share the "few placeholders per query" norm in
//! idiomatic usage.
//!
//! If I-cache measurement (DEF-143, deferred) shows per-arity
//! monomorphisation bloating hot paths, the HList-recursion path
//! documented in deferred.md §21 F-068 becomes the drop-in
//! replacement.
//!
//! Every element type must implement [`ParamEncoder`] — a sealed
//! intermediate trait that adds SQL-NULL handling on top of
//! [`crate::decode::EncodeBinary`]:
//!
//! - `T: EncodeBinary` gets a blanket `impl ParamEncoder for T` that
//!   writes the non-NULL path (4-byte length prefix + body).
//! - `Option<T: EncodeBinary>` gets a dedicated impl that writes
//!   `-1` + no body for `None` (SQL NULL) or defers to the blanket
//!   for `Some(v)`.
//!
//! Result: `push_bind_execute(&(user_id, maybe_email))` where
//! `maybe_email: Option<&str>` encodes `user_id` as non-NULL and
//! `maybe_email` as NULL-or-bytes with zero user intervention.

use crate::decode::{EncodeBinary, FormatCode};
use crate::write_buf::{WriteBuf, WriteBufFull};

/// Module-private seal. Only the tuple impls inside this crate can
/// opt a type into [`ParamsWriter`], and only `ParamEncoder` impls
/// in THIS module can satisfy the per-element trait.
mod sealed {
    /// Supertrait seal for [`super::ParamsWriter`]. Module-private
    /// so external crates cannot impl it, closing downstream
    /// "custom tuple-like types" holes (DEF-115-class seal).
    pub trait ParamsWriterSealed {}

    /// Supertrait seal for [`super::ParamEncoder`]. Gates which
    /// Rust types can appear as PG Bind-frame parameters. Module-
    /// private so no downstream crate can introduce a custom
    /// `ParamEncoder` that bypasses the NULL/non-NULL discipline.
    pub trait ParamEncoderSealed {}
}

/// Per-element parameter encoder — writes the PG Bind-frame's
/// `{len_i32, [u8; len]}` shape for ONE parameter value, handling
/// the SQL NULL case (`len = -1`, no body) if the element is an
/// [`Option`].
///
/// # Sealed
///
/// Module-private seal via [`sealed::ParamEncoderSealed`] — only
/// two impls exist:
/// - `impl<T: EncodeBinary> ParamEncoder for T` — the non-NULL
///   path, writes `len` + bytes via
///   [`WriteBuf::with_i32_length_prefixed_body`].
/// - `impl<T: EncodeBinary> ParamEncoder for Option<T>` — the
///   NULL-aware path.
///
/// These two impls don't overlap at trait-resolution time because
/// `Option<T>` doesn't impl [`EncodeBinary`] — the blanket doesn't
/// apply to it, so the dedicated `Option` impl is the only
/// candidate.
pub trait ParamEncoder: sealed::ParamEncoderSealed {
    /// PG type OID this encoder targets. For `T: EncodeBinary`
    /// this equals `T::OID`; for `Option<T>` it equals `T::OID`
    /// (the SQL NULL's type is the inner value's type).
    const OID: u32;

    /// Write exactly one param's `{len_i32, [u8; len]}` (non-NULL)
    /// or `{-1}` (NULL) pair into `dst`.
    fn write_param(&self, dst: &mut WriteBuf) -> Result<(), WriteBufFull>;
}

// Blanket impl for all non-Option EncodeBinary types.
impl<T: EncodeBinary> sealed::ParamEncoderSealed for T {}
impl<T: EncodeBinary> ParamEncoder for T {
    const OID: u32 = <T as EncodeBinary>::OID;
    #[inline]
    fn write_param(&self, dst: &mut WriteBuf) -> Result<(), WriteBufFull> {
        dst.with_i32_length_prefixed_body(|w| self.encode_to(w))
    }
}

// Dedicated impl for Option<T> — None → SQL NULL wire form.
// Does NOT conflict with the blanket because Option<T> doesn't
// impl EncodeBinary (intentionally — the NULL concept belongs in
// the param layer, not the byte encoder).
impl<T: EncodeBinary> sealed::ParamEncoderSealed for Option<T> {}
impl<T: EncodeBinary> ParamEncoder for Option<T> {
    const OID: u32 = <T as EncodeBinary>::OID;
    #[inline]
    fn write_param(&self, dst: &mut WriteBuf) -> Result<(), WriteBufFull> {
        match self {
            Some(value) => dst.with_i32_length_prefixed_body(|w| value.encode_to(w)),
            // SQL NULL wire form: length = -1, no body bytes follow.
            None => dst.push_i32_be(-1),
        }
    }
}

/// Upper bound on tuple arity supported by [`ParamsWriter`] impls
/// in this module. Referenced by [`crate::write_buf::max_bind_message_size`]
/// for the worst-case Bind-frame size computation — changing this
/// const without updating the tuple-impl invocation list would
/// silently break the budget.
pub const MAX_PARAMS_ARITY: usize = 16;

/// Upper bound on total parameter-data bytes across all 16 params
/// in a single Bind frame. Per-param individual size isn't capped
/// structurally — the caller can send one 1 KB text param OR 16 ×
/// 64-byte params, provided the SUM stays under this limit.
///
/// Enforcement is runtime (via [`crate::write_buf::WriteBufFull`]
/// when the encoded bytes exceed the buffer's remaining capacity),
/// classified as tier-2 structural. The const is consulted by
/// [`crate::write_buf::max_bind_message_size`] to size the worst
/// case against [`crate::write_buf::MAX_OWNED_SEND_LEN`] at build
/// time (tier-1 compile).
pub const MAX_PARAMS_DATA_TOTAL: usize = 1024;

/// Serialise a tuple of PG parameter values into a Bind frame.
///
/// See module-level docs for the full wire-format contract and tier
/// analysis. `ParamsWriter` is sealed; the impls in this module
/// cover tuple arity `0..=16`.
pub trait ParamsWriter: sealed::ParamsWriterSealed {
    /// Number of parameters this tuple encodes. Compile-time
    /// drift-pinned against tuple arity via the generating macro.
    const COUNT: u16;

    /// Per-parameter wire format codes. Always `[FormatCode::Binary; COUNT]`
    /// in 1c-3b — text-format params aren't supported on the write
    /// path because [`EncodeBinary`] is the only sealed encoder
    /// available.
    const FORMATS: &'static [FormatCode];

    /// Per-parameter PG type OIDs, derived from each element's
    /// [`EncodeBinary::OID`]. Exposed as a static slice so Phase 2's
    /// `query!` macro can cross-check the tuple's OID sequence
    /// against the server's `ParameterDescription` response at
    /// compile time — a tier-1 shield against "wrong param types
    /// sent to server" that currently relies on server-side
    /// validation (tier-3).
    const OIDS: &'static [u32];

    /// Write the `n_params` entries of the Bind frame's parameter
    /// block — a sequence of `{len: i32, bytes: [u8; len]}` pairs.
    /// The caller has already emitted everything up to and including
    /// the `n_params` count; this method writes only the parameter
    /// values themselves.
    ///
    /// # Errors
    ///
    /// [`WriteBufFull`] if any element overflows the buffer. The
    /// Bind-frame const-assert pins the worst-case size against
    /// `MAX_OWNED_SEND_LEN`; in production the error branch is
    /// architecturally dead but surfaces as a classified error
    /// rather than a panic.
    fn write_params(&self, dst: &mut WriteBuf) -> Result<(), WriteBufFull>;
}

// ─────────────────────── Tuple impls via macro ───────────────────
//
// For each arity N we generate:
//   - impl sealed::ParamsWriterSealed for (T1, ..., TN)
//   - impl<T1: EncodeBinary, ...> ParamsWriter for (T1, ..., TN)
//     with COUNT=N, FORMATS=[Binary; N], OIDS=[T1::OID, ..., TN::OID]
//   - per-element write using with_i32_length_prefixed_body
//
// Every impl is const-asserted: COUNT == FORMATS.len() == OIDS.len().

macro_rules! params_writer_impl {
    // Zero-arity special case — the unit tuple. No type params, no
    // per-element writes, empty FORMATS/OIDS slices.
    () => {
        impl sealed::ParamsWriterSealed for () {}
        impl ParamsWriter for () {
            const COUNT: u16 = 0;
            const FORMATS: &'static [FormatCode] = &[];
            const OIDS: &'static [u32] = &[];
            #[inline]
            fn write_params(&self, _dst: &mut WriteBuf) -> Result<(), WriteBufFull> {
                Ok(())
            }
        }
        // Coherence drift-pin: COUNT ≡ FORMATS.len() ≡ OIDS.len().
        const _: () = {
            assert!(<() as ParamsWriter>::COUNT == 0);
            assert!(<() as ParamsWriter>::FORMATS.len() == 0);
            assert!(<() as ParamsWriter>::OIDS.len() == 0);
        };
    };

    // N-arity case — generate impl for (T1, ..., TN).
    //
    // `$count` is the compile-time literal, `$($idx:tt)+` is the
    // sequence of tuple field indices (0, 1, ..., N-1), `$($t:ident)+`
    // is the sequence of type parameters (A, B, ..., Z-style).
    ($count:literal, [$($t:ident : $idx:tt),+ $(,)?]) => {
        impl<$($t: ParamEncoder),+> sealed::ParamsWriterSealed for ($($t,)+) {}

        impl<$($t: ParamEncoder),+> ParamsWriter for ($($t,)+) {
            const COUNT: u16 = $count;
            // Every param ships Binary format. Token-muncher macro
            // `emit_binary_per_token!($t)` consumes the `$t` ident
            // and produces the literal `FormatCode::Binary` — this
            // pattern avoids `let _ = <$t>::OID;` (banned by the
            // crate's `never let _` rule) while keeping the macro
            // repetition tied to the type-param list.
            const FORMATS: &'static [FormatCode] = &[$(emit_binary_per_token!($t)),+];
            const OIDS: &'static [u32] = &[$(<$t as ParamEncoder>::OID),+];

            #[inline]
            fn write_params(&self, dst: &mut WriteBuf) -> Result<(), WriteBufFull> {
                $(
                    // Each param goes through `ParamEncoder::write_param`
                    // which handles len-prefix + body (non-NULL) or
                    // len=-1 (SQL NULL for Option<T>).
                    self.$idx.write_param(dst)?;
                )+
                Ok(())
            }
        }

        // Per-arity coherence drift-pin: `COUNT == $count`.
        //
        // `FORMATS.len() == COUNT` and `OIDS.len() == COUNT` are
        // enforced STRUCTURALLY by the macro — the number of `$t`
        // repetitions drives both the arity and the slice lengths,
        // so the caller-invocation `$count, [A:0, B:1, C:2]` can
        // only describe ONE N-triple. Runtime `.len()` asserts
        // would be tautological (and `usize::from(u16)` isn't
        // const-stable in MSRV 1.95).
        const _: () = {
            type Anchor = ($(repeat_as_i32!($t),)+);
            assert!(<Anchor as ParamsWriter>::COUNT == $count);
        };
    };
}

/// Token-munging helper — replace any ident with `i32` in a type
/// context. Used inside `params_writer_impl!` to build a concrete
/// anchor tuple `(i32, i32, …, i32)` for the per-arity drift-pin
/// const-assert (which needs a concrete monomorphic type to
/// reference `<Anchor as ParamsWriter>::COUNT`).
macro_rules! repeat_as_i32 {
    ($_t:ident) => { i32 };
}

/// Token-munging helper — consume a `$t` ident and emit the literal
/// `FormatCode::Binary`. Lets the `params_writer_impl!` macro thread
/// its `$t` repetition through FORMATS without resorting to
/// `let _ = <$t>::OID;` (crate-banned).
macro_rules! emit_binary_per_token {
    ($_t:ident) => { FormatCode::Binary };
}

// ───────────────── Arity 0..=16 impls ─────────────────
//
// Handwriting 16 impls is tedious but generating them individually
// via nested macros tends to obscure the structure more than it
// clarifies. Each impl is one invocation of `params_writer_impl!`.
params_writer_impl!();
params_writer_impl!(1, [A: 0]);
params_writer_impl!(2, [A: 0, B: 1]);
params_writer_impl!(3, [A: 0, B: 1, C: 2]);
params_writer_impl!(4, [A: 0, B: 1, C: 2, D: 3]);
params_writer_impl!(5, [A: 0, B: 1, C: 2, D: 3, E: 4]);
params_writer_impl!(6, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5]);
params_writer_impl!(7, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6]);
params_writer_impl!(8, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7]);
params_writer_impl!(9, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8]);
params_writer_impl!(10, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9]);
params_writer_impl!(11, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10]);
params_writer_impl!(12, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10, L: 11]);
params_writer_impl!(13, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10, L: 11, M: 12]);
params_writer_impl!(14, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10, L: 11, M: 12, N: 13]);
params_writer_impl!(15, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10, L: 11, M: 12, N: 13, O: 14]);
params_writer_impl!(16, [A: 0, B: 1, C: 2, D: 3, E: 4, F: 5, G: 6, H: 7, I: 8, J: 9, K: 10, L: 11, M: 12, N: 13, O: 14, P: 15]);

// Compile-time smoke-test of the canonical shapes: a 0-tuple, a
// singleton, and a mixed-type triplet — pin the anchor invariants
// at module level so the build fails cleanly if the macro gets
// miswired without needing a dedicated unit test.
const _: () = {
    assert!(<() as ParamsWriter>::COUNT == 0);
    assert!(<(i32,) as ParamsWriter>::COUNT == 1);
    assert!(<(i32, i32) as ParamsWriter>::COUNT == 2);
    assert!(<(i32, i32, i32) as ParamsWriter>::COUNT == 3);
    assert!(<(i32, i32, i32, i32, i32, i32, i32, i32) as ParamsWriter>::COUNT == 8);
    // OIDS shape: every slot == EncodeBinary::OID of its element.
    // Use slice pattern-match so `clippy::indexing_slicing` doesn't
    // fire — also more structurally explicit than `oids[0] == X`.
    let oids = <(i32, &str, bool) as ParamsWriter>::OIDS;
    assert!(oids.len() == 3);
    assert!(matches!(oids, [crate::decode::oids::INT4, crate::decode::oids::TEXT, crate::decode::oids::BOOL]));

    // F60 (pass #6): drift-pin for the `Option<T> as ParamEncoder`
    // blanket-vs-dedicated impl dispatch. Instantiates the Option
    // path at compile time — if Rust's trait resolution ever changed
    // so that the `impl<T: EncodeBinary> ParamEncoder for T` blanket
    // started matching `Option<T>` (e.g., someone added `impl EncodeBinary
    // for Option<T>`), this const-assert would produce an ambiguity
    // error at build. Currently produces: OIDS[i] == T::OID for the
    // inner T, proving the Option impl dispatches correctly.
    assert!(<(Option<i32>, Option<&str>) as ParamsWriter>::COUNT == 2);
    let option_oids = <(Option<i32>, Option<&str>) as ParamsWriter>::OIDS;
    assert!(option_oids.len() == 2);
    assert!(matches!(option_oids, [crate::decode::oids::INT4, crate::decode::oids::TEXT]));
    assert!(<(Option<bool>,) as ParamsWriter>::COUNT == 1);
    assert!(matches!(<(Option<bool>,) as ParamsWriter>::OIDS, [crate::decode::oids::BOOL]));
};

/// DEF-154 (B) Phase B4-W P0-3 + P2 test-only `ParamsWriter` impl
/// that always returns `Err(WriteBufFull)` from `write_params`.
///
/// Used by `protocol.rs`'s internal tests to exercise the
/// classified-Err routing from `build_bind_message` through
/// `CrateBugLocus::ParamsWriterOverflow` and into the `FailReply +
/// CloseSocket + Errored` end-to-end path. Sealed impl is only
/// possible inside this module (seal is module-private); re-export
/// via `pub(crate)` so `protocol.rs` can reference the type.
#[cfg(test)]
pub(crate) struct OverflowParams;

#[cfg(test)]
impl sealed::ParamsWriterSealed for OverflowParams {}

#[cfg(test)]
impl ParamsWriter for OverflowParams {
    const COUNT: u16 = 1;
    const FORMATS: &'static [FormatCode] = &[FormatCode::Binary];
    /// Placeholder OID — irrelevant for the classified-Err test
    /// path because the Err fires before any OID is consumed.
    const OIDS: &'static [u32] = &[crate::decode::oids::INT4];

    /// Always errors. Simulates a buggy / adversarial user impl
    /// whose `write_params` overflows its advertised budget.
    #[inline]
    fn write_params(&self, _dst: &mut WriteBuf) -> Result<(), WriteBufFull> {
        Err(WriteBufFull)
    }
}

#[cfg(test)]
mod tests {
    //! Runtime smoke-tests — complement the module-level const
    //! asserts by exercising the actual byte layout `write_params`
    //! emits. Catches any seam where `with_i32_length_prefixed_body`
    //! / `encode_to` pairing desyncs.
    use super::*;

    #[test]
    fn arity_zero_writes_nothing() {
        let mut buf = WriteBuf::new();
        let write_result = ().write_params(&mut buf);
        assert!(write_result.is_ok());
        assert!(buf.as_bytes().is_empty());
    }

    #[test]
    fn arity_one_i32_writes_length_plus_bytes() {
        let mut buf = WriteBuf::new();
        let write_result = (42i32,).write_params(&mut buf);
        assert!(write_result.is_ok());
        // i32(4-byte length) + 4 bytes for the i32 body BE.
        assert_eq!(buf.as_bytes(), &[0, 0, 0, 4, 0, 0, 0, 42]);
    }

    #[test]
    fn arity_three_mixed_types_layout() {
        let mut buf = WriteBuf::new();
        let write_result = (7i32, "hi", true).write_params(&mut buf);
        assert!(write_result.is_ok());
        // i32(4, 0x00000007) + i32(2, "hi") + i32(1, 0x01)
        assert_eq!(
            buf.as_bytes(),
            &[
                0, 0, 0, 4, 0, 0, 0, 7,      // i32 = 7
                0, 0, 0, 2, b'h', b'i',      // text = "hi"
                0, 0, 0, 1, 1,               // bool = true
            ],
        );
    }

    #[test]
    fn arity_three_oids_and_formats_coherent() {
        assert_eq!(<(i32, &str, bool) as ParamsWriter>::COUNT, 3);
        assert_eq!(<(i32, &str, bool) as ParamsWriter>::FORMATS.len(), 3);
        assert_eq!(<(i32, &str, bool) as ParamsWriter>::OIDS.len(), 3);
        assert!(
            <(i32, &str, bool) as ParamsWriter>::FORMATS
                .iter()
                .all(|f| matches!(f, FormatCode::Binary)),
        );
    }

    #[test]
    fn arity_sixteen_supported() {
        // Pin that arity 16 instantiates without hitting a macro bug
        // or type-ceiling surprise. Only tests COUNT — the write
        // path is covered by the smaller-arity tests above.
        assert_eq!(
            <(i32, i32, i32, i32, i32, i32, i32, i32,
              i32, i32, i32, i32, i32, i32, i32, i32) as ParamsWriter>::COUNT,
            16,
        );
    }
}
