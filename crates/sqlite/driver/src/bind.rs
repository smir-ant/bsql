//! Typed parameter binding for the compile-checked `query!` flagship — the
//! SQLite twin of the PostgreSQL `ParamsWriter`.
//!
//! A `query!` carrier's `$N` parameters are a typed tuple ([`SqliteTypedQuery::Params`](crate::SqliteTypedQuery::Params),
//! the SAME tuple the PostgreSQL `TypedQuery::Params` uses). This module lets
//! that tuple bind onto a prepared SQLite statement, so a driver's
//! `query::<Q>(params)` takes the SAME typed `Q::Params` on BOTH backends — no
//! untyped `&[ValueRef]` at the typed call site. The dynamic `*_params` verbs
//! keep taking `&[ValueRef]` as the escape hatch.
//!
//! # Two sealed traits, mirroring `ParamsWriter`
//!
//! - [`SqliteBindValue`] — ONE typed parameter → its SQLite bind value, in the
//!   value's TRUE storage class (an `i32` binds as `INTEGER`, an `&str` as
//!   `TEXT`, a `&[u8]` as `BLOB`, a `None` as SQL `NULL`). Sealed: only the leaf
//!   impls here qualify, so a PostgreSQL-only parameter type (`Uuid`, `Numeric`,
//!   a temporal, an `EnumLabel`) has NO impl and binding it on SQLite is a
//!   located compile error, never a silent mis-bind — the runtime peer of the
//!   PostgreSQL wire-OID pin, now on the parameter side.
//! - [`SqliteBindParams`] — a whole parameter TUPLE → a positional bind. Sealed;
//!   tuple impls cover arity `0..=32` (matching `ParamsWriter`). Each element
//!   binds in `$1..=$N` order to SQLite's `?1..=?N`.
//!
//! # Zero allocation
//!
//! Binding streams each parameter DIRECTLY onto the prepared statement via
//! rusqlite's `raw_bind_parameter` (a `sqlite3_bind_*` call) — no intermediate
//! `Vec`, no boxing, no `params_from_iter` collection. A [`ValueRef`] is 24
//! bytes and `Copy`; the whole bind is stack-only, exactly like the PostgreSQL
//! path streams its Bind block onto the warm send buffer.

use crate::value::ValueRef;

/// Module-private seal. Only the leaf / tuple impls in THIS module can opt a
/// type into the bind traits — no downstream `impl` can bypass the storage-class
/// discipline.
mod sealed {
    /// Supertrait seal for [`super::SqliteBindValue`].
    pub trait BindValueSealed {}
    /// Supertrait seal for [`super::SqliteBindParams`].
    pub trait BindParamsSealed {}
}

/// One typed `query!` parameter, bindable onto a SQLite statement in its true
/// storage class.
///
/// # Sealed
///
/// Module-private seal via [`sealed::BindValueSealed`] — the impls here are the
/// whole set: the SQLite storage-class scalars (`i8`/`i16`/`i32`/`i64` +
/// `u8`/`u16`/`u32` + `bool` → `INTEGER`, `f32`/`f64` → `REAL`, `&str` →
/// `TEXT`, `&[u8]` → `BLOB`) and `Option<T>` over them (`None` → SQL `NULL`). A
/// `u64` is excluded because it cannot losslessly become SQLite's signed 64-bit
/// `INTEGER`; a PostgreSQL-only type (`Uuid` / `Numeric` / a temporal /
/// `EnumLabel`) is excluded because SQLite has no such storage class — a
/// `query!` carrying such a parameter is PostgreSQL-only, and running it on
/// SQLite is a located compile error here, never a silent coercion.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a SQLite-bindable `query!` parameter type",
    label = "not bindable on SQLite",
    note = "SQLite-bindable parameter types are the storage-class scalars — `i8`/`i16`/`i32`/`i64`, `u8`/`u16`/`u32`, `bool`, `f32`/`f64`, `&str`, `&[u8]` — and `Option<T>` over them; a `u64` (not losslessly representable) or a PostgreSQL-only type (`Uuid`, `Numeric`, a temporal, a user enum) makes the `query!` PostgreSQL-only"
)]
pub trait SqliteBindValue: sealed::BindValueSealed {
    /// A borrowed SQLite value view for binding this parameter. `Text`/`Blob`
    /// alias the parameter's bytes (zero-copy); a scalar carries by value; a
    /// `None` is [`ValueRef::Null`].
    fn to_value_ref(&self) -> ValueRef<'_>;
}

// ── Leaf impls ──────────────────────────────────────────────────────────────

/// Integer-storage-class scalars: every one widens LOSSLESSLY into SQLite's
/// signed 64-bit `INTEGER` via `i64: From<_>` (which exists for all of these,
/// including `bool` → 0/1 — matching the `get::<bool>` read side). `i64` itself
/// is a separate impl (no `i64: From<i64>`), and `u64` is deliberately absent
/// (it does not losslessly fit a signed 64-bit integer).
macro_rules! bind_int_from {
    ($($t:ty),+ $(,)?) => { $(
        impl sealed::BindValueSealed for $t {}
        impl SqliteBindValue for $t {
            #[inline]
            fn to_value_ref(&self) -> ValueRef<'_> {
                ValueRef::Integer(i64::from(*self))
            }
        }
    )+ };
}
bind_int_from!(i8, i16, i32, u8, u16, u32, bool);

impl sealed::BindValueSealed for i64 {}
impl SqliteBindValue for i64 {
    #[inline]
    fn to_value_ref(&self) -> ValueRef<'_> {
        ValueRef::Integer(*self)
    }
}

impl sealed::BindValueSealed for f64 {}
impl SqliteBindValue for f64 {
    #[inline]
    fn to_value_ref(&self) -> ValueRef<'_> {
        ValueRef::Real(*self)
    }
}

impl sealed::BindValueSealed for f32 {}
impl SqliteBindValue for f32 {
    #[inline]
    fn to_value_ref(&self) -> ValueRef<'_> {
        ValueRef::Real(f64::from(*self))
    }
}

impl sealed::BindValueSealed for &str {}
impl SqliteBindValue for &str {
    #[inline]
    fn to_value_ref(&self) -> ValueRef<'_> {
        ValueRef::Text(self.as_bytes())
    }
}

impl sealed::BindValueSealed for &[u8] {}
impl SqliteBindValue for &[u8] {
    #[inline]
    fn to_value_ref(&self) -> ValueRef<'_> {
        ValueRef::Blob(self)
    }
}

/// A nullable parameter: `Some(v)` binds `v`'s storage class, `None` binds SQL
/// `NULL`. This does not conflict with the scalar impls — `Option<T>` is a
/// distinct type — and it is the sole NULL-parameter path (the read side's
/// `Option` field is its mirror).
impl<T: SqliteBindValue> sealed::BindValueSealed for Option<T> {}
impl<T: SqliteBindValue> SqliteBindValue for Option<T> {
    #[inline]
    fn to_value_ref(&self) -> ValueRef<'_> {
        match self {
            Some(v) => v.to_value_ref(),
            None => ValueRef::Null,
        }
    }
}

/// A whole `query!` parameter tuple, bindable positionally onto a prepared
/// statement.
///
/// # Sealed
///
/// Module-private seal via [`sealed::BindParamsSealed`] — only the crate's tuple
/// impls (arity `0..=32`, matching `ParamsWriter`) qualify. Each element must be
/// a [`SqliteBindValue`]; a tuple carrying a non-bindable element (a `u64`, a
/// PostgreSQL-only type) fails this bound at the `query::<Q>(params)` call site.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a bindable SQLite parameter tuple",
    label = "expected a tuple `()` through `(T1, …, T32)` where each Ti is `SqliteBindValue`",
    note = "the tuple must have arity 0..=32 and every element must be a SQLite-bindable parameter type (see `SqliteBindValue`)"
)]
pub trait SqliteBindParams: sealed::BindParamsSealed {
    /// The number of parameters this tuple binds — pinned to the tuple arity by
    /// the generating macro, matching the query's `$N` count.
    const COUNT: usize;

    /// Bind every element positionally (`self.0` → `?1`, `self.1` → `?2`, …)
    /// onto `stmt`, ready for `stmt.raw_query()`.
    ///
    /// # Errors
    ///
    /// A `rusqlite::Error` if the underlying `sqlite3_bind_*` fails (e.g. a
    /// `TEXT`/`BLOB` larger than SQLite's per-value limit) — classified, never a
    /// panic.
    fn bind_positional(&self, stmt: &mut rusqlite::Statement<'_>) -> Result<(), rusqlite::Error>;
}

impl sealed::BindParamsSealed for () {}
impl SqliteBindParams for () {
    const COUNT: usize = 0;
    #[inline]
    fn bind_positional(&self, _stmt: &mut rusqlite::Statement<'_>) -> Result<(), rusqlite::Error> {
        Ok(())
    }
}

/// Generate `SqliteBindParams` for one tuple arity. `$count` is the arity;
/// each `$t : $idx : $one` names a type param, its 0-based field index, and its
/// 1-based SQLite parameter index (`?N`). The two indices are passed explicitly
/// (stable `macro_rules!` cannot compute `$idx + 1`), exactly as `ParamsWriter`
/// passes its field indices.
macro_rules! bind_params_impl {
    ($count:literal, [$($t:ident : $idx:tt : $one:literal),+ $(,)?]) => {
        impl<$($t: SqliteBindValue),+> sealed::BindParamsSealed for ($($t,)+) {}
        impl<$($t: SqliteBindValue),+> SqliteBindParams for ($($t,)+) {
            const COUNT: usize = $count;
            #[inline]
            fn bind_positional(
                &self,
                stmt: &mut rusqlite::Statement<'_>,
            ) -> Result<(), rusqlite::Error> {
                $(
                    stmt.raw_bind_parameter($one, self.$idx.to_value_ref())?;
                )+
                Ok(())
            }
        }
    };
}

bind_params_impl!(1, [A: 0: 1]);
bind_params_impl!(2, [A: 0: 1, B: 1: 2]);
bind_params_impl!(3, [A: 0: 1, B: 1: 2, C: 2: 3]);
bind_params_impl!(4, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4]);
bind_params_impl!(5, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5]);
bind_params_impl!(6, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6]);
bind_params_impl!(7, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7]);
bind_params_impl!(8, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8]);
bind_params_impl!(9, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9]);
bind_params_impl!(10, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10]);
bind_params_impl!(11, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11]);
bind_params_impl!(12, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12]);
bind_params_impl!(13, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13]);
bind_params_impl!(14, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14]);
bind_params_impl!(15, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15]);
bind_params_impl!(16, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16]);
bind_params_impl!(17, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17]);
bind_params_impl!(18, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18]);
bind_params_impl!(19, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19]);
bind_params_impl!(20, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20]);
bind_params_impl!(21, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21]);
bind_params_impl!(22, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21, V: 21: 22]);
bind_params_impl!(23, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21, V: 21: 22, W: 22: 23]);
bind_params_impl!(24, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21, V: 21: 22, W: 22: 23, X: 23: 24]);
bind_params_impl!(25, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21, V: 21: 22, W: 22: 23, X: 23: 24, Y: 24: 25]);
bind_params_impl!(26, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21, V: 21: 22, W: 22: 23, X: 23: 24, Y: 24: 25, Z: 25: 26]);
bind_params_impl!(27, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21, V: 21: 22, W: 22: 23, X: 23: 24, Y: 24: 25, Z: 25: 26, AA: 26: 27]);
bind_params_impl!(28, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21, V: 21: 22, W: 22: 23, X: 23: 24, Y: 24: 25, Z: 25: 26, AA: 26: 27, AB: 27: 28]);
bind_params_impl!(29, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21, V: 21: 22, W: 22: 23, X: 23: 24, Y: 24: 25, Z: 25: 26, AA: 26: 27, AB: 27: 28, AC: 28: 29]);
bind_params_impl!(30, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21, V: 21: 22, W: 22: 23, X: 23: 24, Y: 24: 25, Z: 25: 26, AA: 26: 27, AB: 27: 28, AC: 28: 29, AD: 29: 30]);
bind_params_impl!(31, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21, V: 21: 22, W: 22: 23, X: 23: 24, Y: 24: 25, Z: 25: 26, AA: 26: 27, AB: 27: 28, AC: 28: 29, AD: 29: 30, AE: 30: 31]);
bind_params_impl!(32, [A: 0: 1, B: 1: 2, C: 2: 3, D: 3: 4, E: 4: 5, F: 5: 6, G: 6: 7, H: 7: 8, I: 8: 9, J: 9: 10, K: 10: 11, L: 11: 12, M: 12: 13, N: 13: 14, O: 14: 15, P: 15: 16, Q: 16: 17, R: 17: 18, S: 18: 19, T: 19: 20, U: 20: 21, V: 21: 22, W: 22: 23, X: 23: 24, Y: 24: 25, Z: 25: 26, AA: 26: 27, AB: 27: 28, AC: 28: 29, AD: 29: 30, AE: 30: 31, AF: 31: 32]);

// Compile-time drift-pins for the canonical arities — the `COUNT` const must
// track the tuple arity, so a miswired macro invocation fails the build here
// (mirroring the `ParamsWriter` anchor asserts).
const _: () = {
    assert!(<() as SqliteBindParams>::COUNT == 0);
    assert!(<(i64,) as SqliteBindParams>::COUNT == 1);
    assert!(<(i64, &str) as SqliteBindParams>::COUNT == 2);
    assert!(<(i64, &str, bool) as SqliteBindParams>::COUNT == 3);
    assert!(<(Option<i64>, Option<&str>) as SqliteBindParams>::COUNT == 2);
};
