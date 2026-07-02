//! Footprint measurement regime — build-gated `size_of` / `align_of` pins.
//!
//! Footprint is a **measured, build-gated dimension** of this codebase: the
//! byte size and alignment of every stable public type is pinned to its
//! measured value, and any drift fails the build. Two macros carry the regime:
//!
//! - [`crate::footprint_pin`] pins a **nameable** type's `size_of` *and* `align_of`
//!   with a free-standing `const _: () = { … }` item. A drift turns one of the
//!   emitted `assert!`s false and aborts the build with an `E0080` const-eval
//!   failure — **at `cargo check`, including for a type constructed nowhere**.
//!   This is the strongest possible gate: it needs neither a test run nor a
//!   construction site.
//!
//! - [`crate::future_pin`] pins the `size_of_val` of a **concrete future** produced
//!   by an `async fn` or async block. A future's type is *unnameable* and its
//!   size is *not* const-evaluable (you cannot call the `async fn` in a `const`
//!   context — `E0015`), so there is no `E0080` path here. The strongest
//!   available gate is a `#[test]` that constructs the future (never polling it,
//!   so zero I/O) and asserts its measured size. Drift fails `cargo test`.
//!
//! # Why both `size_of` and `align_of`
//!
//! A bare `size_of` anchor is blind to a **size-preserving alignment drift**: a
//! field reorder can raise `align` from 4 to 8 without changing the byte count,
//! or a niche can be lost while the size holds. [`crate::footprint_pin`] pins both in
//! one co-located anchor so the two cannot drift apart.
//!
//! # Runtime cost
//!
//! Zero. [`crate::footprint_pin`] emits a `const _: ()` item that is fully erased by
//! codegen. [`crate::future_pin`] emits a `#[cfg(test)]`-gated `#[test]` that exists
//! only in the test binary.
//!
//! # Baseline footprint (measured @ aarch64-apple-darwin, rustc 1.96.0)
//!
//! These are the captured baseline numbers. A future change that moves one of
//! them must move the matching pin in the same commit, putting the byte cost on
//! the review surface instead of letting it drift silently.
//!
//! Stable public types — `bsql-postgres-core`:
//!
//! ```text
//!   TYPE                      size  align
//!   Row                         16      8   Arc pointer + u32 row index
//!   OwnedRow                    16      8   one Box<[u8]> (ptr + len)
//!   OwnedRowTooLarge             0      1   ZST error marker
//!   ArenaSealError               1      1   2-variant seal-error enum
//!   DriverError                120      8   Db(DbError) dominates
//!   DbError                    120      8   5 String/Option<String> fields
//!   ConnectConfig              112      8   host/user/db/password Strings
//!   SslMode                      1      1   3-variant enum
//!   PreparedStatement          104      8   StmtName(65) + Option<RowDesc> + Arc<[String]>
//!   Notification                56      8   2 String + i32
//!   QueryResult                 72      8   Vec<Row> + String + usize + Arc<[String]>
//! ```
//!
//! Stable wire/error/state types — `bsql-postgres-proto` — carry their own
//! `wire_pin!` anchors (size + align together) co-located with each type:
//! `ProtocolError` = 24/8, `ProtoState` = 24/8, `StmtName` = 65/1,
//! `ErrorRef` = 8/4, `decode::RowDesc` = 16/8, `PasswordError` = 16/8,
//! `NotificationRef` = 4/2.
//!
//! The proto crate's engine-internal types (the `PgProtocol` phases,
//! `OutActions`, `Action`, `DispatchOutcome`, `command::PgCommand`, the
//! outbound command structs, …) are NOT stable surface — they are slated
//! for rework — and stay on plain `const _: () = assert!(…)` size and
//! alignment manifests inside that crate, not on the `wire_pin!` regime.
//!
//! Stable public types — `bsql-sqlite` — carry their own `footprint_pin!`
//! anchors (`Row` = 24, `SqliteValue` = 32, `QueryResult` = 56,
//! `SqliteError` = 32).
//!
//! Per-connection resident footprint (the live cost the application pays per
//! open PostgreSQL connection) is dominated NOT by these handle types but by
//! the protocol state machine and the read buffer the driver owns. The handle
//! types above are the *surface* cost; the engine-owned working set is pinned
//! inside `bsql-postgres-proto` (its `PgProtocol` size pin) and the driver's
//! read buffer (`READ_BUF_CAP`). Those are intentionally NOT re-pinned here —
//! the proto crate owns them, and the driver buffer is a tuning constant, not a
//! type footprint.
//!
//! Minimum consumer binary size: see the `footprint_baseline` gate test, which
//! records the release-stripped size of a minimal `query`-shaped consumer at
//! the time of measurement so future drift is comparable.

/// Pin the **footprint** (`size_of` AND `align_of`) of a nameable type at
/// build time — a layout drift becomes an `E0080` const-eval failure, not a
/// silent regression and not a `cargo test`-only catch.
///
/// The emitted item is a free-standing `const _: () = { … }`, so the compiler
/// evaluates it during `cargo check` / `cargo build`, **including for types
/// that are constructed nowhere in the binary**. Runtime cost is zero — the
/// `const _` item is fully erased by codegen.
///
/// For a type with a lifetime parameter, pin a `'static` instantiation
/// (`size_of` / `align_of` require a concrete type).
///
/// ```
/// # use bsql_postgres_core::footprint_pin;
/// struct Handle { ptr: *const u8, idx: u32 }
/// footprint_pin!(Handle, size = 16, align = 8);
/// ```
///
/// A wrong pin (or a layout change that invalidates a correct pin) aborts the
/// build with `E0080`:
///
/// ```compile_fail,E0080
/// use bsql_postgres_core::footprint_pin;
/// #[repr(C)]
/// struct Drifted { a: u32, b: u32, c: u32 } // 12 B actual
/// footprint_pin!(Drifted, size = 8, align = 4); // pinned 8 → E0080
/// ```
///
/// A size-preserving **alignment** drift — which a bare `size_of` anchor
/// cannot see — also aborts:
///
/// ```compile_fail,E0080
/// use bsql_postgres_core::footprint_pin;
/// #[repr(C, align(8))]
/// struct AlignDrifted { a: u32, b: u32 } // 8 B, align 8
/// footprint_pin!(AlignDrifted, size = 8, align = 4); // pinned align 4 → E0080
/// ```
#[macro_export]
macro_rules! footprint_pin {
    ($t:ty, size = $n:expr, align = $a:expr $(,)?) => {
        const _: () = {
            assert!(
                core::mem::size_of::<$t>() == $n,
                concat!("FOOTPRINT DRIFT (size) for ", stringify!($t))
            );
            assert!(
                core::mem::align_of::<$t>() == $a,
                concat!("FOOTPRINT DRIFT (align) for ", stringify!($t))
            );
        };
    };
}

/// Pin the `size_of_val` of a **concrete future** produced by an `async fn` or
/// async block, by emitting a `#[test]` that constructs the future (without
/// polling it — zero I/O) and asserts its measured size.
///
/// A future's type is unnameable and its size is **not** const-evaluable (the
/// producing `async fn` cannot be called in a `const` context — `E0015`), so
/// there is no `E0080` compile-time path for it the way [`crate::footprint_pin`] has
/// for a nameable type. A `cargo test` assertion is the strongest gate
/// available: it fires whenever the test binary runs, catching any growth of
/// the state-machine the `async fn` lowers to (an added `.await`, a wider
/// captured local, an inlined sub-future).
///
/// The future expression is evaluated but the future is **never polled**, so a
/// future that performs I/O when driven performs none here — only its stack
/// layout is measured.
///
/// ```
/// # use bsql_postgres_core::future_pin;
/// async fn sample(x: u64) -> u64 { x + 1 }
/// future_pin!(sample_future_size, sample(7), size = 16);
/// ```
#[macro_export]
macro_rules! future_pin {
    ($test_name:ident, $fut_expr:expr, size = $n:expr $(,)?) => {
        #[cfg(test)]
        #[test]
        fn $test_name() {
            let fut = $fut_expr;
            let measured = core::mem::size_of_val(&fut);
            // Never poll — measuring layout only, no I/O.
            drop(fut);
            assert_eq!(
                measured, $n,
                "FUTURE FOOTPRINT DRIFT for {}: measured {} B, pinned {} B",
                stringify!($test_name), measured, $n
            );
        }
    };
}

#[cfg(test)]
mod tests {
    // Self-test of the `future_pin!` mechanism on a local async fn (not a
    // driver future): proves the macro measures a real future's size and that
    // the emitted assertion fires on drift. A driver hot-path future is pinned
    // with this same macro once the engine that owns it is in place; that
    // future's type is constructed only behind a live connection, so it is
    // measured against a live driver, not here.
    async fn sample(a: u64, b: u64) -> u64 {
        let s = a.wrapping_add(b);
        // A trailing await widens the state machine; this exercises that the
        // macro measures the lowered future, not just the argument tuple.
        core::future::ready(s).await
    }

    // The measured size of this concrete future on the reference target. If
    // the macro under-measured (e.g. sized the fn pointer instead of the
    // future), or the lowering changed, this pin would fail.
    crate::future_pin!(sample_future_is_pinned, sample(3, 4), size = 40);

    #[test]
    fn footprint_pin_macro_measures_a_real_type() {
        // The `footprint_pin!` doctests prove the E0080 drift path across the
        // crate boundary; this in-crate test proves the happy path resolves and
        // the measured values are the ones the regime documents.
        assert_eq!(core::mem::size_of::<crate::Row>(), 16);
        assert_eq!(core::mem::align_of::<crate::Row>(), 8);
    }
}
