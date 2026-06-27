//! Compile-time budgets for the `query!` macro's DYNAMIC forms.
//!
//! Each dynamic form expands inside one static SQL string. To keep the
//! expanded form and the emitted code bounded, the macro caps the number
//! of toggled filters and the number of runtime `ORDER BY` orderings, and
//! emits a `const` assertion against these limits. Exceeding a budget is a
//! const-evaluation failure (`error[E0080]`) at the `query!` site — a loud
//! build error, never a silent truncation of filters or orderings.
//!
//! The limits live here (the shipped crate the generated code already
//! references) so the macro and the runtime agree on one source of truth.

/// Maximum number of `OPTIONAL(...)` toggled filters in one `query!`.
///
/// Each toggled filter adds a `($N IS NULL OR ...)` term to the expanded
/// SQL and one `Option<T>` parameter; this caps the expanded form size.
pub const MAX_OPTIONAL_FILTERS: usize = 8;

/// Maximum number of runtime `ORDER BY` allow-set orderings in one
/// `query!`.
///
/// Each ordering becomes one baked prepared-query variant and one selector
/// enum variant, so this caps how many full wire artifacts a single
/// `query!` emits.
pub const MAX_ORDER_BY_VARIANTS: usize = 16;

// The budgets are positive and ordered sanely (a query may always declare
// at least one toggled filter / ordering). Pinned at compile time.
const _: () = {
    assert!(MAX_OPTIONAL_FILTERS >= 1);
    assert!(MAX_ORDER_BY_VARIANTS >= 1);
};
