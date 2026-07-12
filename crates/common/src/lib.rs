#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
// A future undocumented `pub` item is a build error, not silent doc rot — the
// same floor the shipped PostgreSQL / SQLite crates carry.
#![deny(missing_docs)]
// Mechanical-cast wall (tier-1) completing the workspace floor's
// `cast_sign_loss` + `integer_division` forbid: an `as` conversion, a truncating
// or sign-flipping `as` cast, and `unreachable!` are all rejected at compile
// time. `deny` (not `forbid`) preserves a greppable, reasoned
// `#[expect(..., reason = "...")]` escape (the workspace keystone
// `allow_attributes_without_reason` forces the reason).
#![deny(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::unreachable
)]
// Panic-class mechanical wall (tier-1): an unbounded `arr[i]` and an overflowing
// `+`/`-`/`*` are rejected by rustc, not review. `deny` (not `forbid`) keeps a
// reasoned `#[expect]` escape. Indexing in test code is exempted by clippy.toml
// `allow-indexing-slicing-in-tests`; `arithmetic_side_effects` has NO such key,
// so the `cfg_attr(test, allow)` below scopes it to production.
#![deny(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
#![cfg_attr(
    test,
    allow(
        clippy::arithmetic_side_effects,
        reason = "test assertions/fixtures use bare arithmetic on small constants; the production deny above is the tier-1 wall"
    )
)]

//! Dependency-free shared logic for bsql's backends.
//!
//! This is the leaf crate the PostgreSQL core (`bsql-postgres-core`) and the
//! embedded SQLite driver (`bsql-sqlite`) BOTH depend on for the pure,
//! transport-agnostic logic that was, before this crate existed, a
//! hand-maintained COPY in each. The copies could — and did — drift; here the
//! logic is ONE compiled source, so the cross-backend behaviour cannot diverge.
//!
//! It has ZERO external dependencies (only `std` / `core`), so neither backend
//! drags the other's heavy runtime tree, and no build-time SQL-parsing crate
//! (`sqlparser` / `bsql-build`) is ever reachable through it.
//!
//! # What lives here
//!
//! - [`migrate`] — the migration pure logic: the FNV-1a-64 content checksum, the
//!   `/`-normalized name ordering authority, the drift classification, the
//!   source loader + duplicate-name pre-flight, and the plain data / error
//!   types. The per-backend RUNNER (the advisory-lock poll on PostgreSQL, the
//!   `BEGIN IMMEDIATE` re-check on SQLite) stays in each driver, bridged through
//!   [`migrate::plan`] and each driver's own `From<`[`migrate::Drift`]`>`.

pub mod migrate;
