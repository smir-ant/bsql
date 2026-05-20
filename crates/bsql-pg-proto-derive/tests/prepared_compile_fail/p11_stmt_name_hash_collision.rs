//! Hostile-bypass probe **P11** — re-use of stmt_name across
//! mismatched prepared queries (hash collision class).
//!
//! # Tier
//!
//! Tier-1 by-construction via SHA-256-96 content-addressing of the
//! `stmt_name`. The macro emits
//! `stmt_name = "bsql_p_" + sha256(sql)[..12].hex()` — a 96-bit
//! collision space (2⁻⁹⁶ probability, ≈ 10⁻²⁹). For any realistic
//! codebase (well under 10¹² distinct queries) the birthday-paradox
//! collision probability is < 10⁻⁵. This is effectively tier-1 for
//! the SQL-injection class; the alternative would be SHA-256-256 at
//! 32 hex chars (negligible perf benefit vs ergonomics cost).
//!
//! # What this probe pins
//!
//! Hash collision is **statistical** — there is no specific
//! `compile_fail` source that "tests" SHA-256-96 collision rate. The
//! pin lives at:
//! - **Source**: macro emits `bsql_p_<24 hex chars>`; format pinned
//!   in `prepared_macro_spec::t9_stmt_name_content_addressed` and
//!   `prepared.rs`'s `sha256_96_stmt_name` const-fn helper.
//! - **Drift**: if a future contributor truncated to fewer hex
//!   chars (e.g. 16 → 64-bit collision space), the test in
//!   `prepared_macro_spec.rs:t9_stmt_name_content_addressed` —
//!   which asserts `a.len() == 31` (7 prefix + 24 hex) — fails.
//!
//! This trybuild file is included for symmetry with the P1-P12
//! enumeration but its load-bearing assertion is the **structural**
//! one: a downstream caller cannot read `stmt_name` (`pub(crate)`),
//! so collision-exploitation requires intra-crate code paths the
//! attacker doesn't reach.
//!
//! # Expected diagnostic
//!
//! `error[E0616]: field 'stmt_name' of struct 'PreparedQuery' is
//! private`. Pins the visibility check that bars external direct
//! `Q.stmt_name` reads (the accessor `q.stmt_name()` is `pub` and
//! returns the validated string).
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P11.

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

const Q: PreparedQuery<(i32,), (i32,)> = prepared!("SELECT id::int4 WHERE id = $1::int4");

fn main() {
    // P11 attack: direct field read to harvest the stmt_name and
    // splice into a hostile parallel query. Should fail with E0616
    // (private field). The accessor `q.stmt_name()` exposes the
    // string for legitimate diagnostic use; the raw field stays
    // crate-private to prevent ad-hoc mutation paths.
    let _hostile: &str = Q.stmt_name;
    let _ = _hostile;
}
