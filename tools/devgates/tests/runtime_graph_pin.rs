//! runtime_graph_pin — the build-time-only boundary, enforced.
//!
//! The compile-checked query API relies on build-time-only machinery that
//! must NEVER be LINKED into a shipped library's RUNTIME binary:
//!   * `sqlparser`         — the SQL grammar (only `bsql-build` uses it),
//!   * `bsql-build`        — the `[build-dependencies]` catalog generator,
//!   * `bsql-query-macros` — the proc-macro that reads the catalog.
//!
//! `bsql-build` is meant to be a `[build-dependencies]` edge only, and
//! `sqlparser` is reachable solely through it. If `bsql-build` were ever
//! moved into a shipped crate's `[dependencies]`, `sqlparser` would leak
//! into that crate's runtime/forbid closure. The `deps_pin` gate pins the
//! lockfile package SET with no dependency-KIND, so that leak would leave
//! it GREEN — it cannot see the build-vs-normal distinction. This gate
//! closes that hole: it parses each shipped crate's runtime dependency graph
//! and asserts the SQL-parsing runtime libraries are absent.
//!
//! Mechanism — `cargo tree -p <crate> --all-features -e normal,no-proc-macro`:
//!
//!   * `-e normal` is the runtime edge kind: it excludes `[build-dependencies]`
//!     (so `bsql-build`, reached only via a build edge, is correctly absent)
//!     and `[dev-dependencies]`.
//!   * `--all-features` activates every optional edge — including the
//!     umbrella crate's non-default `macros` feature, which pulls the
//!     `bsql-query-macros` PROC-MACRO. Without it this gate would be blind to
//!     that feature: a leak reachable only under `macros` would pass unseen.
//!     Turning all features on is what makes the check exhaustive.
//!   * `no-proc-macro` prunes proc-macro packages AND their subtrees. This is
//!     the LINKAGE-faithful model: a proc-macro runs in the compiler at build
//!     time and is never linked into the consumer's runtime binary, so the
//!     `bsql -> bsql-query-macros` edge (and everything reached ONLY through
//!     it — `bsql-build`, `sqlparser`) carries no runtime code and is
//!     correctly excluded. This is precisely why the umbrella crate's
//!     host-only proc-macro dependency is ALLOWED while a genuine runtime
//!     leak is not.
//!
//! Teeth preserved: `bsql-build` (and `sqlparser`) are ordinary libraries,
//! not proc-macros. A DIRECT `[dependencies]` edge to `bsql-build` from a
//! shipped crate does NOT pass through a proc-macro, so `no-proc-macro` does
//! not prune it — it stays visible in the runtime graph and turns this gate
//! RED. (Proven RED->GREEN by temporarily moving `bsql-build` into a shipped
//! crate's `[dependencies]`.) `bsql-query-macros` is therefore dropped from
//! the forbidden set: as a proc-macro it can never appear under
//! `no-proc-macro`, so forbidding it would be a check that can never fire;
//! the runtime leak that MATTERS is a normal-library leak of `sqlparser` /
//! `bsql-build`, which the teeth above still catch.

use std::process::Command;

/// The shipped (publishable) crates — the ones whose runtime closure must
/// stay free of the build-time-only query toolchain. `tools/devgates` and
/// `tools/query_fixture` are `publish = false` and intentionally excluded;
/// `bsql-build` / `bsql-query-macros` are themselves the forbidden crates.
const SHIPPED_CRATES: &[&str] = &[
    "bsql",
    "bsql-postgres-proto",
    "bsql-postgres-derive",
    "bsql-postgres-core",
    "bsql-postgres-async",
    "bsql-postgres-sync",
    "bsql-sqlite",
];

/// Runtime libraries that must be absent from every shipped crate's runtime
/// graph. Both are build-time-only: `bsql-build` is the `[build-dependencies]`
/// catalog generator and `sqlparser` is reachable solely through it. (The
/// `bsql-query-macros` proc-macro is deliberately NOT listed: it is host-only,
/// never linked at runtime, and is pruned by `no-proc-macro`, so it could
/// never appear here — the runtime leak that matters is these two libraries.)
const FORBIDDEN_IN_RUNTIME: &[&str] = &["sqlparser", "bsql-build"];

/// The `cargo` binary: Cargo sets `CARGO` for its child test processes;
/// fall back to the PATH-resolved `cargo` when run outside that context.
fn cargo_bin() -> String {
    match std::env::var("CARGO") {
        Ok(path) => path,
        Err(_) => "cargo".to_string(),
    }
}

/// The runtime (`-e normal`), proc-macro-pruned (`no-proc-macro`),
/// all-features dependency tree for one crate, as raw text. Each line names
/// one crate in the tree (`--prefix none` renders `<name> <version> ...` per
/// line); we only need presence of a forbidden package as a leading token.
fn runtime_tree(crate_name: &str) -> String {
    let output = Command::new(cargo_bin())
        .args([
            "tree",
            "-p",
            crate_name,
            "--all-features",
            "-e",
            "normal,no-proc-macro",
            "--prefix",
            "none",
        ])
        .output()
        .expect("failed to run `cargo tree`");
    assert!(
        output.status.success(),
        "`cargo tree -p {crate_name} --all-features -e normal,no-proc-macro` failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("cargo tree output is not UTF-8")
}

/// Returns the forbidden crate names found at the START of a tree line
/// (i.e. as a tree node, not merely as a substring of an unrelated name).
fn forbidden_nodes_in(tree: &str) -> Vec<&'static str> {
    let mut hits = Vec::new();
    for &forbidden in FORBIDDEN_IN_RUNTIME {
        let appears = tree.lines().any(|line| {
            // `--prefix none` renders each node as `name v1.2.3 (path)`.
            // Match the leading token exactly so `sqlparser` does not
            // false-match a hypothetical `sqlparser-foo`, nor vice versa.
            match line.split_whitespace().next() {
                Some(first) => first == forbidden,
                None => false,
            }
        });
        if appears {
            hits.push(forbidden);
        }
    }
    hits
}

#[test]
fn shipped_crates_have_no_build_time_query_deps_at_runtime() {
    let mut violations = String::new();
    for &shipped in SHIPPED_CRATES {
        let tree = runtime_tree(shipped);
        let hits = forbidden_nodes_in(&tree);
        if !hits.is_empty() {
            violations.push_str(&format!(
                "  - `{shipped}` runtime graph (--all-features -e normal,no-proc-macro) \
                 contains: {}\n",
                hits.join(", ")
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "build-time-only libraries leaked into a shipped crate's RUNTIME graph:\n{violations}\n\
         `bsql-build` (the catalog generator) and `sqlparser` (its SQL grammar) are \
         build-time-only. They must reach a shipped crate ONLY via a \
         `[build-dependencies]` edge, never `[dependencies]`. A normal-edge dependency \
         here pulls the SQL parser into the runtime/forbid closure and survives the \
         `no-proc-macro` prune (neither is a proc-macro). Move the offending \
         dependency to `[build-dependencies]` (for `bsql-build`) or remove it. \
         (The umbrella crate's `bsql -> bsql-query-macros` edge is a host-only \
         proc-macro, pruned by `no-proc-macro`, and is expected/allowed.)"
    );
}
