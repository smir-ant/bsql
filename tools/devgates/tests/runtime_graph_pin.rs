//! runtime_graph_pin — the build-time-only boundary, enforced.
//!
//! The compile-checked query API relies on three crates that must NEVER
//! reach a shipped library's RUNTIME:
//!   * `sqlparser`       — the SQL grammar (only `bsql-build` uses it),
//!   * `bsql-build`      — the `[build-dependencies]` catalog generator,
//!   * `bsql-query-macros` — the proc-macro that reads the catalog.
//!
//! `bsql-build` is meant to be a `[build-dependencies]` edge only, and
//! `sqlparser` is reachable solely through it. If `bsql-build` were ever
//! moved into a shipped crate's `[dependencies]`, `sqlparser` would leak
//! into that crate's runtime/forbid closure. The `deps_pin` gate pins the
//! lockfile package SET with no dependency-KIND, so that leak would leave
//! it GREEN — it cannot see the build-vs-normal distinction. This gate
//! closes that hole: it parses each shipped crate's `-e normal` (runtime)
//! dependency graph and asserts none of the three forbidden crates appear.
//!
//! Mechanism — `cargo tree -p <crate> -e normal`:
//!   `-e normal` is the runtime edge kind: it excludes `[build-dependencies]`
//!   (so `bsql-build`, reached only via a build edge, is correctly absent)
//!   and `[dev-dependencies]`. It DOES include normal proc-macro edges, so
//!   if a shipped crate took a normal dependency on `bsql-query-macros`
//!   (or, transitively, on `bsql-build` / `sqlparser`) it would show up.
//!   This is the faithful, TRANSITIVE runtime graph — a scan of direct
//!   `[dependencies]` keys would miss a leak introduced one hop away.

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

/// Crates that must be absent from every shipped crate's runtime graph.
const FORBIDDEN_IN_RUNTIME: &[&str] = &["sqlparser", "bsql-build", "bsql-query-macros"];

/// The `cargo` binary: Cargo sets `CARGO` for its child test processes;
/// fall back to the PATH-resolved `cargo` when run outside that context.
fn cargo_bin() -> String {
    match std::env::var("CARGO") {
        Ok(path) => path,
        Err(_) => "cargo".to_string(),
    }
}

/// The `-e normal` dependency tree for one crate, as raw text. Each line
/// names one crate in the tree (`cargo tree` renders `<name> <version>
/// ...` per line, indented to show depth); we only need substring
/// presence of a forbidden package name.
fn runtime_tree(crate_name: &str) -> String {
    let output = Command::new(cargo_bin())
        .args(["tree", "-p", crate_name, "-e", "normal", "--prefix", "none"])
        .output()
        .expect("failed to run `cargo tree`");
    assert!(
        output.status.success(),
        "`cargo tree -p {crate_name} -e normal` failed: {}",
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
                "  - `{shipped}` runtime graph (-e normal) contains: {}\n",
                hits.join(", ")
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "build-time-only crates leaked into a shipped crate's RUNTIME graph:\n{violations}\n\
         `sqlparser`, `bsql-build`, and `bsql-query-macros` are build-time-only \
         (the catalog generator + SQL grammar + the catalog-reading proc-macro). \
         They must reach a shipped crate ONLY via a `[build-dependencies]` edge, \
         never `[dependencies]`. A normal-edge dependency here pulls the SQL parser \
         into the runtime/forbid closure. Move the offending dependency to \
         `[build-dependencies]` (for `bsql-build`) or remove it."
    );
}
