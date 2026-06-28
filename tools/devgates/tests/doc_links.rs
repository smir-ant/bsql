//! doc_links — the intra-doc-link wall, enforced.
//!
//! A doc comment on a kept item may reference a symbol that a later deletion
//! removes (a type, module, method, or macro). `cargo build` / `clippy` /
//! `cargo test` never resolve intra-doc links, so such a dangling reference
//! rots silently: the prose now points at nothing, misleading every reader.
//! This gate closes that hole — it runs `cargo doc --workspace --no-deps`
//! with `rustdoc::broken_intra_doc_links` denied and asserts the doc build
//! succeeds, so an orphaned link fails the gate the moment it lands.
//!
//! # Why a devgate (not a declarative `[lints.rustdoc]` / `#![deny]`)
//!
//! Three enforcement options were evaluated on stable 1.96:
//!
//! 1. `[workspace.lints.rustdoc] broken_intra_doc_links = "deny"` + each
//!    crate's `lints.workspace = true`. Blocked: only `bsql-postgres-proto`
//!    opts into the workspace lint table — the other shipped crates
//!    deliberately do NOT inherit it (they would also pick up the strict
//!    forbid floor, which they are not written against). A workspace-wide
//!    rustdoc deny is therefore unreachable through the shared table.
//! 2. A per-crate `#![deny(rustdoc::broken_intra_doc_links)]` crate attr (or
//!    a per-crate `[lints.rustdoc]` table). Works, but a rustdoc lint only
//!    bites during `cargo doc` — it never fires under the project's standard
//!    `cargo test --workspace` gate flow, so a broken link would still pass
//!    every routine check until someone happened to build docs.
//! 3. **This devgate.** It runs inside `cargo test --workspace`, covers every
//!    workspace member in one place, and mirrors the established `deps_pin` /
//!    `runtime_graph_pin` gates (a `cargo` subprocess asserting a property).
//!    Chosen for being the only option that is both workspace-wide AND in the
//!    standard gate flow.
//!
//! # Isolation
//!
//! The nested `cargo doc` writes to a DEDICATED `CARGO_TARGET_DIR`
//! (`target/devgate-doc-links`) so it does not contend for the parent
//! `cargo test` build lock — a shared target dir would deadlock the nested
//! build against the outer invocation that is still holding it.

use std::path::PathBuf;
use std::process::Command;

/// The `cargo` binary: Cargo sets `CARGO` for its child test processes; fall
/// back to the PATH-resolved `cargo` when run outside that context.
fn cargo_bin() -> String {
    match std::env::var("CARGO") {
        Ok(path) => path,
        Err(_) => "cargo".to_string(),
    }
}

/// Workspace root: `CARGO_MANIFEST_DIR` is `<ws>/tools/devgates`.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root two levels above tools/devgates")
        .to_path_buf()
}

#[test]
fn workspace_docs_have_no_broken_intra_doc_links() {
    let root = workspace_root();
    // A dedicated target dir so the nested `cargo doc` build does not contend
    // with the parent `cargo test` for the shared target lock.
    let doc_target = root.join("target").join("devgate-doc-links");

    // Scope: the PUBLIC documented surface of every workspace member (the
    // default `cargo doc` set). This is where a deletion's orphaned links are
    // visible to readers, and where this slice's old-engine doc rot lived.
    // `--document-private-items` was evaluated and rejected for now: it
    // surfaces pre-existing private-doc rot unrelated to this gate's purpose
    // (sqlparser AST references in the build-time inference crate; a
    // `super::flush` fn-vs-module ambiguity in the engine internals), which is
    // a separate cleanup. Tightening to private items is a clean follow-up
    // once that rot is swept.
    let output = Command::new(cargo_bin())
        .current_dir(&root)
        .args(["doc", "--workspace", "--no-deps"])
        .env("RUSTDOCFLAGS", "-D rustdoc::broken_intra_doc_links")
        .env("CARGO_TARGET_DIR", &doc_target)
        .output()
        .expect("failed to run `cargo doc`");

    assert!(
        output.status.success(),
        "`cargo doc --workspace --no-deps` failed under \
         `-D rustdoc::broken_intra_doc_links` — a doc comment links to a symbol \
         that no longer resolves (commonly a reference left dangling by a \
         deletion). Repoint the link to its replacement, or drop the stale \
         prose. rustdoc output:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
}
