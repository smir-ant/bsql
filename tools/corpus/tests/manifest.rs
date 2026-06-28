//! PORT-not-fixturize manifest gate.
//!
//! `port_manifest.list` records the spec-test families that must be kept (or
//! rewritten in place) rather than replaced with a corpus fixture — because
//! they assert properties (memory-zeroize/residue, footprint/size, compile-fail
//! diagnostics, randomised robustness) that are invisible at the observable-I/O
//! seam the corpus replays through. This test mechanically enforces the rule:
//! every listed file must still exist on disk, and the seven known families
//! must all be listed. An accidental deletion of one, or a manifest edit that
//! drops a family, fails loudly here.
//!
//! When the retired push-path engine was deleted, its internal-probe families
//! went too. Two carried properties that still matter and were PORTED to real
//! successor specs on the sans-IO engine — `buf_compact_staleness_spec` →
//! `engine_ingest_residue_spec` (the watermark residue property) and
//! `fuzz_stress_spec` → `engine_active_fuzz_spec` (the randomised PRNG fuzz of
//! `parse_header` + the active ingest framer). The other three
//! (`sole_path_compile_fail`, `error_arena_staleness_spec`,
//! `session_params_staleness_spec`) had no surviving property to port.

#![allow(
    clippy::panic,
    reason = "test harness — a missing manifest family is the loud test-failure signal, not a production fallback; integration-test bodies are not in `#[test]` context so the in-tests carve-out cannot reach these asserts"
)]

use std::path::{Path, PathBuf};

/// The seven families the manifest must always list. Pinned here so a manifest
/// edit that silently drops a line is caught even if the dropped file still
/// happens to exist on disk.
const REQUIRED_FAMILIES: &[&str] = &[
    "scram_fuzz_spec",
    "scram_zeroize_miri_spec",
    "zeroize_coverage_spec",
    "footprint_drift_compile_fail",
    "secret_bounded_str_spec",
    "engine_ingest_residue_spec",
    "engine_active_fuzz_spec",
];

/// Walk up from this crate's manifest dir until the directory that holds
/// `crates/postgres/proto` — the repo (worktree) root.
fn repo_root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        if dir.join("crates").join("postgres").join("proto").is_dir() {
            return dir;
        }
        if !dir.pop() {
            panic!(
                "could not locate the repo root (a dir containing crates/postgres/proto) \
                 walking up from {}",
                env!("CARGO_MANIFEST_DIR")
            );
        }
    }
}

fn manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("port_manifest.list")
}

/// Parse the manifest into `(family_name, relative_path)` entries.
fn parse_manifest(body: &str) -> Vec<(String, String)> {
    let mut entries = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, path)) = line.split_once('=') else {
            panic!("malformed manifest line (expected `name = path`): {line:?}");
        };
        entries.push((name.trim().to_string(), path.trim().to_string()));
    }
    entries
}

#[test]
fn manifest_families_still_exist() {
    let root = repo_root();
    let manifest = manifest_path();
    let body = match std::fs::read_to_string(&manifest) {
        Ok(b) => b,
        Err(e) => panic!("cannot read port manifest {}: {e}", manifest.display()),
    };
    let entries = parse_manifest(&body);

    // Every listed file must still be on disk.
    for (name, rel) in &entries {
        let full = root.join(Path::new(rel));
        assert!(
            full.is_file(),
            "PORT-not-fixturize violation: family `{name}` lists `{rel}`, but that file \
             no longer exists at {}. A deletion removed a spec that must be kept in place, not \
             replaced by a corpus fixture (it asserts a memory/footprint/compile-fail \
             property the observable corpus cannot carry). Restore the file or revise the \
             manifest deliberately.",
            full.display(),
        );
    }

    // Every required family must still be listed.
    for required in REQUIRED_FAMILIES {
        assert!(
            entries.iter().any(|(name, _)| name == required),
            "PORT-not-fixturize violation: required family `{required}` is missing from \
             the manifest. It must remain listed so its spec is never swept.",
        );
    }
    assert_eq!(
        entries.len(),
        REQUIRED_FAMILIES.len(),
        "manifest family count ({}) != the {} required families — a family was added or \
         dropped without updating REQUIRED_FAMILIES.",
        entries.len(),
        REQUIRED_FAMILIES.len(),
    );
}
