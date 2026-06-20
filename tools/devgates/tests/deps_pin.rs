//! deps_pin — deterministic dependency-frontier gate.
//!
//! Pins the workspace dependency tree to a committed golden so an
//! accidental dependency addition or a version drift fails the local
//! gate set instead of landing unreviewed. Same pattern as the trybuild
//! `.stderr` goldens: an audited fact is committed; drift is a loud test
//! failure; a deliberate change is a reviewed golden diff regenerated
//! with `BSQL_DEPS_PIN=overwrite` (mirroring `TRYBUILD=overwrite`).
//!
//! Mechanism — parse `Cargo.lock`, not `cargo tree`:
//!   * Zero dependencies (std only) and zero subprocesses: the gate reads
//!     one committed file and is fully deterministic (no network, no
//!     `cargo` on PATH, no offline/locked flag dance, no machine-specific
//!     absolute-path normalization).
//!   * `Cargo.lock` IS the resolved, committed source of truth for every
//!     crate+version in the build — exactly the frontier we want pinned.
//!     Any new crate (a rogue dep, normal OR dev) appears as a new
//!     `[[package]]`; any version bump changes a `version = "..."`. Both
//!     show up as a golden diff.
//!
//! Two checks, independent:
//!   1. The (name, version) package SET matches the `lockfile_packages`
//!      golden exactly. ANY new crate or version bump (normal OR dev) is a
//!      diff. This is what catches a rogue dependency.
//!   2. The set of crates that resolve to MORE than one version (the
//!      `webpki-roots 0.26 + 1.0` duplication class) matches a separate
//!      `version_duplicates` golden. A handful of pre-existing duplicates
//!      are forced by deep transitive pins outside our control (e.g.
//!      `ring` pins `windows-sys 0.52`, `rusqlite`/`hashlink` pin
//!      `hashbrown 0.15`); recording them as a golden means those known
//!      cases do not block the build, while any NEW duplication — the very
//!      thing this slice removed for `webpki-roots` — fails loudly.

use std::path::PathBuf;

/// Locate the workspace `Cargo.lock` by walking up from this test
/// crate's manifest directory until a `Cargo.lock` is found.
fn lockfile_path() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        let candidate = dir.join("Cargo.lock");
        if candidate.is_file() {
            return candidate;
        }
        if !dir.pop() {
            panic!(
                "could not find Cargo.lock walking up from {}",
                env!("CARGO_MANIFEST_DIR")
            );
        }
    }
}

fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("deps_goldens")
        .join(format!("{name}.txt"))
}

/// Compare `actual` against the named golden. With
/// `BSQL_DEPS_PIN=overwrite` the golden is (re)written instead, mirroring
/// `TRYBUILD=overwrite`. Returns a unified-ish `+`/`-` diff on mismatch.
fn check_golden(name: &str, actual: &str) -> Result<(), String> {
    let golden = golden_path(name);
    if std::env::var("BSQL_DEPS_PIN").as_deref() == Ok("overwrite") {
        if let Some(parent) = golden.parent() {
            std::fs::create_dir_all(parent).expect("failed to create goldens dir");
        }
        std::fs::write(&golden, actual).expect("failed to write golden");
        return Ok(());
    }

    let expected = std::fs::read_to_string(&golden).map_err(|e| {
        format!(
            "missing dependency golden {} ({e}); regenerate with \
             BSQL_DEPS_PIN=overwrite cargo test -p bsql-devgates --test deps_pin",
            golden.display()
        )
    })?;

    if expected == actual {
        return Ok(());
    }

    let mut diff = String::new();
    for line in expected.lines() {
        if !actual.lines().any(|a| a == line) {
            diff.push_str("  - ");
            diff.push_str(line);
            diff.push('\n');
        }
    }
    for line in actual.lines() {
        if !expected.lines().any(|e| e == line) {
            diff.push_str("  + ");
            diff.push_str(line);
            diff.push('\n');
        }
    }
    Err(diff)
}

/// Parse every `[[package]]` block in a `Cargo.lock` body into
/// `(name, version)` pairs. Each block has its own `name = "..."` and
/// `version = "..."` line; we capture the first of each per block so the
/// order of `name`/`version`/`source`/`checksum` lines does not matter.
fn parse_packages(lock: &str) -> Vec<(String, String)> {
    fn unquote(line: &str, key: &str) -> Option<String> {
        let rest = line.strip_prefix(key)?.trim_start();
        let rest = rest.strip_prefix('=')?.trim();
        let inner = rest.strip_prefix('"')?.strip_suffix('"')?;
        Some(inner.to_string())
    }

    /// Emit the just-parsed block's pair (if both fields were seen) and
    /// reset the per-block accumulators for the next block.
    fn flush(
        name: &mut Option<String>,
        version: &mut Option<String>,
        out: &mut Vec<(String, String)>,
    ) {
        if let (Some(n), Some(v)) = (name.take(), version.take()) {
            out.push((n, v));
        }
    }

    let mut packages = Vec::new();
    let mut in_pkg = false;
    let mut name: Option<String> = None;
    let mut version: Option<String> = None;

    for line in lock.lines() {
        let trimmed = line.trim();
        if trimmed == "[[package]]" {
            // Close any in-progress block before opening a new one.
            if in_pkg {
                flush(&mut name, &mut version, &mut packages);
            }
            in_pkg = true;
            continue;
        }
        if !in_pkg {
            continue;
        }
        if trimmed.starts_with('[') {
            // A non-`[[package]]` table ends the package section.
            flush(&mut name, &mut version, &mut packages);
            in_pkg = false;
            continue;
        }
        if name.is_none()
            && let Some(n) = unquote(trimmed, "name")
        {
            name = Some(n);
        }
        if version.is_none()
            && let Some(v) = unquote(trimmed, "version")
        {
            version = Some(v);
        }
    }
    if in_pkg {
        flush(&mut name, &mut version, &mut packages);
    }

    packages.sort();
    packages.dedup();
    packages
}

fn render(packages: &[(String, String)]) -> String {
    let mut s = String::new();
    for (n, v) in packages {
        s.push_str(n);
        s.push(' ');
        s.push_str(v);
        s.push('\n');
    }
    s
}

/// All crates that resolve to more than one version, rendered one per
/// line as `name v1, v2, ...` (versions sorted). Empty string when the
/// graph is duplicate-free.
fn duplicate_versions(packages: &[(String, String)]) -> String {
    let mut out = String::new();
    let mut i = 0;
    while i < packages.len() {
        let name = &packages[i].0;
        let mut versions = vec![packages[i].1.as_str()];
        let mut j = i + 1;
        while j < packages.len() && &packages[j].0 == name {
            versions.push(packages[j].1.as_str());
            j += 1;
        }
        if versions.len() > 1 {
            out.push_str(name);
            out.push(' ');
            out.push_str(&versions.join(", "));
            out.push('\n');
        }
        i = j;
    }
    out
}

/// Read and parse the workspace `Cargo.lock` into its sorted, deduped
/// (name, version) package set.
fn lock_packages() -> Vec<(String, String)> {
    let lock = std::fs::read_to_string(lockfile_path()).expect("failed to read Cargo.lock");
    parse_packages(&lock)
}

#[test]
fn dependency_frontier_is_pinned() {
    let packages = lock_packages();
    let actual = render(&packages);

    if let Err(diff) = check_golden("lockfile_packages", &actual) {
        panic!(
            "dependency frontier drift (Cargo.lock vs golden):\n{diff}\n\
             A `-` line was removed from the build, a `+` line was added. If \
             this change is intentional, regenerate the golden deliberately:\n  \
             BSQL_DEPS_PIN=overwrite cargo test -p bsql-devgates --test deps_pin\n\
             and justify the dependency change in the root Cargo.toml policy block."
        );
    }
}

#[test]
fn version_duplicates_are_pinned() {
    let packages = lock_packages();
    let actual = duplicate_versions(&packages);

    if let Err(diff) = check_golden("version_duplicates", &actual) {
        panic!(
            "version-duplication drift (a crate now resolves to a different \
             SET of versions than the golden allows — the `webpki-roots 0.26 \
             + 1.0` class):\n{diff}\n\
             A `+` line is a NEW duplication: unify it on a single version \
             (usually by bumping the direct dependency that pins the older \
             line). A `-` line means a known duplicate was resolved — good; \
             record it by regenerating the golden:\n  \
             BSQL_DEPS_PIN=overwrite cargo test -p bsql-devgates --test deps_pin"
        );
    }
}
