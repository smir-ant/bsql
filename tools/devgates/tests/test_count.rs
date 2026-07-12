//! test_count — the doc-vs-reality test-count wall.
//!
//! `README.md` advertises how many test functions the workspace has (and how
//! many are `#[ignore]` live suites needing a database). A hard-coded number in
//! prose ROTS: every test added or removed drifts the doc away from reality, and
//! nothing catches it — `cargo build` / `clippy` / `cargo test` never read the
//! README. This gate kills the rot at the source: it runs the EXACT two count
//! commands the README cites, greps the two numbers back OUT of the README, and
//! asserts they match. A test added or removed without updating the doc turns
//! this red; a deliberate change regenerates the doc numbers in place with
//! `BSQL_TEST_COUNT_PIN=overwrite` (mirroring `TRYBUILD=overwrite` /
//! `BSQL_DEPS_PIN=overwrite`). The number can no longer silently rot.
//!
//! # Why a live-checked doc number (three options weighed)
//!
//! 1. **A committed golden file** (like `deps_pin`'s `deps_goldens`) holding the
//!    count, with the README pointing at the file instead of stating a number.
//!    Rejected: the README should SHOW the number to a reader, and a golden
//!    separate from the prose still lets the prose rot independently.
//! 2. **A `#![doc = include_str!]` / build-script splice** injecting the live
//!    count into the rendered doc. Rejected: it hides the number from the raw
//!    `README.md` a GitHub reader sees, and adds build machinery for a figure.
//! 3. **This gate.** The number lives in the README prose, in ONE place, and is
//!    checked against the live count on every `cargo test --workspace` run.
//!    Chosen: the doc stays human-readable AND cannot drift from reality. It is
//!    `publish = false` (this crate) so `deps_pin` is unchanged, and a devgate
//!    (not a runtime crate) so `runtime_graph_pin` is untouched.
//!
//! # The two counts (verbatim the commands the README documents)
//!
//! - total test functions: `#[test]` + `#[tokio::test]` attribute lines;
//! - `#[ignore]` attribute lines (the live suites).
//!
//! Both enumerate the `.rs` sources with `git ls-files` (the TRACKED set only,
//! run from the workspace root), so the count reflects the COMMITTED suite and
//! is immune to an untracked scratch test file or a sibling git worktree — the
//! former `find` walk counted every `*.rs` on disk and could be inflated by
//! either.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The README file, relative to the workspace root.
const README: &str = "README.md";

/// The count command the README cites for total test functions. Kept BYTE-FOR-
/// BYTE identical to the fenced command in the doc, so the gate measures exactly
/// what a reader reproduces.
///
/// It enumerates the `.rs` sources with `git ls-files` — the COMMITTED set only.
/// The former `find` walk counted EVERY `*.rs` on disk, so a stray untracked
/// scratch test left in a `tests/` dir (or a sibling git worktree under
/// `.claude/worktrees/`) inflated the count and turned this gate red for a file
/// that is not part of the suite (exactly what happened during audit-8, when
/// untracked `audit8_*.rs` harnesses drifted it). `git ls-files` lists only
/// TRACKED files, so the count reflects the committed suite and nothing else —
/// immune to untracked scratch AND to sibling worktrees (whose files are not in
/// this checkout's index), with no `-prune` list to keep in sync.
const TOTAL_CMD: &str = "git ls-files -z -- '*.rs' \
     | xargs -0 grep -hE '^[[:space:]]*#\\[(tokio::)?test' | wc -l";

/// The count command the README cites for `#[ignore]` live suites (same
/// tracked-only `git ls-files` enumeration as [`TOTAL_CMD`]).
const IGNORE_CMD: &str = "git ls-files -z -- '*.rs' \
     | xargs -0 grep -hE '^[[:space:]]*#\\[ignore' | wc -l";

/// Unique prose anchor preceding the total in the README; the integer that
/// follows it directly is the documented total.
const TOTAL_MARKER: &str = "Test functions: ";

/// Unique prose anchor preceding the ignore count in the README.
const IGNORE_MARKER: &str = "live suites (need a running database): ";

/// Workspace root: `CARGO_MANIFEST_DIR` is `<ws>/tools/devgates`.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root two levels above tools/devgates")
        .to_path_buf()
}

/// Run one shell count command from the workspace root and parse the integer it
/// prints (`wc -l` output, whitespace-padded).
fn run_count(root: &Path, cmd: &str) -> usize {
    let out = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .current_dir(root)
        .output()
        .expect("spawn sh for the count command");
    assert!(
        out.status.success(),
        "count command exited non-zero: {cmd}\nstderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .expect("count output is UTF-8")
        .trim()
        .parse::<usize>()
        .expect("count output is an integer")
}

/// Extract the integer that immediately follows `marker` in `content`.
fn documented(content: &str, marker: &str) -> usize {
    let idx = match content.find(marker) {
        Some(i) => i,
        None => panic!("README is missing the anchor {marker:?} — its structure changed"),
    };
    let after = &content[idx + marker.len()..];
    let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
    assert!(
        !digits.is_empty(),
        "README anchor {marker:?} is not followed by a number"
    );
    digits.parse().expect("documented count is an integer")
}

/// Replace the digit run immediately after `marker` with `live`, returning the
/// rewritten content. Used only under `BSQL_TEST_COUNT_PIN=overwrite`.
fn rewrite_after(content: &str, marker: &str, live: usize) -> String {
    let idx = match content.find(marker) {
        Some(i) => i,
        None => panic!("README is missing the anchor {marker:?} — its structure changed"),
    };
    let num_start = idx + marker.len();
    let digit_len = content[num_start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .count();
    let mut out = String::with_capacity(content.len());
    out.push_str(&content[..num_start]);
    out.push_str(&live.to_string());
    out.push_str(&content[num_start + digit_len..]);
    out
}

#[test]
fn readme_test_counts_match_live() {
    let root = workspace_root();
    let live_total = run_count(&root, TOTAL_CMD);
    let live_ignore = run_count(&root, IGNORE_CMD);
    let readme_path = root.join(README);
    let content = std::fs::read_to_string(&readme_path).expect("read README.md");

    if std::env::var("BSQL_TEST_COUNT_PIN").as_deref() == Ok("overwrite") {
        let rewritten = rewrite_after(&content, TOTAL_MARKER, live_total);
        let rewritten = rewrite_after(&rewritten, IGNORE_MARKER, live_ignore);
        std::fs::write(&readme_path, rewritten).expect("write README.md");
        eprintln!("test_count: README rewritten — total={live_total} ignore={live_ignore}");
        return;
    }

    let doc_total = documented(&content, TOTAL_MARKER);
    let doc_ignore = documented(&content, IGNORE_MARKER);

    assert_eq!(
        doc_total, live_total,
        "README says {doc_total} test functions but the live count is {live_total}; \
         update the README (or run `BSQL_TEST_COUNT_PIN=overwrite cargo test -p bsql-devgates --test test_count`)"
    );
    assert_eq!(
        doc_ignore, live_ignore,
        "README says {doc_ignore} #[ignore] live suites but the live count is {live_ignore}; \
         update the README (or run `BSQL_TEST_COUNT_PIN=overwrite cargo test -p bsql-devgates --test test_count`)"
    );
}
