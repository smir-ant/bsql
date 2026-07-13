//! Peak-RSS regression gate.
//!
//! Turns the "bsql runs its 10k-SELECT + 1k-INSERT workload under 2 MB" headline
//! into a regression-gated number: it runs the real measured `rss_*` binaries in
//! fresh processes, reads their reported peak resident bytes, and fails if bsql's
//! footprint regresses past a committed ceiling.
//!
//! Live gate — needs a local PostgreSQL with `bench/setup/pg_setup.sql` applied,
//! so it is `#[ignore]` (like the driver live tests). Run it in RELEASE (the
//! ceiling is for the optimized build — the profile the headline number was
//! measured in):
//!
//! ```text
//!   cargo test --release --test rss_ceiling -- --ignored --nocapture
//! ```
//!
//! Regenerate a ceiling ON PURPOSE (a real, reviewed footprint change) by
//! editing the constant below in the same commit that moves the footprint —
//! mirroring how the alloc-count and asm goldens are regenerated deliberately.

use std::process::Command;

/// Peak-RSS ceiling for the BLOCKING driver's workload, in bytes.
///
/// Measured at ~1.73 MB (aarch64-apple-darwin, rustc 1.96.0, release+LTO). The
/// ceiling is 2 MiB exactly: it defends the sub-2MB headline and fails on any
/// real regression (a leak, an unbounded buffer, a per-row retention bug) while
/// tolerating the ±1-page measurement granularity. RSS reflects touched pages,
/// not scheduling, so this number is stable regardless of machine load.
const CEILING_BSQL_SYNC: u64 = 2 * 1024 * 1024;

/// Peak-RSS ceiling for the ASYNC driver's workload, in bytes.
///
/// Measured at ~1.92 MB — the blocking figure plus the current-thread tokio
/// runtime's resident cost. Ceiling 2.25 MiB.
const CEILING_BSQL_ASYNC: u64 = 2_359_296;

/// Run a compiled bench binary in a fresh process and return its reported peak
/// RSS in bytes (parsed from the `PEAK_RSS_BYTES <n>` line). Panics with a
/// diagnostic if the binary fails — the usual cause is PostgreSQL not running or
/// `bench/setup/pg_setup.sql` not applied.
fn measure(bin_path: &str, label: &str) -> u64 {
    let out = Command::new(bin_path)
        .output()
        .unwrap_or_else(|e| panic!("spawn {label} ({bin_path}): {e}"));
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "{label} exited {:?}.\n\
         Is PostgreSQL up and `bench/setup/pg_setup.sql` applied?\n\
         stdout:\n{stdout}\nstderr:\n{stderr}",
        out.status.code()
    );
    let bytes = stdout
        .lines()
        .find_map(|l| l.strip_prefix("PEAK_RSS_BYTES "))
        .and_then(|n| n.trim().parse::<u64>().ok())
        .unwrap_or_else(|| panic!("{label}: no PEAK_RSS_BYTES line in:\n{stdout}"));
    assert!(bytes > 0, "{label}: getrusage returned 0 — measurement failed");
    bytes
}

/// Take the MAX peak-RSS over three fresh runs — robust to a one-off page
/// wobble, and peak RSS is monotone-ish (touched pages), so the max is the
/// conservative figure to gate against.
fn measure_max(bin_path: &str, label: &str) -> u64 {
    (0..3).map(|_| measure(bin_path, label)).max().unwrap_or(0)
}

#[test]
#[ignore = "needs local PostgreSQL with bench/setup/pg_setup.sql applied"]
fn bsql_sync_peak_rss_under_ceiling() {
    let bin = env!("CARGO_BIN_EXE_rss_bsql_sync");
    let peak = measure_max(bin, "rss_bsql_sync");
    eprintln!(
        "bsql_sync peak RSS: {peak} bytes ({:.2} MiB), ceiling {CEILING_BSQL_SYNC} ({:.2} MiB)",
        peak as f64 / (1024.0 * 1024.0),
        CEILING_BSQL_SYNC as f64 / (1024.0 * 1024.0),
    );
    assert!(
        peak <= CEILING_BSQL_SYNC,
        "bsql_sync peak RSS {peak} exceeds ceiling {CEILING_BSQL_SYNC} — a footprint \
         regression. If deliberate, bump CEILING_BSQL_SYNC in this file in the same commit."
    );
}

#[test]
#[ignore = "needs local PostgreSQL with bench/setup/pg_setup.sql applied"]
fn bsql_async_peak_rss_under_ceiling() {
    let bin = env!("CARGO_BIN_EXE_rss_bsql_async");
    let peak = measure_max(bin, "rss_bsql_async");
    eprintln!(
        "bsql_async peak RSS: {peak} bytes ({:.2} MiB), ceiling {CEILING_BSQL_ASYNC} ({:.2} MiB)",
        peak as f64 / (1024.0 * 1024.0),
        CEILING_BSQL_ASYNC as f64 / (1024.0 * 1024.0),
    );
    assert!(
        peak <= CEILING_BSQL_ASYNC,
        "bsql_async peak RSS {peak} exceeds ceiling {CEILING_BSQL_ASYNC} — a footprint \
         regression. If deliberate, bump CEILING_BSQL_ASYNC in this file in the same commit."
    );
}
