//! cross_platform — the Windows/Linux cross-target regression wall.
//!
//! A prior fix made the two PostgreSQL drivers compile on non-unix targets by
//! `#[cfg(unix)]`-gating the unix-domain-socket transport arm
//! (`std::os::unix::net::UnixStream` in each driver's `transport.rs`) and adding
//! a 64-bit `compile_error!`. There is **no CI** here (by owner mandate), so
//! nothing guards that fix: a future unconditional `use std::os::unix::…`, or a
//! non-64-bit assumption, would silently re-break Windows and no routine check on
//! the dev's macOS/Linux host would notice. This gate closes that hole LOCALLY.
//!
//! # What it proves
//!
//! For each shipped PostgreSQL driver, a `cargo check` for a cross target
//! (`x86_64-pc-windows-msvc`, `x86_64-unknown-linux-gnu`) succeeds. An ungated
//! `use std::os::unix::…` fails that check with `E0433` (the `std::os::unix`
//! module does not exist on Windows) — the exact regression the `#[cfg(unix)]`
//! gate prevents. Proven RED->GREEN by temporarily adding an unconditional
//! `use std::os::unix::net::UnixListener;` to a driver's `transport.rs`: the
//! windows-msvc check turns red (`E0433`), and reverting restores green.
//!
//! # Scope: `--no-default-features` (the pure-Rust surface)
//!
//! The default `tls` feature pulls `ring`, which compiles C — cross-compiling
//! that needs a C cross-toolchain for the target, which a bare `rustup target
//! add` does NOT install. So this gate checks the drivers with
//! `--no-default-features`, exercising the pure-Rust cross-platform surface
//! (transport gating, footprint pins, tokio/socket2/mio) WITHOUT any C compile.
//! The `#[cfg(unix)]` transport gate is feature-independent (it is gated on the
//! TARGET, not a cargo feature), so `--no-default-features` still compiles
//! `transport.rs` and would catch an ungated unix import. Cross-compiling with
//! `tls` ON additionally needs the target's C toolchain (for `ring`); that is
//! documented in `CLAUDE.md`/`README.md` under *Platform support*, not gated
//! here.
//!
//! `cargo check` (not `build`) is deliberate: `check` emits only metadata and
//! never LINKS, so it needs no target linker (no MSVC `link.exe` on macOS/Linux)
//! — only the target's prebuilt `std`, which `rustup target add <triple>`
//! provides. That is what makes a Windows regression catchable from a unix host.
//!
//! # No-CI, single-dev reality: skip-when-absent, never a false red
//!
//! A developer who has not run `rustup target add x86_64-pc-windows-msvc` must
//! NOT get a red gate — mirroring how the live `--ignored` suites skip without a
//! database. So the gate PROBES `rustup target list --installed` and, for a
//! target that is absent (or if `rustup` itself is unavailable), SKIPS it with an
//! `eprintln!` note and passes. It FAILS only when a target IS installed and its
//! check fails — i.e. a real, reproducible regression on this machine. On a host
//! with the Windows and/or Linux target installed (the intended setup), the gate
//! actively guards every `cargo test --workspace`; elsewhere it is an instant
//! pass.

use std::path::PathBuf;
use std::process::Command;

/// The cross targets guarded. Both are 64-bit (the only supported width — see the
/// `target_pointer_width = "64"` `compile_error!` in each driver's `lib.rs`).
/// `windows-msvc` is the primary regression surface (no `std::os::unix`);
/// `linux-gnu` guards a from-macOS build of the common deploy target.
const CROSS_TARGETS: &[&str] = &["x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"];

/// The shipped crates whose cross-platform surface this guards: the two drivers
/// own `transport.rs` (the `#[cfg(unix)]` unix-socket arm). Checking them
/// transitively checks `bsql-postgres-core` and `bsql-postgres-proto`.
const DRIVER_CRATES: &[&str] = &["bsql-postgres-async", "bsql-postgres-sync"];

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

/// The set of targets `rustup` reports as installed for the ACTIVE toolchain
/// (the workspace's pinned 1.96.0, since the command runs in the workspace dir
/// and respects `rust-toolchain.toml`). `None` when `rustup` is unavailable — a
/// non-rustup environment cannot have added a cross target, so the whole gate
/// then skips.
fn installed_targets(root: &PathBuf) -> Option<Vec<String>> {
    let output = Command::new("rustup")
        .current_dir(root)
        .args(["target", "list", "--installed"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    Some(text.lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect())
}

/// Run `cargo check -p <crate> --target <target> --no-default-features` in a
/// dedicated target dir. Returns `Ok(())` on success, `Err(stderr)` on failure.
fn cross_check(root: &PathBuf, crate_name: &str, target: &str) -> Result<(), String> {
    // A dedicated CARGO_TARGET_DIR so this nested `cargo check` does not contend
    // for the parent `cargo test` build lock (same isolation as `doc_links`).
    // ONE dir shared across targets/crates: the `--target <triple>` subdir keeps
    // each target's artifacts separate, and the checks run sequentially here (one
    // `#[test]`), so there is no concurrent-build-lock contention within it.
    let target_dir = root.join("target").join("devgate-cross");
    let output = Command::new(cargo_bin())
        .current_dir(root)
        .args(["check", "-p", crate_name, "--target", target, "--no-default-features"])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()
        .map_err(|e| format!("failed to spawn `cargo check` for {crate_name}/{target}: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).into_owned())
    }
}

#[test]
fn shipped_drivers_compile_for_cross_targets() {
    let root = workspace_root();

    let Some(installed) = installed_targets(&root) else {
        eprintln!(
            "cross_platform: `rustup target list --installed` unavailable (no rustup on \
             PATH?) — SKIPPING the cross-target regression gate. This is a pass: a \
             non-rustup environment cannot have added a cross target. To enforce it, \
             install rustup and `rustup target add x86_64-pc-windows-msvc`."
        );
        return;
    };

    let mut checked_any = false;
    let mut failures = String::new();

    for &target in CROSS_TARGETS {
        if !installed.iter().any(|t| t == target) {
            eprintln!(
                "cross_platform: target `{target}` not installed — SKIPPING (pass). \
                 Add it with `rustup target add {target}` to enforce the \
                 cross-platform regression guard on this machine."
            );
            continue;
        }
        for &crate_name in DRIVER_CRATES {
            checked_any = true;
            if let Err(stderr) = cross_check(&root, crate_name, target) {
                failures.push_str(&format!(
                    "\n=== `{crate_name}` failed `cargo check --target {target} \
                     --no-default-features` ===\n{stderr}\n"
                ));
            }
        }
    }

    if !checked_any {
        eprintln!(
            "cross_platform: no cross target installed — the gate ran nothing (pass). \
             `rustup target add x86_64-pc-windows-msvc` (and/or \
             x86_64-unknown-linux-gnu) to activate it."
        );
    }

    assert!(
        failures.is_empty(),
        "a shipped PostgreSQL driver no longer cross-compiles for an INSTALLED target:\n{failures}\n\
         The classic cause is an unconditional `use std::os::unix::…` (or another \
         unix-only item) reachable on a non-unix target — `E0433` above. The \
         unix-domain-socket transport arm must stay behind `#[cfg(unix)]` (see each \
         driver's `transport.rs`); a unix-socket host on a non-unix target is \
         rejected at connect with `UNIX_SOCKET_UNSUPPORTED`, never a compile of a \
         unix item. (Cross-compiling with the default `tls` feature ON additionally \
         needs the target's C toolchain for `ring`; this gate scopes to \
         `--no-default-features` on purpose — see the module docs.)"
    );
}
