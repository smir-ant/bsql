//! ASM-identity gate for the zero-cost observer seam.
//!
//! Proves the [`NoObserver`] policy is genuinely free: an observer hook
//! reached through the generic [`Observer`] seam and monomorphised at the
//! `NoObserver` ZST
//! (`engine::seams::engine_observe_via_seam`) lowers to the *same machine
//! instructions* as a hand-written baseline with no seam at all
//! (`engine::seams::engine_observe_no_seam`).
//!
//! Reuses the project asm machinery: it emits release assembly for the
//! `bsql-postgres-proto` lib (the same `cargo rustc --release --emit=asm`
//! the `scripts/asm-dump.sh` foundation tool uses), then extracts and
//! compares the two `#[inline(never)]` witness bodies. A seam that secretly
//! did work would add instructions and diverge here.
//!
//! [`NoObserver`]: bsql_postgres_proto::engine::NoObserver
//! [`Observer`]: bsql_postgres_proto::engine::Observer

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::Command;

#[test]
fn no_observer_seam_is_asm_identical_to_no_seam_baseline() {
    // Workspace root: CARGO_MANIFEST_DIR is `<ws>/crates/postgres/proto`.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest
        .ancestors()
        .nth(3)
        .expect("workspace root three levels above the proto crate")
        .to_path_buf();

    // Honour CARGO_TARGET_DIR; otherwise the default `<ws>/target`.
    let target_dir = match std::env::var_os("CARGO_TARGET_DIR") {
        Some(v) if !v.is_empty() => PathBuf::from(v),
        _ => workspace.join("target"),
    };

    let cargo = match std::env::var_os("CARGO") {
        Some(c) => c,
        None => std::ffi::OsString::from("cargo"),
    };

    // Emit release assembly for the proto lib — identical invocation to the
    // project's asm-dump foundation tool.
    let status = Command::new(&cargo)
        .current_dir(&workspace)
        .args([
            "rustc",
            "-p",
            "bsql-postgres-proto",
            "--release",
            "--lib",
            "--",
            "--emit=asm",
            "-C",
            "debuginfo=0",
        ])
        .status()
        .expect("spawn cargo rustc --emit=asm");
    assert!(status.success(), "cargo rustc --emit=asm failed");

    // Newest emitted assembly file for the lib.
    let deps = target_dir.join("release").join("deps");
    let asm_path = std::fs::read_dir(&deps)
        .expect("read target/release/deps")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| match p.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.starts_with("bsql_postgres_proto-") && name.ends_with(".s"),
            None => false,
        })
        .filter_map(|p| {
            let mtime = p.metadata().and_then(|m| m.modified()).ok()?;
            Some((mtime, p))
        })
        .max_by_key(|(mtime, _)| *mtime)
        .map(|(_, p)| p)
        .expect("an emitted bsql_postgres_proto-*.s file");

    let asm = std::fs::read_to_string(&asm_path).expect("read emitted .s");

    // Extract a symbol's instruction body: the lines after its definition
    // label (a line ending `:` that names the symbol) up to the blank line
    // that follows its `ret`. The label line itself — the only part that
    // differs between the two functions — is dropped.
    let extract = |name: &str| -> Option<String> {
        let mut lines = asm.lines();
        let mut found = false;
        for l in lines.by_ref() {
            let t = l.trim_end();
            if t.ends_with(':') && t.contains(name) {
                found = true;
                break;
            }
        }
        if !found {
            return None;
        }
        let mut body = String::new();
        for l in lines.by_ref() {
            if l.trim().is_empty() {
                break;
            }
            body.push_str(l.trim_end());
            body.push('\n');
        }
        if body.is_empty() {
            None
        } else {
            Some(body)
        }
    };

    let via = extract("engine_observe_via_seam")
        .expect("engine_observe_via_seam body in emitted assembly");
    let base = extract("engine_observe_no_seam")
        .expect("engine_observe_no_seam body in emitted assembly");

    assert_eq!(
        via, base,
        "NoObserver seam is NOT instruction-for-instruction identical to the \
         no-seam baseline.\n--- via NoObserver seam ---\n{via}\n--- no-seam baseline ---\n{base}"
    );
}
