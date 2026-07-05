//! Codegen-stability gate for the inbound hot dispatch
//! [`ActiveEngine::next_event`].
//!
//! `next_event` is the pull-cursor that every driver verb turns socket bytes
//! through: it frames one inbound message and classifies it into a borrowing
//! [`Event`]. It is THE inbound hot path. Its per-row steady-state cost is
//! already pinned two ways — the warm cache-HIT round-trip is proven
//! *zero-allocation* by `engine_query_alloc` (which drives `query_params`
//! through `pump_active`, and `pump_active` surfaces every row by calling
//! `next_event`), and its ns/op is tracked by the `ingest/framing_loop` bench.
//! What NEITHER of those pins is the *compiled shape* of the dispatch itself: a
//! future edit could bloat it, newly-inline a cold helper into the hot frame,
//! or slip a panic / unwind edge into it and every existing gate would stay
//! green. This gate closes that hole by inspecting the machine code directly.
//!
//! # What is actually in `next_event`'s compiled body
//!
//! `next_event` inlines the framing + dispatch tree (`drive`, `step_frame`, and
//! the HOT per-state step fns), so its body is NOT uniformly zero-alloc /
//! panic-free — and a gate that pretended otherwise would be a false claim. The
//! COLD one-time-per-query setup handlers (the oversize machinery, and the fused
//! runtime-param setup chain `step_fused`) are `#[inline(never)]`, so they live
//! OUT of this frame — the extraction stops at their adjacent label (see
//! `extract_body`), pinning next_event's OWN hot body. Measured at the pinned
//! toolchain, that body contains:
//!
//! * **NO** call into `core::panicking` (`panic`, `panic_bounds_check`,
//!   `panic_fmt`, …), **NO** `rust_begin_unwind`, and **NO** `_Unwind_Resume`
//!   landing-pad edge. Every slice access the dispatch performs is
//!   provably in-bounds, so the optimiser elided every bounds-check panic.
//!   This is the property this gate PROVES — it is strictly stronger than the
//!   source-level `deny(clippy::indexing_slicing)` / `deny(clippy::panic)`
//!   floor, because it holds on the *emitted instructions* after inlining, not
//!   on the syntax.
//! * A handful of `bl` calls to allocation helpers (`__rust_alloc_zeroed`,
//!   a `RawVec` reserve, `__rust_dealloc`, `handle_alloc_error`) and one
//!   `copy_from_slice` length guard (`len_mismatch_fail`). These live ONLY in
//!   the COLD control-frame branches: parsing a `RowDescription` (which must
//!   allocate to own the column OIDs and names) and buffering an oversize frame
//!   (a body larger than the inline ingest tier). They are NOT on the hot
//!   DataRow arm — that arm's zero-allocation is the thing `engine_query_alloc`
//!   proves. This gate therefore does NOT forbid the allocation family (doing
//!   so would be red on correct code); the cold allocations are legitimate and
//!   the hot arm is covered elsewhere.
//!
//! # The three robust properties pinned here
//!
//! 1. **No reachable panic / unwind edge** — the panic/unwind symbol family
//!    above must have ZERO occurrences in the body. A regression that
//!    reintroduced an un-elidable bounds check (`arr[i]` on an unproven index),
//!    a fallible `unwrap`, or an unwind edge would make one of these symbols
//!    appear and turn this gate red. Machine-proof, not syntax-proof.
//! 2. **Instruction-count ceiling** — the body must compile to no more than a
//!    committed ceiling. A ceiling fails only on real GROWTH (bloat, a cold
//!    helper newly inlined into the hot frame, a slipped-in branch), never on
//!    an unrelated codegen shift, and it is deterministic on the pinned
//!    toolchain. The ceiling is a golden regenerated with
//!    `BSQL_HOTPATH_PIN=overwrite` (mirroring `TRYBUILD=overwrite` /
//!    `BSQL_DEPS_PIN=overwrite`); a deliberate change is a reviewed one-line
//!    golden diff.
//! 3. **Cold helpers stay outlined** — the cold dispatch helpers the hot-frame
//!    design relies on being SEPARATE (the fused runtime-param setup chain and
//!    the oversize machinery) must each remain their OWN mangled symbol. This is
//!    the structural companion to the ceiling: a fixed margin cannot distinguish
//!    a cold helper emitted ADJACENT to the hot frame (fine) from one INLINED
//!    into it (the regression), because the margin can absorb a small helper's
//!    inlined instructions and pass green. Inlining a helper makes its definition
//!    label VANISH, failing this check independent of the margin — and it
//!    validates the `extract_body` boundary, which pins next_event's OWN body
//!    only because those helpers are distinct symbols it stops at.
//!
//! # Why properties, not an exact-asm golden
//!
//! An exact byte-for-byte body golden is deliberately NOT used for a
//! 1000-instruction dispatch: it would break on any unrelated
//! instruction-scheduling shift and teach reviewers to rubber-stamp the diff.
//! Robust *properties* (no panic edge; a growth ceiling; outlined cold helpers)
//! fail only on a real regression.
//!
//! # Machinery
//!
//! Reuses the project asm foundation (`scripts/asm-dump.sh`): emit release
//! assembly for the proto lib with
//! `cargo rustc --release --lib -- --emit=asm`, find the newest emitted `.s`,
//! and extract `next_event`'s instruction body between its definition label and
//! its `.cfi_endproc`. The nested build writes to a DEDICATED `CARGO_TARGET_DIR`
//! (like the `doc_links` gate) so it never contends with the parent
//! `cargo test` for the shared target lock, keeping the gate deterministic on a
//! clean rebuild. No new dependency — std plus the asm machinery only.
//!
//! [`ActiveEngine::next_event`]: bsql_postgres_proto::engine::ActiveEngine
//! [`Event`]: bsql_postgres_proto::engine::Event

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "asm-inspection gate — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use std::path::PathBuf;
use std::process::Command;

/// The mangled-symbol infix that uniquely names `ActiveEngine::next_event` in
/// the emitted assembly. The crate also emits `IngestBuf::next_event`, so the
/// owning type is part of the match to pick the right symbol.
const NEXT_EVENT_SYMBOL_INFIX: &str = "dispatch_active12ActiveEngine10next_event";

/// The panic / unwind symbol family that is machine-provably ABSENT from
/// `next_event`'s compiled body. Each substring must occur ZERO times. See the
/// module docs for why the allocation family (present in cold control-frame
/// branches) is deliberately NOT in this set.
const FORBIDDEN_PANIC_UNWIND_SYMBOLS: &[&str] = &[
    // core::panicking::{panic, panic_bounds_check, panic_fmt, panic_nounwind, …}
    // — the panic entry points every reachable `panic!` / bounds check routes
    // through. `len_mismatch_fail` (a `core::slice` helper, cold branch) is a
    // distinct symbol and holds its own panic OUT of this frame, so it does not
    // match here (see the module docs).
    "panicking",
    // The unwind landing-pad edge a `panic=unwind` frame would carry.
    "_Unwind_Resume",
    // rust_begin_unwind — the panic runtime entry the std hook installs.
    "begin_unwind",
    // The abort shim for a nounwind context.
    "panic_cannot_unwind",
];

/// The cold dispatch helpers the hot-frame design RELIES on staying OUT of
/// `next_event`'s frame — each must remain its OWN mangled symbol in the emitted
/// assembly. If any is inlined into `next_event`, its definition label VANISHES
/// and the [`cold_helpers_stay_outlined`] check fails — independent of the
/// instruction-count margin, which cannot distinguish "cold code emitted ADJACENT
/// to the hot frame" (fine) from "cold code INLINED into the hot frame" (the
/// regression). This is the structural half of the ceiling gate.
///
/// It also validates [`extract_body`]'s boundary assumption: the extraction stops
/// at the next mangled-symbol label, which only pins `next_event`'s OWN body when
/// these helpers are DISTINCT symbols. If a helper folded into `next_event`, both
/// the isolation and this check would be wrong together — so pinning their
/// separateness keeps the measurement honest.
///
/// Each string is the mangled-symbol infix (`…dispatch_active12ActiveEngine<len><name>`)
/// that uniquely names one helper's definition label, mirroring
/// [`NEXT_EVENT_SYMBOL_INFIX`]. `<len>` is the identifier's byte length in the
/// legacy Rust mangling.
const COLD_HELPER_SYMBOL_INFIXES: &[&str] = &[
    // The fused runtime-param setup chain (Parse/Bind/Describe-portal acks): the
    // one-time-per-query cold setup that must not bleed into the per-row frame.
    "dispatch_active12ActiveEngine10step_fused",
    // The oversize machinery: rare control-frame paths kept off the DataRow arm.
    "dispatch_active12ActiveEngine14begin_oversize",
    "dispatch_active12ActiveEngine13step_oversize",
    "dispatch_active12ActiveEngine21append_oversize_accum",
    "dispatch_active12ActiveEngine29dispatch_accumulated_row_desc",
];

/// The `cargo` binary Cargo hands its child test processes; fall back to the
/// PATH-resolved `cargo` when run outside that context.
fn cargo_bin() -> std::ffi::OsString {
    match std::env::var_os("CARGO") {
        Some(c) => c,
        None => std::ffi::OsString::from("cargo"),
    }
}

/// Workspace root: `CARGO_MANIFEST_DIR` is `<ws>/crates/postgres/proto`.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("workspace root three levels above the proto crate")
        .to_path_buf()
}

/// Path to the committed instruction-count ceiling golden.
fn ceiling_golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("hotpath_goldens")
        .join("next_event_insn_ceiling.txt")
}

/// Emit release assembly for the proto lib into a dedicated target dir and
/// return the newest emitted `.s` path.
fn emit_proto_asm() -> PathBuf {
    let workspace = workspace_root();

    // A dedicated target dir so the nested build does not contend with the
    // parent `cargo test` for the shared target lock. Based on any outer
    // `CARGO_TARGET_DIR` so a caller-overridden target base still isolates.
    let base = match std::env::var_os("CARGO_TARGET_DIR") {
        Some(v) if !v.is_empty() => PathBuf::from(v),
        _ => workspace.join("target"),
    };
    let asm_target = base.join("hotpath-codegen-asm");

    // Identical invocation to the project's asm-dump foundation tool.
    let status = Command::new(cargo_bin())
        .current_dir(&workspace)
        .env("CARGO_TARGET_DIR", &asm_target)
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

    let deps = asm_target.join("release").join("deps");
    std::fs::read_dir(&deps)
        .expect("read <dedicated>/release/deps")
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
        .expect("an emitted bsql_postgres_proto-*.s file")
}

/// Extract a function's instruction body: the lines after its definition label
/// (a line ending `:` that names the symbol — NOT the `.globl` directive, which
/// ends in the mangled `E`) up to the FIRST of two terminators — its
/// `.cfi_endproc`, OR the definition label of the NEXT top-level function.
///
/// The next-label terminator is what keeps the measurement pinned to THIS
/// function's own body when the toolchain outlines a `#[inline(never)]` cold
/// callee ADJACENT to it under a SHARED CFI region (LLVM emits the outlined body
/// right after the caller with NO intervening `.cfi_endproc`, so a
/// `.cfi_endproc`-only stop would fold the sibling's instructions into this
/// function's count). A function's own body ends where the next symbol's
/// definition label begins, so that label is the tighter, correct bound; the
/// `.cfi_endproc` stop still applies for the common case where no sibling is
/// outlined adjacently. Both are unambiguous for a large jump-table body — a
/// jump table's labels are local (`LJTI*`/`LBB*`/`Lloh*`, no mangled `_ZN`), so
/// they never trip the next-function-label stop.
fn extract_body<'a>(asm: &'a str, symbol_infix: &str) -> Vec<&'a str> {
    let mut lines = asm.lines();
    let mut found = false;
    for l in lines.by_ref() {
        let t = l.trim_end();
        if t.ends_with(':') && t.contains(symbol_infix) {
            found = true;
            break;
        }
    }
    assert!(
        found,
        "definition label for `{symbol_infix}` not found in the emitted assembly"
    );
    let mut body = Vec::new();
    for l in lines.by_ref() {
        if l.trim_start().starts_with(".cfi_endproc") {
            return body;
        }
        // The next top-level function's definition label bounds this body even
        // when no `.cfi_endproc` separates them (an adjacent outlined cold
        // callee). A definition label is a mangled Rust symbol (`_ZN…`) ending in
        // `:`; local labels (`LBB`/`Lloh`/`LJTI…`) carry no `_ZN` and the `.globl`
        // directive does not end in `:`, so neither is mistaken for one.
        let t = l.trim_end();
        if t.ends_with(':') && t.contains("_ZN") {
            return body;
        }
        body.push(l);
    }
    panic!("no `.cfi_endproc` closed the body of `{symbol_infix}`");
}

/// Whether `symbol_infix` appears as a DEFINITION label — a line ending in `:`
/// containing the infix (NOT a `.globl` directive, which ends in the mangled `E`,
/// and NOT a `bl <sym>` call operand, which does not end in `:`). Used to assert a
/// cold helper is still emitted as its OWN symbol rather than inlined away.
fn symbol_is_defined(asm: &str, symbol_infix: &str) -> bool {
    asm.lines().any(|l| {
        let t = l.trim_end();
        t.ends_with(':') && t.contains(symbol_infix)
    })
}

/// Count real instructions in an extracted body: non-empty lines whose trimmed
/// form is neither an assembler directive (`.` prefix) nor a label (`:` suffix).
/// This drops `.cfi_*`, `.p2align`, `Lloh*`/`LBB*` labels, and blank lines,
/// leaving only mnemonics.
fn instruction_count(body: &[&str]) -> usize {
    body.iter()
        .map(|l| l.trim())
        .filter(|t| !t.is_empty() && !t.starts_with('.') && !t.ends_with(':'))
        .count()
}

/// Read the committed ceiling; with `BSQL_HOTPATH_PIN=overwrite` (re)write it to
/// `measured + MARGIN` instead, mirroring `TRYBUILD=overwrite`. The margin gives
/// a fixed headroom so minor unrelated codegen jitter on the pinned toolchain
/// does not churn the golden, while a real bloat still trips the ceiling.
const CEILING_MARGIN: usize = 64;

fn resolve_ceiling(measured: usize) -> usize {
    let golden = ceiling_golden_path();
    if std::env::var("BSQL_HOTPATH_PIN").as_deref() == Ok("overwrite") {
        let ceiling = measured + CEILING_MARGIN;
        if let Some(parent) = golden.parent() {
            std::fs::create_dir_all(parent).expect("create hotpath goldens dir");
        }
        std::fs::write(&golden, format!("{ceiling}\n")).expect("write ceiling golden");
        return ceiling;
    }
    let raw = match std::fs::read_to_string(&golden) {
        Ok(s) => s,
        Err(e) => panic!(
            "missing instruction-count ceiling golden {} ({e}); regenerate with \
             BSQL_HOTPATH_PIN=overwrite cargo test -p bsql-postgres-proto \
             --test engine_hotpath_codegen",
            golden.display()
        ),
    };
    raw.trim()
        .parse()
        .expect("ceiling golden holds a single usize")
}

#[test]
fn next_event_is_panic_free_and_within_the_instruction_ceiling() {
    let asm_path = emit_proto_asm();
    let asm = std::fs::read_to_string(&asm_path).expect("read emitted .s");
    let body = extract_body(&asm, NEXT_EVENT_SYMBOL_INFIX);
    let joined = body.join("\n");

    // ---- Property 1: no reachable panic / unwind edge. ----
    for sym in FORBIDDEN_PANIC_UNWIND_SYMBOLS {
        let hits = joined.matches(sym).count();
        assert_eq!(
            hits, 0,
            "ActiveEngine::next_event's compiled body references `{sym}` \
             ({hits} time(s)) — a panic or unwind edge reached the inbound hot \
             dispatch. This is stronger than the source-level lint floor: it \
             means the optimiser could NOT elide a bounds check / panic in the \
             emitted instructions. Prove the access in-bounds (`get` / \
             `first_chunk`) or remove the fallible operation from the hot frame."
        );
    }

    // ---- Property 2: instruction-count ceiling. ----
    let measured = instruction_count(&body);
    let ceiling = resolve_ceiling(measured);
    assert!(
        measured <= ceiling,
        "ActiveEngine::next_event compiled to {measured} instructions, over the \
         ceiling of {ceiling}. The inbound hot dispatch grew (bloat, a cold \
         helper newly inlined into the hot frame, or a slipped-in branch). If \
         this growth is intended and reviewed, regenerate the ceiling with \
         BSQL_HOTPATH_PIN=overwrite cargo test -p bsql-postgres-proto --test \
         engine_hotpath_codegen (the new number lands as a golden diff)."
    );

    // ---- Property 3: the cold helpers stay OUTLINED. ----
    //
    // The margin-based ceiling cannot distinguish a cold helper emitted ADJACENT
    // to next_event (fine) from one INLINED into its hot frame (the regression the
    // ceiling docs advertise catching): CEILING_MARGIN absorbs a small helper's
    // inlined instructions, so an `#[inline(always)]` slip could keep the count
    // under the golden and pass GREEN. This structural check closes that hole —
    // each helper the hot-frame design relies on being separate must still be its
    // OWN mangled symbol. Inlining one makes its definition label VANISH, failing
    // this assertion independent of the instruction margin, AND invalidating the
    // extract_body boundary (which pins next_event's own body only because these
    // helpers are distinct symbols the extraction stops at).
    for infix in COLD_HELPER_SYMBOL_INFIXES {
        assert!(
            symbol_is_defined(&asm, infix),
            "cold helper `{infix}` has no definition label in the emitted assembly \
             — it was inlined into ActiveEngine::next_event's hot frame (or renamed). \
             The hot-frame design REQUIRES it stay outlined so its one-time-per-query \
             / rare-control-frame instructions never ride the per-row DataRow arm, \
             and so the ceiling gate measures next_event's own body in isolation. \
             Restore `#[inline(never)]` on it (or update this infix if it was \
             deliberately renamed)."
        );
    }
}
