//! Static source-guard: memset-freedom of the single-residence ingest hot
//! path.
//!
//! # The blind spot this gate closes
//!
//! The allocation bench (`engine_ingest_alloc`) proves the hot path makes
//! zero *allocations*. But a counting allocator is structurally blind to a
//! `slice.fill(0)` (or `.resize_default(..)`, or a per-read `.zeroize()`)
//! over storage the buffer *already owns*: those memset the bytes without
//! allocating. The freeze-forbidden regression — reintroducing the per-read
//! zero-fill the zero-once+watermark design exists to remove — therefore
//! would NOT allocate, so it would pass every allocation gate GREEN while
//! silently restoring exactly that recurring cost.
//!
//! Until now the no-memset property held only by code review. This gate makes
//! it a REAL gate: a pure source-text scan that fails if any memset-family
//! call appears in the ingest hot-path function bodies.
//!
//! # What it scans, and why scoping is the robustness
//!
//! It reads the ingest source at compile time via [`include_str!`] (so the
//! gate tracks the file contents at build time, never a stale runtime FS),
//! strips comments, isolates the bodies of the three hot-path functions —
//! `read_slot`, `commit`, `next_event` — and fails if any of the forbidden
//! memset-family fragments ([`FORBIDDEN`]) appears inside them.
//!
//! The scan is function-body-scoped, NOT a whole-file grep — and that is the
//! point:
//!
//! - `compact()`'s `copy_within` is a bounded *memmove* that relocates the
//!   live bytes (it never writes a constant across a region), so it is not a
//!   memset and is deliberately not forbidden. It lives in `compact`, outside
//!   the three guarded bodies, so it is never even scanned. (`read_slot`
//!   *calls* `self.compact()`; the call site is a plain call token, not a
//!   `copy_within`/`.fill` token.)
//! - The legitimate one-time `[0u8; N]` zero-fills live in `new()` and
//!   `escape()`, also outside the guarded bodies — the gate would not flag
//!   them even if `[0u8;` were forbidden, but the scoping makes the boundary
//!   explicit rather than relying on a carve-out.
//! - Comments are stripped before scanning, so a doc-comment that mentions a
//!   forbidden fragment (e.g. the rejected-alternatives prose that names
//!   `resize_default`) cannot trip the gate.
//!
//! # Regenerating after an intentional change
//!
//! There is nothing to regenerate: the gate has no golden. If a future change
//! legitimately needs one of these calls in a guarded body, the fix is to
//! change [`FORBIDDEN`] / [`GUARDED_FNS`] here with a stated reason — a
//! reviewed source edit, not a silent overwrite.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    reason = "test harness — the source-scan helpers use expect() as the loud failure signal when the ingest source cannot be parsed (a renamed fn or an unbalanced brace), which is itself a gate failure; the allow-expect-in-tests carve-out reaches #[test] fns but not the free helper fns this scan is built from"
)]
#![allow(
    clippy::panic,
    reason = "test harness — the `matching_brace` helper panics on an unbalanced fn body (a source it cannot parse, which is itself a gate failure); the allow-panic-in-tests carve-out reaches #[test] fns but not free helper fns"
)]

extern crate alloc;
use alloc::string::String;

/// The ingest source, embedded at compile time so the gate cannot drift from
/// a stale checkout.
const INGEST_SRC: &str = include_str!("../src/engine/ingest.rs");

/// The hot-path functions whose bodies must contain no recurring zero-fill /
/// memset. These are the per-read functions a driver pump drives in a tight
/// loop; a memset in any of them is an O(n) cost paid on every read. The
/// one-time `new()` / `escape()` zero-fills and `compact()`'s bounded
/// `copy_within` memmove are intentionally absent from this list.
const GUARDED_FNS: &[&str] = &["read_slot", "commit", "next_event"];

/// Memset-family call fragments forbidden in the guarded bodies. Each writes
/// a value across a region; in a per-read body each is a recurring memset —
/// the exact cost the watermark design removes. A regression reintroducing
/// any of them allocates nothing, so only this source scan (not the
/// allocation bench) can catch it.
const FORBIDDEN: &[&str] = &[
    ".fill(",            // slice/array constant-fill memset
    ".fill_with(",       // slice/array fill via a per-element closure
    ".resize(",          // Vec grow-with-value (zero-fills the new tail)
    ".resize_default(",  // heapless/Vec grow-with-Default (the rejected alt 2)
    ".resize_with(",     // Vec grow via a per-element closure
    ".zeroize(",         // explicit scrub — legitimate ONCE in Drop, never per-read
];

/// Strip `//`/`///`/`//!` line comments and `/* ... */` block comments from
/// `src`, preserving newlines and all non-comment characters verbatim.
///
/// Line-oriented and not string-literal-aware — the same simplification the
/// crate's zeroize-coverage gate relies on. It is sound here because the
/// ingest source carries no string literal containing a `//` or `/*`
/// sequence, and truncation could in any case only DROP characters, never
/// synthesise a `.fill(` token that is not already present in real code.
fn strip_comments(src: &str) -> String {
    let bytes = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        let next = bytes.get(i + 1).copied();
        if b == b'/' && next == Some(b'*') {
            // Block comment: skip through the matching `*/`.
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        if b == b'/' && next == Some(b'/') {
            // Line comment: skip to (but keep) the newline.
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Regular character — decode the full UTF-8 scalar so a multi-byte
        // char (an em-dash in code, were there one) is copied intact.
        let ch = src[i..]
            .chars()
            .next()
            .expect("byte index sits on a char boundary");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Find the index of the `}` matching the `{` at `open` in `code`.
fn matching_brace(code: &str, open: usize) -> usize {
    let bytes = code.as_bytes();
    assert_eq!(
        bytes.get(open).copied(),
        Some(b'{'),
        "matching_brace: the open index must point at a `{{`"
    );
    let mut depth = 0i32;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return i;
                }
            }
            _ => {}
        }
        i += 1;
    }
    panic!("matching_brace: no closing brace before end of source");
}

/// Extract the brace-delimited body of `fn <name>` from `code` (which must be
/// comment-stripped). The returned slice includes the enclosing braces.
///
/// `expect`-panics if the function is absent (e.g. renamed) — a missing
/// guarded function is itself a gate failure, surfaced loudly rather than
/// silently scanning nothing.
fn fn_body<'a>(code: &'a str, name: &str) -> &'a str {
    let mut needle = String::from("fn ");
    needle.push_str(name);
    needle.push('(');
    let sig_start = code
        .find(&needle)
        .expect("guarded fn must be present in the ingest source");
    // The body opens at the first `{` after the signature. The signatures of
    // the guarded fns contain no `{` (only `[u8]` / `<..>` brackets), so the
    // first brace is unambiguously the body's.
    let after = code
        .get(sig_start..)
        .expect("sig_start is a valid char boundary");
    let rel = after
        .find('{')
        .expect("guarded fn must have a brace-delimited body");
    let open = sig_start + rel;
    let close = matching_brace(code, open);
    code.get(open..=close)
        .expect("fn body span is a valid char boundary")
}

/// **THE GATE** — no memset-family call appears in any guarded hot-path body.
///
/// In a TEMP probe, inserting `self.active_mut().fill(0);` into `read_slot`
/// turns this RED: the `.fill(` fragment is found in the `read_slot` body and
/// the assertion fires, naming the function and the fragment.
#[test]
fn hot_path_bodies_are_memset_free() {
    let code = strip_comments(INGEST_SRC);

    for &fn_name in GUARDED_FNS {
        let body = fn_body(&code, fn_name);
        for &frag in FORBIDDEN {
            assert!(
                !body.contains(frag),
                "memset-freedom gate: forbidden memset-family call `{frag}` found \
                 in `IngestBuf::{fn_name}`. The single-residence ingest hot path \
                 must perform no per-read zero-fill/memset — a counting allocator \
                 cannot catch one, so this source gate does. If the call is \
                 genuinely required, justify it and amend FORBIDDEN/GUARDED_FNS \
                 in this test with a stated reason.",
            );
        }
    }
}

/// Sanity: the scan actually located real, substantial bodies for all three
/// guarded functions. Guards against a brace-matcher regression silently
/// returning an empty/wrong span (which would make the gate above vacuously
/// pass). Each anchor is a token unique to that body.
#[test]
fn guard_locates_the_real_function_bodies() {
    let code = strip_comments(INGEST_SRC);

    let read_slot = fn_body(&code, "read_slot");
    assert!(
        read_slot.contains("self.compact()") && read_slot.contains("escape"),
        "read_slot body extraction is wrong — missing its compact/escape calls"
    );

    let commit = fn_body(&code, "commit");
    assert!(
        commit.contains("IngestCommitOverflow") && commit.contains("available"),
        "commit body extraction is wrong — missing its overflow surface"
    );

    let next_event = fn_body(&code, "next_event");
    assert!(
        next_event.contains("HEADER_LEN") && next_event.contains("Event::Row"),
        "next_event body extraction is wrong — missing its framing anchors"
    );
}

/// Sanity: comment-stripping is what makes the gate immune to a comment that
/// merely MENTIONS a forbidden fragment. A forbidden token inside a `//` or
/// `/* */` comment must be erased; the same token in real code must survive.
/// This is the property that lets the docstring discuss the rejected
/// `resize_default` alternative without tripping the gate.
#[test]
fn comment_stripping_erases_comment_mentions_but_keeps_real_code() {
    // `.fill(` appears only inside comments here.
    let only_in_comments = "fn demo() {\n\
        // explanatory note about why slot.fill(0) is forbidden\n\
        let _ = /* avoid .resize_default( here */ 1;\n\
    }\n";
    let stripped = strip_comments(only_in_comments);
    assert!(
        !stripped.contains(".fill(") && !stripped.contains(".resize_default("),
        "comment stripping regressed: a forbidden fragment inside a comment \
         survived into the scanned code text — the gate would false-positive"
    );

    // The identical fragment in real code must survive stripping.
    let in_real_code = "fn demo() {\n    slot.fill(0);\n}\n";
    let stripped_code = strip_comments(in_real_code);
    assert!(
        stripped_code.contains(".fill("),
        "comment stripping regressed: it erased a forbidden fragment from \
         real (non-comment) code — the gate would false-negative"
    );
}
