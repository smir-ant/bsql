//! Static source-guard: the TLS inbound read path's ONE known per-read memset,
//! made VISIBLE and PINNED.
//!
//! # The blind spot this gate closes
//!
//! The engine's ingest hot path has a source-scan (`engine_ingest_memset_guard`
//! in the proto crate) that FORBIDS any per-read `.fill(`/`.resize(`/`.zeroize(`
//! memset — a counting allocator cannot see one, because a memset over
//! already-owned storage allocates nothing. But that scan covers only the
//! engine's ingest buffer. The TLS read path in THIS crate has its own inbound
//! staging buffer, and `TlsTransport::recv_more` currently does a per-socket-read
//! zero-fill (`staging.resize(base + RECV_CHUNK, 0)` — a 16 KiB memset on every
//! `read`, to give the socket a written window without an unsafe uninit read).
//! Nothing scanned it, so that recurring cost was invisible.
//!
//! # What this gate does — DOCUMENT + PIN, do not fix
//!
//! It does NOT forbid the memset (removing it is a later perf slice — an
//! in-place decrypt or a `read`-into-uninit API). It makes it VISIBLE so it
//! cannot GROW or MULTIPLY silently, and gives the later fix a witness:
//!
//! 1. [`RECV_CHUNK`](RECV_CHUNK_PIN) — the per-read zero-fill size — is PINNED
//!    at 16384. Bumping it (a bigger per-read memset) fails this gate.
//! 2. `recv_more`'s body contains EXACTLY the ONE documented memset (a single
//!    `.resize(` referencing `RECV_CHUNK`) and NONE of the other memset-family
//!    calls. A second memset, or a switch to `.fill(`/`.zeroize(`, fails.
//! 3. `read_impl` (the plaintext copy-out read path) contains NO memset at all.
//!    A per-read zero-fill sneaking into it fails.
//!
//! When a later slice removes the `recv_more` zero-fill, assertion (2) flips
//! (zero resizes found) — the slice updates this gate to the forbidding form,
//! which is exactly the RED→GREEN witness the removal wants.
//!
//! The scan is comment-stripped and function-body-scoped (the `scrub` method's
//! `.zeroize()` calls are the legitimate ONE-TIME plaintext-residue wipe on
//! close, not a per-read memset, and live outside the two guarded read-path
//! bodies, so they are never scanned).

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    reason = "source-scan harness — expect() is the loud gate-failure signal when the TLS source cannot be parsed (a renamed fn / unbalanced brace, itself a gate failure); the allow-expect-in-tests carve-out reaches #[test] fns but not the free helper fns the scan is built from"
)]
#![allow(
    clippy::panic,
    reason = "source-scan harness — the brace matcher panics on an unbalanced body (a source it cannot parse, itself a gate failure); the allow-panic-in-tests carve-out does not reach free helper fns"
)]

/// The TLS source, embedded at compile time so the gate cannot drift from a
/// stale checkout.
const TLS_SRC: &str = include_str!("../src/tls.rs");

/// The PINNED per-read zero-fill size (`RECV_CHUNK`). The gate asserts the const
/// declaration in the source equals this. A later slice that removes the memset
/// removes the const with it.
const RECV_CHUNK_PIN: usize = 16384;

/// The read-path functions the scan guards. `recv_more` holds the ONE allowed
/// memset; `read_impl` must hold none.
const RECV_MORE: &str = "recv_more";
const READ_IMPL: &str = "read_impl";

/// Memset-family fragments that must NOT appear (in `recv_more`, on top of the
/// single allowed `.resize(RECV_CHUNK)`; in `read_impl`, at all).
const FORBIDDEN_EXTRA: &[&str] = &[
    ".fill(",
    ".fill_with(",
    ".resize_default(",
    ".resize_with(",
    ".zeroize(",
];

/// Strip `//` line comments and `/* */` block comments, preserving newlines and
/// all non-comment characters. Line-oriented, not string-literal-aware — sound
/// here because truncation can only DROP characters, never synthesise a memset
/// token that is not already present in real code.
fn strip_comments(src: &str) -> String {
    let bytes = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        let next = bytes.get(i + 1).copied();
        if b == b'/' && next == Some(b'*') {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        if b == b'/' && next == Some(b'/') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
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

/// Extract the brace-delimited body of `fn <name>` (comment-stripped `code`),
/// including the enclosing braces. `expect`-panics if the function is absent
/// (a rename is itself a gate failure).
fn fn_body<'a>(code: &'a str, name: &str) -> &'a str {
    let mut needle = String::from("fn ");
    needle.push_str(name);
    needle.push('(');
    let sig_start = code
        .find(&needle)
        .expect("guarded fn must be present in the TLS source");
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

/// Count non-overlapping occurrences of `needle` in `hay`.
fn count(hay: &str, needle: &str) -> usize {
    hay.matches(needle).count()
}

/// **THE PIN** — `RECV_CHUNK`, the per-read zero-fill size, is exactly 16384.
#[test]
fn recv_chunk_zero_fill_size_is_pinned() {
    let needle = format!("const RECV_CHUNK: usize = {RECV_CHUNK_PIN};");
    assert!(
        TLS_SRC.contains(&needle),
        "TLS per-read zero-fill size pin: expected `{needle}` in tls.rs. The known \
         `recv_more` memset writes RECV_CHUNK zero bytes per socket read; its size \
         is pinned so it cannot grow silently. If it legitimately changes, update \
         RECV_CHUNK_PIN with the new reviewed number."
    );
}

/// **THE GATE (recv_more)** — exactly one documented memset (a single `.resize(`
/// referencing `RECV_CHUNK`), and none of the other memset-family calls.
#[test]
fn recv_more_holds_exactly_the_one_documented_memset() {
    let code = strip_comments(TLS_SRC);
    let body = fn_body(&code, RECV_MORE);

    let resizes = count(body, ".resize(");
    assert_eq!(
        resizes, 1,
        "TLS memset baseline drift: `recv_more` must hold EXACTLY the ONE documented \
         per-read zero-fill (`staging.resize(base + RECV_CHUNK, 0)`), found {resizes} \
         `.resize(` calls. A second memset is a NEW recurring cost — if intended, \
         document it here; if the one memset was REMOVED (the later perf fix), flip \
         this gate to the forbidding form."
    );
    assert!(
        body.contains("RECV_CHUNK"),
        "the one `recv_more` `.resize(` must be the documented `RECV_CHUNK` zero-fill \
         — its argument no longer references RECV_CHUNK, so the pinned size is bypassed"
    );
    for &frag in FORBIDDEN_EXTRA {
        assert!(
            !body.contains(frag),
            "TLS memset gate: forbidden memset-family call `{frag}` found in `recv_more`. \
             Only the single documented `.resize(RECV_CHUNK)` zero-fill is allowed here; \
             any other per-read memset is a new invisible cost."
        );
    }
}

/// **THE GATE (read_impl)** — the plaintext copy-out read path holds NO memset.
#[test]
fn read_impl_is_memset_free() {
    let code = strip_comments(TLS_SRC);
    let body = fn_body(&code, READ_IMPL);

    let mut forbidden = vec![".resize("];
    forbidden.extend_from_slice(FORBIDDEN_EXTRA);
    for frag in forbidden {
        assert!(
            !body.contains(frag),
            "TLS memset gate: forbidden memset-family call `{frag}` found in `read_impl`. \
             The plaintext copy-out read path must perform no per-read zero-fill/memset."
        );
    }
}

/// Sanity: the scan located real, substantial bodies for both guarded functions
/// (guards against a brace-matcher regression silently returning an empty span,
/// which would make the gates above vacuously pass).
#[test]
fn guard_locates_the_real_function_bodies() {
    let code = strip_comments(TLS_SRC);

    let recv_more = fn_body(&code, RECV_MORE);
    assert!(
        recv_more.contains("compact_staging") && recv_more.contains("truncate"),
        "recv_more body extraction is wrong — missing its compact/truncate anchors"
    );

    let read_impl = fn_body(&code, READ_IMPL);
    assert!(
        read_impl.contains("plaintext") && read_impl.contains("copy_from_slice"),
        "read_impl body extraction is wrong — missing its plaintext copy-out anchors"
    );
}
