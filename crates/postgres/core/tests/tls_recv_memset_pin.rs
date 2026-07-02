//! Static source-guard: the TLS inbound read path performs NO per-read memset.
//!
//! # The blind spot this gate closes
//!
//! The engine's ingest hot path has a source-scan (`engine_ingest_memset_guard`
//! in the proto crate) that FORBIDS any per-read `.fill(`/`.resize(`/`.zeroize(`
//! memset — a counting allocator cannot see one, because a memset over
//! already-owned storage allocates nothing. But that scan covers only the
//! engine's ingest buffer. The TLS read path in THIS crate has its own inbound
//! staging buffer, and nothing scanned it, so a per-read zero-fill there would
//! be an invisible recurring cost.
//!
//! # What this gate enforces
//!
//! `TlsTransport::recv_more` reads socket bytes straight into the
//! already-initialized spare region of a fixed staging buffer, past a
//! `staging_filled` watermark — so there is NO per-read zero-fill at all. The
//! one-time zero-fill lives at construction (`with_conn`), a single grow-once of
//! [`STAGING_CAP`](STAGING_CAP_COMPOSITION) bytes, O(capacity) once per
//! connection, never per read. This gate pins exactly that shape:
//!
//! 1. The read window headroom [`RECV_CHUNK`](RECV_CHUNK_PIN) is PINNED at
//!    16384 and the staging capacity is a BOUNDED composition of it (a bigger
//!    one-time buffer cannot grow silently).
//! 2. EVERY function on the per-read inbound staging path
//!    ([`PER_READ_MEMSET_FREE_FNS`]) contains NONE of the memset family (no
//!    `.resize(`, no `.fill(`/`.zeroize(`/…, and no `vec![0u8;` construction). A
//!    per-read memset sneaking back into ANY of them fails this gate — the
//!    RED→GREEN witness for the removal (the `recv_more` assertion flipped from
//!    "exactly one" to "zero").
//! 3. The one-time zero-fill (`vec![0u8; STAGING_CAP]`) appears EXACTLY ONCE in
//!    the whole source and inside `with_conn` — the grow-once happens once per
//!    connection, not per read, and cannot migrate into a read path.
//!
//! # Why a SET of functions, not just `recv_more`
//!
//! `fn_body` extracts a SINGLE function's brace span; it does NOT descend into
//! the callees named inside it. `recv_more`'s very first statement is
//! `self.compact_staging()`, and `read_impl` calls `pump_inbound` on every
//! iteration — so scanning only `recv_more`/`read_impl` sees the *call token*
//! but never the callee's body. A `.fill(0)` / `.resize(_, 0)` / `.zeroize(`
//! re-introduced INSIDE `compact_staging` or `pump_inbound` would restore
//! exactly the per-read memset cost this path removed while a `recv_more`-only
//! scan stayed green. So the gate scans every per-read callee in its own right;
//! [`PER_READ_MEMSET_FREE_FNS`] is that authoritative set — extend it whenever a
//! new helper lands on the per-read staging path.
//!
//! The scan is comment-stripped and function-body-scoped. Deliberately NOT
//! scanned: `scrub` (a Drop-time full-capacity `.zeroize()` — the legitimate
//! one-time plaintext-residue wipe on close, proof the file-at-large is not
//! scanned), `with_conn` (the one-time grow-once, pinned separately at point 3),
//! and the OUTBOUND flush path (`flush_impl` / `reclaim_out` operate on
//! `out_buf`, a different buffer with its own copy-based drain — not the inbound
//! staging read).

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

/// The PINNED spare-read-window headroom (`RECV_CHUNK`). The staging buffer is
/// sized to guarantee at least this many initialized bytes past the watermark
/// for every read; a change here is a reviewed tuning change.
const RECV_CHUNK_PIN: usize = 16384;

/// The PINNED staging-capacity composition. The one-time zero-fill is exactly
/// `STAGING_CAP` bytes, and `STAGING_CAP` is a bounded sum of the max on-wire
/// record and the read headroom — pinning the composition keeps the one-time
/// cost from growing silently.
const MAX_CIPHERTEXT_RECORD_DECL: &str = "const MAX_CIPHERTEXT_RECORD: usize = 5 + 16384 + 256;";
const STAGING_CAP_COMPOSITION: &str = "const STAGING_CAP: usize = MAX_CIPHERTEXT_RECORD + RECV_CHUNK;";

/// The one-time grow-once initialization. It must appear exactly once, at
/// construction — never in a read path.
const GROW_ONCE: &str = "vec![0u8; STAGING_CAP]";

/// **The authoritative per-read inbound staging path.** Every function that runs
/// on an inbound `read` and could memset the staging buffer — scanned in its own
/// right because `fn_body` does not descend into a callee named in another body:
///
/// - `read_impl` — the plaintext copy-out read loop (the `read` entry point).
/// - `recv_more` — the socket read into the staging spare region.
/// - `compact_staging` — the staging front-drain `recv_more` runs as its first
///   statement (the callee a `recv_more`-only scan would miss).
/// - `pump_inbound` — the decrypt step `read_impl` runs on every iteration.
///
/// NOT here (see the module docs): `scrub` (Drop-only zeroize), `with_conn` (the
/// grow-once, pinned separately), and the outbound flush path (`out_buf`).
/// Extend this list whenever a new helper lands on the per-read staging path.
const PER_READ_MEMSET_FREE_FNS: &[&str] = &["read_impl", "recv_more", "compact_staging", "pump_inbound"];

/// The construction function holding the single grow-once.
const WITH_CONN: &str = "with_conn";

/// The full memset family that must NOT appear per-read. Every function in
/// [`PER_READ_MEMSET_FREE_FNS`] must contain none of these.
const FORBIDDEN_MEMSET: &[&str] = &[
    ".resize(",
    ".fill(",
    ".fill_with(",
    ".resize_default(",
    ".resize_with(",
    ".zeroize(",
    "vec![0u8;",
    "vec![0;",
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

/// **THE HEADROOM PIN** — `RECV_CHUNK`, the guaranteed spare read window, is 16384.
#[test]
fn recv_chunk_headroom_is_pinned() {
    let needle = format!("const RECV_CHUNK: usize = {RECV_CHUNK_PIN};");
    assert!(
        TLS_SRC.contains(&needle),
        "TLS spare-read-window pin: expected `{needle}` in tls.rs. The staging buffer \
         guarantees at least RECV_CHUNK initialized bytes past the watermark for every \
         read; its size is pinned so it cannot drift silently. If it legitimately \
         changes, update RECV_CHUNK_PIN with the new reviewed number."
    );
}

/// **THE CAPACITY PIN** — the one-time staging buffer is a bounded composition.
#[test]
fn staging_capacity_is_bounded_and_pinned() {
    assert!(
        TLS_SRC.contains(MAX_CIPHERTEXT_RECORD_DECL),
        "TLS staging-capacity pin: expected `{MAX_CIPHERTEXT_RECORD_DECL}` in tls.rs. \
         The max on-wire record bounds the unconsumed residue, which bounds the fixed \
         staging capacity. If it legitimately changes, update the pin."
    );
    assert!(
        TLS_SRC.contains(STAGING_CAP_COMPOSITION),
        "TLS staging-capacity pin: expected `{STAGING_CAP_COMPOSITION}` in tls.rs. The \
         one-time zero-fill is exactly STAGING_CAP bytes; pinning the composition keeps \
         the one-time cost bounded and visible. If it legitimately changes, update the pin."
    );
}

/// **THE FLIP + THE FULL PATH** — EVERY function on the per-read inbound staging
/// path holds NO memset. For `recv_more` this assertion was previously "exactly
/// one `.resize(RECV_CHUNK)`"; the removal flips it to zero. `compact_staging`
/// and `pump_inbound` — per-read callees `recv_more`/`read_impl` never descend
/// into — are scanned in their own right so a memset cannot hide behind a call.
#[test]
fn per_read_inbound_path_has_no_memset() {
    let code = strip_comments(TLS_SRC);
    for &name in PER_READ_MEMSET_FREE_FNS {
        let body = fn_body(&code, name);
        for &frag in FORBIDDEN_MEMSET {
            let n = count(body, frag);
            assert_eq!(
                n, 0,
                "TLS memset gate: forbidden per-read memset `{frag}` found {n} time(s) in \
                 `{name}`. Every function on the inbound read path must touch the staging \
                 buffer without a per-read zero-fill — the socket reads straight into the \
                 fixed buffer's initialized spare capacity. The ONLY staging zero-fill is \
                 the one-time grow-once in `with_conn`."
            );
        }
    }
}

/// **THE GROW-ONCE PIN** — the single one-time zero-fill lives in `with_conn`
/// and nowhere else.
#[test]
fn grow_once_initialization_happens_exactly_once_in_with_conn() {
    let code = strip_comments(TLS_SRC);

    let whole = count(&code, GROW_ONCE);
    assert_eq!(
        whole, 1,
        "TLS grow-once pin: the one-time staging zero-fill `{GROW_ONCE}` must appear \
         EXACTLY ONCE in the whole source, found {whole}. A second occurrence is a \
         second memset — if it landed in a read path it is a per-read cost."
    );

    let with_conn = fn_body(&code, WITH_CONN);
    assert!(
        with_conn.contains(GROW_ONCE),
        "TLS grow-once pin: the one-time staging zero-fill `{GROW_ONCE}` must live in \
         `with_conn` (construction — once per connection), not in any read path."
    );
}

/// Sanity: the scan located real, substantial bodies for EVERY guarded function
/// (guards against a brace-matcher regression silently returning an empty span,
/// which would make the memset scan vacuously pass on that function — a memset
/// could then hide in an unlocated body). One distinctive anchor pair per fn.
#[test]
fn guard_locates_the_real_function_bodies() {
    let code = strip_comments(TLS_SRC);

    // (fn name, two distinctive substrings its real body must contain)
    let anchors: &[(&str, &str, &str)] = &[
        ("read_impl", "plaintext", "copy_from_slice"),
        ("recv_more", "compact_staging", "staging_filled"),
        ("compact_staging", "copy_within", "staging_start"),
        ("pump_inbound", "process_tls_records", "next_record"),
        ("with_conn", "staging_start", "scratch"),
    ];
    for &(name, a, b) in anchors {
        let body = fn_body(&code, name);
        assert!(
            body.contains(a) && body.contains(b),
            "`{name}` body extraction is wrong — missing its `{a}`/`{b}` anchors, so its \
             memset scan may be vacuous"
        );
    }
}
