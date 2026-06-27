//! Static source-guard: the flush loop has NO `.await` between the transport
//! write and the cursor advance.
//!
//! # The invariant this gate machine-enforces
//!
//! The cancellation-unrolling guarantee (a flush future dropped mid-drain
//! leaves the engine-owned send cursor consistent) rests on ONE structural
//! property of the drain loop: the cursor `advance` is **synchronous**,
//! executed immediately after the `transport.write(...).await` resolves, with
//! NO suspension point in between. If a future refactor slipped an `.await`
//! between the resolved write and the advance, a cancellation landing there
//! would commit bytes to the socket without recording them in the cursor — a
//! silent double-send on resume. The cancellation sweep
//! (`engine_flush_cancel`) would still pass against the *current* code, so the
//! property would hold only by reviewer vigilance.
//!
//! This gate makes it a REAL gate: a pure source-text scan of the `flush` body
//! that locates the transport write, its `.await`, and the cursor `.advance(`,
//! and fails unless they are ordered `.write(` (awaited) < `.advance(` with
//! **no `.await` token in the span between the write's `.await` and the
//! advance**. That span check is the precise adjacency invariant; it
//! deliberately permits other awaits OUTSIDE the span — notably the legitimate
//! post-drain `transport.flush().await` that follows the advance — while still
//! catching any suspension slipped between the resolved write and the commit.
//!
//! # What it scans
//!
//! It reads the flush source at compile time via [`include_str!`], strips
//! comments (so a doc comment that merely MENTIONS `.await` cannot inflate the
//! count), isolates the `flush` function body, and applies the order check.
//!
//! # Regenerating after an intentional change
//!
//! There is nothing to regenerate: the gate has no golden. If a future change
//! legitimately restructures the loop, the fix is to update the check here
//! with a stated reason — a reviewed source edit, not a silent overwrite.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    reason = "test harness — the source-scan helpers use expect() as the loud failure signal when the flush source cannot be parsed (a renamed fn or an unbalanced brace), which is itself a gate failure; the allow-expect-in-tests carve-out reaches #[test] fns but not the free helper fns this scan is built from"
)]
#![allow(
    clippy::panic,
    reason = "test harness — the `matching_brace` helper panics on an unbalanced fn body (a source it cannot parse, which is itself a gate failure); the allow-panic-in-tests carve-out reaches #[test] fns but not free helper fns"
)]

extern crate alloc;
use alloc::string::String;

/// The flush source, embedded at compile time so the gate cannot drift from a
/// stale checkout.
const FLUSH_SRC: &str = include_str!("../src/engine/flush.rs");

/// Strip `//`/`///`/`//!` line comments and `/* ... */` block comments,
/// preserving newlines and all non-comment characters verbatim. Line-oriented
/// and not string-literal-aware — sound here because the flush source carries
/// no string literal containing a `//` or `/*` sequence, and stripping could
/// only DROP a forbidden token, never synthesise one.
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

/// Extract the brace-delimited body of the free `async fn flush` (which is
/// generic, so the simple `fn name(` needle does not apply). The returned
/// slice includes the enclosing braces. `expect`-panics if the function is
/// absent (e.g. renamed) — itself a gate failure, surfaced loudly.
fn flush_body(code: &str) -> &str {
    // `async fn flush` is unique to the free drain fn (the SendBuf methods are
    // synchronous), so this never matches a different item.
    let sig_start = code
        .find("async fn flush")
        .expect("the free `async fn flush` must be present in the flush source");
    let after = code
        .get(sig_start..)
        .expect("sig_start is a valid char boundary");
    // The signature uses only `<..>` and `(..)` before the body, so the first
    // `{` after it is unambiguously the body's.
    let rel = after
        .find('{')
        .expect("flush must have a brace-delimited body");
    let open = sig_start + rel;
    let close = matching_brace(code, open);
    code.get(open..=close)
        .expect("flush body span is a valid char boundary")
}

/// Apply the await-discipline check to one function body. Returns `Some(reason)`
/// on a violation, `None` when the body is clean. Pure (no source I/O), so it
/// can be exercised on synthetic bodies to prove the gate has teeth.
fn await_discipline_violation(body: &str) -> Option<&'static str> {
    let write_idx = match body.find(".write(") {
        Some(i) => i,
        None => {
            return Some("flush body must call `.write(` — the suspension is the transport write")
        }
    };
    // The write's own `.await` is the first `.await` at or after the write
    // call. (A legitimate post-drain `transport.flush().await` is a SEPARATE,
    // later await — it must not be confused for the write's.)
    let rel = match body.get(write_idx..).and_then(|s| s.find(".await")) {
        Some(r) => r,
        None => return Some("the transport `.write(` must be awaited"),
    };
    let write_await_idx = write_idx.saturating_add(rel);
    let advance_idx = match body.find(".advance(") {
        Some(i) => i,
        None => return Some("flush body must call `.advance(` — the cursor commit is missing"),
    };
    if advance_idx <= write_await_idx {
        return Some("the cursor `.advance(` must FOLLOW the write `.await`");
    }
    // THE adjacency invariant: no suspension may appear in the span between the
    // resolved write and the commit. A drop landing on a `.await` there would
    // commit bytes to the socket without recording them in the cursor.
    let span_start = write_await_idx.saturating_add(".await".len());
    // `advance_idx > write_await_idx` and the write's `.await` is six bytes, so
    // `span_start <= advance_idx` always holds and the range is valid; the `?`
    // early-return on a missing range is a dead arm (treated as clean).
    let span = body.get(span_start..advance_idx)?;
    if span.contains(".await") {
        return Some("no `.await` may appear between the write `.await` and the cursor `.advance(`");
    }
    None
}

/// **THE GATE** — the real `flush` body satisfies the await discipline.
#[test]
fn flush_loop_has_no_await_between_write_and_advance() {
    let code = strip_comments(FLUSH_SRC);
    let body = flush_body(&code);
    if let Some(reason) = await_discipline_violation(body) {
        panic!(
            "await-discipline gate: the flush loop violates the \
             no-`.await`-between-write-and-advance invariant: {reason}.\n\
             The cursor advance must be synchronous immediately after the \
             transport write resolves; otherwise a mid-drain cancellation can \
             double-send. Fix the loop, or update this gate with a reason."
        );
    }
}

/// Sanity: the scan located a real, substantial `flush` body (guards against a
/// brace-matcher regression silently returning an empty/wrong span, which
/// would make the gate above vacuously pass).
#[test]
fn guard_locates_the_real_flush_body() {
    let code = strip_comments(FLUSH_SRC);
    let body = flush_body(&code);
    assert!(
        body.contains("is_drained") && body.contains("EngineError::WriteZero"),
        "flush body extraction is wrong — missing its loop guard / error anchors"
    );
    assert!(
        body.contains(".write(") && body.contains(".advance("),
        "flush body extraction is wrong — missing its write/advance calls"
    );
}

/// Teeth: the check distinguishes a compliant body from each corruption shape.
/// This proves the gate would catch an injected `.await`, a reordered advance,
/// a removed suspension, or a missing write — without mutating the real source.
#[test]
fn the_check_has_teeth() {
    // Compliant: write awaited, advance immediately after (no await between),
    // and a LEGITIMATE post-drain `transport.flush().await` after the loop —
    // which must NOT trip the gate, since it follows the advance.
    let good = "{ while x { let n = t.write(p).await?; if n == 0 { } s.advance(n)?; } t.flush().await?; }";
    assert!(
        await_discipline_violation(good).is_none(),
        "the compliant shape (incl. a post-drain flush await) must pass"
    );

    // A stray `.await` between the write and the advance — the exact
    // regression the gate exists to catch.
    let stray = "{ let n = t.write(p).await?; other().await; s.advance(n)?; }";
    assert!(
        await_discipline_violation(stray).is_some(),
        "a stray `.await` between write and advance must be caught"
    );

    // Advance BEFORE the write await.
    let reordered = "{ s.advance(n)?; let n = t.write(p).await?; }";
    assert!(
        await_discipline_violation(reordered).is_some(),
        "an advance before the write await must be caught"
    );

    // The write call present but NOT awaited.
    let unawaited_write = "{ let n = t.write(p)?; s.advance(n)?; }";
    assert!(
        await_discipline_violation(unawaited_write).is_some(),
        "a write that is not awaited must be caught"
    );

    // No write call at all (suspension is on something else).
    let no_write = "{ let n = other(p).await?; s.advance(n)?; }";
    assert!(
        await_discipline_violation(no_write).is_some(),
        "a body whose suspension is not the transport write must be caught"
    );
}

/// Sanity: comment-stripping erases a `.await` that appears only in a comment,
/// so a docstring discussing the invariant cannot inflate the await count.
#[test]
fn comment_stripping_erases_comment_await_mentions() {
    let only_in_comments = "fn demo() {\n\
        // there must be no second .await here\n\
        let n = t.write(p).await?; /* the .await above is the only one */ s.advance(n)?;\n\
    }\n";
    let stripped = strip_comments(only_in_comments);
    assert_eq!(
        stripped.matches(".await").count(),
        1,
        "comment stripping must leave exactly the one real `.await`"
    );
}
