//! Exhaustiveness gate for the per-secret zeroize-on-drop manifest.
//!
//! # Tier-1-by-construction promise
//!
//! Every type in `bsql-pg-proto` that derives `zeroize::ZeroizeOnDrop`
//! (or carries a manual `impl Drop` whose body calls `.zeroize()`)
//! MUST appear in the [`crate::drop_witness::CrateZeroizeSecret`]
//! manifest at `src/drop_witness.rs`. This test parses the source
//! tree and the manifest, computes the difference, and fails if it
//! is non-empty.
//!
//! # Mechanism
//!
//! Pure source-text scanning. The crate's source files are read at
//! **compile time** via [`include_str!`] (so the test depends on
//! the file contents at build time, NOT runtime FS state — meaning
//! the test cannot drift from a stale checkout). Two passes.
//!
//! Pass 1 (discovery): scan every `src/**/*.rs` file for two
//! patterns. (a) `#[derive(... ZeroizeOnDrop ...)]` immediately
//! preceding a `(pub )?(struct|enum)` declaration. (b) `impl Drop
//! for <type>` blocks whose body contains `.zeroize()` /
//! `.zeroize_in_place()` calls (after stripping comments).
//! Discovered set example: `{ Password, Sensitive, ScramSession, ... }`.
//!
//! Pass 2 (manifest): scan `src/drop_witness.rs` for
//! `impl<...> CrateZeroizeSecret for <type>` lines and extract the
//! bare type names. Manifest set example: `{ ReadBufN, ErrorPayload, ... }`.
//!
//! Test asserts `discovered ⊆ manifest` AND `manifest ⊆ discovered`
//! (set equality). A new secret type added without manifest update
//! → discovered \ manifest non-empty → test fails with the exact
//! type name in the message.
//!
//! # Why pure-text rather than a proc-macro / build.rs / inventory!
//!
//! - No new build infra: stable Rust 1.95+ alone, no codegen, no
//!   `inventory` crate.
//! - The test is auditable in one file.
//! - The regex patterns mirror existing CREDO §11 grep patterns
//!   (every secret-bearing type's docstring already cites
//!   `derive(ZeroizeOnDrop)` or manual `impl Drop`).
//!
//! # Limitations / out-of-scope
//!
//! - Function-local `Zeroizing<T>` scope-guards
//!   (`dispatch.rs::dispatch_auth_sasl_continue` `proof_b64_buf`,
//!   `md5.rs::compute_response_body` `inner_digest` etc.) are NOT
//!   types — they are stack-local bindings whose Drop semantics are
//!   guaranteed by the upstream `zeroize` crate's `Zeroizing<T>`
//!   wrapper. The gate covers TYPES, not bindings.
//! - The regex is line-oriented and requires `derive(ZeroizeOnDrop)`
//!   on a single line for unambiguous matching. Multi-line derives
//!   (split across lines via `\\n`) are handled via the
//!   `derive([\s\S]*?)` non-greedy multi-line pattern.

#![allow(
    clippy::expect_used,
    reason = "test-only file; expect() messages are diagnostic anchors. \
              The crate-internal forbid bundle does not extend to tests/."
)]

#![allow(clippy::disallowed_methods, reason = "test/bench harness — fixtures use the sanctioned try_from(..).unwrap_or(SAT) / slice.get(..).unwrap_or(&[]) dead-arm shape, not production data fallbacks")]
extern crate alloc;
use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::{String, ToString};

/// Embed the source file contents at compile time. Each entry is a
/// `(path-for-diagnostics, content)` tuple. Adding a new src file
/// requires extending this list — which is INTENTIONAL: a contributor
/// adding `src/new_secret_module.rs` must explicitly opt in here, OR
/// the new file's secrets simply aren't scanned. The audit-trail is
/// explicit.
///
/// Order: alphabetical by path for diff stability.
const SRC_FILES: &[(&str, &str)] = &[
    ("src/buf.rs", include_str!("../src/buf.rs")),
    ("src/drop_witness.rs", include_str!("../src/drop_witness.rs")),
    ("src/engine/ingest.rs", include_str!("../src/engine/ingest.rs")),
    ("src/error_arena.rs", include_str!("../src/error_arena.rs")),
    ("src/ident.rs", include_str!("../src/ident.rs")),
    ("src/md5.rs", include_str!("../src/md5.rs")),
    ("src/password.rs", include_str!("../src/password.rs")),
    ("src/scram/session.rs", include_str!("../src/scram/session.rs")),
    ("src/scram/types.rs", include_str!("../src/scram/types.rs")),
    ("src/sensitive.rs", include_str!("../src/sensitive.rs")),
    ("src/write_buf.rs", include_str!("../src/write_buf.rs")),
];

/// Source for the manifest itself. The exhaustiveness gate parses
/// this file's `impl ... CrateZeroizeSecret for X` lines.
const MANIFEST_SRC: &str = include_str!("../src/drop_witness.rs");

/// Discover all type-names with zeroize-on-drop semantics by scanning
/// `SRC_FILES`. Returns a sorted set of bare type names (e.g.
/// `"Password"`, not `"crate::password::Password"`).
fn discover_zeroize_types() -> BTreeSet<String> {
    let mut found = BTreeSet::new();

    for (path, src) in SRC_FILES {
        // Pattern A: `#[derive(...ZeroizeOnDrop...)]` followed by
        // a `pub struct X` / `pub enum X` / `struct X` / `enum X`
        // line. The derive may span multiple lines; we look for
        // `ZeroizeOnDrop` anywhere in a `#[derive(...)]` macro,
        // then walk forward to the next `struct` / `enum` keyword.
        for_each_derive_zeroize_on_drop_target(src, |type_name| {
            // Skip the helpful witness module's own marker types
            // (CrateZeroizeSecret has no derive — this filter
            // would be a no-op there, but keeps the check robust).
            if !type_name.is_empty() {
                found.insert(type_name.to_string());
            }
        });

        // Pattern B: manual `impl Drop for X { fn drop(...) {
        // ... .zeroize() ... } }`. We scan for `impl Drop for X` /
        // `impl<...> Drop for X<...>` and inspect the following
        // body for `.zeroize()` calls inside `fn drop`.
        for_each_manual_drop_with_zeroize_target(src, path, |type_name| {
            if !type_name.is_empty() {
                found.insert(type_name.to_string());
            }
        });
    }

    found
}

/// Discover all type-names registered in the
/// `impl CrateZeroizeSecret for X` manifest at `src/drop_witness.rs`.
fn discover_manifest_types() -> BTreeSet<String> {
    let mut found = BTreeSet::new();

    // Walk lines; match `impl <generics>? CrateZeroizeSecret for X<...>`.
    // Split on `for ` and take the bare type name from the next token.
    for line in MANIFEST_SRC.lines() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("impl") {
            continue;
        }
        if !trimmed.contains("CrateZeroizeSecret for ") {
            continue;
        }
        // Skip `mod sealed` lines + the `pub(crate) trait CrateZeroizeSecret`
        // declaration line (which has `trait`, not `impl`).
        if trimmed.contains("Sealed for ") {
            continue;
        }
        // Extract the type name after `for `. Type signature ends
        // at `{`, `;`, `<` (generic begin), or whitespace.
        let after_for = match trimmed.split("CrateZeroizeSecret for ").nth(1) {
            Some(s) => s,
            None => continue,
        };
        // The portion up to the first `{` or `;` is the type
        // expression; strip generics.
        let type_expr_end = after_for.find('{').unwrap_or(after_for.len());
        let type_expr = after_for.get(..type_expr_end).map(str::trim).unwrap_or("");
        // Bare type name is the segment between the last `::` and
        // the first `<` (or whole string if none).
        let bare = bare_type_name(type_expr);
        if !bare.is_empty() {
            found.insert(bare.to_string());
        }
    }

    found
}

/// Strip leading path segments (`crate::buf::ReadBufN<N>` →
/// `ReadBufN`) and trailing const-generics.
fn bare_type_name(type_expr: &str) -> &str {
    // After last `::`.
    let after_path = type_expr.rsplit("::").next().unwrap_or(type_expr).trim();
    // Before `<`.
    match after_path.find('<') {
        Some(idx) => after_path.get(..idx).map(str::trim).unwrap_or(""),
        None => after_path,
    }
}

/// Scan `src` for `#[derive(... ZeroizeOnDrop ...)]` patterns and
/// invoke `f` with the bare type name of the next `struct` / `enum`
/// declaration. Multi-line derives are supported via `\n`-aware
/// scanning.
fn for_each_derive_zeroize_on_drop_target(src: &str, mut f: impl FnMut(&str)) {
    // Find every `#[derive(` opening. For each, collect the macro
    // body up to the matching `)]`, check for `ZeroizeOnDrop`, then
    // walk past the closing bracket to the next `struct` or `enum`
    // line and extract the type name.
    let bytes = src.as_bytes();
    let mut idx = 0_usize;
    while idx < bytes.len() {
        let rest = src.get(idx..).unwrap_or("");
        let Some(local_idx) = rest.find("#[derive(") else {
            break;
        };
        let derive_start = idx.saturating_add(local_idx);
        // Walk forward to find the closing `)]` accounting for
        // nested parens (e.g. `derive(Foo(Bar))` is unusual but
        // possible in attribute-args). For our purposes a simple
        // parenthesis counter from `derive(` onwards suffices.
        let body_start = derive_start.saturating_add("#[derive(".len());
        let body_end = match find_matching_paren(src, body_start) {
            Some(p) => p,
            None => {
                // Malformed; advance one byte and continue scan.
                idx = derive_start.saturating_add(1);
                continue;
            }
        };
        // Body is `src[body_start..body_end]`.
        let body = src.get(body_start..body_end).unwrap_or("");
        if body.contains("ZeroizeOnDrop") {
            // Walk forward from `body_end + 2` (past `)]`) to find
            // the type name.
            let after = src
                .get(body_end.saturating_add(2)..)
                .unwrap_or("");
            if let Some(name) = extract_next_type_name(after) {
                f(name);
            }
        }
        // Advance past this derive.
        idx = body_end.saturating_add(2);
    }
}

/// Find the matching `)` for an open paren at `open_idx` (caller
/// guarantees `bytes[open_idx - 1] == b'('`; we count from
/// `open_idx` onward, starting depth = 1 since we're already inside).
fn find_matching_paren(src: &str, open_idx: usize) -> Option<usize> {
    let bytes = src.as_bytes();
    let mut depth = 1_i32;
    let mut i = open_idx;
    while i < bytes.len() {
        let b = match bytes.get(i) {
            Some(byte) => *byte,
            None => return None,
        };
        if b == b'(' {
            depth = depth.saturating_add(1);
        } else if b == b')' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(i);
            }
        }
        i = i.saturating_add(1);
    }
    None
}

/// Walk `after` until the next `struct ` or `enum ` keyword that is
/// at the start of a line (preceded by whitespace or newline) and
/// extract the bare type name. Returns None if no match.
fn extract_next_type_name(after: &str) -> Option<&str> {
    // Each line: skip blank / attribute / comment / whitespace until
    // we find `(pub )?(struct|enum) Name`.
    for line in after.lines() {
        let trimmed = line.trim_start();
        // Skip empty / comment / attribute lines.
        if trimmed.is_empty()
            || trimmed.starts_with("//")
            || trimmed.starts_with("#[")
            || trimmed.starts_with("#![")
            || trimmed.starts_with("/*")
        {
            continue;
        }
        // Found a real declaration line. Strip `pub ` /
        // `pub(crate) ` etc.
        let body = strip_visibility(trimmed);
        // Try `struct ` first, then `enum `. If neither matches the
        // derive does NOT precede a struct/enum (ill-formed for our
        // purposes — only struct / enum can have ZeroizeOnDrop).
        let after_kw = body
            .strip_prefix("struct ")
            .or_else(|| body.strip_prefix("enum "))?;
        // The type name is the leading identifier before `<`,
        // `{`, `(`, or whitespace.
        let end = after_kw
            .char_indices()
            .find(|(_, c)| !c.is_alphanumeric() && *c != '_')
            .map(|(i, _)| i)
            .unwrap_or(after_kw.len());
        return after_kw.get(..end);
    }
    None
}

/// Strip `pub`, `pub(crate)`, `pub(super)`, etc. visibility prefix.
fn strip_visibility(line: &str) -> &str {
    let trimmed = line.trim_start();
    if let Some(after) = trimmed.strip_prefix("pub(crate) ") {
        return after.trim_start();
    }
    if let Some(after) = trimmed.strip_prefix("pub(super) ") {
        return after.trim_start();
    }
    if let Some(after) = trimmed.strip_prefix("pub ") {
        return after.trim_start();
    }
    trimmed
}

/// Scan `src` for `impl Drop for X { ... }` blocks whose `fn drop`
/// body contains a zeroize-equivalent call (one of `.zeroize()`,
/// `.zeroize_in_place()`). Comments are stripped before the body
/// is scanned, so a `// .zeroize()` comment in an unrelated Drop
/// (e.g. a docstring referencing the term) does NOT match.
/// `path` is for diagnostic only — the scan logic is path-agnostic.
fn for_each_manual_drop_with_zeroize_target(
    src: &str,
    _path: &str,
    mut f: impl FnMut(&str),
) {
    // Iterate over `impl ` headers. For each, check if it's an
    // `impl Drop for <type>`. If yes, find the matching `{ ... }`
    // body, strip comments, and scan for zeroize-equivalent calls.
    let bytes = src.as_bytes();
    let mut idx = 0_usize;
    while idx < bytes.len() {
        let rest = src.get(idx..).unwrap_or("");
        let Some(local_idx) = rest.find("impl") else {
            break;
        };
        let impl_start = idx.saturating_add(local_idx);
        // Verify "impl" is a word boundary (preceded by start of
        // file, newline, or whitespace).
        let preceded_ok = if impl_start == 0 {
            true
        } else {
            let prev = bytes.get(impl_start.saturating_sub(1));
            matches!(
                prev,
                Some(b'\n') | Some(b' ') | Some(b'\t') | Some(b'\r')
            )
        };
        if !preceded_ok {
            // Substring like "simpl" — advance and continue.
            idx = impl_start.saturating_add(1);
            continue;
        }
        // Find the next `{` — that's where the impl block body starts.
        let after_impl = src.get(impl_start..).unwrap_or("");
        let header_end = match after_impl.find('{') {
            Some(p) => impl_start.saturating_add(p),
            None => break,
        };
        let header = src.get(impl_start..header_end).unwrap_or("");
        // Check this is `impl Drop for <type>` (with optional
        // generics `impl<...>` and possibly trailing whitespace).
        if let Some(type_expr) = parse_drop_for_target(header) {
            // Find the body's matching `}` by brace counting.
            let body_open = header_end;
            if let Some(body_close) = find_matching_brace(src, body_open) {
                let body_raw = src
                    .get(body_open..body_close.saturating_add(1))
                    .unwrap_or("");
                // Strip comments — line and block — before scanning
                // for zeroize-equivalent calls. Otherwise a
                // docstring like `// scrub via .zeroize()` would
                // false-positive on an unrelated Drop impl.
                let body_stripped = strip_comments(body_raw);
                if body_stripped.contains(".zeroize()")
                    || body_stripped.contains(".zeroize_in_place()")
                {
                    let bare = bare_type_name(type_expr.trim());
                    f(bare);
                }
            }
        }
        // Advance past this `impl`.
        idx = impl_start.saturating_add(4);
    }
}

/// Strip `//` line comments and `/* ... */` block comments from
/// `src`. Returns an owned String. Doc-comments (`///`, `//!`) are
/// also stripped — they're comment-like for our purposes (we want
/// only executable code text).
///
/// String literals are NOT preserved across; this means a literal
/// `"// not actually a comment"` inside a string would be partially
/// truncated. For our use case (Drop bodies in src/ that legitimately
/// shouldn't carry such adversarial literals), this is acceptable.
fn strip_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let bytes = src.as_bytes();
    let mut i = 0_usize;
    while i < bytes.len() {
        // Block comment.
        if i.saturating_add(1) < bytes.len()
            && bytes.get(i) == Some(&b'/')
            && bytes.get(i.saturating_add(1)) == Some(&b'*')
        {
            // Find matching `*/`.
            let mut j = i.saturating_add(2);
            while j.saturating_add(1) < bytes.len() {
                if bytes.get(j) == Some(&b'*')
                    && bytes.get(j.saturating_add(1)) == Some(&b'/')
                {
                    break;
                }
                j = j.saturating_add(1);
            }
            i = j.saturating_add(2);
            continue;
        }
        // Line comment.
        if i.saturating_add(1) < bytes.len()
            && bytes.get(i) == Some(&b'/')
            && bytes.get(i.saturating_add(1)) == Some(&b'/')
        {
            // Skip to end of line.
            while i < bytes.len() && bytes.get(i) != Some(&b'\n') {
                i = i.saturating_add(1);
            }
            // Keep the newline.
            if i < bytes.len() {
                out.push('\n');
                i = i.saturating_add(1);
            }
            continue;
        }
        // Regular byte. We push the source character at this byte
        // position; for multi-byte UTF-8, walk char-wise.
        let c = match src.get(i..) {
            Some(s) => s.chars().next(),
            None => None,
        };
        let c = match c {
            Some(ch) => ch,
            None => break,
        };
        out.push(c);
        i = i.saturating_add(c.len_utf8());
    }
    out
}

/// Parse `impl<...> Drop for <type>` header. Returns the type
/// expression substring (everything after `for ` up to the first
/// `{` or trailing whitespace).
fn parse_drop_for_target(header: &str) -> Option<&str> {
    // `header` starts with `impl`. After optional `<...>` generics,
    // we expect ` Drop for <type>`.
    let rest = header.strip_prefix("impl")?;
    // Skip optional generics.
    let after_generics = if rest.trim_start().starts_with('<') {
        let inner = rest.trim_start().get(1..).unwrap_or("");
        let close = find_matching_angle(inner)?;
        inner.get(close.saturating_add(1)..).unwrap_or("")
    } else {
        rest
    };
    // Now expect ` Drop for `.
    let after_trim = after_generics.trim_start();
    let after_drop = after_trim.strip_prefix("Drop")?;
    let after_for = after_drop.trim_start().strip_prefix("for")?;
    Some(after_for.trim())
}

/// Find the matching `>` for an open `<` at index 0 of `inner`
/// (caller guarantees inner is the substring AFTER the `<`).
/// Tracks nested `<`/`>` for generics-with-generics.
fn find_matching_angle(inner: &str) -> Option<usize> {
    let bytes = inner.as_bytes();
    let mut depth = 1_i32;
    let mut i = 0_usize;
    while i < bytes.len() {
        let b = match bytes.get(i) {
            Some(byte) => *byte,
            None => return None,
        };
        if b == b'<' {
            depth = depth.saturating_add(1);
        } else if b == b'>' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(i);
            }
        }
        i = i.saturating_add(1);
    }
    None
}

/// Find the matching `}` for an open `{` at index `open` of `src`.
fn find_matching_brace(src: &str, open: usize) -> Option<usize> {
    let bytes = src.as_bytes();
    let mut depth = 1_i32;
    let mut i = open.saturating_add(1);
    while i < bytes.len() {
        let b = match bytes.get(i) {
            Some(byte) => *byte,
            None => return None,
        };
        if b == b'{' {
            depth = depth.saturating_add(1);
        } else if b == b'}' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(i);
            }
        }
        i = i.saturating_add(1);
    }
    None
}

// ─────────────────────────────────────────────────────────────────────
// Tier-1-by-construction gate test.
// ─────────────────────────────────────────────────────────────────────

/// **THE GATE** — discovered (source) set must equal manifest set.
///
/// On failure, the assertion message lists exact missing-from-each-set
/// type names so the contributor knows precisely what to add.
#[test]
fn manifest_covers_every_zeroize_on_drop_secret_type() {
    let discovered = discover_zeroize_types();
    let manifest = discover_manifest_types();

    let in_src_not_in_manifest: BTreeSet<&String> =
        discovered.difference(&manifest).collect();
    let in_manifest_not_in_src: BTreeSet<&String> =
        manifest.difference(&discovered).collect();

    if !in_src_not_in_manifest.is_empty() || !in_manifest_not_in_src.is_empty() {
        let mut msg = String::new();
        msg.push_str("zeroize-on-drop manifest drift detected:\n");
        if !in_src_not_in_manifest.is_empty() {
            msg.push_str(
                "\nThe following secret-bearing types are present in `src/**/*.rs` \
                 (via derive(ZeroizeOnDrop) or manual impl Drop with .zeroize()) \
                 but MISSING from the manifest at `src/drop_witness.rs`. \
                 Add a matching\n  `impl<...> sealed::Sealed for crate::path::TypeName {}`\n\
                 + `impl<...> CrateZeroizeSecret for crate::path::TypeName {}`\n\
                 entry in alphabetical order:\n",
            );
            for t in in_src_not_in_manifest {
                msg.push_str(&format!("  - {t}\n"));
            }
        }
        if !in_manifest_not_in_src.is_empty() {
            msg.push_str(
                "\nThe following types are listed in the manifest at \
                 `src/drop_witness.rs` but no longer have a discoverable \
                 derive(ZeroizeOnDrop) or manual impl Drop with .zeroize() \
                 in src. Either restore the production zeroize-on-drop \
                 chain or remove the manifest entry:\n",
            );
            for t in in_manifest_not_in_src {
                msg.push_str(&format!("  - {t}\n"));
            }
        }
        msg.push_str(
            "\nRationale: the manifest is the audit anchor for tier-1 \
             zeroize-on-drop coverage. Drift between source and manifest \
             indicates either an unmonitored secret type (missing entry) \
             or a stale manifest (orphan entry).\n",
        );
        panic!("{msg}");
    }
}

/// Sanity: the discovered set is non-empty. Catches a regression
/// that breaks the source-scan logic (e.g., regex pattern drift).
/// The crate has at least 6 `derive(ZeroizeOnDrop)` types and 2
/// manual `impl Drop` with `.zeroize()`. If we discover zero, the
/// scanner is broken, not the source.
#[test]
fn source_scanner_finds_at_least_baseline_count() {
    let discovered = discover_zeroize_types();
    assert!(
        discovered.len() >= 8,
        "zeroize-on-drop source scanner regressed: discovered only {} types \
         but baseline is ≥ 8 (Password, Sensitive, ScramSession, \
         SecretDigest, Md5HandshakeState, ErrorPayload, SecretBoundedStr, \
         ReadBufN, WriteBuf). Discovered: {discovered:?}",
        discovered.len(),
    );
}

/// Sanity: the manifest set is non-empty. Catches a regression that
/// truncates `src/drop_witness.rs` or breaks the impl-list scanner.
#[test]
fn manifest_scanner_finds_at_least_baseline_count() {
    let manifest = discover_manifest_types();
    assert!(
        manifest.len() >= 8,
        "zeroize-on-drop manifest scanner regressed: manifest reads only {} \
         types but baseline is ≥ 8. Manifest: {manifest:?}",
        manifest.len(),
    );
}
