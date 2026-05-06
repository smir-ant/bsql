//! DEF-215 + DEF-216 audit (2026-05-07): tier-1 size-pin coverage
//! for the cleartext + MD5 PasswordMessage frame sizes.
//!
//! Internal `const _: () = assert!(MAX_OWNED_SEND_LEN >=
//! max_password_message_size())` in `write_buf.rs` is a build-time
//! check; this file provides the external (downstream-crate-POV)
//! formula verification — catches regressions where someone
//! changes the size formula but the global drift-pin still passes
//! (e.g. inadvertent padding alterations).
//!
//! Pre-2026-05-07, the cleartext + MD5 builders had `Err(WriteBufFull)`
//! arms classified as tier-3 by-classification (forensic visibility
//! via `InternalCrateBug { BuilderCapacityOverflow }`). Post-pin,
//! the error path is **architecturally unreachable** — the build-
//! time assert proves the invariant, this file tests the formula.

#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::as_conversions,
    clippy::arithmetic_side_effects
)]

use bsql_pg_proto::write_buf::{
    max_password_message_size, max_password_message_size_cleartext,
    max_password_message_size_md5,
};
use bsql_pg_proto::MAX_OWNED_SEND_LEN;

// Compile-time pins from the consumer side. If any formula drifts
// or a re-export demotes, this fails to build before any test runs.
const _PIN_CLEARTEXT_FORMULA: () = assert!(
    max_password_message_size_cleartext() == 6 + 512,
    "Cleartext PasswordMessage: 'p'(1) + len(4) + password(MAX=512) + NUL(1) = 518",
);
const _PIN_MD5_FORMULA: () = assert!(
    max_password_message_size_md5() == 41,
    "MD5 PasswordMessage: 'p'(1) + len(4) + 'md5'+32hex(35) + NUL(1) = 41",
);
const _PIN_UMBRELLA_IS_MAX: () = assert!(
    max_password_message_size() == max_password_message_size_cleartext(),
    "umbrella size = max(cleartext, md5) = cleartext (cleartext dominates)",
);
const _PIN_MAX_OWNED_SEND_LEN_FITS: () = assert!(
    MAX_OWNED_SEND_LEN >= max_password_message_size(),
    "MAX_OWNED_SEND_LEN must accommodate worst-case PasswordMessage",
);

#[test]
fn cleartext_size_formula() {
    // Byte-by-byte breakdown:
    //   tag       = 1
    //   length    = 4
    //   password  ≤ 512 (MAX_PASSWORD_LEN)
    //   NUL       = 1
    //   total     = 518
    assert_eq!(max_password_message_size_cleartext(), 518);
}

#[test]
fn md5_size_formula() {
    // Byte-by-byte breakdown:
    //   tag       = 1
    //   length    = 4
    //   "md5"     = 3
    //   hex       = 32
    //   NUL       = 1
    //   total     = 41
    assert_eq!(max_password_message_size_md5(), 41);
}

#[test]
fn umbrella_takes_max_of_both() {
    // Cleartext (518) dominates MD5 (41) — the umbrella const must
    // return the cleartext value.
    let umbrella = max_password_message_size();
    let cleartext = max_password_message_size_cleartext();
    let md5 = max_password_message_size_md5();
    assert_eq!(umbrella, cleartext, "umbrella must equal cleartext (dominant)");
    assert!(umbrella >= md5, "umbrella must be at least md5 size");
}

#[test]
fn max_owned_send_len_accommodates_password_message() {
    // The headline tier-1 invariant: WriteBuf can always hold any
    // PasswordMessage frame. If MAX_OWNED_SEND_LEN ever shrinks
    // below max_password_message_size(), the const-assert in
    // write_buf.rs fires at build time. This runtime check
    // duplicates that guarantee for explicit visibility.
    assert!(
        MAX_OWNED_SEND_LEN >= max_password_message_size(),
        "MAX_OWNED_SEND_LEN ({MAX_OWNED_SEND_LEN}) must accommodate max PasswordMessage \
         ({})",
        max_password_message_size(),
    );
}

#[test]
fn cleartext_size_strictly_exceeds_md5() {
    // Sanity: cleartext is always larger than MD5 because passwords
    // can be up to 512 bytes (MAX_PASSWORD_LEN) while MD5 digest is
    // a fixed 35-byte body. This invariant lets the umbrella const
    // pick cleartext unconditionally.
    assert!(
        max_password_message_size_cleartext() > max_password_message_size_md5(),
        "cleartext (variable, up to 512 B body) must exceed MD5 (fixed 35 B body)",
    );
}

#[test]
fn headroom_is_substantial() {
    // Sanity check that we have meaningful headroom — not just
    // "fits exactly". A future bump of MAX_PASSWORD_LEN should not
    // immediately exhaust the buffer.
    let headroom = MAX_OWNED_SEND_LEN.saturating_sub(max_password_message_size());
    assert!(
        headroom > 1000,
        "headroom is {headroom} bytes; expected > 1000 for safe future bumps",
    );
}
