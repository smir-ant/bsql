//! RFC 4013 SASLprep normalisation of the SCRAM password.
//!
//! RFC 5802 (SCRAM) mandates that the client feed `SASLprep(password)` — not
//! the raw bytes — to PBKDF2, exactly as PostgreSQL / libpq / pgAdmin / JDBC do
//! both when `ALTER ROLE … PASSWORD` builds the stored verifier and when they
//! authenticate. Skipping it rejects a legitimate unicode password whose
//! SASLprep form differs from its raw bytes (a non-breaking space `U+00A0` that
//! maps to a plain space, a soft hyphen that maps to nothing, an
//! NFKC-normalisable codepoint) with a spurious `28P01`: the server holds a
//! verifier for the NORMALISED form while the un-normalised proof never matches.
//!
//! SASLprep is expert-domain Unicode (RFC 3454 mapping + NFKC normalisation +
//! bidirectional checks + prohibited-codepoint tables). Policy 9 forbids
//! hand-rolling it, so this is a thin composition over the vetted `stringprep`
//! crate — the same implementation `sqlx` and `tokio-postgres` use for SCRAM.
//!
//! # Why here and not in the `proto` kernel
//!
//! `stringprep` is a `std` crate (it carries the Unicode tables), so it belongs
//! in this `std` driver-support crate, NOT in the `#![no_std]`
//! `bsql-postgres-proto` sans-IO kernel. Normalising at credential construction
//! (before the bytes reach proto) keeps proto byte-pure and no_std-buildable
//! with `scram` on: its `compute_client_proof` receives an already-SASLprepped
//! password, so its RFC-7677 test vectors (ASCII, SASLprep-invariant) are
//! unchanged.
//!
//! # Zero-copy on the common path
//!
//! `stringprep::saslprep` returns `Cow::Borrowed` for an ASCII password with no
//! ASCII control character — the overwhelmingly common case — which is a single
//! cheap scan with no allocation and no new secret buffer. Only a password
//! SASLprep actually rewrites (non-ASCII → mapping / NFKC) yields a `Cow::Owned`
//! heap `String`, wrapped in `Zeroizing` so the normalised plaintext is scrubbed
//! after it is copied into the zeroize-on-drop [`Password`].
//!
//! # Honest zeroize limitation
//!
//! On the non-ASCII path, `stringprep`'s internal NFKC normalisation allocates
//! its own transient `String` / iterator buffers that briefly hold the password
//! plaintext, and those are NOT zeroized — this is inherent to the vetted crate
//! (the same holds for `sqlx` / `tokio-postgres`, which use it identically), and
//! bsql does not fork it to add scrubbing. Only bsql's OWN output copy is
//! scrubbed: the `Cow::Owned` `String` (via `Zeroizing`) and the `Password`
//! buffer (`ZeroizeOnDrop`). So a reader should not over-trust the zeroize
//! coverage here as end-to-end — the guarantee is "bsql's retained copies are
//! scrubbed", not "no plaintext ever touches un-scrubbed heap". The ASCII
//! fast-path allocates nothing and so has no such transient.

use crate::error::DriverError;
use bsql_postgres_proto::Password;
use std::borrow::Cow;
use zeroize::Zeroizing;

/// Apply RFC 4013 SASLprep to a SCRAM password and copy the normalised form
/// into a zeroize-on-drop [`Password`].
///
/// `pw` comes from the consumer's configuration (a Rust `&str`), so it is valid
/// UTF-8 by construction and SASLprep's Unicode domain is total over it — there
/// is no non-UTF-8 branch at this seam (a binary password could only arrive
/// through proto's own `Password::try_from_bytes`, which is exercised by proto
/// tests with ASCII, SASLprep-invariant input).
///
/// # Failure — classified, never silent, never a panic
///
/// A codepoint RFC 4013 PROHIBITS (an ASCII / non-ASCII control character, a
/// private-use or non-character codepoint, a surrogate, an unassigned codepoint,
/// or a bidirectional-rule violation) is a classified pre-connect
/// [`DriverError::Config`]. This is a deliberate, fail-SAFE, LOUD divergence
/// from PostgreSQL/libpq, which fall through to the RAW password on a prohibited
/// codepoint: bsql refuses a malformed password with a named reason rather than
/// silently deriving a proof from bytes the RFC declares invalid (the project's
/// no-silent-fallback discipline). The mapping / deletion / NFKC cases — the
/// actual interop gap a non-breaking-space or soft-hyphen password hits — DO
/// normalise and authenticate. The error message deliberately does NOT name the
/// offending codepoint: that codepoint is part of the secret password.
///
/// An empty or over-length normalised result maps to the existing
/// `"invalid password"` config error, matching the raw `Password::try_from_str`
/// this replaced.
pub fn saslprep_password(pw: &str) -> Result<Password, DriverError> {
    match stringprep::saslprep(pw) {
        // ASCII / already-normal: the borrow aliases `pw` unchanged, so the
        // SASLprep form IS `pw`. Copy it straight in — no extra secret buffer.
        Ok(Cow::Borrowed(_)) => {
            Password::try_from_str(pw).map_err(|_| DriverError::Config("invalid password"))
        }
        // Rewritten by mapping / NFKC: `normalized` is a fresh heap `String`
        // holding the normalised password — a secret. Zeroize the intermediate
        // once it is copied into the zeroize-on-drop `Password`.
        Ok(Cow::Owned(normalized)) => {
            let normalized = Zeroizing::new(normalized);
            Password::try_from_bytes(normalized.as_bytes())
                .map_err(|_| DriverError::Config("invalid password"))
        }
        // Prohibited codepoint / bidirectional-rule violation (RFC 4013 §2.3-2.5).
        Err(_) => Err(DriverError::Config(
            "password contains a codepoint prohibited by RFC 4013 SASLprep",
        )),
    }
}

#[cfg(test)]
mod tests {
    //! RFC 4013 §3 SASLprep known-answer conformance + the credential-builder
    //! contract. These are OFFLINE (no server, no crypto) — they pin the exact
    //! transform so a future `stringprep` bump or a mis-wired call turns red
    //! without a database.
    //!
    //! Category (A) spec-conformance: the SASLprep transform bsql feeds to
    //! PBKDF2 matches the RFC 4013 §3 example table + the reported interop case.

    use super::saslprep_password;
    use crate::error::DriverError;

    /// Assert the normalised bytes `saslprep_password` will feed to PBKDF2 equal
    /// `expected`, by reading them back out of the produced `Password`.
    #[track_caller]
    fn assert_prepped(input: &str, expected: &[u8]) {
        let pw = match saslprep_password(input) {
            Ok(p) => p,
            Err(e) => panic!("saslprep_password({input:?}) errored: {e:?}"),
        };
        assert_eq!(
            pw.as_bytes(),
            expected,
            "SASLprep({input:?}) must normalise to the RFC 4013 form",
        );
    }

    /// RFC 4013 §3 example 2: an ASCII password is unchanged (the common,
    /// zero-copy path).
    #[test]
    fn ascii_password_is_unchanged() {
        assert_prepped("user", b"user");
        assert_prepped("test_password_123", b"test_password_123");
    }

    /// RFC 4013 §3 example 3: case is PRESERVED (SASLprep is not case-folding).
    #[test]
    fn case_is_preserved() {
        assert_prepped("USER", b"USER");
    }

    /// The reported interop case: `U+00A0` (NO-BREAK SPACE, RFC 3454 table
    /// C.1.2) MAPS to a plain space `U+0020`. A password `pa\u{00A0}ss` set via
    /// psql (which SASLpreps it to `pa ss`) must normalise to `pa ss` here so the
    /// proof matches the server's verifier.
    #[test]
    fn non_breaking_space_maps_to_space() {
        assert_prepped("pa\u{00A0}ss", b"pa ss");
        assert_prepped("\u{00A0}", b" ");
    }

    /// RFC 4013 §3 example 1: SOFT HYPHEN `U+00AD` (table B.1) maps to NOTHING
    /// (deleted). `I\u{00AD}X` → `IX`.
    #[test]
    fn soft_hyphen_maps_to_nothing() {
        assert_prepped("I\u{00AD}X", b"IX");
    }

    /// RFC 4013 §3 examples 4 & 5: NFKC normalisation. `U+00AA` (FEMININE
    /// ORDINAL INDICATOR) → `a`; `U+2168` (ROMAN NUMERAL NINE) → `IX`.
    #[test]
    fn nfkc_normalisation() {
        assert_prepped("\u{00AA}", b"a");
        assert_prepped("\u{2168}", b"IX");
    }

    /// RFC 4013 §3 example 6: `U+0007` (BELL, a prohibited ASCII control
    /// character) is a classified `DriverError::Config`, never a panic and never
    /// a silent raw pass-through.
    #[test]
    fn prohibited_control_character_is_classified() {
        let err = saslprep_password("ab\u{0007}cd");
        match err {
            Err(DriverError::Config(msg)) => {
                assert!(
                    msg.contains("SASLprep"),
                    "prohibited-codepoint error must name SASLprep, got {msg:?}",
                );
                assert!(
                    !msg.contains('\u{0007}'),
                    "the error must NOT leak the secret codepoint",
                );
            }
            other => panic!("expected a classified Config error, got {other:?}"),
        }
    }

    /// RFC 4013 §3 example 7: a bidirectional-rule violation (a RandALCat
    /// character `U+0627` not bracketing the string, followed by an LCat digit)
    /// is a classified `DriverError::Config`.
    #[test]
    fn prohibited_bidirectional_text_is_classified() {
        let err = saslprep_password("\u{0627}\u{0031}");
        assert!(
            matches!(err, Err(DriverError::Config(_))),
            "a bidirectional-rule violation must be a classified Config error, got {err:?}",
        );
    }
}
