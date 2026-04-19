//! SCRAM-SHA-256 authentication (RFC 5802 + RFC 7677).
//!
//! - [`crypto`] — cryptographic operations composed over RustCrypto
//!   crates. Never hand-rolled (DEF-META-01).
//! - [`wire`] — SCRAM text-protocol message construction and parsing.
//! - [`types`] — [`SecretDigest`] (no `PartialEq`, DEF-039) and
//!   [`CappedServerNonce`] (DEF-040).
//! - [`session`] — [`ScramSession`] typestate eliminating the
//!   `Trust`-vs-`ScramPassword` double-match seam (audit A2).
//!
//! Channel binding (SCRAM-SHA-256-PLUS) is deferred to Phase 1e
//! (DEF-053). The GS2 header is always `n,,` and the channel binding
//! data is always `biws`.
//!
//! [`ScramSession`]: session::ScramSession

pub mod crypto;
pub mod session;
pub mod types;
pub mod wire;
