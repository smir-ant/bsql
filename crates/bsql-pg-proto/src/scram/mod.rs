//! SCRAM-SHA-256 authentication (RFC 5802 + RFC 7677).
//!
//! - [`crypto`] — cryptographic operations composed over RustCrypto
//!   crates. Never hand-rolled (expert-domain policy).
//! - [`wire`] — SCRAM text-protocol message construction and parsing.
//! - [`types`] — [`SecretDigest`] (no `PartialEq`; constant-time compare
//!   only) and [`CappedServerNonce`].
//! - [`session`] — [`ScramSession`] typestate eliminating the
//!   `Trust`-vs-`ScramPassword` double-match seam.
//!
//! # Exchange flow (RFC 5802 mapped to our state machine)
//!
//! ```text
//! Client                                            Server
//! ------                                            ------
//!   │
//!   │  StartupMessage (user/db)                ─────►
//!   │  [state: ConnectingStartupScram { reply }]
//!   │  [scram_state: Some(Session(ScramSession))]
//!   │
//!   │                                           ◄───── AuthenticationSASL('R'/10)
//!   │                                                  + "SCRAM-SHA-256\0\0"
//!   │  [dispatch_auth_in_startup_scram:
//!   │   verify SCRAM-SHA-256 listed,
//!   │   build client-first-message]
//!   │
//!   │  SASLInitialResponse ('p')               ─────►
//!   │  "n,,n=user,r=<client_nonce_b64>"
//!   │  [state: ConnectingScramAwaitingServerFirst { reply }]
//!   │  [scram_state: Some(AwaitingFirst {
//!   │      session, client_first_bare, client_nonce_b64 })]
//!   │
//!   │                                           ◄───── AuthenticationSASLContinue('R'/11)
//!   │                                                  "r=<server_nonce>,s=<salt>,i=<iter>"
//!   │  [dispatch_auth_sasl_continue:
//!   │   parse server-first,
//!   │   compute SaltedPassword = PBKDF2(pw, salt, i),
//!   │   compute ClientKey = HMAC(SaltedPassword,"Client Key"),
//!   │   compute StoredKey = SHA-256(ClientKey),
//!   │   compute AuthMessage = c1bare + "," + sfirst + "," + cfinalnoproof,
//!   │   compute ClientSignature = HMAC(StoredKey, AuthMessage),
//!   │   compute ClientProof = ClientKey XOR ClientSignature,
//!   │   compute ServerKey = HMAC(SaltedPassword,"Server Key"),
//!   │   expected_server_sig = HMAC(ServerKey, AuthMessage),
//!   │   zeroize SaltedPassword + ClientKey + StoredKey immediately]
//!   │
//!   │  SASLResponse ('p')                      ─────►
//!   │  client-final-message + base64(ClientProof)
//!   │  [state: ConnectingScramAwaitingServerFinal { reply }]
//!   │  [scram_state: Some(AwaitingFinal { expected_server_sig })]
//!   │
//!   │                                           ◄───── AuthenticationSASLFinal('R'/12)
//!   │                                                  "v=<server_signature_b64>"
//!   │  [dispatch_auth_sasl_final:
//!   │   parse server-final,
//!   │   constant-time compare via `subtle::ConstantTimeEq`
//!   │   against expected_server_sig → tier-2 timing safety]
//!   │
//!   │                                           ◄───── AuthenticationOk('R'/0)
//!   │  [dispatch_auth_ok_after_scram:
//!   │   state → ConnectingPostAuthAwaitingKey(reply);
//!   │   scram_state cleared naturally (taken + not re-populated)]
//!   │
//!   │                                           ◄───── BackendKeyData('K') + RFQ('Z')
//!   │  [DeliverReply(StartupComplete { pid, secret_key, tx_status })]
//!   │  [state: Idle]
//!   │
//! ```
//!
//! # Security-critical discipline
//!
//! - `subtle::ConstantTimeEq` for `expected_server_sig` comparison
//!   — never `==`, protects against signature-timing side channel.
//! - `zeroize::ZeroizeOnDrop` on every secret-material type:
//!   `Password`, `ScramSession`, `SaltedPassword`, `ClientKey`,
//!   `StoredKey`, `ClientSignature`, `SecretDigest`.
//! - `getrandom` for client nonce — OS-provided entropy, never
//!   `SystemTime`-seeded.
//! - RustCrypto primitives only (sha2, hmac, pbkdf2, base64ct,
//!   subtle) — expert-domain code is never hand-rolled.
//! - On any Errored mid-SCRAM, `dispatch()` clears `scram_state`
//!   immediately so password material doesn't linger in memory on
//!   terminal connections.
//!
//! Channel binding (SCRAM-SHA-256-PLUS) is not supported in this
//! crate yet. The GS2 header is always `n,,` and the channel binding
//! data is always `biws`.
//!
//! [`ScramSession`]: session::ScramSession

pub mod crypto;
pub mod session;
pub mod types;
pub mod wire;
