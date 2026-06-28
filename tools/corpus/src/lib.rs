#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

//! Replay corpus for the bsql PostgreSQL engine.
//!
//! # What this is
//!
//! A Rust-native typed corpus of protocol transcripts replayed through an
//! [`Adapter`] seam defined entirely over OBSERVABLE protocol I/O. Each
//! [`Transcript`] carries a pinned [`ObservedRun`] golden — the observable
//! result captured from the real engine — and the regression asserts
//! `adapter.run(t) == t.expect` for the engine under test. Because the goldens
//! name only values that survive a rebuild, the same corpus pins one engine's
//! behaviour now and any future re-implementation later: an
//! `adapter_a.run(t) == adapter_b.run(t)` differential across two adapters
//! proves two engines agree (the mechanism that gated the engine cutover).
//!
//! # The observable boundary
//!
//! [`ObservedRun`] and [`Transcript`] name NO internal engine type. They carry
//! only values that survive a rebuild: the client→server wire bytes, raw
//! per-column result bytes (decode policy is engine-specific, so values are NOT
//! typed here), server notices/notifications/parameter statuses, the command
//! tag, and a coarse terminal status. An adapter is free to touch internal
//! engine types — it is the throwaway half of the seam — but everything it
//! returns is observable-only.
//!
//! # The engine adapter
//!
//! The adapter over the engine under test lives in the test crates
//! (`src/engine_adapter.rs`, compiled in via `#[path]`), not in this library:
//! nothing shipped depends on it, and keeping it in the test crate keeps it in
//! the test lint context. It drives the engine over a transcript's scripted
//! server bytes — honouring a transport [`ChunkSchedule`] so one fixture
//! replays under several fragmentations (all-at-once, one byte per read,
//! header/body split), exercising partial-frame resumption — and returns the
//! observable [`ObservedRun`] the regression compares against the pinned golden.
//!
//! # Extensibility
//!
//! Adding a fixture is adding a data value: build a [`Transcript`] from the
//! frame vocabulary in [`frames`] and the request vocabulary in
//! [`ClientRequest`]. No adapter code changes for the data-driven request kinds.

pub mod adapter;
pub mod corpus;
pub mod frames;
pub mod observed;
pub mod transcript;
pub mod transport;

pub use adapter::Adapter;
pub use observed::{
    ObservedErr, ObservedNotice, ObservedNotify, ObservedOk, ObservedResultSet, ObservedRun,
    ObservedStatus, ObservedTxStatus, ProtocolFailureKind, TerminalErrorKind,
};
pub use transcript::{
    ChunkSchedule, ClientRequest, ParamSpec, Setup, Step, Transcript,
};
pub use transport::split_into_chunks;
