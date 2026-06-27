#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

//! Differential-replay corpus for the bsql PostgreSQL engine.
//!
//! # What this is
//!
//! A Rust-native typed corpus of protocol transcripts replayed through an
//! [`Adapter`] seam defined entirely over OBSERVABLE protocol I/O. It is a
//! behavioural-equivalence oracle: the same [`Transcript`] corpus must stay
//! green on the CURRENT engine now (via [`SansIoAdapter`], driving the public
//! sans-IO `Session`) and on any FUTURE re-implementation later (a second
//! adapter over the rebuilt engine). The assertion
//! `adapter.run(t) == t.expect` pins one engine's behaviour; the assertion
//! `adapter_a.run(t) == adapter_b.run(t)` proves two engines agree.
//!
//! # The observable boundary
//!
//! [`ObservedRun`] and [`Transcript`] name NO internal engine type. They carry
//! only values that survive a rebuild: the client→server wire bytes, raw
//! per-column result bytes (decode policy is engine-specific, so values are
//! NOT typed here), server notices/notifications/parameter statuses, the
//! command tag, and a coarse terminal status. The [`SansIoAdapter`] (this
//! engine's bridge) is free to touch internal types — it is the throwaway
//! half of the seam — but everything it returns is observable-only.
//!
//! # Twins
//!
//! [`SansIoAdapter`] runs each transcript two ways over the *same* public
//! `Session`: a SYNC twin (a scripted blocking byte source/sink, no runtime)
//! and an ASYNC twin (`block_on` over a scripted `AsyncRead`/`AsyncWrite` on a
//! current-thread runtime). Both honour a transport [`ChunkSchedule`] so one
//! fixture replays under several fragmentations (all-at-once, one byte per
//! read, header/body split) — exercising partial-frame resumption.
//!
//! # Extensibility
//!
//! Adding a fixture is adding a data value: build a [`Transcript`] from the
//! frame vocabulary in [`frames`] and the request vocabulary in
//! [`ClientRequest`]. No adapter code changes for the data-driven request
//! kinds.

pub mod adapter;
pub mod corpus;
pub mod frames;
pub mod observed;
pub mod sans_io;
pub mod transcript;
pub mod transport;

pub use adapter::Adapter;
pub use observed::{
    ObservedErr, ObservedNotice, ObservedNotify, ObservedOk, ObservedResultSet, ObservedRun,
    ObservedStatus, ObservedTxStatus, ProtocolFailureKind, TerminalErrorKind,
};
pub use sans_io::SansIoAdapter;
pub use transcript::{
    ChunkSchedule, ClientRequest, ParamSpec, Setup, Step, Transcript,
};
pub use transport::split_into_chunks;
