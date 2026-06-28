//! A scripted [`Transport`] for driving the new engine's real verbs.
//!
//! Implements the strangler engine's `Transport` seam over a pre-fragmented
//! reply script: `read` delivers the next scripted chunk (honouring the
//! transcript's [`ChunkSchedule`] for partial-frame resumption), and `write`
//! captures every client byte into a shared buffer the adapter reads back after
//! the session closure returns (the engine owns the transport for the session's
//! life, so the capture must escape through a `Send`-able shared handle).
//! `flush`/`shutdown` are ready no-ops. Every operation resolves synchronously,
//! so a verb future over this transport completes under one `poll_once`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bsql_postgres_proto::engine::Transport;
use core::convert::Infallible;
use core::future::{ready, Future};

/// Shared, `Send` capture of the client→server wire bytes.
pub type ClientCapture = Arc<Mutex<Vec<u8>>>;

/// A scripted transport: a FIFO of reply chunks to deliver, plus a shared
/// capture of written client bytes.
pub struct EngineScriptTransport {
    chunks: VecDeque<Vec<u8>>,
    captured: ClientCapture,
}

impl EngineScriptTransport {
    /// Build from a pre-fragmented reply chunk list and a shared capture handle.
    #[must_use]
    pub fn new(chunks: Vec<Vec<u8>>, captured: ClientCapture) -> Self {
        Self {
            chunks: chunks.into(),
            captured,
        }
    }
}

impl Transport for EngineScriptTransport {
    type Error = Infallible;

    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = match self.chunks.front_mut() {
            Some(chunk) => {
                let n = chunk.len().min(buf.len());
                if let (Some(dst), Some(src)) = (buf.get_mut(..n), chunk.get(..n)) {
                    dst.copy_from_slice(src);
                }
                if n == chunk.len() {
                    self.chunks.pop_front();
                } else {
                    // Chunk larger than the offered slot: keep the remainder for
                    // the next read (the AllAtOnce schedule is one big chunk).
                    chunk.drain(..n);
                }
                n
            }
            // Script exhausted ⇒ EOF (read of 0).
            None => 0,
        };
        ready(Ok(n))
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        // Capture synchronously before the future is created — no lock is held
        // across an await. A poisoned mutex is impossible single-threaded; the
        // dropped write would surface as a client-bytes mismatch in the
        // differential, never a silent corruption.
        if let Ok(mut sink) = self.captured.lock() {
            sink.extend_from_slice(buf);
        }
        ready(Ok(buf.len()))
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}
