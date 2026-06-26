//! Scripted transports honouring a [`ChunkSchedule`].
//!
//! [`split_into_chunks`] fragments a server-reply byte stream per the schedule
//! into a chunk queue. The SYNC twin pops chunks directly from a
//! [`ChunkQueue`]; the ASYNC twin reads them through a [`ScriptedReader`]
//! (`AsyncRead`) and writes captured client bytes through a [`ScriptedWriter`]
//! (`AsyncWrite`). Both feed the *same* chunk fragmentation, so the resulting
//! observation must match across twins — the chunk schedule fragments READS,
//! never the observed outcome.

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bsql_postgres_proto::{HeaderParse, parse_header};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::transcript::ChunkSchedule;

/// Fragment `bytes` into a chunk list per `schedule`.
///
/// - `AllAtOnce`: one chunk with everything (empty input → no chunks).
/// - `OneBytePerRead`: one chunk per byte.
/// - `SplitHeaders`: per frame, a 5-byte header chunk then a body chunk;
///   bytes that do not parse as a frame header are emitted as a final chunk
///   verbatim (so malformed/partial tails still replay).
#[must_use]
pub fn split_into_chunks(bytes: &[u8], schedule: ChunkSchedule) -> Vec<Vec<u8>> {
    match schedule {
        ChunkSchedule::AllAtOnce => {
            if bytes.is_empty() {
                Vec::new()
            } else {
                vec![bytes.to_vec()]
            }
        }
        ChunkSchedule::OneBytePerRead => bytes.iter().map(|b| vec![*b]).collect(),
        ChunkSchedule::SplitHeaders => split_headers(bytes),
    }
}

/// Per-frame header/body split. The header is always the first 5 bytes; the
/// body is the remaining `total_len - 5`.
fn split_headers(bytes: &[u8]) -> Vec<Vec<u8>> {
    const HEADER_LEN: usize = 5;
    let mut chunks = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let rest = match bytes.get(offset..) {
            Some(r) => r,
            None => break,
        };
        let HeaderParse::Ok { total_len, .. } = parse_header(rest) else {
            // Not a parseable frame header (a deliberately-truncated tail in an
            // adversarial fixture): emit the remainder verbatim so the engine
            // still observes exactly these bytes.
            chunks.push(rest.to_vec());
            break;
        };
        let total = usize::from(total_len);
        let frame_end = offset.saturating_add(total);
        let Some(frame) = bytes.get(offset..frame_end.min(bytes.len())) else {
            chunks.push(rest.to_vec());
            break;
        };
        match frame.split_at_checked(HEADER_LEN) {
            Some((header, body)) => {
                chunks.push(header.to_vec());
                if !body.is_empty() {
                    chunks.push(body.to_vec());
                }
            }
            // A frame shorter than its own 5-byte header cannot occur for a
            // parsed header (total_len >= 5), but handle defensively: emit
            // verbatim rather than dropping bytes.
            None => chunks.push(frame.to_vec()),
        }
        if frame_end <= offset {
            // Zero-advance guard: a degenerate total_len would loop forever;
            // emit the remainder and stop.
            chunks.push(rest.to_vec());
            break;
        }
        offset = frame_end;
    }
    chunks
}

/// A FIFO of scripted read chunks. Shared shape behind both twins.
#[derive(Debug, Default)]
pub struct ChunkQueue {
    chunks: VecDeque<Vec<u8>>,
}

impl ChunkQueue {
    /// Build from a pre-fragmented chunk list.
    #[must_use]
    pub fn new(chunks: Vec<Vec<u8>>) -> Self {
        Self { chunks: chunks.into() }
    }

    /// Pop the next scripted chunk, or `None` when exhausted.
    pub fn next_chunk(&mut self) -> Option<Vec<u8>> {
        self.chunks.pop_front()
    }

    /// True when no chunks remain.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

/// `AsyncRead` over a scripted chunk queue: each `poll_read` delivers at most
/// the next chunk's bytes. When the queue is empty it returns `Ready(Ok(()))`
/// with an unfilled buffer — i.e. EOF (read of 0). Genuinely drives the async
/// path: the engine's reads go through `poll_read`.
#[derive(Debug, Default)]
pub struct ScriptedReader {
    queue: ChunkQueue,
}

impl ScriptedReader {
    /// Build from a pre-fragmented chunk list.
    #[must_use]
    pub fn new(chunks: Vec<Vec<u8>>) -> Self {
        Self { queue: ChunkQueue::new(chunks) }
    }
}

impl AsyncRead for ScriptedReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // `ScriptedReader` is `Unpin` (only a `VecDeque`), so a safe `get_mut`
        // suffices — no `unsafe` pin projection.
        let this = self.get_mut();
        if let Some(mut chunk) = this.queue.next_chunk() {
            let cap = buf.remaining();
            if chunk.len() <= cap {
                buf.put_slice(&chunk);
            } else {
                // Chunk larger than the read buffer's headroom: deliver what
                // fits and push the remainder back so no bytes are lost.
                let tail = chunk.split_off(cap);
                buf.put_slice(&chunk);
                this.queue.chunks.push_front(tail);
            }
        }
        // Empty queue ⇒ leave `buf` untouched ⇒ the caller's `read` returns 0
        // (EOF for this step).
        Poll::Ready(Ok(()))
    }
}

/// `AsyncWrite` capturing every client byte into an in-memory buffer.
#[derive(Debug, Default)]
pub struct ScriptedWriter {
    /// Captured client→server bytes, in write order.
    pub captured: Vec<u8>,
}

impl AsyncWrite for ScriptedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().captured.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
