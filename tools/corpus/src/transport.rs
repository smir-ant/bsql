//! Transport chunk fragmentation honouring a [`ChunkSchedule`].
//!
//! [`split_into_chunks`] fragments a server-reply byte stream per the schedule
//! into a chunk list. The engine adapter feeds those chunks to the engine under
//! test, so one fixture replays under several fragmentations — the chunk
//! schedule fragments READS, never the observed outcome (fragmentation
//! invariance is asserted corpus-wide).

use bsql_postgres_proto::{HeaderParse, parse_header};

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
