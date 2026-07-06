//! Active-phase outbound frame builders.
//!
//! Each builder assembles one PostgreSQL request frame through the
//! [`FrameSink`] push surface (length prefixes patched by
//! [`FrameSink::with_length_prefix`], exactly the PG §55.7 "length includes
//! itself" convention). The FIXED-size frames (Sync, Execute, Describe, Close,
//! the Parse / simple-query headers) build into a transient bounded
//! [`WriteBuf`] and are copied onto the engine's persistent
//! [`SendBuf`](super::SendBuf) — the transient-scratch / persistent-queue split
//! the connecting handshake uses. [`WriteBuf`] is `heapless` and scrub-on-drop,
//! so a parameter-bearing assembly never outlives the build, and its capacity is
//! const-asserted to fit every bounded frame's worst case.
//!
//! The `Bind` frame is the ONE exception: its parameter block is UNBOUNDED (a
//! multi-kilobyte `jsonb`, a large `bytea`, a several-hundred-element array), so
//! [`build_bind`] / [`build_bind_prepared`] are generic over [`FrameSink`] and
//! the verb layer streams them straight onto the growable send buffer with a
//! back-patched length — never a fixed cap. The same generic builder can target
//! a bounded [`WriteBuf`] (the byte-twin reference), so the streaming production
//! bytes and the bounded reference cannot drift.
//!
//! # Why engine-local builders, not the crate's existing frame encoders
//!
//! Three shapes were weighed for the active frames:
//!
//! 1. **Reuse the crate's `protocol.rs` builders directly** — rejected: they take
//!    a `BrandedWriteReserved` (a brand-locked `WriteBuf` view minted by the
//!    staging apparatus) and return a `WriteRange` (offsets a `StagedAction`
//!    consumes), threading the old push-engine's `WriteRange`/`ProtocolError`/
//!    staging types into the new sans-I/O engine — the exact coupling the
//!    strangler separation exists to avoid.
//! 2. **Widen those builders to also accept a plain `&mut WriteBuf`** — rejected:
//!    it edits the old engine's surface (which must stay byte-identical), and the
//!    `WriteRange` return is intrinsic to the staging path, not removable.
//! 3. **Engine-local builders into a `WriteBuf` via the public push API** —
//!    chosen: zero coupling to the old engine, and byte-identical by construction
//!    because they drive the same `with_length_prefix` / `push_*` primitives and,
//!    for the prepared-macro Bind, the same [`ParamsWriter`] format/encode source
//!    and the same `BIND_RESULT_FORMATS_ALL_BINARY` trailer the macro path uses.
//!    This mirrors the connecting engine, whose `build_startup_message` is itself
//!    an engine-local `WriteBuf` builder rather than a `protocol.rs` reuse.

use crate::params::ParamsWriter;
use crate::prepared::BIND_RESULT_FORMATS_ALL_BINARY;
use crate::wire::{
    CloseTargetByte, DescribeTargetByte, TAG_BIND, TAG_CLOSE, TAG_DESCRIBE, TAG_EXECUTE, TAG_PARSE,
    TAG_QUERY,
};
use crate::write_buf::{FrameSink, WriteBuf, WriteBufFull};

/// `CopyDone` (`'c'`) wire literal: tag + a length field of 4 (the length
/// includes itself, no body). Mirrors the shape of the crate's `Sync`/`Terminate`
/// literals.
pub(super) const COPY_DONE_WIRE: [u8; 5] = [crate::wire::TAG_COPY_DONE.byte(), 0, 0, 0, 4];

/// `'Q'` simple-query frame: `tag | len | sql | NUL`.
///
/// The length prefix (self-inclusive) is `4 + sql.len() + 1`. `Err` only if the
/// SQL plus framing overflows the bounded [`WriteBuf`] — a classified
/// frame-too-long, never a silent truncation.
#[inline]
pub(crate) fn build_simple_query(wb: &mut WriteBuf, sql: &[u8]) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_QUERY.byte())?;
    wb.with_length_prefix(|w| {
        w.push_bytes(sql)?;
        w.push_u8(0)
    })
}

/// `'Q'` simple-query frame HEADER only: `tag | len`, where `len` is the
/// self-inclusive `4 + sql_len + 1` (the length field, the SQL body, and the
/// trailing NUL). The SQL body and the NUL are queued directly onto the send
/// buffer by the caller, so a multi-kilobyte query never has to fit the bounded
/// [`WriteBuf`] — the same header-in-scratch / body-on-send-buffer split the
/// `CopyData` path uses. `Err` only if `4 + sql_len + 1` overflows `u32`.
#[inline]
pub(crate) fn build_simple_query_header(wb: &mut WriteBuf, sql_len: u32) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_QUERY.byte())?;
    // Self-inclusive length: 4 (the length field) + sql_len + 1 (the NUL).
    let len = sql_len.checked_add(5).ok_or(WriteBufFull)?;
    wb.push_u32_be(len)
}

/// `'P'` Parse frame: `tag | len | stmt_name NUL | sql NUL | n_param_types=0`.
///
/// `#[cfg(test)]` — the byte-twin REFERENCE only. The production prepare path
/// streams the SQL via [`build_parse_header`] + the send buffer (so a large SQL
/// never has to fit the bounded [`WriteBuf`]); this whole-frame builder produces
/// byte-identical output for SQL that fits, so it is kept as the reference the
/// `protocol.rs` byte-twin pins against (and the in-module twin chains the
/// streaming assembly to it). No parameter-type OIDs are declared
/// (`n_param_types = 0`): the server infers them.
#[cfg(test)]
#[inline]
pub(crate) fn build_parse(
    wb: &mut WriteBuf,
    stmt_name: &[u8],
    sql: &[u8],
) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_PARSE.byte())?;
    wb.with_length_prefix(|w| {
        w.push_nul_terminated(stmt_name)?;
        w.push_nul_terminated(sql)?;
        w.push_i16_be(0)
    })
}

/// `'P'` Parse frame HEADER only: `tag | len | stmt_name NUL`, where `len` is the
/// self-inclusive `4 + (stmt_name_len + 1) + (sql_len + 1) + 2` (the length
/// field, the NUL-terminated statement name, the NUL-terminated SQL, and the
/// `n_param_types = 0` trailer). The SQL body, its NUL, and the 2-byte
/// zero-param-types trailer ([`PARSE_SQL_TRAILER`]) are queued directly onto the
/// send buffer by the caller, so a multi-kilobyte prepared SQL never has to fit
/// the bounded [`WriteBuf`] — the same header-in-scratch / body-on-send-buffer
/// split the simple-query and `CopyData` paths use. No parameter-type OIDs are
/// declared (the server infers them); the compile-checked `query!` macro bakes
/// its own Parse template (with OIDs) enqueued verbatim elsewhere. `Err` only if
/// the computed length overflows `u32`.
#[inline]
pub(crate) fn build_parse_header(
    wb: &mut WriteBuf,
    stmt_name: &[u8],
    sql_len: u32,
) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_PARSE.byte())?;
    let stmt_len = u32::try_from(stmt_name.len()).map_err(|_| WriteBufFull)?;
    // Self-inclusive length: 4 (len field) + stmt_name + NUL + sql + NUL +
    // n_param_types(2). Checked throughout (the forbid wall bars wrapping add).
    let len = 4u32
        .checked_add(stmt_len)
        .and_then(|v| v.checked_add(1))
        .and_then(|v| v.checked_add(sql_len))
        .and_then(|v| v.checked_add(1))
        .and_then(|v| v.checked_add(2))
        .ok_or(WriteBufFull)?;
    wb.push_u32_be(len)?;
    wb.push_nul_terminated(stmt_name)
}

/// The trailing bytes the streaming Parse path queues after the SQL body: the
/// SQL's NUL terminator + `n_param_types = 0` (i16 BE) — completing the frame the
/// [`build_parse_header`] header opened.
pub(super) const PARSE_SQL_TRAILER: [u8; 3] = [0, 0, 0];

/// `'D'` Describe-statement frame: `tag | len | 'S' | stmt_name NUL`.
#[inline]
pub(crate) fn build_describe_statement(wb: &mut WriteBuf, name: &[u8]) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_DESCRIBE.byte())?;
    wb.with_length_prefix(|w| {
        w.push_u8(DescribeTargetByte::Statement.byte())?;
        w.push_nul_terminated(name)
    })
}

/// `'D'` Describe-portal frame: `tag | len | 'P' | portal_name NUL`.
///
/// A portal describe answers with `RowDescription` (or `NoData`) — and, unlike a
/// statement describe, NO `ParameterDescription` (the portal is already bound).
/// Emitted in the fused one-round-trip runtime-param batch
/// (Parse + Bind + Describe-portal + Execute + Sync) so the result schema
/// arrives INLINE (right after `BindComplete`, before the `DataRow`s) instead of
/// on a separate `prepare` round trip.
#[inline]
pub(crate) fn build_describe_portal(wb: &mut WriteBuf, portal: &[u8]) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_DESCRIBE.byte())?;
    wb.with_length_prefix(|w| {
        w.push_u8(DescribeTargetByte::Portal.byte())?;
        w.push_nul_terminated(portal)
    })
}

/// `'C'` Close-statement frame: `tag | len | 'S' | stmt_name NUL`.
#[inline]
pub(crate) fn build_close_statement(wb: &mut WriteBuf, name: &[u8]) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_CLOSE.byte())?;
    wb.with_length_prefix(|w| {
        w.push_u8(CloseTargetByte::Statement.byte())?;
        w.push_nul_terminated(name)
    })
}

/// `'E'` Execute frame: `tag | len | portal NUL | max_rows_i32`.
///
/// `max_rows` is the row cap (`0` = fetch all, PG §55.2.7).
#[inline]
pub(crate) fn build_execute(
    wb: &mut WriteBuf,
    portal: &[u8],
    max_rows: i32,
) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_EXECUTE.byte())?;
    wb.with_length_prefix(|w| {
        w.push_nul_terminated(portal)?;
        w.push_i32_be(max_rows)
    })
}

/// `'B'` Bind frame against a named prepared statement (the runtime
/// prepare/execute path).
///
/// Wire (PG §55.7 Bind): `tag | len | portal NUL | stmt NUL | format-block |
/// n_params | params | n_result_formats=0`. The format block is the compact PG
/// form: for `P::COUNT >= 1` a single `Binary` code (`[0,1,0,1]`) applied to all
/// parameters; for `P::COUNT == 0` an empty `[0,0]`. Result columns default to
/// text (`n_result_formats = 0`); the caller decodes per the recovered
/// `RowDescription`'s declared format. Parameter values flow through
/// [`ParamsWriter`] — the sole binary-encoding authority.
#[inline]
pub(crate) fn build_bind<P: ParamsWriter, S: FrameSink>(
    wb: &mut S,
    portal: &[u8],
    stmt_name: &[u8],
    params: &P,
) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_BIND.byte())?;
    let mut params_err: Option<WriteBufFull> = None;
    wb.with_length_prefix(|w| {
        w.push_nul_terminated(portal)?;
        w.push_nul_terminated(stmt_name)?;
        if P::COUNT == 0 {
            // No parameters: n_format_codes = 0 (PG's all-default form).
            w.push_u16_be(0)?;
        } else {
            // n_format_codes = 1, format[0] = Binary — applied to all params.
            w.push_bytes(&[0, 1, 0, 1])?;
        }
        w.push_u16_be(P::COUNT)?;
        if let Err(e) = params.write_params(w) {
            params_err = Some(e);
        }
        // n_result_formats = 0 → server default (all text).
        w.push_u16_be(0)
    })?;
    match params_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `'B'` Bind frame for the prepared-macro path, from the macro's baked
/// `bind_execute_prefix` (portal NUL + content-addressed stmt NUL).
///
/// Wire (PG §55.7 Bind): `tag | len | prefix | n_format_codes | per-param
/// formats | n_params | params | n_result_formats=1,[Binary]`. The format codes
/// are emitted straight from `P::FORMATS`, the same `&'static [FormatCode]` that
/// drives `write_params` — declared-vs-encoded format drift is unrepresentable.
/// Result columns are all binary (`BIND_RESULT_FORMATS_ALL_BINARY`), matching the
/// macro's synthetic `RowDesc` and `RowDecode` binary decode. This reproduces the
/// crate's macro-execute Bind exactly, so a verb driving a baked `PreparedQuery`
/// puts identical bytes on the wire as the legacy macro push.
#[inline]
pub(crate) fn build_bind_prepared<P: ParamsWriter, S: FrameSink>(
    wb: &mut S,
    prefix: &[u8],
    params: &P,
) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_BIND.byte())?;
    let mut params_err: Option<WriteBufFull> = None;
    wb.with_length_prefix(|w| {
        w.push_bytes(prefix)?;
        // n_format_codes == P::COUNT, one code per parameter from P::FORMATS.
        w.push_u16_be(P::COUNT)?;
        for fc in P::FORMATS {
            w.push_i16_be(fc.as_wire_i16())?;
        }
        w.push_u16_be(P::COUNT)?;
        if let Err(e) = params.write_params(w) {
            params_err = Some(e);
        }
        w.push_bytes(&BIND_RESULT_FORMATS_ALL_BINARY)
    })?;
    match params_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `'d'` CopyData frame header: `tag | len`, where `len` is self-inclusive
/// (`4 + body_len`). The body bytes are queued separately onto the send buffer,
/// so an oversize COPY chunk never needs to fit the bounded [`WriteBuf`].
#[inline]
pub(crate) fn build_copy_data_header(
    wb: &mut WriteBuf,
    body_len: u32,
) -> Result<(), WriteBufFull> {
    wb.push_u8(crate::wire::TAG_COPY_DATA.byte())?;
    let len = body_len.checked_add(4).ok_or(WriteBufFull)?;
    wb.push_u32_be(len)
}

/// `'f'` CopyFail frame: `tag | len | reason | NUL`. Aborts an in-progress
/// COPY IN from the client; the server echoes the reason in its `ErrorResponse`
/// and then returns to idle (PG §55.7). The length prefix (self-inclusive) is
/// `4 + reason.len() + 1`. `Err` only if the reason plus framing overflows the
/// bounded [`WriteBuf`] — a classified frame-too-long, never a silent truncation.
#[inline]
pub(crate) fn build_copy_fail(wb: &mut WriteBuf, reason: &[u8]) -> Result<(), WriteBufFull> {
    wb.push_u8(crate::wire::TAG_COPY_FAIL_OUTBOUND.byte())?;
    wb.with_length_prefix(|w| {
        w.push_bytes(reason)?;
        w.push_u8(0)
    })
}

#[cfg(test)]
mod parse_stream_twin {
    //! Byte-twin for the streaming Parse assembly: the production prepare path
    //! emits `build_parse_header(stmt, sql_len)` (into scratch) ++ `sql` ++
    //! [`PARSE_SQL_TRAILER`] onto the send buffer. This proves that assembly is
    //! byte-identical to the whole-frame [`build_parse`] (which the `protocol.rs`
    //! byte-twin in turn pins to the proven `build_parse_message_cfgtest`), so the
    //! streaming path is transitively byte-correct — AND that a multi-kilobyte SQL
    //! that would overflow the bounded `WriteBuf` builds without `WriteBufFull`.

    use super::{build_parse, build_parse_header, PARSE_SQL_TRAILER};
    use crate::write_buf::WriteBuf;
    use alloc::string::ToString;
    use alloc::vec::Vec;

    /// Assemble the streaming Parse path into a flat byte vector.
    fn streamed(stmt: &[u8], sql: &[u8]) -> Vec<u8> {
        let sql_len = u32::try_from(sql.len()).expect("sql fits u32");
        let mut header = WriteBuf::new();
        build_parse_header(&mut header, stmt, sql_len).expect("header fits scratch");
        let mut out = Vec::new();
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(sql);
        out.extend_from_slice(&PARSE_SQL_TRAILER);
        out
    }

    /// Assemble the whole-frame reference into a flat byte vector.
    fn whole(stmt: &[u8], sql: &[u8]) -> Vec<u8> {
        let mut wb = WriteBuf::new();
        build_parse(&mut wb, stmt, sql).expect("whole frame fits scratch");
        wb.as_bytes().to_vec()
    }

    #[test]
    fn streamed_parse_matches_whole_frame_for_small_sql() {
        let stmt = b"_bsql_0";
        let sql = b"SELECT id, name FROM demo WHERE id = $1";
        let s = streamed(stmt, sql);
        let w = whole(stmt, sql);
        assert_eq!(s.first().copied(), Some(b'P'), "non-vacuous: 'P' frame");
        assert_eq!(s, w, "streamed Parse must equal the whole-frame reference");
    }

    #[test]
    fn streamed_parse_builds_large_sql_without_overflow() {
        // A multi-kilobyte SQL: the whole-frame builder would overflow the bounded
        // WriteBuf, but the streaming header is tiny (tag + len + stmt NUL) and
        // the SQL rides the send buffer, so the header builds with no WriteBufFull.
        let stmt = b"_bsql_big";
        let sql = "SELECT 1 -- ".to_string() + &"x".repeat(3000);
        let s = streamed(stmt, sql.as_bytes());
        // Framing is correct: 'P' tag, and the self-inclusive length matches the
        // emitted byte count (len field counts itself + everything after the tag).
        assert_eq!(s.first().copied(), Some(b'P'));
        let len_bytes: [u8; 4] = s.get(1..5).expect("len field").try_into().expect("4 bytes");
        let declared = u32::from_be_bytes(len_bytes);
        let actual = u32::try_from(s.len().checked_sub(1).expect("tag")).expect("fits u32");
        assert_eq!(declared, actual, "self-inclusive Parse length must match the body");
    }
}

#[cfg(test)]
mod bind_stream_twin {
    //! Byte-twin for the streaming `Bind` assembly. Production streams the Bind
    //! straight onto the growable send buffer (a [`SendFrame`](super::super::flush)
    //! view of [`SendBuf`]) with a back-patched length; this proves two things:
    //!
    //! 1. **Byte-identity for small params.** The SAME generic [`build_bind`]
    //!    over the growable [`SendFrame`] emits bytes identical to the bounded
    //!    [`WriteBuf`] reference — so the streaming production path cannot drift
    //!    from the byte-twin the corpus / engine specs pin.
    //! 2. **The cap is lifted.** A multi-kilobyte parameter that OVERFLOWS the
    //!    bounded `WriteBuf` (the old ~2 KB Bind cap) streams onto `SendFrame`
    //!    with correct framing — the unit-level RED (`WriteBuf` `Err`) → GREEN
    //!    (`SendFrame` `Ok`, self-inclusive length matches the body).

    use super::{build_bind, build_bind_prepared};
    use crate::engine::SendBuf;
    use crate::write_buf::WriteBuf;
    use alloc::vec::Vec;

    /// Assemble a `Bind` via the streaming `SendFrame` path into a flat vector.
    fn streamed<P: crate::params::ParamsWriter>(portal: &[u8], stmt: &[u8], params: &P) -> Vec<u8> {
        let mut sb = SendBuf::new();
        build_bind(&mut sb.frame(), portal, stmt, params).expect("streamed Bind fits");
        sb.queued().to_vec()
    }

    /// Assemble a `Bind` via the bounded `WriteBuf` reference into a flat vector.
    fn whole<P: crate::params::ParamsWriter>(portal: &[u8], stmt: &[u8], params: &P) -> Vec<u8> {
        let mut wb = WriteBuf::new();
        build_bind(&mut wb, portal, stmt, params).expect("whole Bind fits scratch");
        wb.as_bytes().to_vec()
    }

    #[test]
    fn streamed_bind_matches_whole_frame_for_small_params() {
        // A mixed-type, NULL-bearing, empty tuple set — cover the format block
        // (0 vs 1 code), the per-param length prefix, and the SQL-NULL `-1`.
        let s0 = streamed(b"", b"stmt", &());
        let w0 = whole(b"", b"stmt", &());
        assert_eq!(s0.first().copied(), Some(b'B'), "non-vacuous: 'B' frame");
        assert_eq!(s0, w0, "streamed Bind (no params) must equal the reference");

        let s1 = streamed(b"", b"s", &(7i32, "hi", true));
        let w1 = whole(b"", b"s", &(7i32, "hi", true));
        assert_eq!(s1, w1, "streamed Bind (mixed params) must equal the reference");

        let s2 = streamed(b"portal", b"s", &(Option::<i32>::None, Some("x")));
        let w2 = whole(b"portal", b"s", &(Option::<i32>::None, Some("x")));
        assert_eq!(s2, w2, "streamed Bind (NULL param) must equal the reference");

        // A small array param exercises the array header + per-element framing.
        let arr: &[i32] = &[10, 20, 30];
        let s3 = streamed(b"", b"s", &(arr,));
        let w3 = whole(b"", b"s", &(arr,));
        assert_eq!(s3, w3, "streamed Bind (array param) must equal the reference");
    }

    #[test]
    fn streamed_bind_lifts_the_bounded_cap() {
        // ~8 KiB single text param — far past the bounded `WriteBuf`'s ~2 KiB.
        let big = "x".repeat(8000);
        let params = (big.as_str(),);

        // RED: the SAME builder OVERFLOWS the bounded `WriteBuf` (the old cap).
        let mut wb = WriteBuf::new();
        assert!(
            build_bind(&mut wb, b"", b"s", &params).is_err(),
            "an 8 KiB param must overflow the bounded WriteBuf (the lifted cap)",
        );

        // GREEN: it streams onto `SendFrame` with correct self-inclusive framing.
        let s = streamed(b"", b"s", &params);
        assert_eq!(s.first().copied(), Some(b'B'), "'B' Bind frame");
        let len_bytes: [u8; 4] = s.get(1..5).expect("len field").try_into().expect("4 bytes");
        let declared = u32::from_be_bytes(len_bytes);
        let actual = u32::try_from(s.len().checked_sub(1).expect("tag")).expect("fits u32");
        assert_eq!(declared, actual, "self-inclusive Bind length must match the body");
        // The 8 KiB body is really present (framing + payload, not a truncation).
        assert!(s.len() > 8000, "the full 8 KiB param body streamed, uncapped");
    }

    #[test]
    fn streamed_bind_prepared_matches_whole_frame() {
        // The macro-path Bind (baked `portal NUL + stmt NUL` prefix, per-param
        // FORMATS, all-binary result formats) must also be byte-identical.
        let prefix: &[u8] = b"\0_bsql_0\0";
        let s = {
            let mut sb = SendBuf::new();
            build_bind_prepared(&mut sb.frame(), prefix, &(42i32, "hi"))
                .expect("streamed prepared Bind fits");
            sb.queued().to_vec()
        };
        let w = {
            let mut wb = WriteBuf::new();
            build_bind_prepared(&mut wb, prefix, &(42i32, "hi"))
                .expect("whole prepared Bind fits");
            wb.as_bytes().to_vec()
        };
        assert_eq!(s.first().copied(), Some(b'B'), "non-vacuous: 'B' frame");
        assert_eq!(s, w, "streamed prepared Bind must equal the reference");
    }
}
