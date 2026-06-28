//! Active-phase outbound frame builders.
//!
//! Each builder assembles one PostgreSQL request frame into a transient
//! [`WriteBuf`] via its public `push_*` surface (length prefixes patched by
//! [`WriteBuf::with_length_prefix`], exactly the PG §55.7 "length includes
//! itself" convention). The verb layer builds into a scratch buffer and copies
//! the bytes onto the engine's persistent [`SendBuf`](super::SendBuf) — the same
//! transient-scratch / persistent-queue split the connecting handshake uses for
//! its startup/auth frames. [`WriteBuf`] is `heapless` and scrub-on-drop, so a
//! parameter-bearing assembly never outlives the build, and its capacity is
//! const-asserted to fit every active frame's worst case.
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
use crate::write_buf::{WriteBuf, WriteBufFull};

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
/// No parameter-type OIDs are declared (`n_param_types = 0`): the server infers
/// them. This is the runtime prepare path; the compile-checked `prepared!` macro
/// bakes its own Parse template (with OIDs) consumed verbatim elsewhere.
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

/// `'D'` Describe-statement frame: `tag | len | 'S' | stmt_name NUL`.
#[inline]
pub(crate) fn build_describe_statement(wb: &mut WriteBuf, name: &[u8]) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_DESCRIBE.byte())?;
    wb.with_length_prefix(|w| {
        w.push_u8(DescribeTargetByte::Statement.byte())?;
        w.push_nul_terminated(name)
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
pub(crate) fn build_bind<P: ParamsWriter>(
    wb: &mut WriteBuf,
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
pub(crate) fn build_bind_prepared<P: ParamsWriter>(
    wb: &mut WriteBuf,
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

/// `'Q'` `LISTEN <channel>` frame: `tag | len | "LISTEN " channel | NUL`.
///
/// The channel is a pre-validated identifier (the verb takes an
/// [`Ident`](crate::ident::Ident)), so the assembled SQL cannot inject — there is
/// no string interpolation of untrusted text.
#[inline]
pub(crate) fn build_listen(wb: &mut WriteBuf, channel: &[u8]) -> Result<(), WriteBufFull> {
    wb.push_u8(TAG_QUERY.byte())?;
    wb.with_length_prefix(|w| {
        w.push_bytes(b"LISTEN ")?;
        w.push_bytes(channel)?;
        w.push_u8(0)
    })
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
