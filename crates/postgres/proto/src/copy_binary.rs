//! Typed binary `COPY … FROM STDIN` — the compile-checked bulk-insert seam.
//!
//! `COPY` in the PGCOPY *binary* format (PG §55.7 "Binary Format") is the
//! fastest bulk-load path there is: no text parse/format on either side, and no
//! per-value escaping. The raw [`copy_in_write`](crate::engine) path takes
//! `&[u8]` — the caller hand-formats COPY *text* with correct escaping / NULL
//! sentinels, and a mis-escaped tab or newline SILENTLY corrupts a row (the
//! classic COPY footgun). This module is the injection-safe-by-construction
//! peer: a TYPED row cannot carry an escaping bug, because there is no text to
//! mis-escape — every field is encoded through the SAME
//! [`ParamsWriter`](crate::ParamsWriter) binary leaves the `query!` parameter
//! path uses, so the binary-COPY field encoding CANNOT drift from the query
//! encoding.
//!
//! # The PGCOPY binary stream
//!
//! A COPY-in binary stream is: a fixed 19-byte [file header](PGCOPY_BINARY_HEADER),
//! then one framing per ROW, then a fixed 2-byte [trailer](PGCOPY_BINARY_TRAILER).
//! The stream is a flat byte sequence carried over one or more `CopyData` frames
//! — the frame boundaries are irrelevant to the format, so the header, each row,
//! and the trailer can each ride their own `CopyData` frame and stream through
//! the existing 64 KiB batcher.
//!
//! Each ROW is: an `int16` field-count, then per FIELD an `int32` length
//! (`-1` = NULL) followed by the field's binary bytes. That per-field
//! `{len, bytes}` / `-1` shape is EXACTLY what
//! [`ParamsWriter::write_params`](crate::ParamsWriter::write_params) emits for a
//! Bind parameter block — so a row body is `int16 field-count` + `write_params`,
//! and the wire encoders are shared, never duplicated. The row builder is
//! [`crate::engine::frames::build_copy_binary_row`]; the batched engine verb is
//! [`copy_in_write_binary_row`](crate::engine::Engine::copy_in_write_binary_row).
//!
//! # Compile-time guarantee
//!
//! The `copy!(Name, "table", (cols))` macro validates the target table, its
//! columns, and their types against the build catalog (exactly like `query!`)
//! and emits a [`TypedCopyIn`] carrier whose [`Row`](TypedCopyIn::Row) tuple pins
//! the column Rust types. A wrong-typed or wrong-arity row is a compile error at
//! the `copy_in_typed` call (the row tuple does not match
//! [`Row`](TypedCopyIn::Row)), and the target table + column list are baked into
//! the const [`SQL`](TypedCopyIn::SQL) at build time — so the identifiers are
//! injection-safe by construction (a compile-time constant, never a runtime
//! splice).
//!
//! Unlike the `query!` result path, the PGCOPY stream carries NO per-column OID
//! negotiation, so there is no server-side OID cross-check; the load-bearing
//! guarantee is the compile-time row-tuple match. A binary value whose declared
//! type disagrees with the live column type is rejected LOUDLY by the server at
//! ingest (a classified [`crate::DecodeError`]-class server error, connection
//! recovers), never a silent corruption.

use crate::params::ParamsWriter;

/// The 19-byte PGCOPY binary file header: the 11-byte signature
/// `PGCOPY\n\377\r\n\0`, then a 4-byte flags field (`0`), then a 4-byte
/// header-extension length (`0`).
///
/// The signature's high bit in byte 7 (`\377`) lets the server detect a stream
/// that was mangled by a non-8-bit-clean transfer; the `\r\n` / `\n` bytes catch
/// end-of-line translation. Emitted ONCE at the start of a binary COPY-in stream.
pub const PGCOPY_BINARY_HEADER: [u8; 19] = [
    // Signature: "PGCOPY" + \n + \377 (0xFF) + \r + \n + \0.
    b'P', b'G', b'C', b'O', b'P', b'Y', b'\n', 0xFF, b'\r', b'\n', 0x00,
    // Flags field: 0 (no OID column; the only flag bit defined).
    0x00, 0x00, 0x00, 0x00,
    // Header-extension area length: 0 (no extension).
    0x00, 0x00, 0x00, 0x00,
];

/// The PGCOPY binary file trailer: an `int16` field-count of `-1`, which a
/// per-row field-count (`>= 0`) can never collide with. Emitted ONCE after the
/// last row to close a binary COPY-in stream.
pub const PGCOPY_BINARY_TRAILER: [u8; 2] = [0xFF, 0xFF];

// Drift-pins: the header is signature(11) + flags(4) + ext-len(4), the trailer is
// one big-endian int16 = -1. A byte-twin test asserts the full layout; these
// pin the fixed sizes so a stray edit to either array is a build error.
const _: () = assert!(PGCOPY_BINARY_HEADER.len() == 19);
const _: () = assert!(PGCOPY_BINARY_TRAILER.len() == 2);
const _: () = assert!(matches!(
    PGCOPY_BINARY_TRAILER,
    // i16::to_be_bytes(-1) — the trailer sentinel.
    [0xFF, 0xFF]
));

/// Ties a compile-checked `copy!` carrier to its catalog-derived binary COPY
/// command and its typed row tuple — the bulk-insert peer of
/// [`TypedQuery`](crate::TypedQuery).
///
/// A `copy!(Name, "table", (cols))` invocation emits, in the consumer crate, an
/// uninhabited carrier `Name` implementing this trait. A driver's
/// `copy_in_typed::<Name>(rows)` verb reads [`SQL`](Self::SQL) to open the COPY,
/// then streams each `rows` item — a [`Row`](Self::Row) tuple — as one PGCOPY
/// binary row through the shared [`ParamsWriter`] encoders.
///
/// # Why NOT sealed
///
/// Mirrors [`TypedQuery`](crate::TypedQuery) and
/// [`QueryFingerprint`](crate::QueryFingerprint): the carrier and its impl are
/// emitted in the consumer crate, so a seal would be unsatisfiable from there.
/// The load-bearing guarantee is not the trait's openness but that
/// [`SQL`](Self::SQL) and [`Row`](Self::Row) are BOTH derived by the macro from
/// the same build catalog — a hand-written impl gains nothing a hand-written raw
/// `copy_in_write` could not already do (the raw escape hatch stays available),
/// and cannot fabricate a catalog-checked bulk load.
pub trait TypedCopyIn {
    /// The row tuple whose per-element types are the target columns' Rust encode
    /// types (a `NOT NULL` column is `T`, a nullable column is `Option<T>`),
    /// pinned by the macro from the catalog. A `text` / `bytea` column borrows
    /// the caller's data as `&'q str` / `&'q [u8]`, so a streamed bulk load
    /// copies each field ONCE (encode → send buffer) with no owned-`String`
    /// per field — the GAT lifetime `'q` is the row-source borrow.
    ///
    /// Bounded on [`ParamsWriter`] so a row encodes through the SAME binary
    /// leaves the `query!` parameter path uses. A row value whose tuple does not
    /// match this associated type is a compile error at the `copy_in_typed` call.
    type Row<'q>: ParamsWriter;

    /// The exact `COPY <table> (<col>, …) FROM STDIN WITH (FORMAT binary)`
    /// command, baked by the macro from the build catalog's table + column
    /// identifiers. Because it is a compile-time constant assembled from
    /// validated catalog identifiers (never a runtime splice), it is
    /// injection-safe by construction — the compile-time peer of the runtime
    /// `SafeTable` guard the raw COPY path uses.
    const SQL: &'static str;
}
