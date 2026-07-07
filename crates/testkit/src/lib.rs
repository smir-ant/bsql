#![forbid(unsafe_code)]
// A future undocumented `pub` item is a build error, not silent doc rot.
#![deny(missing_docs)]

//! Deterministic in-memory fake PostgreSQL for testing bsql driver code.
//!
//! A [`FakePostgres`] scripts query replies; [`FakePostgres::connect`] hands
//! back a REAL [`bsql_postgres_async::Connection`] — the same concrete type a
//! socket `connect` returns — backed by an in-memory fake. Driver code under
//! test then runs unchanged (`query_sql`, transactions, the whole decode path)
//! against the fake, with **no network, no socket, no PostgreSQL** — fully
//! deterministic.
//!
//! This is possible because the driver's engine is sans-IO: it drives a
//! `Transport` seam, and the fake implements that seam over in-memory buffers.
//! The bytes the fake serves are real PostgreSQL wire frames the real engine
//! parses, so a passing test proves genuine end-to-end behaviour, not a mock.
//!
//! ```no_run
//! use bsql_testkit::{rows, FakePostgres};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let mut fake = FakePostgres::new();
//! fake.on("SELECT id FROM users").returns(rows![[1_i64], [2_i64]]);
//!
//! let mut conn = fake.connect().await?;
//! let result = conn.query_sql("SELECT id FROM users").await?;
//!
//! assert_eq!(result.len(), 2);
//! # Ok(())
//! # }
//! ```
//!
//! # Scope
//!
//! Handles the trust-auth handshake, scripted queries over BOTH the simple
//! protocol ([`query_sql`](bsql_postgres_async::Connection::query_sql)) and the
//! compile-checked `query!` extended protocol — one
//! [`fake.on(sql)`](FakePostgres::on)`.returns(...)` script answers both — plus
//! scripted errors. A query reply can also carry interleaved asynchronous
//! `NOTIFY`s (via [`Responder::notifying`]) — the pending-notification
//! interleaving the real backend does — so a test can prove the driver captures
//! a notification arriving DURING a query. An unscripted query is answered with a
//! loud, classified `ErrorResponse` — never a silent empty result or a hang. The
//! runtime `prepare`/`describe` extended path, multi-query scripting,
//! expectations (`assert_all_queried`), and COPY are not yet supported.
//!
//! Scriptable cell types are [`FakeValue`]'s vocabulary — the full scalar
//! surface the compile-checked `query!` path decodes: `bigint` (`i64`),
//! `integer` (`i32`), `text` (`&str` / `String`), `boolean`, `float4` (`f32`),
//! `float8` (`f64`), `bytea` (`&[u8]` / `Vec<u8>`), `uuid`, `numeric`,
//! `timestamptz`, `timestamp`, `date`, `time`, `interval`, `json`, `jsonb`, and
//! NULL (`Option<T>`) — PLUS a one-dimensional array (`T[]`) of any of those
//! scalars. The dep-free bsql-native types are scripted through the very types
//! a consumer decodes into (`bsql::Uuid`, `bsql::Numeric`, `bsql::Timestamptz`,
//! …). Each type's BINARY bytes — the wire the `query!` extended path decodes —
//! are produced by the crate's REAL `EncodeBinary` encoder (the fixed-width /
//! grouped types) or an unbounded raw-byte encoder (`bytea` / `json` /
//! `jsonb`), so they are byte-identical to a real server's, proven by a
//! round-trip through the real decoder.
//!
//! An array cell is scripted as a `Vec<T>` (or `Vec<Option<T>>` to carry a NULL
//! element) of a supported scalar — `rows![[ vec![10_i32, 20, 30] ]]` for an
//! `int4[]` column, `rows![[ vec![Some("a"), None] ]]` for a `text[]` with a
//! NULL element — and decodes back into the `query!` path's `Vec<Option<T>>`.
//! The element type is uniform by construction (a `Vec<T>` is homogeneous, so a
//! mixed-type array does not type-check), drawn from the scriptable scalar set
//! (an unsupported or nested `Vec<Vec<_>>` element has no `From` and is a
//! compile error), and one-dimensional only (there is no `From` for a
//! multi-dimensional array). The array's element bytes are the SAME scalar
//! encoder output, framed with the one-dimensional array header, so a scripted
//! `int4[]` / `text[]` / `numeric[]` / `uuid[]` decodes byte-for-byte back into
//! the record — an empty array to an empty `Vec`, a NULL element to a `None`.
//!
//! The SIMPLE-query (`query_sql`, text) path is FAIL-CLOSED for any type whose
//! text form cannot be rendered byte-faithfully to what a real server sends:
//! `timestamptz` / `timestamp` (binary-only bsql types, no PostgreSQL-ISO text
//! form), `float4` / `float8` (Rust's `Display` diverges from PostgreSQL's
//! `float ::text` for large / small magnitudes and `±Infinity`), and EVERY
//! array (PostgreSQL's `{a,b,NULL}` array text has involved quoting / escaping
//! and its elements can themselves be unfaithful — and arrays are decoded from
//! the binary wire anyway). A `query_sql`
//! over such a cell is a loud, classified `DriverError::Db` naming the faithful
//! routes — never plausible-but-wrong bytes a consumer could bake into a green
//! `get_str` assertion (the testkit proves genuine behaviour, not a mock). The
//! `query!` (binary) reply for the SAME script is byte-exact and unaffected, so
//! a `query!` over a scripted `timestamptz` / `float8` works. Every other type's
//! text rendering is its canonical PostgreSQL `::text` form.

use bsql_postgres_async::Connection;
use bsql_postgres_sync::Connection as SyncConnection;
use bsql_postgres_core::testkit::wire::{
    self, FakeEncodeError, OID_BOOL, OID_BYTEA, OID_DATE, OID_FLOAT4, OID_FLOAT8, OID_INT4,
    OID_INT8, OID_INTERVAL, OID_JSON, OID_JSONB, OID_NUMERIC, OID_TEXT, OID_TIME, OID_TIMESTAMP,
    OID_TIMESTAMPTZ, OID_UUID, TX_IDLE,
};
use bsql_postgres_core::testkit::{FakeScript, FakeTransport, QueryReply};
use bsql_postgres_core::DriverError;
// The dep-free bsql-native PG types a consumer scripts via `bsql::Uuid` etc.
// (the umbrella re-exports these very types), named directly from the wire
// crate so `FakeValue`'s `From` impls accept a consumer's value by type
// identity. See the proto direct-edge note in `Cargo.toml`.
use bsql_postgres_proto::{
    Date, Interval, Json, Jsonb, Numeric, Time, Timestamp, Timestamptz, Uuid,
};

/// A single scripted column value, rendered to PostgreSQL text OR binary wire
/// format (the simple-query and `query!` extended paths respectively).
///
/// Construct through the [`From`] impls (or the [`rows!`] macro), e.g.
/// `FakeValue::from(1_i64)` or `FakeValue::from(uuid_value)`. `Option<T>` maps
/// `None` to a SQL `NULL`.
#[derive(Debug, Clone)]
pub enum FakeValue {
    /// A `bigint` (`int8`) value.
    Int8(i64),
    /// An `integer` (`int4`) value.
    Int4(i32),
    /// A `text` value.
    Text(String),
    /// A `boolean` value.
    Bool(bool),
    /// A `float4` (`real`) value.
    F32(f32),
    /// A `float8` (`double precision`) value.
    F64(f64),
    /// A `bytea` (raw byte string) value.
    Bytea(Vec<u8>),
    /// A `uuid` value.
    Uuid(Uuid),
    /// A `numeric` / `decimal` value (exact, arbitrary precision).
    Numeric(Numeric),
    /// A `timestamptz` value.
    Timestamptz(Timestamptz),
    /// A `timestamp` (zone-less) value.
    Timestamp(Timestamp),
    /// A `date` value.
    Date(Date),
    /// A `time` value.
    Time(Time),
    /// An `interval` value.
    Interval(Interval),
    /// A `json` document.
    Json(Json),
    /// A `jsonb` document.
    Jsonb(Jsonb),
    /// A one-dimensional array (`T[]`) of a UNIFORM scalar element type — the
    /// element type's scalar OID plus the per-element values (a `None` element
    /// is a SQL-NULL element). This is the wire shape the compile-checked
    /// `query!` path decodes into `Vec<Option<T>>`.
    ///
    /// Constructed ONLY through the `From<Vec<T>>` / `From<Vec<Option<T>>>`
    /// impls (below), so:
    ///
    /// - the element type is uniform BY CONSTRUCTION — a `Vec<T>` is
    ///   homogeneous, so a mixed-type array (`vec![1_i32, "x"]`) does not even
    ///   type-check; and
    /// - the element type is drawn from the scriptable scalar set — only those
    ///   types have a `From`, so an unsupported or nested (`Vec<Vec<_>>`)
    ///   element has no impl and is a compile error.
    ///
    /// The variant is `#[non_exhaustive]`, so a consumer cannot build it
    /// directly (`E0639`); the `From` path is the only door. A mixed-type or
    /// unsupported-element or multi-dimensional array is therefore not "rejected
    /// at encode" — it is structurally impossible to construct.
    #[non_exhaustive]
    Array {
        /// The SCALAR element type's PostgreSQL OID (e.g. `wire::OID_INT4` for
        /// `int4[]`), written into the array wire header and cross-checked by
        /// the decoder against the row tuple's element type.
        element_oid: i32,
        /// The array's elements in wire order; a `None` is a SQL-NULL element.
        /// Each `Some` is a scalar [`FakeValue`] of the uniform element type.
        elements: Vec<Option<FakeValue>>,
    },
    /// A SQL `NULL`.
    Null,
}

impl From<i64> for FakeValue {
    fn from(v: i64) -> Self {
        Self::Int8(v)
    }
}
impl From<i32> for FakeValue {
    fn from(v: i32) -> Self {
        Self::Int4(v)
    }
}
impl From<&str> for FakeValue {
    fn from(v: &str) -> Self {
        Self::Text(v.to_owned())
    }
}
impl From<String> for FakeValue {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}
impl From<bool> for FakeValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}
impl From<f32> for FakeValue {
    fn from(v: f32) -> Self {
        Self::F32(v)
    }
}
impl From<f64> for FakeValue {
    fn from(v: f64) -> Self {
        Self::F64(v)
    }
}
impl From<&[u8]> for FakeValue {
    fn from(v: &[u8]) -> Self {
        Self::Bytea(v.to_vec())
    }
}
impl From<Vec<u8>> for FakeValue {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytea(v)
    }
}
impl From<Uuid> for FakeValue {
    fn from(v: Uuid) -> Self {
        Self::Uuid(v)
    }
}
impl From<Numeric> for FakeValue {
    fn from(v: Numeric) -> Self {
        Self::Numeric(v)
    }
}
impl From<Timestamptz> for FakeValue {
    fn from(v: Timestamptz) -> Self {
        Self::Timestamptz(v)
    }
}
impl From<Timestamp> for FakeValue {
    fn from(v: Timestamp) -> Self {
        Self::Timestamp(v)
    }
}
impl From<Date> for FakeValue {
    fn from(v: Date) -> Self {
        Self::Date(v)
    }
}
impl From<Time> for FakeValue {
    fn from(v: Time) -> Self {
        Self::Time(v)
    }
}
impl From<Interval> for FakeValue {
    fn from(v: Interval) -> Self {
        Self::Interval(v)
    }
}
impl From<Json> for FakeValue {
    fn from(v: Json) -> Self {
        Self::Json(v)
    }
}
impl From<Jsonb> for FakeValue {
    fn from(v: Jsonb) -> Self {
        Self::Jsonb(v)
    }
}
impl<T: Into<FakeValue>> From<Option<T>> for FakeValue {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => Self::Null,
        }
    }
}

/// Generate the array-column ergonomics for one scriptable element type:
/// `From<Vec<T>>` (every element present) and `From<Vec<Option<T>>>` (a `None`
/// element is a SQL-NULL element). Each element is mapped through the EXISTING
/// scalar `From<T> for FakeValue`, so the element vocabulary — and its wire
/// encoding — is single-sourced; this macro only wraps it in the uniform
/// `Array` shape with the element type's scalar OID. A `Vec<T>` is homogeneous,
/// so the element type is uniform by construction, and only the types named
/// here (the scriptable scalars) get an impl, so an unsupported or nested
/// element is a missing-impl compile error.
macro_rules! impl_array_from {
    ($($t:ty => $oid:expr),+ $(,)?) => {
        $(
            impl From<Vec<$t>> for FakeValue {
                fn from(v: Vec<$t>) -> Self {
                    Self::array(
                        $oid,
                        v.into_iter().map(|x| Some(FakeValue::from(x))).collect(),
                    )
                }
            }
            impl From<Vec<Option<$t>>> for FakeValue {
                fn from(v: Vec<Option<$t>>) -> Self {
                    Self::array(
                        $oid,
                        v.into_iter().map(|x| x.map(FakeValue::from)).collect(),
                    )
                }
            }
        )+
    };
}

// Value-typed element scalars — one Rust type per PostgreSQL element type.
impl_array_from! {
    i64 => OID_INT8,
    i32 => OID_INT4,
    bool => OID_BOOL,
    f32 => OID_FLOAT4,
    f64 => OID_FLOAT8,
    Uuid => OID_UUID,
    Numeric => OID_NUMERIC,
    Timestamptz => OID_TIMESTAMPTZ,
    Timestamp => OID_TIMESTAMP,
    Date => OID_DATE,
    Time => OID_TIME,
    Interval => OID_INTERVAL,
    Json => OID_JSON,
    Jsonb => OID_JSONB,
}

// `text[]` and `bytea[]` each accept BOTH a borrowed and an owned element
// spelling — the array peers of the scalar `&str` / `String` and `&[u8]` /
// `Vec<u8>` `From` impls. `Vec<&str>` / `Vec<Vec<u8>>` are distinct types from
// the scalar `Vec<u8>` (a single `bytea`), so no coherence conflict.
impl_array_from! {
    &str => OID_TEXT,
    String => OID_TEXT,
    Vec<u8> => OID_BYTEA,
    &[u8] => OID_BYTEA,
}

impl FakeValue {
    /// Build a uniform array cell from an element OID and its per-element
    /// values (a `None` is a SQL-NULL element). The sole constructor of the
    /// `#[non_exhaustive]` [`Array`](Self::Array) variant; the `From<Vec<T>>`
    /// impls funnel through it so every array is built one way.
    fn array(element_oid: i32, elements: Vec<Option<FakeValue>>) -> Self {
        Self::Array { element_oid, elements }
    }
}

impl FakeValue {
    /// The PostgreSQL type OID this value advertises in `RowDescription`.
    fn oid(&self) -> i32 {
        match self {
            Self::Int8(_) => OID_INT8,
            Self::Int4(_) => OID_INT4,
            Self::Text(_) => OID_TEXT,
            Self::Bool(_) => OID_BOOL,
            Self::F32(_) => OID_FLOAT4,
            Self::F64(_) => OID_FLOAT8,
            Self::Bytea(_) => OID_BYTEA,
            Self::Uuid(_) => OID_UUID,
            Self::Numeric(_) => OID_NUMERIC,
            Self::Timestamptz(_) => OID_TIMESTAMPTZ,
            Self::Timestamp(_) => OID_TIMESTAMP,
            Self::Date(_) => OID_DATE,
            Self::Time(_) => OID_TIME,
            Self::Interval(_) => OID_INTERVAL,
            Self::Json(_) => OID_JSON,
            Self::Jsonb(_) => OID_JSONB,
            // The array column's OWN type OID (its `T[]` OID), derived from the
            // scalar element OID. The mapping is total over every element OID a
            // `From<Vec<T>>` can produce, so the `None` arm is unreachable for a
            // constructed value; it reports the element OID rather than a
            // fabricated one. This OID reaches the wire only via the simple
            // path's `RowDescription`, which fails closed for arrays (see
            // `render`), so it is never actually served today — but it is
            // reported faithfully so a future faithful array text path is
            // correct without a change here.
            Self::Array { element_oid, .. } => match wire::array_oid_for_element(*element_oid) {
                Some(array_oid) => array_oid,
                None => *element_oid,
            },
            Self::Null => OID_TEXT,
        }
    }

    /// The value in PostgreSQL TEXT wire format, or `None` for a SQL `NULL`.
    /// Used by the simple-query (`query_sql`) reply path.
    ///
    /// FAIL-CLOSED for any type whose text form the testkit cannot render
    /// byte-faithfully to what a real PostgreSQL server emits. The testkit's
    /// contract is that a passing test proves genuine end-to-end behaviour, not
    /// a mock — so a cell that would serve bytes a real server never sends is a
    /// classified [`TestkitError::UnfaithfulTextRender`], never a plausible-but-
    /// wrong string a consumer could bake into a green `get_str` assertion. The
    /// unfaithful set:
    ///
    /// - `timestamptz` / `timestamp`: binary-only bsql types with no
    ///   PostgreSQL-ISO text form derivable from the value alone (bsql decodes
    ///   them from the binary wire, by design).
    /// - `float4` / `float8`: Rust's `Display` diverges from PostgreSQL's
    ///   `float ::text` for large / small magnitudes (`1e+20`, `1e-10`),
    ///   subnormals, and `±Infinity` (Rust writes `inf` / full positional
    ///   digits). A per-value check would mean reimplementing PostgreSQL's float
    ///   formatter, so the whole type fails closed.
    ///
    /// The faithful routes for these types are the compile-checked `query!`
    /// (binary) protocol — byte-exact via [`render_binary`](Self::render_binary)
    /// — or a [`Text`](Self::Text) cell carrying the exact text the consumer's
    /// real PostgreSQL emits.
    fn render(&self) -> Result<Option<Vec<u8>>, TestkitError> {
        Ok(match self {
            Self::Int8(v) => Some(v.to_string().into_bytes()),
            Self::Int4(v) => Some(v.to_string().into_bytes()),
            Self::Text(s) => Some(s.clone().into_bytes()),
            Self::Bool(b) => Some(if *b { b"t".to_vec() } else { b"f".to_vec() }),
            // PostgreSQL `bytea` TEXT output (the default `hex` format) — the
            // canonical, unambiguous `\x<hex>` form.
            Self::Bytea(b) => Some(bytea_text_body(b)),
            // Each renders its UNIQUE, canonical PostgreSQL `::text` form
            // (verified `== ::text` for every value): uuid hyphenated-lowercase-
            // hex, numeric exact decimal, date/time/interval ISO, json/jsonb the
            // consumer's verbatim document text.
            Self::Uuid(v) => Some(v.to_string().into_bytes()),
            Self::Numeric(v) => Some(v.to_string().into_bytes()),
            Self::Date(v) => Some(v.to_string().into_bytes()),
            Self::Time(v) => Some(v.to_string().into_bytes()),
            Self::Interval(v) => Some(v.to_string().into_bytes()),
            Self::Json(v) => Some(v.as_str().as_bytes().to_vec()),
            Self::Jsonb(v) => Some(v.as_str().as_bytes().to_vec()),
            // FAIL-CLOSED — no PostgreSQL-faithful text form (see fn doc).
            Self::Timestamptz(_) => {
                return Err(TestkitError::UnfaithfulTextRender { type_name: "timestamptz" });
            }
            Self::Timestamp(_) => {
                return Err(TestkitError::UnfaithfulTextRender { type_name: "timestamp" });
            }
            Self::F32(_) => {
                return Err(TestkitError::UnfaithfulTextRender { type_name: "float4" });
            }
            Self::F64(_) => {
                return Err(TestkitError::UnfaithfulTextRender { type_name: "float8" });
            }
            // FAIL-CLOSED — PostgreSQL's array TEXT output (`{a,b,NULL}`) has
            // involved quoting / escaping rules (an element containing a comma,
            // brace, quote, backslash, whitespace, an empty string, or the
            // literal `NULL` must be double-quoted with inner escaping), and its
            // elements can themselves be unfaithful-text types (`float8[]`,
            // `timestamptz[]`). Rather than a bespoke, error-prone array text
            // formatter that could serve bytes a real server never sends, the
            // simple-query (text) path fails closed for EVERY array — steering
            // to the compile-checked `query!` (binary) protocol, which is the
            // ONLY path that decodes arrays anyway (there is no text-format array
            // decoder). The `query!` (binary) reply for the same script is
            // byte-exact and unaffected.
            Self::Array { element_oid, .. } => {
                return Err(TestkitError::UnfaithfulTextRender {
                    type_name: wire::array_type_name(*element_oid),
                });
            }
            Self::Null => None,
        })
    }

    /// The value in PostgreSQL BINARY wire format, or `None` for a SQL `NULL`.
    /// Used by the extended-query (`query!`) reply path — the flagship decodes
    /// each cell via `Cell<BinaryFmt>`, so the bytes must be binary, not text.
    ///
    /// The fixed-width / non-trivially-laid-out types route through the REAL
    /// [`wire::binary_via_encoder`] — the identical `EncodeBinary` encoder the
    /// `query!` parameter path uses — so the fake's bytes are byte-identical to
    /// a real server's BY CONSTRUCTION (a `numeric` grouping or a `date` epoch
    /// can never drift from the decoder). The raw-byte types encode into an
    /// unbounded buffer. Every encoder is proven wire-correct by a round-trip
    /// through the real decoder in the [`wire`] module.
    ///
    /// # Errors
    ///
    /// [`FakeEncodeError`] only for a value whose binary encoding overflows the
    /// bounded encode buffer (a `numeric` with thousands of significant digits)
    /// — never a realistic fixture; surfaced classified, never a panic.
    fn render_binary(&self) -> Result<Option<Vec<u8>>, FakeEncodeError> {
        Ok(match self {
            Self::Int8(v) => Some(wire::binary_int8(*v)),
            Self::Int4(v) => Some(wire::binary_int4(*v)),
            Self::Text(s) => Some(wire::binary_text(s)),
            Self::Bool(b) => Some(wire::binary_bool(*b)),
            Self::F32(v) => Some(wire::binary_via_encoder(v)?),
            Self::F64(v) => Some(wire::binary_via_encoder(v)?),
            Self::Uuid(v) => Some(wire::binary_via_encoder(v)?),
            Self::Numeric(v) => Some(wire::binary_via_encoder(v)?),
            Self::Timestamptz(v) => Some(wire::binary_via_encoder(v)?),
            Self::Timestamp(v) => Some(wire::binary_via_encoder(v)?),
            Self::Date(v) => Some(wire::binary_via_encoder(v)?),
            Self::Time(v) => Some(wire::binary_via_encoder(v)?),
            Self::Interval(v) => Some(wire::binary_via_encoder(v)?),
            Self::Bytea(b) => Some(wire::binary_bytea(b)),
            Self::Json(v) => Some(wire::binary_json(v.as_str())),
            Self::Jsonb(v) => Some(wire::binary_jsonb(v.as_str())),
            // A 1-D array: render each element through the SAME scalar
            // `render_binary` (a `None` element, or a NULL-valued element,
            // becomes a `-1` NULL), then frame the bodies with the array header
            // via `wire::binary_array` — no element bytes are produced here that
            // the scalar path did not. The element type is uniform by
            // construction (the `From<Vec<T>>` door), so the header's element
            // OID matches every element the decoder reads back.
            Self::Array { element_oid, elements } => {
                let mut rendered: Vec<Option<Vec<u8>>> = Vec::with_capacity(elements.len());
                for elem in elements {
                    let body = match elem {
                        None => None,
                        Some(fv) => fv.render_binary()?,
                    };
                    rendered.push(body);
                }
                Some(wire::binary_array(*element_oid, &rendered)?)
            }
            Self::Null => None,
        })
    }
}

/// PostgreSQL `bytea` TEXT output (the default `hex` format): the two-byte `\x`
/// prefix followed by the bytes as lowercase hex — the canonical text a
/// simple-query (`query_sql`) result carries for a `bytea` column, so a
/// `get_str` over the scripted value reads exactly what a real server sends.
fn bytea_text_body(bytes: &[u8]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(bytes.len().saturating_mul(2).saturating_add(2));
    out.push(b'\\');
    out.push(b'x');
    for &byte in bytes {
        out.push(HEX[usize::from(byte >> 4)]);
        out.push(HEX[usize::from(byte & 0x0F)]);
    }
    out
}

/// A scripted result set — a grid of [`FakeValue`] rows. Build it with the
/// [`rows!`] macro.
#[derive(Debug, Clone)]
pub struct ScriptedRows {
    rows: Vec<Vec<FakeValue>>,
}

impl ScriptedRows {
    /// Build from a grid of rows (each an equal-length list of cells). Prefer
    /// the [`rows!`] macro.
    #[must_use]
    pub fn from_rows(rows: Vec<Vec<FakeValue>>) -> Self {
        Self { rows }
    }
}

/// Build a [`ScriptedRows`] from row literals: `rows![[1_i64], [2_i64]]`.
///
/// Each inner `[...]` is one row; each element is any value with a
/// [`FakeValue`] `From` impl — an `i64` / `i32` / `f32` / `f64` / `bool`, a
/// `&str` / `String`, a `&[u8]` / `Vec<u8>` (`bytea`), a dep-free bsql-native
/// type (`bsql::Uuid`, `bsql::Numeric`, `bsql::Timestamptz`, `bsql::Date`,
/// `bsql::Interval`, `bsql::Json`, …), a `Vec<T>` / `Vec<Option<T>>` of any of
/// those scalars for a one-dimensional array column (`vec![10_i32, 20]` for an
/// `int4[]`, `vec![Some("a"), None]` for a `text[]` with a NULL element), or an
/// `Option<T>` for a `NULL`.
#[macro_export]
macro_rules! rows {
    ( $( [ $( $cell:expr ),* $(,)? ] ),* $(,)? ) => {
        $crate::ScriptedRows::from_rows(::std::vec![
            $( ::std::vec![ $( $crate::FakeValue::from($cell) ),* ] ),*
        ])
    };
}

/// One asynchronous `NOTIFY` scripted to arrive DURING a query's reply — the
/// interleaving the real backend does when a `NOTIFY` is pending while a command
/// runs. Used to prove the driver captures it rather than dropping it.
#[derive(Debug, Clone)]
struct ScriptedNotification {
    pid: i32,
    channel: String,
    payload: String,
}

/// A scripted reply to one query: either a result set (optionally with
/// notifications interleaved into its reply stream) or a server error.
#[derive(Debug, Clone)]
enum ScriptedReply {
    Rows {
        rows: ScriptedRows,
        /// Notifications spliced into the reply stream, after the rows and before
        /// the terminating `CommandComplete` — so the driver's query pump surfaces
        /// each `Surface::Notify` mid-command.
        notifications: Vec<ScriptedNotification>,
    },
    Error {
        sqlstate: String,
        message: String,
    },
}

/// Why building or connecting a [`FakePostgres`] failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum TestkitError {
    /// A scripted reply could not be encoded to wire bytes (an oversized value).
    Encode(FakeEncodeError),
    /// A scripted result set had rows of differing column counts, which the
    /// wire's single-width `RowDescription` cannot represent.
    RaggedRows {
        /// The column count established by the first row.
        expected: usize,
        /// The differing column count of a later row.
        found: usize,
    },
    /// The driver failed to connect over the fake (a malformed scripted
    /// handshake, surfaced by the real engine).
    Driver(DriverError),
    /// A cell's type has no PostgreSQL-faithful SIMPLE-query (text) wire form,
    /// so the `query_sql` reply path fails closed rather than serve bytes a real
    /// server never sends. The `query!` (binary) reply for the SAME script is
    /// byte-exact and unaffected; this only fails a `query_sql` over the cell.
    /// Surfaced to the driver as a classified `DriverError::Db` when the
    /// simple-query protocol serves the row (so a `query!` over the same script
    /// stays green). `type_name` is the offending PostgreSQL type.
    UnfaithfulTextRender {
        /// The PostgreSQL type whose text form the fake cannot render faithfully
        /// (e.g. `timestamptz`, `float8`).
        type_name: &'static str,
    },
}

impl core::fmt::Display for TestkitError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Encode(e) => write!(f, "fake reply encode error: {e}"),
            Self::RaggedRows { expected, found } => write!(
                f,
                "scripted rows have differing column counts: expected {expected}, found {found}"
            ),
            Self::Driver(e) => write!(f, "fake connect failed: {e}"),
            Self::UnfaithfulTextRender { type_name } => write!(
                f,
                "bsql-testkit: a `{type_name}` column has no PostgreSQL-faithful \
                 simple-query (text) form — a real server's text bytes cannot be \
                 reproduced from the value alone, so the query_sql (text) path \
                 fails closed rather than serve bytes PostgreSQL never sends. Use \
                 the compile-checked query! (binary) protocol (byte-exact), or \
                 script a Text cell carrying the exact text your PostgreSQL emits."
            ),
        }
    }
}

impl std::error::Error for TestkitError {}

impl From<FakeEncodeError> for TestkitError {
    fn from(e: FakeEncodeError) -> Self {
        Self::Encode(e)
    }
}
impl From<DriverError> for TestkitError {
    fn from(e: DriverError) -> Self {
        Self::Driver(e)
    }
}

/// A deterministic in-memory fake PostgreSQL server.
///
/// Script replies with [`on`](Self::on), then obtain a real connection with
/// [`connect`](Self::connect). One fake can back several connections (each call
/// replays the same script).
#[derive(Debug, Clone)]
pub struct FakePostgres {
    responses: Vec<(String, ScriptedReply)>,
    server_version: String,
    backend_pid: i32,
}

impl FakePostgres {
    /// A fresh fake with no scripted queries.
    #[must_use]
    pub fn new() -> Self {
        Self {
            responses: Vec::new(),
            server_version: "17.0 (bsql-testkit)".to_owned(),
            backend_pid: 1,
        }
    }

    /// Set the `server_version` the fake reports at connect time.
    #[must_use]
    pub fn with_server_version(mut self, version: impl Into<String>) -> Self {
        self.server_version = version.into();
        self
    }

    /// Begin scripting a reply for a simple query. The SQL is matched by exact
    /// text after trimming surrounding whitespace. Finish with
    /// [`Responder::returns`] or [`Responder::returns_error`].
    pub fn on(&mut self, sql: impl Into<String>) -> Responder<'_> {
        Responder {
            fake: self,
            sql: sql.into(),
            notifications: Vec::new(),
        }
    }

    /// Open a real async [`Connection`] backed by this fake — no socket, no
    /// network.
    ///
    /// # Errors
    ///
    /// [`TestkitError`] if a scripted reply cannot be encoded (an oversized
    /// value or ragged rows) or the driver rejects the fake handshake.
    pub async fn connect(&self) -> Result<Connection, TestkitError> {
        let script = self.build_script()?;
        let conn = Connection::connect_fake(FakeTransport::new(script)).await?;
        Ok(conn)
    }

    /// Open a real blocking [`SyncConnection`] backed by this fake — no socket,
    /// no network. The sync twin of [`connect`](Self::connect): the same script
    /// backs either driver.
    ///
    /// # Errors
    ///
    /// [`TestkitError`] if a scripted reply cannot be encoded (an oversized
    /// value or ragged rows) or the driver rejects the fake handshake.
    pub fn connect_sync(&self) -> Result<SyncConnection, TestkitError> {
        let script = self.build_script()?;
        let conn = SyncConnection::connect_fake(FakeTransport::new(script))?;
        Ok(conn)
    }

    /// Encode the whole script to the pre-built reply bytes the fake serves.
    fn build_script(&self) -> Result<FakeScript, TestkitError> {
        let handshake = encode_handshake(&self.server_version, self.backend_pid)?;
        let mut queries = Vec::with_capacity(self.responses.len());
        for (sql, reply) in &self.responses {
            // One scripted reply answers both protocols: a simple-query byte
            // stream and an extended-query Execute payload.
            let query_reply = match reply {
                ScriptedReply::Rows { rows, notifications } => QueryReply {
                    simple: encode_rows_simple_faithful(rows, notifications)?,
                    extended: encode_rows_extended(rows, notifications)?,
                },
                ScriptedReply::Error { sqlstate, message } => QueryReply {
                    simple: encode_error_simple(sqlstate, message)?,
                    extended: encode_error_extended(sqlstate, message)?,
                },
            };
            queries.push((sql.trim().to_owned(), query_reply));
        }
        let scripted = self
            .responses
            .iter()
            .map(|(sql, _)| format!("{sql:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        let unmatched_message = format!(
            "bsql-testkit: no scripted reply for the received query. \
             Scripted queries: [{scripted}]. \
             Add fake.on(<sql>).returns(...) to script it."
        );
        let unmatched_simple = encode_error_simple("XX000", &unmatched_message)?;
        // The extended unmatched error is a bare ErrorResponse (no trailing
        // ReadyForQuery): it rides the Execute, and the batch's Sync supplies the
        // ReadyForQuery — so an unscripted `query!` is a loud classified error,
        // never a silent empty result.
        let unmatched_extended = encode_error_extended("XX000", &unmatched_message)?;
        // The unsupported error is served for a frontend message the fake does
        // not model (a Describe/Flush — the runtime `prepare` path), WITHOUT a
        // trailing ReadyForQuery: the fake emits it once, then supplies the
        // single `ready_for_query` at the batch's Sync (PostgreSQL's
        // error-then-skip-to-Sync recovery), so the connection stays clean.
        let unsupported_error = wire::error_response(
            "ERROR",
            "0A000",
            "bsql-testkit: this in-memory fake supports the simple-query \
             (query_sql) and compile-checked query! protocols; the runtime \
             prepare / describe extended path is not supported.",
        )?;
        let ready_for_query = wire::ready_for_query(TX_IDLE)?;
        Ok(FakeScript {
            handshake,
            queries,
            unmatched_simple,
            unmatched_extended,
            parse_complete: wire::parse_complete()?,
            bind_complete: wire::bind_complete()?,
            close_complete: wire::close_complete()?,
            unsupported_error,
            ready_for_query,
        })
    }
}

impl Default for FakePostgres {
    fn default() -> Self {
        Self::new()
    }
}

/// A pending scripted reply for one query. Finish it with
/// [`returns`](Self::returns) or [`returns_error`](Self::returns_error).
#[derive(Debug)]
#[must_use = "call .returns(...) or .returns_error(...) to record the scripted reply"]
pub struct Responder<'a> {
    fake: &'a mut FakePostgres,
    sql: String,
    notifications: Vec<ScriptedNotification>,
}

impl Responder<'_> {
    /// Script an asynchronous `NOTIFY` to arrive DURING this query's reply — the
    /// interleaving the real backend does when a notification is pending while a
    /// command runs. Chainable before [`returns`](Self::returns); the driver's
    /// query pump surfaces each as a `Surface::Notify` mid-command, and a correct
    /// driver captures it into its notification ledger rather than dropping it.
    pub fn notifying(mut self, pid: i32, channel: impl Into<String>, payload: impl Into<String>) -> Self {
        self.notifications.push(ScriptedNotification {
            pid,
            channel: channel.into(),
            payload: payload.into(),
        });
        self
    }

    /// Script this query to return the given rows (plus any notifications added
    /// with [`notifying`](Self::notifying) interleaved into the reply stream).
    pub fn returns(self, rows: ScriptedRows) {
        self.fake.responses.push((
            self.sql,
            ScriptedReply::Rows {
                rows,
                notifications: self.notifications,
            },
        ));
    }

    /// Script this query to fail with a PostgreSQL `ErrorResponse` — the driver
    /// surfaces it as `DriverError::Db`.
    pub fn returns_error(self, sqlstate: impl Into<String>, message: impl Into<String>) {
        self.fake.responses.push((
            self.sql,
            ScriptedReply::Error {
                sqlstate: sqlstate.into(),
                message: message.into(),
            },
        ));
    }
}

/// Derive the `(name, oid)` columns for a result set from its rows: the width
/// is the first row's; each column's OID is the first non-NULL cell's type
/// (defaulting to `text` when a column is entirely NULL).
fn columns(rows: &[Vec<FakeValue>]) -> Vec<(String, i32)> {
    let width = match rows.first() {
        Some(first) => first.len(),
        None => 0,
    };
    (0..width)
        .map(|col| {
            let oid = match rows.iter().find_map(|row| match row.get(col) {
                Some(cell) if !matches!(cell, FakeValue::Null) => Some(cell.oid()),
                _ => None,
            }) {
                Some(found) => found,
                None => OID_TEXT,
            };
            (format!("col{col}"), oid)
        })
        .collect()
}

/// Validate that every row has the established column width, returning the
/// derived `(name, oid)` columns. A ragged grid cannot be represented on the
/// wire (a single-width `RowDescription`), so it is a loud error.
fn checked_columns(rows: &ScriptedRows) -> Result<Vec<(String, i32)>, TestkitError> {
    let cols = columns(&rows.rows);
    for row in &rows.rows {
        if row.len() != cols.len() {
            return Err(TestkitError::RaggedRows {
                expected: cols.len(),
                found: row.len(),
            });
        }
    }
    Ok(cols)
}

/// Render each scripted notification to its `NotificationResponse` wire frame.
fn notification_frames(
    notifications: &[ScriptedNotification],
) -> Result<Vec<Vec<u8>>, TestkitError> {
    notifications
        .iter()
        .map(|n| Ok(wire::notification_response(n.pid, &n.channel, &n.payload)?))
        .collect()
}

/// The SIMPLE-query reply, FAIL-CLOSED on an unfaithful text render.
///
/// Wraps [`encode_rows_simple`]: if any cell's type has no PostgreSQL-faithful
/// text form ([`TestkitError::UnfaithfulTextRender`] — `timestamptz` /
/// `timestamp` / `float4` / `float8`), the simple-query reply becomes a
/// classified `ErrorResponse` naming the faithful routes, so a `query_sql` over
/// the cell is a loud `DriverError::Db`, never silently-wrong text bytes. The
/// substitution happens HERE (not at [`build_script`](FakePostgres::build_script)
/// return) on PURPOSE: the EXTENDED (`query!`) reply for the same script is
/// byte-exact and unaffected, so a `query!` over a scripted `timestamptz` /
/// `float8` stays green while a `query_sql` over it fails closed. Any OTHER
/// encode failure (a genuinely oversized value, ragged rows) still propagates.
fn encode_rows_simple_faithful(
    rows: &ScriptedRows,
    notifications: &[ScriptedNotification],
) -> Result<Vec<u8>, TestkitError> {
    match encode_rows_simple(rows, notifications) {
        Ok(bytes) => Ok(bytes),
        Err(e @ TestkitError::UnfaithfulTextRender { .. }) => {
            // Fail the SIMPLE (query_sql) path closed with a classified server
            // error carrying the guidance message. `0A000` = feature_not_supported.
            encode_error_simple("0A000", &e.to_string())
        }
        Err(other) => Err(other),
    }
}

/// Encode a scripted result set for the SIMPLE-query protocol:
/// `RowDescription` + text `DataRow`s + interleaved `NotificationResponse`s +
/// `CommandComplete` + `ReadyForQuery`. The notifications ride after the rows and
/// before the command boundary, so the driver's query pump surfaces each one
/// mid-command. A cell whose type has no PostgreSQL-faithful text form
/// ([`FakeValue::render`]) makes this fail closed with
/// [`TestkitError::UnfaithfulTextRender`]; [`encode_rows_simple_faithful`] turns
/// that into a classified `ErrorResponse` for the wire.
fn encode_rows_simple(
    rows: &ScriptedRows,
    notifications: &[ScriptedNotification],
) -> Result<Vec<u8>, TestkitError> {
    let cols = checked_columns(rows)?;
    let mut frames = Vec::with_capacity(rows.rows.len().saturating_add(notifications.len()).saturating_add(3));
    frames.push(wire::row_description(&cols)?);
    for row in &rows.rows {
        let cells: Vec<Option<Vec<u8>>> =
            row.iter().map(FakeValue::render).collect::<Result<Vec<_>, _>>()?;
        frames.push(wire::data_row(&cells)?);
    }
    frames.extend(notification_frames(notifications)?);
    frames.push(wire::command_complete(&format!("SELECT {}", rows.rows.len()))?);
    frames.push(wire::ready_for_query(TX_IDLE)?);
    Ok(wire::concat(&frames))
}

/// Encode a scripted result set as the EXTENDED-query Execute PAYLOAD: binary
/// `DataRow`s + `CommandComplete`, with NO `RowDescription` (the extended path
/// sends no Describe, so the real server sends none either) and NO trailing
/// `ReadyForQuery` (the fake's framer emits the acknowledgements before and the
/// `Sync`'s `ReadyForQuery` after). The flagship `query!` decodes each cell via
/// `Cell<BinaryFmt>`, so the cells are rendered in binary.
fn encode_rows_extended(
    rows: &ScriptedRows,
    notifications: &[ScriptedNotification],
) -> Result<Vec<u8>, TestkitError> {
    // Reuse the same ragged-rows validation as the simple path; the extended
    // path advertises no column metadata, so only the check matters here.
    checked_columns(rows)?;
    let mut frames = Vec::with_capacity(rows.rows.len().saturating_add(notifications.len()).saturating_add(1));
    for row in &rows.rows {
        let cells: Vec<Option<Vec<u8>>> = row
            .iter()
            .map(FakeValue::render_binary)
            .collect::<Result<Vec<_>, _>>()?;
        frames.push(wire::data_row(&cells)?);
    }
    frames.extend(notification_frames(notifications)?);
    frames.push(wire::command_complete(&format!("SELECT {}", rows.rows.len()))?);
    Ok(wire::concat(&frames))
}

/// Encode a scripted `ErrorResponse` + `ReadyForQuery` for the SIMPLE protocol.
fn encode_error_simple(sqlstate: &str, message: &str) -> Result<Vec<u8>, TestkitError> {
    let frames = [
        wire::error_response("ERROR", sqlstate, message)?,
        wire::ready_for_query(TX_IDLE)?,
    ];
    Ok(wire::concat(&frames))
}

/// Encode a scripted `ErrorResponse` as the EXTENDED-query Execute payload — a
/// bare frame with no trailing `ReadyForQuery` (the `Sync` supplies it). The
/// engine drives it `BindAwaitingData -> fail_recoverable -> drain -> RFQ`, so
/// a scripted error surfaces loudly and the connection recovers clean.
fn encode_error_extended(sqlstate: &str, message: &str) -> Result<Vec<u8>, TestkitError> {
    Ok(wire::error_response("ERROR", sqlstate, message)?)
}

/// Encode the trust-auth handshake chain the fake serves for the startup packet.
fn encode_handshake(server_version: &str, backend_pid: i32) -> Result<Vec<u8>, TestkitError> {
    let frames = [
        wire::auth_ok()?,
        wire::parameter_status("server_version", server_version)?,
        wire::backend_key_data(backend_pid, 0)?,
        wire::ready_for_query(TX_IDLE)?,
    ];
    Ok(wire::concat(&frames))
}
