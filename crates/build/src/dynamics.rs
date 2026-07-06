//! Build-time preprocessing for the `query!` macro's DYNAMIC forms — the
//! sugar that expands, inside ONE static SQL form, to a parameter the
//! caller can toggle or vary at runtime WITHOUT building any SQL string.
//!
//! Three forms are recognised, each lowered to plain PostgreSQL the
//! inference engine already types, plus a small amount of metadata the
//! macro turns into the parameter tuple / selector enum:
//!
//! * **Optional filter** — `OPTIONAL(<predicate>)` lowers to
//!   `($N IS NULL OR <predicate>)`, where `$N` is the single placeholder
//!   inside the predicate. The toggled parameter becomes `Option<T>`:
//!   passing `Some(v)` enables the filter, passing `None` (a SQL NULL
//!   bind) disables it — the SQL form never changes.
//! * **`ANY($N)` in-list** — `col = ANY($N)` keeps its shape on the wire
//!   but the parameter becomes a SINGLE array (`&[T]`), array-OID encoded.
//!   One bind value carries the whole in-list; no `IN (...)` arity is
//!   baked into the SQL.
//! * **Runtime `ORDER BY` allow-set** — `ORDER BY { a ASC | b DESC | ... }`
//!   declares a CLOSED set of `(column, direction)` orderings. The macro
//!   emits one prepared query per ordering plus a selector enum, so the
//!   caller picks one of N at runtime with no string building and no
//!   injection surface. An ordering outside the set cannot be named (the
//!   enum has only the declared variants); a sort column that does not
//!   exist is a build error (each variant is inference-validated).
//!
//! # Why preprocess textually
//!
//! The untouched parts of the SQL are spliced through byte-for-byte, so a
//! query with no dynamic forms lowers to itself verbatim — its
//! content-addressed statement name and baked wire bytes are identical to
//! the non-dynamic path. The input is the macro's own `&'static str`
//! literal (author-written, never runtime data), so this is compile-time
//! text rewriting, not a runtime SQL builder.
//!
//! # Fail-closed
//!
//! A malformed marker (an unbalanced `OPTIONAL(`, an `OPTIONAL(...)` that
//! does not hold exactly one `$N`, an `ORDER BY { ... }` whose option is
//! not `column [ASC|DESC]`, two options colliding on one enum variant) is
//! a loud [`DynamicError`], surfaced by the macro as a `compile_error!`.
//! Nothing is silently dropped.

use std::collections::BTreeSet;
use std::fmt;

use crate::infer::{infer_query, InferError, InferredColumn, RustType};
use crate::Catalog;

/// The Rust-side shape of one `$N` parameter after dynamic lowering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamShape {
    /// A plain scalar `$N` — Rust type `T`.
    Scalar(RustType),
    /// A toggled optional-filter `$N` — Rust type `Option<T>`; `None`
    /// disables the filter (a SQL NULL bind), the SQL form is unchanged.
    Optional(RustType),
    /// A single array `$N` for a `col = ANY($N)` in-list — Rust type
    /// `&[T]`, encoded as one array parameter with the element type's
    /// array OID.
    Array(RustType),
}

impl ParamShape {
    /// The underlying scalar element type, regardless of wrapper.
    #[must_use]
    pub fn element(self) -> RustType {
        match self {
            ParamShape::Scalar(ty) | ParamShape::Optional(ty) | ParamShape::Array(ty) => ty,
        }
    }
}

/// One allowed `(column, direction)` ordering of a runtime `ORDER BY`
/// allow-set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByVariant {
    /// PascalCase enum-variant identifier, e.g. `IdAsc`, `TotalDesc`.
    pub variant_ident: String,
    /// The `ORDER BY` clause body this variant sorts by, e.g. `id ASC`.
    pub clause: String,
}

/// One wire form of the query — the SQL actually parsed and baked. There
/// is exactly one for a query with no runtime `ORDER BY` allow-set, and
/// one per allowed ordering otherwise (parallel to [`DynamicShape::order_by`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireVariant {
    /// The lowered, plain-PostgreSQL SQL baked into this variant's
    /// prepared-query wire artifact.
    pub wire_sql: String,
    /// The lowered SQL the inference engine actually typed for this variant
    /// — `OPTIONAL(p)` collapsed to `(p)` and `ANY($N)` collapsed to `$N`
    /// (the toggle / array sugar removed). This is the portable form: it
    /// has no PostgreSQL-only `= ANY($N)` array operator, so the build-time
    /// SQLite backend can `prepare_v2` it to cross-check that real SQLite
    /// agrees with the lattice on this variant's row shape. (The `wire_sql`
    /// above keeps `ANY($N)`, which only PostgreSQL accepts.)
    pub infer_sql: String,
    /// The lowered SQL the SQLite full-scan-on-toggle check `EXPLAIN QUERY
    /// PLAN`s for this variant — `OPTIONAL(p)` kept in its toggle form
    /// (`$N IS NULL OR p`, so the plan reflects what the enabled filter
    /// actually does to the index), but `ANY($N)` collapsed to `$N` (the
    /// SAME collapse `infer_sql` applies). This is the SQLite-PREPARABLE
    /// toggle form: it keeps the toggle the scan check must see, yet drops
    /// the PostgreSQL-only `= ANY($N)` operator — which SQLite parses as a
    /// call to an unknown function `ANY` and rejects, falsely failing a
    /// valid OPTIONAL + `= ANY($M)` query. `wire_sql` keeps both forms (it
    /// is the bytes baked for PostgreSQL); `infer_sql` drops both (it is the
    /// portable row-shape prepare); this drops only `ANY` and keeps the
    /// toggle.
    pub scan_sql: String,
}

/// The fully resolved dynamic shape of one `query!` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicShape {
    /// Projected output columns (shared across every wire variant — only
    /// the `ORDER BY` differs between variants).
    pub columns: Vec<InferredColumn>,
    /// Parameter shapes, `params[i]` is the shape of `$(i + 1)`.
    pub params: Vec<ParamShape>,
    /// The wire variants. One when [`Self::order_by`] is `None`; one per
    /// allowed ordering otherwise.
    pub variants: Vec<WireVariant>,
    /// The runtime `ORDER BY` allow-set, if the query declared one. When
    /// `Some`, it is parallel to [`Self::variants`].
    pub order_by: Option<Vec<OrderByVariant>>,
}

/// A dynamic-lowering or inference failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DynamicError {
    /// A malformed dynamic sugar marker — names the problem.
    Sugar(String),
    /// The underlying inference engine rejected the lowered SQL.
    Infer(InferError),
}

impl fmt::Display for DynamicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DynamicError::Sugar(message) => f.write_str(message),
            DynamicError::Infer(err) => write!(f, "{err}"),
        }
    }
}

impl From<InferError> for DynamicError {
    fn from(err: InferError) -> Self {
        DynamicError::Infer(err)
    }
}

/// Lower one `query!` SQL string (with dynamic sugar) to its
/// [`DynamicShape`]: the inferred columns, the per-`$N` parameter shapes,
/// and the wire SQL variant(s).
///
/// # Errors
///
/// [`DynamicError`] on a malformed sugar marker or an inference failure
/// in any lowered variant.
pub fn infer_dynamic_query(catalog: &Catalog, sql: &str) -> Result<DynamicShape, DynamicError> {
    let pre = preprocess(sql)?;

    match &pre.order_by {
        None => {
            let shape = infer_query(catalog, &pre.infer_base)?;
            let params = build_param_shapes(
                &shape.params,
                &pre.optional_positions,
                &pre.array_positions,
            )?;
            Ok(DynamicShape {
                columns: shape.columns,
                params,
                variants: vec![WireVariant {
                    wire_sql: pre.wire_base,
                    infer_sql: pre.infer_base,
                    scan_sql: pre.scan_base,
                }],
                order_by: None,
            })
        }
        Some(options) => {
            // Type + validate EACH ordering against the catalog (so a
            // non-existent or unsupported sort column is a loud build
            // error), then keep the first variant's shape as THE shape —
            // every variant projects the same columns and binds the same
            // parameters, since only the ORDER BY differs.
            let mut shared: Option<(Vec<InferredColumn>, Vec<ParamShape>)> = None;
            let mut variants = Vec::with_capacity(options.len());
            let mut order_variants = Vec::with_capacity(options.len());
            for option in options {
                let infer_sql = format!("{} ORDER BY {}", pre.infer_base, option.clause);
                let shape = infer_query(catalog, &infer_sql)?;
                let params = build_param_shapes(
                    &shape.params,
                    &pre.optional_positions,
                    &pre.array_positions,
                )?;
                match &shared {
                    None => shared = Some((shape.columns.clone(), params.clone())),
                    Some((cols, ps)) => {
                        if *cols != shape.columns || *ps != params {
                            return Err(DynamicError::Sugar(
                                "runtime ORDER BY variants disagree on the projected row \
                                 shape or parameter types — every ordering must share one \
                                 row shape"
                                    .to_string(),
                            ));
                        }
                    }
                }
                variants.push(WireVariant {
                    wire_sql: format!("{} ORDER BY {}", pre.wire_base, option.clause),
                    infer_sql: infer_sql.clone(),
                    scan_sql: format!("{} ORDER BY {}", pre.scan_base, option.clause),
                });
                order_variants.push(OrderByVariant {
                    variant_ident: option.variant_ident.clone(),
                    clause: option.clause.clone(),
                });
            }
            let (columns, params) = match shared {
                Some(pair) => pair,
                // `preprocess` rejects an empty `ORDER BY { }`, so the loop
                // always ran at least once; fail closed rather than assert.
                None => {
                    return Err(DynamicError::Sugar(
                        "runtime ORDER BY allow-set has no options".to_string(),
                    ))
                }
            };
            Ok(DynamicShape {
                columns,
                params,
                variants,
                order_by: Some(order_variants),
            })
        }
    }
}

/// Combine the inferred per-position scalar types with the optional /
/// array overrides discovered during lowering.
fn build_param_shapes(
    scalar_params: &[RustType],
    optional_positions: &[usize],
    array_positions: &[usize],
) -> Result<Vec<ParamShape>, DynamicError> {
    let optionals: BTreeSet<usize> = optional_positions.iter().copied().collect();
    let arrays: BTreeSet<usize> = array_positions.iter().copied().collect();
    let mut out = Vec::with_capacity(scalar_params.len());
    for (idx, ty) in scalar_params.iter().enumerate() {
        let position = idx.saturating_add(1);
        let is_optional = optionals.contains(&position);
        let is_array = arrays.contains(&position);
        if is_optional && is_array {
            return Err(DynamicError::Sugar(format!(
                "parameter ${position} is used as BOTH an OPTIONAL(...) filter and a \
                 `= ANY(${position})` in-list; a single parameter cannot be both"
            )));
        }
        let shape = if is_array {
            ParamShape::Array(*ty)
        } else if is_optional {
            ParamShape::Optional(*ty)
        } else {
            ParamShape::Scalar(*ty)
        };
        out.push(shape);
    }
    // A toggled / array position must actually exist among the inferred
    // parameters; if lowering recorded one the inference never saw, fail
    // closed rather than silently ignore it.
    for position in optionals.iter().chain(arrays.iter()) {
        if *position == 0 || *position > scalar_params.len() {
            return Err(DynamicError::Sugar(format!(
                "dynamic parameter ${position} has no inferred type — it must compare \
                 against a catalog column"
            )));
        }
    }
    Ok(out)
}

/// The textual result of dynamic lowering, before inference.
struct Preprocessed {
    /// SQL the inference engine types: `OPTIONAL(p)` -> `(p)`,
    /// `ANY($N)` -> `$N`, the `ORDER BY { ... }` block removed.
    infer_base: String,
    /// SQL baked on the wire: `OPTIONAL(p)` -> `($N IS NULL OR p)`,
    /// `ANY($N)` kept, the `ORDER BY { ... }` block removed.
    wire_base: String,
    /// SQL the SQLite scan check `EXPLAIN QUERY PLAN`s: `OPTIONAL(p)` ->
    /// `($N IS NULL OR p)` (toggle kept), `ANY($N)` -> `$N` (collapsed, as
    /// `infer_base` does), the `ORDER BY { ... }` block removed. The toggle
    /// the scan check must see, without the PostgreSQL-only `= ANY($N)`
    /// SQLite cannot prepare.
    scan_base: String,
    /// 1-based positions toggled to `Option<T>`.
    optional_positions: Vec<usize>,
    /// 1-based positions that are a single array parameter.
    array_positions: Vec<usize>,
    /// The runtime ORDER BY allow-set, if declared.
    order_by: Option<Vec<OrderByOption>>,
}

/// One parsed `ORDER BY { ... }` option, before inference validation.
struct OrderByOption {
    clause: String,
    variant_ident: String,
}

/// Lower the dynamic sugar textually. See the module docs for the forms.
fn preprocess(sql: &str) -> Result<Preprocessed, DynamicError> {
    let (prefix, order_by) = split_order_by_allow_set(sql)?;
    let expanded = expand_markers(prefix)?;
    Ok(Preprocessed {
        infer_base: expanded.infer,
        wire_base: expanded.wire,
        scan_base: expanded.scan,
        optional_positions: expanded.optional_positions,
        array_positions: expanded.array_positions,
        order_by,
    })
}

/// The textual result of expanding the `OPTIONAL(...)` / `= ANY($N)`
/// markers in one SQL prefix: the three lowered SQL forms (differing only
/// at the markers) plus the toggled / array `$N` positions discovered.
struct Expanded {
    /// `OPTIONAL(p)` -> `(p)`, `ANY($N)` -> `$N` (the portable infer form).
    infer: String,
    /// `OPTIONAL(p)` -> `($N IS NULL OR p)`, `ANY($N)` kept (the PG wire form).
    wire: String,
    /// `OPTIONAL(p)` -> `($N IS NULL OR p)`, `ANY($N)` -> `$N` (the
    /// SQLite-preparable scan form).
    scan: String,
    /// 1-based positions toggled to `Option<T>`.
    optional_positions: Vec<usize>,
    /// 1-based positions that are a single array parameter.
    array_positions: Vec<usize>,
}

/// Split off a trailing `ORDER BY { ... }` allow-set block, if present.
/// Returns the SQL prefix before the block and the parsed options.
fn split_order_by_allow_set(
    sql: &str,
) -> Result<(&str, Option<Vec<OrderByOption>>), DynamicError> {
    let bytes = sql.as_bytes();
    let Some(brace_open) = find_order_by_brace(bytes) else {
        return Ok((sql, None));
    };
    // The allow-set must be the trailing clause; find its closing brace.
    let Some(brace_close) = find_byte_outside_quotes(bytes, brace_open + 1, b'}') else {
        return Err(DynamicError::Sugar(
            "runtime ORDER BY allow-set: unterminated `{` — expected `ORDER BY { a ASC | b \
             DESC }`"
                .to_string(),
        ));
    };
    // Nothing but whitespace may follow the closing brace (the allow-set
    // is the last clause; anything after it could not be applied to every
    // variant uniformly). All slice bounds below are ASCII byte positions
    // (`{`, `}`, and keyword offsets), so they sit on `char` boundaries.
    let trailing = &sql[brace_close + 1..];
    if !trailing.trim().is_empty() {
        return Err(DynamicError::Sugar(format!(
            "runtime ORDER BY allow-set must be the final clause; found trailing SQL after \
             the `{{ ... }}` block: `{}`",
            trailing.trim()
        )));
    }
    let order_by_kw_start = find_order_by_keyword_before(bytes, brace_open);
    let prefix = &sql[..order_by_kw_start];
    let inner = &sql[brace_open + 1..brace_close];
    let options = parse_order_by_options(inner)?;
    Ok((prefix, Some(options)))
}

/// Parse the inside of an `ORDER BY { ... }` block into options.
fn parse_order_by_options(inner: &str) -> Result<Vec<OrderByOption>, DynamicError> {
    let mut options = Vec::new();
    let mut seen_idents: BTreeSet<String> = BTreeSet::new();
    for raw in inner.split('|') {
        let option = raw.trim();
        if option.is_empty() {
            return Err(DynamicError::Sugar(
                "runtime ORDER BY allow-set has an empty option (a stray `|`)".to_string(),
            ));
        }
        let mut parts = option.split_whitespace();
        let column = match parts.next() {
            Some(col) => col,
            None => {
                return Err(DynamicError::Sugar(
                    "runtime ORDER BY allow-set has an empty option".to_string(),
                ))
            }
        };
        let direction = match parts.next() {
            Some(dir) => {
                let upper = dir.to_ascii_uppercase();
                if upper != "ASC" && upper != "DESC" {
                    return Err(DynamicError::Sugar(format!(
                        "runtime ORDER BY option `{option}` has direction `{dir}`; only \
                         `ASC` or `DESC` are allowed"
                    )));
                }
                upper
            }
            None => "ASC".to_string(),
        };
        if parts.next().is_some() {
            return Err(DynamicError::Sugar(format!(
                "runtime ORDER BY option `{option}` is not a simple `column [ASC|DESC]` — \
                 qualifiers, expressions, and extra tokens are not allowed in the allow-set"
            )));
        }
        if !is_plain_identifier(column) {
            return Err(DynamicError::Sugar(format!(
                "runtime ORDER BY option `{option}`: `{column}` is not a plain column name \
                 (no qualifiers or expressions in the allow-set)"
            )));
        }
        let variant_ident = order_by_variant_ident(column, &direction);
        if !seen_idents.insert(variant_ident.clone()) {
            return Err(DynamicError::Sugar(format!(
                "runtime ORDER BY allow-set has two options that map to the same selector \
                 variant `{variant_ident}`"
            )));
        }
        options.push(OrderByOption {
            clause: format!("{column} {direction}"),
            variant_ident,
        });
    }
    if options.is_empty() {
        return Err(DynamicError::Sugar(
            "runtime ORDER BY allow-set `{ }` is empty — list at least one `column \
             [ASC|DESC]`"
                .to_string(),
        ));
    }
    Ok(options)
}

/// Build a PascalCase enum-variant identifier from a column + direction,
/// e.g. `user_id` + `DESC` -> `UserIdDesc`.
fn order_by_variant_ident(column: &str, direction: &str) -> String {
    let mut ident = String::new();
    for segment in column.split('_') {
        let mut chars = segment.chars();
        if let Some(first) = chars.next() {
            ident.extend(first.to_uppercase());
            ident.push_str(chars.as_str());
        }
    }
    // direction is "ASC" / "DESC" -> "Asc" / "Desc".
    let mut dir_chars = direction.chars();
    if let Some(first) = dir_chars.next() {
        ident.extend(first.to_uppercase());
        ident.push_str(&dir_chars.as_str().to_lowercase());
    }
    ident
}

/// `true` when `name` is a plain `[A-Za-z_][A-Za-z0-9_]*` identifier.
fn is_plain_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Expand the `OPTIONAL(...)` and `= ANY($N)` markers in `sql`, producing
/// the inference SQL, the wire SQL, the SQLite scan-check SQL, and the
/// toggled / array `$N` positions. Untouched SQL is spliced through
/// byte-for-byte into all three forms.
///
/// The three forms differ ONLY at the two markers, derived in one pass so
/// they cannot drift:
///
/// | marker        | infer        | wire                 | scan                 |
/// |---------------|--------------|----------------------|----------------------|
/// | `OPTIONAL(p)` | `(p)`        | `($N IS NULL OR p)`  | `($N IS NULL OR p)`  |
/// | `ANY($N)`     | `$N`         | `ANY($N)`            | `$N`                 |
///
/// `scan` keeps the toggle (so the plan reflects the enabled filter) but
/// collapses `ANY` (so SQLite can prepare it — `= ANY($N)` is parsed by
/// SQLite as a call to an unknown function `ANY`).
fn expand_markers(sql: &str) -> Result<Expanded, DynamicError> {
    let bytes = sql.as_bytes();
    let mut infer = String::with_capacity(sql.len());
    let mut wire = String::with_capacity(sql.len());
    let mut scan = String::with_capacity(sql.len());
    let mut optional_positions = Vec::new();
    let mut array_positions = Vec::new();

    let mut copied = 0usize; // next byte not yet flushed to infer/wire
    let mut i = 0usize;
    let mut in_squote = false;
    let mut in_dquote = false;
    while i < bytes.len() {
        let c = bytes[i];
        if in_squote {
            if c == b'\'' {
                in_squote = false;
            }
            i += 1;
            continue;
        }
        if in_dquote {
            if c == b'"' {
                in_dquote = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' => {
                in_squote = true;
                i += 1;
                continue;
            }
            b'"' => {
                in_dquote = true;
                i += 1;
                continue;
            }
            _ => {}
        }

        if let Some(paren_open) = keyword_call_open(bytes, i, b"OPTIONAL") {
            let paren_close = match_paren(bytes, paren_open).ok_or_else(|| {
                DynamicError::Sugar(
                    "OPTIONAL(...): unbalanced parentheses".to_string(),
                )
            })?;
            // `paren_open` / `paren_close` are ASCII `(` / `)` positions —
            // `char` boundaries, so the slice is valid.
            let inner = &sql[paren_open + 1..paren_close];
            let position = sole_placeholder(inner)?;
            // flush the verbatim run before the marker
            flush(&mut infer, &mut wire, &mut scan, sql, copied, i);
            infer.push('(');
            infer.push_str(inner);
            infer.push(')');
            // wire AND scan keep the toggle form `($N IS NULL OR p)`: the
            // scan check must see the toggle to detect the index-defeating
            // plan; wire bakes it for PostgreSQL.
            let toggle = format!("(${position} IS NULL OR {inner})");
            wire.push_str(&toggle);
            scan.push_str(&toggle);
            optional_positions.push(position);
            i = paren_close + 1;
            copied = i;
            continue;
        }

        if let Some(paren_open) = keyword_call_open(bytes, i, b"ANY")
            && let Some(paren_close) = match_paren(bytes, paren_open)
        {
            let inner = &sql[paren_open + 1..paren_close];
            // Only a BARE-placeholder `ANY($N)` is an array in-list. An
            // `ANY(ARRAY[...])`, a subquery, etc. is left untouched — the
            // inference engine types it directly.
            if let Some(position) = bare_placeholder(inner) {
                flush(&mut infer, &mut wire, &mut scan, sql, copied, i);
                // inference AND scan: drop the ANY wrapper, leaving `... $N`
                // (SQLite cannot prepare `= ANY($N)`).
                let collapsed = format!("${position}");
                infer.push_str(&collapsed);
                scan.push_str(&collapsed);
                // wire: keep `ANY($N)` verbatim (`i` is the ASCII `A`
                // of `ANY`, `paren_close` the matching `)` — both
                // `char` boundaries).
                wire.push_str(&sql[i..paren_close + 1]);
                array_positions.push(position);
                i = paren_close + 1;
                copied = i;
                continue;
            }
        }

        i += 1;
    }
    flush(&mut infer, &mut wire, &mut scan, sql, copied, bytes.len());
    Ok(Expanded {
        infer,
        wire,
        scan,
        optional_positions,
        array_positions,
    })
}

/// Flush `sql[start..end]` verbatim into all three outputs. `start`/`end`
/// are always flush points at marker boundaries (ASCII positions), so they
/// sit on `char` boundaries.
fn flush(infer: &mut String, wire: &mut String, scan: &mut String, sql: &str, start: usize, end: usize) {
    let slice = &sql[start..end];
    infer.push_str(slice);
    wire.push_str(slice);
    scan.push_str(slice);
}

/// If `bytes[pos..]` begins with the keyword `kw` (ASCII case-insensitive)
/// at a word boundary, followed by optional whitespace then `(`, return
/// the index of that `(`.
fn keyword_call_open(bytes: &[u8], pos: usize, kw: &[u8]) -> Option<usize> {
    if !matches_keyword(bytes, pos, kw) {
        return None;
    }
    let mut j = pos + kw.len();
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    if j < bytes.len() && bytes[j] == b'(' {
        Some(j)
    } else {
        None
    }
}

/// ASCII case-insensitive keyword match at `pos` with identifier word
/// boundaries on both sides (so `ANY` does not match inside `company`).
fn matches_keyword(bytes: &[u8], pos: usize, kw: &[u8]) -> bool {
    if pos > 0 {
        let prev = bytes[pos - 1];
        if prev == b'_' || prev.is_ascii_alphanumeric() {
            return false;
        }
    }
    let end = pos + kw.len();
    if end > bytes.len() {
        return false;
    }
    let mut k = 0;
    while k < kw.len() {
        if !bytes[pos + k].eq_ignore_ascii_case(&kw[k]) {
            return false;
        }
        k += 1;
    }
    if end < bytes.len() {
        let next = bytes[end];
        if next == b'_' || next.is_ascii_alphanumeric() {
            return false;
        }
    }
    true
}

/// From an open-paren index, return the index of the matching close paren,
/// respecting nested parens and quoted strings.
fn match_paren(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut i = open;
    let mut in_squote = false;
    let mut in_dquote = false;
    while i < bytes.len() {
        let c = bytes[i];
        if in_squote {
            if c == b'\'' {
                in_squote = false;
            }
        } else if in_dquote {
            if c == b'"' {
                in_dquote = false;
            }
        } else {
            match c {
                b'\'' => in_squote = true,
                b'"' => in_dquote = true,
                b'(' => depth += 1,
                b')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// Find the first occurrence of `target` at or after `from`, outside any
/// quoted string.
fn find_byte_outside_quotes(bytes: &[u8], from: usize, target: u8) -> Option<usize> {
    let mut i = from;
    let mut in_squote = false;
    let mut in_dquote = false;
    while i < bytes.len() {
        let c = bytes[i];
        if in_squote {
            if c == b'\'' {
                in_squote = false;
            }
        } else if in_dquote {
            if c == b'"' {
                in_dquote = false;
            }
        } else if c == b'\'' {
            in_squote = true;
        } else if c == b'"' {
            in_dquote = true;
        } else if c == target {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Find the `{` that opens an `ORDER BY {` allow-set block (the keyword
/// `ORDER` then `BY` then `{`, all outside quotes), returning the `{`'s
/// index.
fn find_order_by_brace(bytes: &[u8]) -> Option<usize> {
    let mut i = 0usize;
    let mut in_squote = false;
    let mut in_dquote = false;
    while i < bytes.len() {
        let c = bytes[i];
        if in_squote {
            if c == b'\'' {
                in_squote = false;
            }
            i += 1;
            continue;
        }
        if in_dquote {
            if c == b'"' {
                in_dquote = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' => {
                in_squote = true;
                i += 1;
                continue;
            }
            b'"' => {
                in_dquote = true;
                i += 1;
                continue;
            }
            _ => {}
        }
        if matches_keyword(bytes, i, b"ORDER") {
            let mut j = i + 5;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if matches_keyword(bytes, j, b"BY") {
                let mut k = j + 2;
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                if k < bytes.len() && bytes[k] == b'{' {
                    return Some(k);
                }
            }
        }
        i += 1;
    }
    None
}

/// Given the index of the `{` of an `ORDER BY {` block, return the byte
/// index where the `ORDER` keyword starts (so the prefix can be sliced).
fn find_order_by_keyword_before(bytes: &[u8], brace_open: usize) -> usize {
    // Walk backwards over whitespace, the `BY`, whitespace, and `ORDER`.
    let mut k = brace_open;
    while k > 0 && bytes[k - 1].is_ascii_whitespace() {
        k -= 1;
    }
    // Skip `BY`.
    k = k.saturating_sub(2);
    while k > 0 && bytes[k - 1].is_ascii_whitespace() {
        k -= 1;
    }
    // Skip `ORDER`.
    k.saturating_sub(5)
}

/// The single distinct `$N` in `text`, or a loud error when there is not
/// exactly one. Used by `OPTIONAL(...)`, which toggles exactly one param.
fn sole_placeholder(text: &str) -> Result<usize, DynamicError> {
    let mut found: BTreeSet<usize> = BTreeSet::new();
    collect_placeholders(text, &mut found)?;
    let mut iter = found.into_iter();
    match (iter.next(), iter.next()) {
        (Some(only), None) => Ok(only),
        (None, _) => Err(DynamicError::Sugar(
            "OPTIONAL(...) must contain exactly one `$N` parameter, found none".to_string(),
        )),
        (Some(_), Some(_)) => Err(DynamicError::Sugar(
            "OPTIONAL(...) must contain exactly one `$N` parameter, found more than one"
                .to_string(),
        )),
    }
}

/// If `text` trimmed is exactly one `$N` placeholder, return `N`.
fn bare_placeholder(text: &str) -> Option<usize> {
    let trimmed = text.trim();
    let digits = trimmed.strip_prefix('$')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    match digits.parse::<usize>() {
        Ok(n) if n >= 1 => Some(n),
        _ => None,
    }
}

/// Collect every `$N` placeholder position in `text` into `out`.
fn collect_placeholders(text: &str, out: &mut BTreeSet<usize>) -> Result<(), DynamicError> {
    let bytes = text.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                // `i` is the ASCII `$`, `j` walks ASCII digits — both
                // `char` boundaries.
                let digits = &text[i + 1..j];
                match digits.parse::<usize>() {
                    Ok(n) if n >= 1 => {
                        out.insert(n);
                    }
                    _ => {
                        return Err(DynamicError::Sugar(format!(
                            "malformed placeholder `${digits}` in OPTIONAL(...)"
                        )))
                    }
                }
                i = j;
                continue;
            }
        }
        i += 1;
    }
    Ok(())
}

/// Rewrite PostgreSQL `$N` placeholders to SQLite `?N` numbered parameters,
/// outside string literals. SQLite does not accept the `$N` numeric-after-dollar
/// form; `?N` is its numbered-parameter spelling and preserves reuse (`?1` twice
/// binds one slot, matching `$1` twice).
///
/// The SINGLE authority for the placeholder form: the build-time SQLite
/// conformance oracle prepares this exact rewrite of a query's `infer_sql`, and
/// the `query!` macro bakes the SAME rewrite as a SQLite carrier's `const SQL`,
/// so the string executed at runtime is byte-identical to the one build-time
/// validation proved SQLite prepares — no drift between the two.
#[must_use]
pub fn sqlite_placeholder_form(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut copied = 0usize;
    let mut i = 0usize;
    let mut in_squote = false;
    let mut in_dquote = false;
    while i < bytes.len() {
        let c = bytes[i];
        if in_squote {
            if c == b'\'' {
                in_squote = false;
            }
            i += 1;
            continue;
        }
        if in_dquote {
            if c == b'"' {
                in_dquote = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' => {
                in_squote = true;
                i += 1;
            }
            b'"' => {
                in_dquote = true;
                i += 1;
            }
            b'$' => {
                let mut j = i + 1;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > i + 1 {
                    // `i` (the `$`) and `j` (after the digits) are ASCII byte
                    // positions, so these slices sit on char boundaries.
                    out.push_str(&sql[copied..i]);
                    out.push('?');
                    out.push_str(&sql[i + 1..j]);
                    i = j;
                    copied = i;
                } else {
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }
    out.push_str(&sql[copied..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_placeholder_form_rewrites_outside_string_literals() {
        assert_eq!(
            sqlite_placeholder_form("WHERE id = $1 AND x = $12"),
            "WHERE id = ?1 AND x = ?12"
        );
        // A `$1` inside a string literal is left untouched; the real placeholder
        // outside it is rewritten.
        assert_eq!(sqlite_placeholder_form("'$1' = $1"), "'$1' = ?1");
    }

    fn cat() -> Catalog {
        // users(id int8 PK, email text NOT NULL); orders(id int8 PK,
        // user_id int8 NOT NULL, total int4). Catalog rows are
        // `table\tcolumn\tpg_type\tnot_null\tprimary_key`.
        crate::parse_catalog(
            "orders\tid\tint8\t1\t1\n\
             orders\tuser_id\tint8\t1\t0\n\
             orders\ttotal\tint4\t0\t0\n\
             users\tid\tint8\t1\t1\n\
             users\temail\ttext\t1\t0\n",
        )
        .expect("test catalog parses")
    }

    #[test]
    fn plain_query_lowers_to_itself() {
        let pre = preprocess("SELECT id FROM orders WHERE id = $1").expect("preprocess");
        assert_eq!(pre.infer_base, "SELECT id FROM orders WHERE id = $1");
        assert_eq!(pre.wire_base, "SELECT id FROM orders WHERE id = $1");
        assert_eq!(pre.scan_base, "SELECT id FROM orders WHERE id = $1");
        assert!(pre.optional_positions.is_empty());
        assert!(pre.array_positions.is_empty());
        assert!(pre.order_by.is_none());
    }

    #[test]
    fn optional_filter_expands() {
        let pre =
            preprocess("SELECT id FROM orders WHERE OPTIONAL(total = $1)").expect("preprocess");
        assert_eq!(pre.infer_base, "SELECT id FROM orders WHERE (total = $1)");
        assert_eq!(
            pre.wire_base,
            "SELECT id FROM orders WHERE ($1 IS NULL OR total = $1)"
        );
        // No `ANY`, so the scan form is the wire form: the toggle is kept.
        assert_eq!(
            pre.scan_base,
            "SELECT id FROM orders WHERE ($1 IS NULL OR total = $1)"
        );
        assert_eq!(pre.optional_positions, vec![1]);
    }

    #[test]
    fn any_in_list_keeps_wire_drops_for_infer() {
        let pre = preprocess("SELECT id FROM orders WHERE id = ANY($1)").expect("preprocess");
        assert_eq!(pre.infer_base, "SELECT id FROM orders WHERE id = $1");
        assert_eq!(pre.wire_base, "SELECT id FROM orders WHERE id = ANY($1)");
        // No toggle, so the scan form collapses `ANY` like the infer form:
        // SQLite cannot prepare `= ANY($1)`.
        assert_eq!(pre.scan_base, "SELECT id FROM orders WHERE id = $1");
        assert_eq!(pre.array_positions, vec![1]);
    }

    #[test]
    fn optional_and_any_scan_base_preserves_toggle_collapses_any() {
        // The reachable dynamic combination: an OPTIONAL($1) toggle AND a
        // `= ANY($2)` in-list on DIFFERENT params. The scan form must keep
        // the toggle (so the plan reflects the enabled filter) yet collapse
        // `= ANY($2)` to `= $2` (so SQLite can prepare it — `= ANY($2)` is
        // parsed by SQLite as a call to an unknown function `ANY`).
        let pre = preprocess(
            "SELECT id FROM orders WHERE OPTIONAL(total = $1) AND user_id = ANY($2)",
        )
        .expect("preprocess");
        assert_eq!(
            pre.infer_base,
            "SELECT id FROM orders WHERE (total = $1) AND user_id = $2"
        );
        assert_eq!(
            pre.wire_base,
            "SELECT id FROM orders WHERE ($1 IS NULL OR total = $1) AND user_id = ANY($2)"
        );
        assert_eq!(
            pre.scan_base,
            "SELECT id FROM orders WHERE ($1 IS NULL OR total = $1) AND user_id = $2"
        );
        assert_eq!(pre.optional_positions, vec![1]);
        assert_eq!(pre.array_positions, vec![2]);
    }

    #[test]
    fn order_by_allow_set_parses() {
        let pre = preprocess("SELECT id, total FROM orders ORDER BY { id ASC | total DESC }")
            .expect("preprocess");
        assert_eq!(pre.infer_base.trim(), "SELECT id, total FROM orders");
        let options = pre.order_by.expect("order by options");
        assert_eq!(options.len(), 2);
        assert_eq!(options[0].variant_ident, "IdAsc");
        assert_eq!(options[0].clause, "id ASC");
        assert_eq!(options[1].variant_ident, "TotalDesc");
        assert_eq!(options[1].clause, "total DESC");
    }

    #[test]
    fn order_by_default_direction_is_asc() {
        let pre = preprocess("SELECT id FROM orders ORDER BY { id }").expect("preprocess");
        let options = pre.order_by.expect("order by options");
        assert_eq!(options[0].clause, "id ASC");
        assert_eq!(options[0].variant_ident, "IdAsc");
    }

    #[test]
    fn optional_without_placeholder_is_error() {
        assert!(preprocess("SELECT id FROM orders WHERE OPTIONAL(id = id)").is_err());
    }

    #[test]
    fn order_by_bad_direction_is_error() {
        assert!(preprocess("SELECT id FROM orders ORDER BY { id SIDEWAYS }").is_err());
    }

    #[test]
    fn order_by_qualified_column_is_error() {
        assert!(preprocess("SELECT id FROM orders ORDER BY { orders.id ASC }").is_err());
    }

    #[test]
    fn infer_dynamic_optional_types_option() {
        let shape = infer_dynamic_query(&cat(), "SELECT id FROM orders WHERE OPTIONAL(total = $1)")
            .expect("infer");
        assert_eq!(shape.params, vec![ParamShape::Optional(RustType::I32)]);
        assert_eq!(shape.variants.len(), 1);
    }

    #[test]
    fn infer_dynamic_any_types_array() {
        let shape = infer_dynamic_query(&cat(), "SELECT id FROM orders WHERE id = ANY($1)")
            .expect("infer");
        assert_eq!(shape.params, vec![ParamShape::Array(RustType::I64)]);
    }

    #[test]
    fn infer_dynamic_order_by_makes_variants() {
        let shape = infer_dynamic_query(
            &cat(),
            "SELECT id, total FROM orders ORDER BY { id ASC | total DESC }",
        )
        .expect("infer");
        assert_eq!(shape.variants.len(), 2);
        let order = shape.order_by.expect("order by variants");
        assert_eq!(order.len(), 2);
        assert_eq!(order[0].variant_ident, "IdAsc");
    }

    #[test]
    fn infer_dynamic_order_by_unknown_column_is_error() {
        assert!(infer_dynamic_query(
            &cat(),
            "SELECT id FROM orders ORDER BY { nonexistent ASC }"
        )
        .is_err());
    }
}
