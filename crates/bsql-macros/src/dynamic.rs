//! Dynamic query variant expansion for optional clauses.
//!
//! Given a `ParsedQuery` with N optional clauses, generates 2^N `QueryVariant`s.
//! Each variant is a complete SQL string with correct parameter numbering,
//! ready for PREPARE validation and runtime execution.

use smallvec::SmallVec;

use crate::parse::{Param, ParsedQuery};

/// A single concrete SQL variant with a specific combination of
/// included/excluded optional clauses.
#[derive(Debug, Clone)]
pub struct QueryVariant {
    /// Complete SQL string for this variant (positional params, not normalized).
    pub sql: String,
    /// All parameters for this variant, in positional order.
    /// Base params first, then included optional clause params.
    pub params: SmallVec<[Param; 4]>,
    /// Bitmask of which optional clauses are included.
    /// Bit 0 = clause 0, bit 1 = clause 1, etc.
    pub mask: u32,
}

/// Expand a parsed query with optional clauses into 2^N concrete variants.
///
/// For queries with no optional clauses, returns a single variant identical
/// to the original query (the common path pays no cost).
pub fn expand_variants(parsed: &ParsedQuery) -> Result<Vec<QueryVariant>, String> {
    let n = parsed.optional_clauses.len();

    // Fast path: no optional clauses — return the query as-is (no placeholders)
    if n == 0 {
        return Ok(vec![QueryVariant {
            sql: parsed.positional_sql.clone(),
            params: parsed.params.clone(),
            mask: 0,
        }]);
    }

    // n <= 8 enforced by parser, but guard here too
    if n > 8 {
        return Err(format!(
            "query has {} optional clauses ({} variants) — maximum is 8 (256 variants). \
             Split the query into smaller queries with fewer optional filters.",
            n,
            1u32 << n
        ));
    }

    let total = 1u32 << n;
    let mut variants = Vec::with_capacity(total as usize);

    for mask in 0..total {
        let variant = build_variant(parsed, mask)?;
        variants.push(variant);
    }

    Ok(variants)
}

/// Build a single variant for the given bitmask.
fn build_variant(parsed: &ParsedQuery, mask: u32) -> Result<QueryVariant, String> {
    // Collect all params for this variant: base params + included clause params
    let mut all_params: SmallVec<[Param; 4]> = SmallVec::with_capacity(parsed.params.len() + 4);

    // Start with base params (they are always present)
    for p in &parsed.params {
        all_params.push(Param {
            name: p.name.clone(),
            rust_type: p.rust_type.clone(),
            position: all_params.len() + 1,
        });
    }

    // Build the SQL by replacing {OPT_N} placeholders.
    // For included clauses: splice in the clause SQL with renumbered params.
    // For excluded clauses: remove the placeholder entirely.
    let mut sql = parsed.positional_sql.clone();

    for (clause_idx, clause) in parsed.optional_clauses.iter().enumerate() {
        let placeholder = format!("{{OPT_{clause_idx}}}");
        let included = (mask & (1 << clause_idx)) != 0;

        if included {
            // Build position mapping for clause params, then single-pass replace
            let mut pos_map: Vec<(usize, usize)> = Vec::with_capacity(clause.params.len());
            for p in &clause.params {
                let new_pos = all_params.len() + 1;
                pos_map.push((p.position, new_pos));
                all_params.push(Param {
                    name: p.name.clone(),
                    rust_type: p.rust_type.clone(),
                    position: new_pos,
                });
            }

            // Single-pass: scan clause SQL for ${P_N} placeholders
            let frag = &clause.sql_fragment;
            let mut clause_sql = String::with_capacity(frag.len());
            let frag_bytes = frag.as_bytes();
            let frag_len = frag_bytes.len();
            let mut j = 0;
            while j < frag_len {
                if frag_bytes[j] == b'$'
                    && j + 3 < frag_len
                    && frag_bytes[j + 1] == b'{'
                    && frag_bytes[j + 2] == b'P'
                    && frag_bytes[j + 3] == b'_'
                {
                    // Parse ${P_N}
                    let num_start = j + 4;
                    let mut num_end = num_start;
                    while num_end < frag_len && frag_bytes[num_end].is_ascii_digit() {
                        num_end += 1;
                    }
                    if num_end < frag_len && frag_bytes[num_end] == b'}' {
                        let old_pos: usize = frag[num_start..num_end].parse().unwrap_or(0);
                        if let Some(&(_, new_pos)) = pos_map.iter().find(|&&(op, _)| op == old_pos)
                        {
                            clause_sql.push('$');
                            clause_sql.push_str(&new_pos.to_string());
                            j = num_end + 1;
                            continue;
                        }
                    }
                }
                clause_sql.push(frag_bytes[j] as char);
                j += 1;
            }

            sql = sql.replace(&placeholder, &format!(" {clause_sql} "));
        } else {
            sql = sql.replace(&placeholder, " ");
        }
    }

    // Single-pass collapse of consecutive spaces
    let mut collapsed = String::with_capacity(sql.len());
    let mut prev_space = false;
    for c in sql.chars() {
        if c == ' ' {
            if !prev_space {
                collapsed.push(' ');
            }
            prev_space = true;
        } else {
            prev_space = false;
            collapsed.push(c);
        }
    }
    let sql = collapsed.trim().to_owned();

    Ok(QueryVariant {
        sql,
        params: all_params,
        mask,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_query;

    #[test]
    fn no_optional_clauses_returns_single_variant() {
        let parsed = parse_query("SELECT id FROM users WHERE id = $id: i32").unwrap();
        let variants = expand_variants(&parsed).unwrap();
        assert_eq!(variants.len(), 1);
        assert_eq!(variants[0].mask, 0);
        assert_eq!(variants[0].params.len(), 1);
        assert!(variants[0].sql.contains("$1"));
    }

    #[test]
    fn one_optional_clause_produces_two_variants() {
        let parsed = parse_query(
            "SELECT id FROM tickets WHERE deleted_at IS NULL \
             [AND department_id = $dept: Option<i32>] ORDER BY id",
        )
        .unwrap();
        assert_eq!(parsed.optional_clauses.len(), 1);

        let variants = expand_variants(&parsed).unwrap();
        assert_eq!(variants.len(), 2);

        // Variant 0: clause excluded — no dept param
        assert_eq!(variants[0].mask, 0);
        assert_eq!(variants[0].params.len(), 0);
        assert!(
            !variants[0].sql.contains("department_id"),
            "excluded clause should not appear: {}",
            variants[0].sql
        );

        // Variant 1: clause included — dept param present
        assert_eq!(variants[1].mask, 1);
        assert_eq!(variants[1].params.len(), 1);
        assert_eq!(variants[1].params[0].name, "dept");
        assert!(
            variants[1].sql.contains("department_id"),
            "included clause should appear: {}",
            variants[1].sql
        );
        assert!(
            variants[1].sql.contains("$1"),
            "dept should be $1: {}",
            variants[1].sql
        );
    }

    #[test]
    fn two_optional_clauses_produce_four_variants() {
        let parsed = parse_query(
            "SELECT id FROM tickets WHERE deleted_at IS NULL \
             [AND department_id = $dept: Option<i32>] \
             [AND assignee_id = $assignee: Option<i32>] \
             ORDER BY id",
        )
        .unwrap();
        assert_eq!(parsed.optional_clauses.len(), 2);

        let variants = expand_variants(&parsed).unwrap();
        assert_eq!(variants.len(), 4);

        // Variant 0 (0b00): neither
        assert_eq!(variants[0].params.len(), 0);

        // Variant 1 (0b01): dept only
        assert_eq!(variants[1].params.len(), 1);
        assert_eq!(variants[1].params[0].name, "dept");
        assert_eq!(variants[1].params[0].position, 1);

        // Variant 2 (0b10): assignee only
        assert_eq!(variants[2].params.len(), 1);
        assert_eq!(variants[2].params[0].name, "assignee");
        assert_eq!(variants[2].params[0].position, 1);

        // Variant 3 (0b11): both
        assert_eq!(variants[3].params.len(), 2);
        assert_eq!(variants[3].params[0].name, "dept");
        assert_eq!(variants[3].params[0].position, 1);
        assert_eq!(variants[3].params[1].name, "assignee");
        assert_eq!(variants[3].params[1].position, 2);
    }

    #[test]
    fn base_params_precede_optional_params() {
        let parsed = parse_query(
            "SELECT id FROM tickets WHERE status = $status: &str \
             [AND department_id = $dept: Option<i32>] ORDER BY id",
        )
        .unwrap();

        let variants = expand_variants(&parsed).unwrap();
        assert_eq!(variants.len(), 2);

        // Variant 0: only base param
        assert_eq!(variants[0].params.len(), 1);
        assert_eq!(variants[0].params[0].name, "status");
        assert_eq!(variants[0].params[0].position, 1);

        // Variant 1: base + optional
        assert_eq!(variants[1].params.len(), 2);
        assert_eq!(variants[1].params[0].name, "status");
        assert_eq!(variants[1].params[0].position, 1);
        assert_eq!(variants[1].params[1].name, "dept");
        assert_eq!(variants[1].params[1].position, 2);
    }

    #[test]
    fn three_optional_clauses_produce_eight_variants() {
        let parsed = parse_query(
            "SELECT id FROM tickets WHERE 1 = 1 \
             [AND a = $a: Option<i32>] \
             [AND b = $b: Option<i32>] \
             [AND c = $c: Option<i32>]",
        )
        .unwrap();

        let variants = expand_variants(&parsed).unwrap();
        assert_eq!(variants.len(), 8);

        // Check variant 7 (all included) has all 3 params
        assert_eq!(variants[7].mask, 7);
        assert_eq!(variants[7].params.len(), 3);
        assert_eq!(variants[7].params[0].name, "a");
        assert_eq!(variants[7].params[1].name, "b");
        assert_eq!(variants[7].params[2].name, "c");
    }

    #[test]
    fn param_renumbering_correct_for_non_contiguous_inclusion() {
        // Variant where clause 0 is excluded but clause 1 is included
        let parsed = parse_query(
            "SELECT id FROM tickets WHERE status = $s: &str \
             [AND a = $a: Option<i32>] \
             [AND b = $b: Option<i32>]",
        )
        .unwrap();

        let variants = expand_variants(&parsed).unwrap();

        // Variant 2 (0b10): clause 0 excluded, clause 1 included
        let v2 = &variants[2];
        assert_eq!(v2.mask, 2);
        assert_eq!(v2.params.len(), 2); // s + b
        assert_eq!(v2.params[0].name, "s");
        assert_eq!(v2.params[0].position, 1);
        assert_eq!(v2.params[1].name, "b");
        assert_eq!(v2.params[1].position, 2);
        assert!(v2.sql.contains("$2"), "b should be $2: {}", v2.sql);
    }

    #[test]
    fn each_variant_has_unique_sql() {
        let parsed = parse_query(
            "SELECT id FROM tickets WHERE 1 = 1 \
             [AND a = $a: Option<i32>] \
             [AND b = $b: Option<i32>]",
        )
        .unwrap();

        let variants = expand_variants(&parsed).unwrap();
        let sqls: Vec<&str> = variants.iter().map(|v| v.sql.as_str()).collect();
        let unique: std::collections::HashSet<&str> = sqls.iter().copied().collect();
        assert_eq!(
            unique.len(),
            sqls.len(),
            "variant SQL strings must be unique: {sqls:?}"
        );
    }

    #[test]
    fn variant_sql_has_no_placeholders() {
        let parsed = parse_query(
            "SELECT id FROM tickets WHERE 1 = 1 \
             [AND a = $a: Option<i32>]",
        )
        .unwrap();

        let variants = expand_variants(&parsed).unwrap();
        for v in &variants {
            assert!(
                !v.sql.contains("{OPT_"),
                "variant SQL should not contain OPT placeholders: {}",
                v.sql
            );
            assert!(
                !v.sql.contains("{P_"),
                "variant SQL should not contain P_ placeholders: {}",
                v.sql
            );
        }
    }
}
