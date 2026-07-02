// A conforming, SQLite-validated query compiles: every projected column's
// (type, nullable) agrees between the inference lattice and real SQLite.
// The OPTIONAL(...) toggle's full scan is acknowledged with the recognized
// marker, so it is accepted.
bsql::query!(Ok1, "SELECT id, email, name FROM users");
bsql::query!(Ok2, "SELECT id, user_id, total FROM orders");
bsql::query!(
    Ok3,
    "SELECT id FROM users WHERE OPTIONAL(name = $1) \
     /* bsql:allow-scan: small lookup table; revisit when it grows */"
);

// An expression aliased back to a NULLABLE base column's name. `COALESCE(name,
// 'x')` is genuinely NOT NULL (the lattice types it so), and although its
// result column NAME `name` collides with the nullable base column
// `users.name`, the conformance nullability check applies only to a genuine
// base-column reference (an expression has no SQLite decltype). The aliased
// expression is left to the lattice, so this valid query is ACCEPTED.
bsql::query!(Ok4, "SELECT COALESCE(name, 'x') AS name FROM users");

// An OPTIONAL($1) toggle combined with a `= ANY($2)` in-list on a DIFFERENT
// param. The full-scan check runs on a SQLite-preparable SCAN form: the
// `$1 IS NULL OR ...` toggle is preserved, but the PostgreSQL-only `= ANY($2)`
// is collapsed to `= $2` (SQLite parses `= ANY(...)` as a call to an unknown
// function `ANY`). The toggle's scan is acknowledged, so it is ACCEPTED — this
// is the valid OPTIONAL + `= ANY($M)` combination the wire-form scan check
// used to falsely reject.
bsql::query!(
    Ok5,
    "SELECT id FROM users WHERE OPTIONAL(name = $1) AND id = ANY($2) \
     /* bsql:allow-scan: small lookup table; revisit when it grows */"
);

fn main() {
    // Touch the generated artifacts so the expansion is fully exercised.
    let _ = Ok1Query::PREPARED.param_oids();
    let _ = Ok2Query::PREPARED.param_oids();
    let _ = Ok3Query::PREPARED.param_oids();
    let _ = Ok4Query::PREPARED.param_oids();
    let _ = Ok5Query::PREPARED.param_oids();
}
