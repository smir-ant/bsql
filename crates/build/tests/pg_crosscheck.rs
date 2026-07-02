// Build-time cross-check of the inference engine's loud/Ok verdicts against the
// hand-verified live-PostgreSQL verdicts for the tuple-SET row-source rule, the
// DEFAULT value keyword, the FROM-clause feature dimension (column-alias lists,
// sibling CTEs, LATERAL, table functions, recursive CTEs), the duplicate-CTE
// rule, the ONLY inheritance modifier, parenthesized/nested joins, the
// comparison-family inferable parameters (LIKE/ILIKE/SIMILAR TO, IN-list,
// IS [NOT] DISTINCT FROM, LIMIT/OFFSET/FETCH), the public-schema qualifier
// (unquoted AND quoted), set-operation integer widening, a WITH RECURSIVE whose
// CTE never self-references, a single $N reconciled across two integer-width
// contexts, the subquery-tail scope (a subquery's trailing ORDER BY / LIMIT
// resolves against the subquery body's own scope), the aggregate / window
// PLACEMENT rules (an aggregate is loud in WHERE / JOIN-ON / GROUP BY, a window
// is loud in WHERE / GROUP BY / HAVING), the GROUP BY coverage rule with
// primary-key functional dependency (every non-aggregated selected column must
// be grouped or determined by a grouped primary key), the grouping-set
// functional dependency (a primary key grants a dependency only when it is
// grouped in EVERY generated grouping set — ROLLUP/CUBE grant none), the
// subquery / derived / CTE LIMIT/OFFSET/FETCH parameter (a `$N` there is a
// bigint), the correlated-ungrouped-outer subquery rule (a correlated reference
// to an ungrouped column of the enclosing aggregate query is loud; a pure-outer
// aggregate over it is accepted), the nested-aggregate rule (an aggregate inside
// another aggregate's argument is loud, a windowed outer is not — including
// inside a named WINDOW definition's PARTITION BY / ORDER BY, AND the co-owned
// case where two aggregates written one inside the other both float to the SAME
// enclosing level — a real nesting / contained-window there — while two that
// reach DIFFERENT levels are not nested, AND the subquery-crossing case where the
// inner aggregate is written in a scalar subquery of the outer aggregate's
// argument yet associates with the outer aggregate's own writing level or a level
// enclosing it — a real nesting — while an inner aggregate reaching its own
// subquery's level is not nested), the array-quantifier element parameter
// (`<col> <op> ANY/ALL/SOME(ARRAY[...])` types `<col>` against the array element
// type: a bare-placeholder constructor resolves to text so it types text-column
// elements and is loud on a non-text column, a LITERAL or CAST member fixes the
// element type so each placeholder element types from it, an `::T[]` cast pins the
// element type, the reversed `$1 = ANY(ARRAY[col])` types from the column member,
// and a bare ARRAY-typed parameter is the outside-the-scalar-set boundary), the
// row-constructor positional parameter (a row-vs-row comparison / IS [NOT]
// DISTINCT FROM / BETWEEN / IN of EQUAL width types each placeholder element from
// the aligned element on the other side, while an unequal-width row comparison
// stays the loud arity error), the
// no-column-list INSERT parameter (the unordered-catalog boundary), and a family of
// STRUCTURAL-INTEGRITY rules — uniqueness and arity, not just existence: a
// duplicate INSERT target column, a duplicate UPDATE / ON CONFLICT DO UPDATE SET
// target, a duplicate FROM relation alias (an inner subquery scope may still
// shadow an outer name), a position-dependent subquery arity (a scalar value /
// scalar comparison operand must project one column; an IN / ANY / ALL / row
// comparison must match the LHS arity; EXISTS is arity-agnostic) — INCLUDING a
// `SELECT *` / `SELECT t.*` subquery whose true width is resolved by expanding
// the wildcard against the subquery's own FROM scope rather than silently
// accepted, a row-constructor-vs-row-constructor width mismatch (two `(...)` /
// `ROW(...)` constructors compared with `=` / `<>` / `<` / `<=` / `>` / `>=` /
// `IS [NOT] DISTINCT FROM` / `BETWEEN` / `IN` must have equal entry counts), and
// an `INSERT INTO t AS alias` (the alias resolves, the bare name is hidden), the
// ON CONFLICT DO UPDATE arbiter requirement (a `DO UPDATE` with no conflict
// target is rejected — the update needs an arbiter PG will not infer — while a
// targetless `DO NOTHING` and any column-list target stay valid), and the
// DISTINCT ON / ORDER BY prefix rule (every leading ORDER BY key, up to the
// DISTINCT ON count, must be a DISTINCT ON expression matched by sort-key
// identity — bare/qualified/alias/ordinal spellings of one column all match, an
// arbitrary expression by canonical text — while the distinct list may be longer
// than ORDER BY and a query with no ORDER BY is unconstrained), and the plain
// SELECT DISTINCT / ORDER BY rule (a plain DISTINCT de-duplicates the projected
// rows, so every ORDER BY key must match a projected select-list expression by
// the same sort-key identity, else loud — while a non-DISTINCT query is not
// constrained), the DELETE-target catalog-only resolution (a CTE name is never a
// valid DELETE target and never shadows a base table as one, while a USING
// relation CAN reference a CTE), the aggregate / window placement on the
// left-hand side of `x IN (subquery)` (loud in WHERE; an ungrouped-column
// GROUP-BY error in a projection over an aggregating query; a plain column
// unaffected), the FETCH modifier rules (`WITH TIES` requires an ORDER BY
// — satisfied by the EFFECTIVE query's ORDER BY, so a parenthesized inner
// query's / parenthesized set-op's ORDER BY satisfies an outer WITH TIES, while
// no ORDER BY anywhere is loud — and `PERCENT` is never implemented; both loud
// at top level and in a derived / CTE tail), the outer-correlated aggregate
// ASSOCIATION rule (an aggregate binds to the query level owning its argument
// columns: one nested in a subquery of an aggregate-forbidding clause whose
// arguments are pure-outer is loud in that clause, while a count(*) / own-inner
// aggregate is the subquery's own; one associating to an enclosing SELECT's
// projection / ORDER BY promotes that query to aggregate so an ungrouped column
// is loud, while a sole projected subquery aggregate stays Ok), and the
// data-modifying CTE body (`WITH c AS (DELETE/UPDATE/INSERT ... RETURNING ...)`
// exposes its RETURNING columns; a body without RETURNING and a DML nested as a
// FROM-derived table stay loud).
// Each case carries its expected verdict, confirmed against the live server; a
// divergence fails the test loudly.
//
// Three verdict categories:
//   * `Ok`   — PG accepts AND the engine accepts (the construct is MODELED).
//   * `Loud` — PG rejects AND the engine rejects loudly (a genuine error).
//   * `UnmodelableLoud` — PG ACCEPTS, but the engine deliberately rejects with
//     an ACCURATE diagnostic naming the form. These are the genuinely
//     unmodelable-but-wire-legal constructs: a positional column-alias list on a
//     relation whose declaration order the schema catalog does not preserve
//     (a base table, a transparent `SELECT *`), a table-valued function, a
//     `WITH RECURSIVE` fixpoint, an `UNNEST(...)` array source, a
//     `SELECT ... INTO <table>` (a table-creating utility statement PG runs as
//     CREATE TABLE AS — it returns NO result row set, so the engine refuses to
//     fabricate a row shape for it), and an ALL-TYPELESS construct (a bare-NULL
//     projection / CASE / COALESCE / set-op arm whose every branch is `NULL`):
//     PG types the unknown as `text` (its default), which the engine refuses to
//     guess. The engine never silently accepts and never misleads; it names the
//     form. This category documents the engine/PG divergence as INTENTIONAL — a
//     loud-accurate posture on a construct the build-time catalog cannot model
//     faithfully (or, for the all-typeless case, refuses to default-guess).
//
// The catalog is built through the public `catalog_from_dir` entry point from a
// temporary migration directory, exactly as a consumer's build would.
use bsql_build::{catalog_from_dir, infer_query};
use std::io::Write;

const USERS: &str =
    "CREATE TABLE s10k_users (id BIGINT PRIMARY KEY, name TEXT NOT NULL, bio TEXT, age INT);";
const ACCOUNTS: &str =
    "CREATE TABLE s10k_accounts (id INT PRIMARY KEY, user_id BIGINT NOT NULL, balance INT NOT NULL);";
// A composite PRIMARY KEY, for the grouping functional-dependency cases: the
// WHOLE key (a, b) must be grouped to determine c.
const COMP: &str = "CREATE TABLE s10k_comp (a INT, b INT, c TEXT, PRIMARY KEY (a, b));";
// Two tables sharing a NULLABLE non-key column `k`, and two sharing a NOT NULL
// non-key column `k`, for the USING / NATURAL merged-column nullability matrix
// (all 12 join x base-nullability combos). The standard fixtures share only the
// NOT NULL primary key `id`, so they cannot express the both-nullable / both-NOT
// -NULL-non-key corners these defects hid in.
const NN_A: &str = "CREATE TABLE s10k_nn_a (k INT, lonly INT);";
const NN_B: &str = "CREATE TABLE s10k_nn_b (k INT, ronly INT);";
const PK_A: &str = "CREATE TABLE s10k_pk_a (k INT NOT NULL, lonly INT);";
const PK_B: &str = "CREATE TABLE s10k_pk_b (k INT NOT NULL, ronly INT);";
// Three tables that share a join key `k` AND each carry their OWN primary key
// `id`, for the merged-column GROUP-BY COVERAGE matrix: the join-type x
// grouped-side truth table, chained / mixed join folding, and the primary-key
// functional dependency of a SOURCE side (grouping a source relation's whole PK
// covers the merge even without grouping `k` itself).
const CJA: &str = "CREATE TABLE s10k_cja (id INT PRIMARY KEY, k INT);";
const CJB: &str = "CREATE TABLE s10k_cjb (id INT PRIMARY KEY, k INT);";
const CJC: &str = "CREATE TABLE s10k_cjc (id INT PRIMARY KEY, k INT);";
// Two tables sharing a NOT NULL key `k`, for the super-aggregate merged-column
// nullability matrix: NOT NULL bases pin the base rule to NOT NULL, so any NULL
// result row is PURELY the ROLLUP / CUBE / GROUPING SETS super-aggregate.
const MS_A: &str = "CREATE TABLE s10k_ms_a (k INT NOT NULL, av INT);";
const MS_B: &str = "CREATE TABLE s10k_ms_b (k INT NOT NULL, bv INT);";

#[derive(Clone, Copy)]
enum Pg {
    Ok,
    Loud,
    /// PG accepts the form, but the engine deliberately rejects it loudly with
    /// an accurate diagnostic naming a construct the catalog cannot model.
    UnmodelableLoud,
}

#[test]
fn engine_matches_live_pg() {
    let cases: &[(Pg, &str, &str)] = &[
        // Multi-column SET row-source classification.
        (Pg::Loud, "SET (name)=(scalar)", "UPDATE s10k_users SET (name)=('x') WHERE id=$1 RETURNING id"),
        (Pg::Ok,   "SET (name,bio)=(v1,v2)", "UPDATE s10k_users SET (name,bio)=('x','y') WHERE id=$1 RETURNING id"),
        (Pg::Loud, "SET (name,bio)=ROW(one)", "UPDATE s10k_users SET (name,bio)=ROW('x') WHERE id=$1 RETURNING id"),
        (Pg::Loud, "SET (name)=(v1,v2) arity", "UPDATE s10k_users SET (name)=('x','y') WHERE id=$1 RETURNING id"),
        (Pg::Ok,   "SET (name)=((SELECT))", "UPDATE s10k_users SET (name)=((SELECT name FROM s10k_users)) WHERE id=$1 RETURNING id"),
        (Pg::Ok,   "SET (name)=(ROW(one))", "UPDATE s10k_users SET (name)=(ROW('x')) WHERE id=$1 RETURNING id"),
        (Pg::Ok,   "SET (name,bio)=(SELECT name,bio)", "UPDATE s10k_users SET (name,bio)=(SELECT name,bio FROM s10k_users) WHERE id=$1 RETURNING id"),
        (Pg::Loud, "SET (name,bio)=(SELECT name)", "UPDATE s10k_users SET (name,bio)=(SELECT name FROM s10k_users) WHERE id=$1 RETURNING id"),
        // ON CONFLICT tuple-SET.
        (Pg::Loud, "OC SET (name,bio)=ROW(x)", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET (name,bio)=ROW('x') RETURNING id"),
        (Pg::Loud, "OC SET (name)=('x')", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET (name)=('x') RETURNING id"),
        (Pg::Ok,   "OC SET (name,bio)=('x','y')", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET (name,bio)=('x','y') RETURNING id"),
        // DEFAULT value keyword placement.
        (Pg::Ok,   "INSERT VALUES ($1,$2,DEFAULT)", "INSERT INTO s10k_users (id,name,age) VALUES ($1,$2,DEFAULT) RETURNING id"),
        (Pg::Ok,   "UPDATE SET age=DEFAULT", "UPDATE s10k_users SET age=DEFAULT WHERE id=$1 RETURNING id"),
        (Pg::Ok,   "OC SET age=DEFAULT", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET age=DEFAULT RETURNING id"),
        (Pg::Loud, "DEFAULT in WHERE", "UPDATE s10k_users SET age=1 WHERE id=DEFAULT RETURNING id"),
        (Pg::Loud, "DEFAULT+1 in cell", "INSERT INTO s10k_users (id,age) VALUES ($1,DEFAULT+1) RETURNING id"),
        (Pg::Loud, "DEFAULT in RETURNING", "UPDATE s10k_users SET age=1 WHERE id=$1 RETURNING DEFAULT"),
        (Pg::Ok,   "SET (name,age)=(DEFAULT,1)", "UPDATE s10k_users SET (name,age)=(DEFAULT,1) WHERE id=$1 RETURNING id"),
        (Pg::Ok,   "SET (name,age)=ROW(DEFAULT,1)", "UPDATE s10k_users SET (name,age)=ROW(DEFAULT,1) WHERE id=$1 RETURNING id"),
        (Pg::Loud, "SELECT default", "SELECT default FROM s10k_users"),
        (Pg::Ok,   "multi-row DEFAULT", "INSERT INTO s10k_users (id,name,age) VALUES ($1,$2,DEFAULT),($3,DEFAULT,$4) RETURNING id"),

        // Column-alias list: positional rename on an ORDERED projection.
        (Pg::Loud, "derived t(x,y) orig id", "SELECT id FROM (SELECT id,name FROM s10k_users) AS t(x,y)"),
        (Pg::Ok,   "derived t(x,y) aliases", "SELECT x,y FROM (SELECT id,name FROM s10k_users) AS t(x,y)"),
        (Pg::Ok,   "derived t(x) trailing orig", "SELECT name FROM (SELECT id,name FROM s10k_users) AS t(x)"),
        (Pg::Loud, "derived t(x,y,z) too many", "SELECT x FROM (SELECT id,name FROM s10k_users) AS t(x,y,z)"),
        (Pg::Ok,   "VALUES derived t(p,q)", "SELECT p,q FROM (VALUES (1,2)) AS t(p,q)"),
        (Pg::Ok,   "top-level VALUES", "VALUES (1),(2)"),
        (Pg::Ok,   "CTE t(a,b)", "WITH t(a,b) AS (SELECT id,name FROM s10k_users) SELECT a,b FROM t"),
        (Pg::Loud, "CTE t(a,b,c) too many", "WITH t(a,b,c) AS (SELECT id,name FROM s10k_users) SELECT a FROM t"),
        (Pg::Ok,   "CTE re-alias ref site", "WITH t AS (SELECT id,name FROM s10k_users) SELECT p,q FROM t AS x(p,q)"),
        // Genuinely-unmodelable: a positional alias list on a catalog-
        // ordered-unknown relation (PG accepts; the catalog cannot model order).
        (Pg::UnmodelableLoud, "base table t(a,b,c,d)", "SELECT a,b,c,d FROM s10k_users AS t(a,b,c,d)"),
        (Pg::UnmodelableLoud, "transparent star t(p,q,r,s)", "SELECT p FROM (SELECT * FROM s10k_users) AS t(p,q,r,s)"),

        // Sibling CTE: a later CTE references an earlier one.
        (Pg::Ok,   "sibling CTE no params", "WITH a AS (SELECT id FROM s10k_users), b AS (SELECT id FROM a) SELECT id FROM b"),
        (Pg::Ok,   "sibling CTE with param", "WITH a AS (SELECT id FROM s10k_users), b AS (SELECT id FROM a WHERE id=$1) SELECT id FROM b"),

        // Tuple-SET parameter typing from the target columns.
        (Pg::Ok,   "tuple SET ($1,$2)", "UPDATE s10k_users SET (name,bio)=($1,$2) WHERE id=$3 RETURNING id"),
        (Pg::Ok,   "tuple SET ROW($1,$2)", "UPDATE s10k_users SET (name,bio)=ROW($1,$2) WHERE id=$3 RETURNING id"),
        (Pg::Ok,   "OC tuple SET ($3,$4)", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET (name,bio)=($3,$4) RETURNING id"),
        (Pg::Ok,   "tuple SET subq WHERE param", "UPDATE s10k_users SET (name,bio)=(SELECT name,bio FROM s10k_users WHERE age=$1 LIMIT 1) WHERE id=$2 RETURNING id"),

        // Row-constructor positional parameter typing: a row-vs-row comparison
        // types each placeholder element from the aligned element of the row on
        // the other side, exactly as PG types each entry of a row comparison
        // (parameter_types live-verified). Covered for the comparison family,
        // IS [NOT] DISTINCT FROM, BETWEEN, and IN. An UNEQUAL-width row-vs-row
        // comparison stays the loud arity error PG reports ("unequal number of
        // entries in row expressions").
        (Pg::Ok,   "row (a,b)=($1,$2)", "SELECT a FROM s10k_comp WHERE (a,b)=($1,$2)"),
        (Pg::Ok,   "row (id,name)=($1,$2) mixed types", "SELECT id FROM s10k_users WHERE (id,name)=($1,$2)"),
        (Pg::Ok,   "row (a,c)=($1,$2) int+text", "SELECT a FROM s10k_comp WHERE (a,c)=($1,$2)"),
        (Pg::Ok,   "row ($1,$2)=(a,b) reversed", "SELECT a FROM s10k_comp WHERE ($1,$2)=(a,b)"),
        (Pg::Ok,   "row (a,b)<($1,$2) less-than", "SELECT a FROM s10k_comp WHERE (a,b)<($1,$2)"),
        (Pg::Ok,   "row ROW(a,b)=ROW($1,$2)", "SELECT a FROM s10k_comp WHERE ROW(a,b)=ROW($1,$2)"),
        (Pg::Ok,   "row (a,b) IS DISTINCT FROM ($1,$2)", "SELECT a FROM s10k_comp WHERE (a,b) IS DISTINCT FROM ($1,$2)"),
        (Pg::Ok,   "row (a,b) IN (($1,$2),($3,$4))", "SELECT a FROM s10k_comp WHERE (a,b) IN (($1,$2),($3,$4))"),
        (Pg::Ok,   "row (a,b) BETWEEN ($1,$2) AND ($3,$4)", "SELECT a FROM s10k_comp WHERE (a,b) BETWEEN ($1,$2) AND ($3,$4)"),
        (Pg::Loud, "row (a,b)=($1,$2,$3) unequal width", "SELECT a FROM s10k_comp WHERE (a,b)=($1,$2,$3)"),

        // LATERAL: outer-correlated body accepted; internal typo loud.
        (Pg::Ok,   "comma LATERAL", "SELECT u.id FROM s10k_users u, LATERAL (SELECT a.balance FROM s10k_accounts a WHERE a.user_id=u.id) s"),
        (Pg::Ok,   "LATERAL projects outer", "SELECT s.uid FROM s10k_users u, LATERAL (SELECT u.id AS uid) s"),
        (Pg::Ok,   "CROSS JOIN LATERAL", "SELECT u.id FROM s10k_users u CROSS JOIN LATERAL (SELECT a.balance FROM s10k_accounts a WHERE a.user_id=u.id) s"),
        (Pg::Ok,   "LEFT JOIN LATERAL", "SELECT u.id FROM s10k_users u LEFT JOIN LATERAL (SELECT a.balance FROM s10k_accounts a WHERE a.user_id=u.id) s ON true"),
        // A lateral body in an UPDATE FROM may reference PRIOR FROM items but
        // NOT the UPDATE target: PG rejects the target reference ("invalid
        // reference to FROM-clause entry for table u"), accepts the prior-item
        // reference. Both verdicts re-verified against live PG.
        (Pg::Loud, "UPDATE FROM LATERAL ref target", "UPDATE s10k_users u SET age=s.balance FROM LATERAL (SELECT a.balance FROM s10k_accounts a WHERE a.user_id=u.id) s RETURNING u.id"),
        (Pg::Ok,   "UPDATE FROM LATERAL ref prior", "UPDATE s10k_users SET age=s.balance FROM s10k_accounts a, LATERAL (SELECT a.balance) s RETURNING s10k_users.id"),
        (Pg::Loud, "LATERAL internal typo", "SELECT u.id FROM s10k_users u, LATERAL (SELECT a.balance FROM s10k_accounts a WHERE a.user_id=u.nope) s"),

        // Table function + recursive CTE: PG accepts; engine loud-accurate.
        (Pg::UnmodelableLoud, "generate_series", "SELECT g FROM generate_series(1,10) AS g"),
        (Pg::UnmodelableLoud, "WITH RECURSIVE", "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<5) SELECT n FROM t"),
        (Pg::UnmodelableLoud, "UNNEST source", "SELECT u.v FROM UNNEST(ARRAY[1,2]) AS u(v)"),

        // Duplicate CTE name: PG rejects ("WITH query name t specified more than
        // once"); engine loud. A distinct sibling pair stays Ok.
        (Pg::Loud, "duplicate CTE name", "WITH t AS (SELECT id FROM s10k_users), t AS (SELECT id FROM s10k_accounts) SELECT id FROM t"),

        // ONLY inheritance modifier: PG accepts `ONLY <table>` and resolves it
        // to the real table; engine models it (ONLY does not change the column
        // shape). A missing real table is still loud.
        (Pg::Ok,   "SELECT FROM ONLY", "SELECT id FROM ONLY s10k_users"),
        (Pg::Ok,   "UPDATE ONLY", "UPDATE ONLY s10k_users SET age=1 WHERE id=$1 RETURNING id"),
        (Pg::Ok,   "DELETE FROM ONLY", "DELETE FROM ONLY s10k_users WHERE id=$1 RETURNING id"),
        (Pg::Loud, "FROM ONLY nonexistent", "SELECT id FROM ONLY s10k_nonexistent"),

        // Parenthesized / nested join: PG accepts; engine models it (same column
        // scope as the un-parenthesized chain). An internal typo is loud. An
        // ALIAS on the nested join is a column shape the catalog cannot model
        // faithfully (PG accepts it, merging the columns under one name), so the
        // engine is loud-accurate — the UnmodelableLoud category.
        (Pg::Ok,   "nested join", "SELECT u.id, a.balance FROM (s10k_users u JOIN s10k_accounts a ON a.user_id=u.id)"),
        (Pg::Loud, "nested join internal typo", "SELECT u.id FROM (s10k_users u JOIN s10k_nonexistent a ON a.user_id=u.id)"),
        (Pg::Loud, "nested join ON column typo", "SELECT u.id FROM (s10k_users u JOIN s10k_accounts a ON a.user_id=u.nope)"),
        (Pg::UnmodelableLoud, "aliased nested join", "SELECT x.name FROM (s10k_users u JOIN s10k_accounts a ON a.user_id=u.id) AS x"),

        // Comparison-family inferable params: PG's parameter_types are
        // {text}/{integer}/{bigint}; engine types them so a valid query needs no
        // explicit cast (all Ok). A genuinely non-inferable param stays loud.
        (Pg::Ok,   "LIKE pattern text", "SELECT id FROM s10k_users WHERE name LIKE $1"),
        (Pg::Ok,   "ILIKE pattern text", "SELECT id FROM s10k_users WHERE name ILIKE $1"),
        (Pg::Ok,   "SIMILAR TO pattern text", "SELECT id FROM s10k_users WHERE name SIMILAR TO $1"),
        (Pg::Ok,   "IN-list int", "SELECT id FROM s10k_users WHERE age IN ($1,$2)"),
        (Pg::Ok,   "IS DISTINCT FROM int", "SELECT id FROM s10k_users WHERE age IS DISTINCT FROM $1"),
        (Pg::Ok,   "IS NOT DISTINCT FROM text", "SELECT id FROM s10k_users WHERE name IS NOT DISTINCT FROM $1"),
        (Pg::Ok,   "LIMIT i64", "SELECT id FROM s10k_users LIMIT $1"),
        (Pg::Ok,   "OFFSET i64", "SELECT id FROM s10k_users OFFSET $1"),
        (Pg::Ok,   "LIMIT+OFFSET i64", "SELECT id FROM s10k_users LIMIT $1 OFFSET $2"),
        (Pg::Ok,   "FETCH FIRST i64", "SELECT id FROM s10k_users FETCH FIRST $1 ROWS ONLY"),

        // Plain-table modifiers: `WITH ORDINALITY` (function-only) and a
        // `WITH (...)` table hint (MSSQL) are PG syntax errors — engine loud.
        // `TABLESAMPLE` is valid PG and shape-preserving — engine Ok.
        (Pg::Loud, "FROM t WITH ORDINALITY", "SELECT id FROM s10k_users WITH ORDINALITY"),
        (Pg::Loud, "FROM t WITH (NOLOCK)", "SELECT id FROM s10k_users WITH (NOLOCK)"),
        (Pg::Ok,   "FROM t TABLESAMPLE", "SELECT id FROM s10k_users TABLESAMPLE SYSTEM (50)"),

        // TOP <quantity>: an MSSQL clause with no PG equivalent in EITHER shape
        // (a bare constant or a parenthesized expression) — PG syntax error,
        // engine loud.
        (Pg::Loud, "SELECT TOP 5 id", "SELECT TOP 5 id FROM s10k_users"),
        (Pg::Loud, "SELECT TOP (5) id", "SELECT TOP (5) id FROM s10k_users"),

        // SELECT ... INTO: PG ACCEPTS it but runs it as CREATE TABLE AS — it
        // returns NO result row set, so the engine deliberately rejects it
        // (the UnmodelableLoud posture: a row-shape it must not fabricate).
        (Pg::UnmodelableLoud, "SELECT id INTO foo", "SELECT id INTO s10k_into_target FROM s10k_users"),

        // DISTINCT ON (...): core PG, shape-preserving — engine models it. A
        // valid list (column, qualified, projection alias, with a param) is Ok;
        // a typo inside is loud.
        (Pg::Ok,   "DISTINCT ON (age) age,id", "SELECT DISTINCT ON (age) age, id FROM s10k_users ORDER BY age"),
        (Pg::Ok,   "DISTINCT ON qualified", "SELECT DISTINCT ON (u.age) u.id FROM s10k_users u ORDER BY u.age"),
        (Pg::Ok,   "DISTINCT ON alias", "SELECT DISTINCT ON (a) age AS a, id FROM s10k_users ORDER BY a"),
        (Pg::Ok,   "DISTINCT ON cast param", "SELECT DISTINCT ON ($1::int4) id FROM s10k_users ORDER BY $1::int4"),
        (Pg::Loud, "DISTINCT ON (typo)", "SELECT DISTINCT ON (nope) age, id FROM s10k_users ORDER BY nope"),

        // public-schema qualifier: PG resolves a bare table to the `public`
        // schema, and the catalog keys it by its bare name — so `public.t` is
        // accepted symmetrically. A non-`public` schema (or a 3-part path)
        // names a namespace the catalog does not model — PG rejects it, engine
        // loud naming the full path.
        (Pg::Ok,   "FROM public.users", "SELECT id FROM public.s10k_users"),
        (Pg::Loud, "FROM wrongschema.users", "SELECT id FROM wrongschema.s10k_users"),
        (Pg::Ok,   "UPDATE public.users", "UPDATE public.s10k_users SET age = 1 WHERE id = $1 RETURNING id"),
        (Pg::Ok,   "DELETE public.users", "DELETE FROM public.s10k_users WHERE id = $1 RETURNING id"),
        (Pg::Ok,   "INSERT public.users", "INSERT INTO public.s10k_users (id, name) VALUES ($1, $2) RETURNING id"),
        (Pg::Loud, "UPDATE wrongschema.users", "UPDATE wrongschema.s10k_users SET age = 1 WHERE id = $1 RETURNING id"),
        // Quoted "public": PG resolves a quoted "public" to the default schema
        // (verified: `SELECT id FROM "public".s10k_users` succeeds), so it is
        // accepted symmetrically; a quoted "PUBLIC" (preserved upper case) is a
        // distinct schema PG rejects (`relation "PUBLIC.s10k_users" does not
        // exist`), so the engine stays loud. Both verdicts re-verified live.
        (Pg::Ok,   "FROM quoted public.users", "SELECT id FROM \"public\".s10k_users"),
        (Pg::Loud, "FROM quoted PUBLIC.users", "SELECT id FROM \"PUBLIC\".s10k_users"),
        (Pg::Ok,   "UPDATE quoted public.users", "UPDATE \"public\".s10k_users SET age = 1 WHERE id = $1 RETURNING id"),

        // Set-operation integer widening: PG accepts a UNION/INTERSECT/EXCEPT of
        // compatible integer widths and widens to the wider (verified: `SELECT
        // id(bigint) UNION SELECT id(int)` -> result type `bigint`; `SELECT
        // age(int4) UNION SELECT user_id(int8)` -> `bigint`; same for INTERSECT
        // and EXCEPT), so the engine widens and accepts. A text-vs-int pairing
        // has no common type (`UNION types text and bigint cannot be matched`),
        // so the engine stays loud. Both verdicts re-verified live.
        (Pg::Ok,   "UNION bigint vs int", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts"),
        (Pg::Ok,   "UNION int4 vs int8", "SELECT age FROM s10k_users UNION SELECT user_id FROM s10k_accounts"),
        (Pg::Ok,   "INTERSECT bigint vs int", "SELECT id FROM s10k_users INTERSECT SELECT id FROM s10k_accounts"),
        (Pg::Ok,   "EXCEPT bigint vs int", "SELECT id FROM s10k_users EXCEPT SELECT id FROM s10k_accounts"),
        (Pg::Loud, "UNION text vs int", "SELECT name FROM s10k_users UNION SELECT id FROM s10k_users"),

        // COALESCE / CASE integer-width widening: PG types COALESCE / CASE of
        // compatible integer widths as the WIDER (verified: `COALESCE(balance
        // int4, user_id int8)` -> `bigint`, order-independent; `CASE .. THEN
        // int4 ELSE int8 END` -> `bigint`), the same widening the set-op path
        // uses, so the engine widens and accepts. A same-type pairing is
        // unchanged (`COALESCE(id, id)` -> `integer`). A text-vs-int pairing has
        // no common type (`COALESCE types text and integer cannot be matched`,
        // `CASE types integer and text cannot be matched`), so the engine stays
        // loud and matching. Both directions re-verified live.
        (Pg::Ok,   "COALESCE(int4, int8) widens", "SELECT COALESCE(balance, user_id) FROM s10k_accounts"),
        (Pg::Ok,   "COALESCE(int8, int4) widens", "SELECT COALESCE(user_id, balance) FROM s10k_accounts"),
        (Pg::Ok,   "CASE int4/int8 widens", "SELECT CASE WHEN balance > 0 THEN balance ELSE user_id END FROM s10k_accounts"),
        (Pg::Ok,   "COALESCE(int4, int4) unchanged", "SELECT COALESCE(id, id) FROM s10k_accounts"),
        (Pg::Loud, "COALESCE text vs int", "SELECT COALESCE(name, age) FROM s10k_users"),
        (Pg::Loud, "CASE text vs int", "SELECT CASE WHEN age > 0 THEN name ELSE age END FROM s10k_users"),

        // Cast of a resolvable column reference: PG accepts (the cast names the
        // output type) and never introduces NULL, so its nullability follows
        // the inner column — `id::int8` of a NOT NULL primary key is NOT NULL
        // (live: `id::int8 IS NULL` is always false), `age::int8` of a nullable
        // column is nullable. A cast-of-cast (`id::int4::int8`) and a cast
        // through parentheses peel to the same reference. Each accept verdict
        // re-verified live; the per-column nullability is asserted by
        // `engine_cast_nullability_matches_live_pg` (execution-checked).
        (Pg::Ok,   "cast not-null PK", "SELECT id::int8 FROM s10k_users"),
        (Pg::Ok,   "cast nullable col", "SELECT age::int8 FROM s10k_users"),
        (Pg::Ok,   "cast-of-cast not-null PK", "SELECT id::int4::int8 FROM s10k_users"),
        (Pg::Ok,   "cast through parens not-null", "SELECT (id)::int8 FROM s10k_users"),

        // WITH RECURSIVE that never self-references: PG accepts (verified —
        // `WITH RECURSIVE t AS (SELECT id FROM s10k_users) SELECT id FROM t`
        // returns the rows), the `RECURSIVE` keyword being inert, so the engine
        // resolves it as a plain CTE and accepts. A genuine fixpoint stays
        // UnmodelableLoud (above). Re-verified live.
        (Pg::Ok,   "WITH RECURSIVE no self-ref", "WITH RECURSIVE t AS (SELECT id FROM s10k_users) SELECT id FROM t"),
        (Pg::Ok,   "WITH RECURSIVE sibling earlier", "WITH RECURSIVE a AS (SELECT id FROM s10k_users), b AS (SELECT id FROM a) SELECT id FROM b"),

        // Single $N used in two integer-width contexts: PG unifies (verified —
        // `PREPARE ... WHERE age=$1 AND id=$1` reports parameter_types
        // {integer}), so the engine unifies the i32/i64 contexts to the wider
        // i64 and accepts (the wider pick is intentional and bind-safe). An
        // int-vs-text pairing is a genuine conflict PG also rejects (`operator
        // does not exist: text = integer`), so the engine stays loud. Both
        // verdicts re-verified live.
        (Pg::Ok,   "$1 in int4 and int8", "SELECT name FROM s10k_users WHERE age=$1 AND id=$1"),
        (Pg::Ok,   "$1 in int8 and int4 reversed", "SELECT name FROM s10k_users WHERE id=$1 AND age=$1"),
        (Pg::Loud, "$1 in int and text", "SELECT bio FROM s10k_users WHERE age=$1 AND name=$1"),

        // Proactive dialect forms: PIVOT / UNPIVOT parse (engine rejects the
        // non-plain-table FROM item); MATCH_RECOGNIZE / USE INDEX / LIMIT n BY
        // are PG syntax errors (engine loud as a Parse error). All PG-LOUD.
        (Pg::Loud, "PIVOT", "SELECT id FROM s10k_accounts PIVOT (SUM(balance) FOR user_id IN (1, 2)) AS p"),
        (Pg::Loud, "UNPIVOT", "SELECT id FROM s10k_accounts UNPIVOT (val FOR col IN (balance)) AS u"),
        (Pg::Loud, "USE INDEX", "SELECT id FROM s10k_users USE INDEX (idx)"),
        (Pg::Loud, "LIMIT n BY", "SELECT id FROM s10k_users LIMIT 1 BY age"),

        // Subquery-tail scope: a scalar / IN subquery's trailing ORDER BY /
        // LIMIT resolves against the SUBQUERY body's own scope, so a reference
        // to the body's own alias is accepted (PG accepts) and a typo inside the
        // tail is a precise loud error (PG rejects). Both verdicts live-verified.
        (Pg::Ok,   "scalar subq ORDER BY ownalias", "SELECT (SELECT a.balance FROM s10k_accounts a WHERE a.user_id=s10k_users.id ORDER BY a.balance LIMIT 1) AS bal FROM s10k_users"),
        (Pg::Loud, "scalar subq tail typo", "SELECT (SELECT a.balance FROM s10k_accounts a WHERE a.user_id=s10k_users.id ORDER BY a.nope LIMIT 1) FROM s10k_users"),
        (Pg::Ok,   "IN subq ORDER BY ownalias", "SELECT id FROM s10k_users WHERE id IN (SELECT a.user_id FROM s10k_accounts a ORDER BY a.balance LIMIT 5)"),

        // Aggregate / window PLACEMENT: PG rejects an aggregate in WHERE / JOIN
        // ON / GROUP BY, and a window in WHERE / GROUP BY / HAVING; both are
        // allowed only where they belong (HAVING for aggregates; SELECT and
        // top-level ORDER BY for windows). All verdicts live-verified.
        (Pg::Loud, "aggregate in WHERE", "SELECT id FROM s10k_users WHERE SUM(age) > 0"),
        (Pg::Loud, "aggregate in JOIN ON", "SELECT u.id FROM s10k_users u JOIN s10k_accounts a ON COUNT(a.id) > 0"),
        (Pg::Loud, "window in WHERE", "SELECT id FROM s10k_users WHERE ROW_NUMBER() OVER () > 1"),
        (Pg::Loud, "window in HAVING", "SELECT name FROM s10k_users GROUP BY name HAVING ROW_NUMBER() OVER () > 1"),
        (Pg::Ok,   "window in SELECT", "SELECT name, (ROW_NUMBER() OVER ())::int8 FROM s10k_users"),
        (Pg::Ok,   "window in ORDER BY", "SELECT name FROM s10k_users ORDER BY ROW_NUMBER() OVER ()"),
        (Pg::Ok,   "aggregate in HAVING", "SELECT name FROM s10k_users GROUP BY name HAVING COUNT(*) > 0"),

        // GROUP BY coverage with primary-key functional dependency. PG accepts a
        // non-grouped column only when it is functionally determined by a
        // grouped primary key; otherwise it rejects it. All verdicts
        // live-verified, BOTH directions.
        (Pg::Loud, "id, COUNT(*) no GROUP BY", "SELECT id, COUNT(*) FROM s10k_users"),
        (Pg::Ok,   "COUNT(*) only", "SELECT COUNT(*) FROM s10k_users"),

        // ── Redundant parentheses around a projected MIN/MAX of a column are
        // TRANSPARENT: PG treats a parenthesised scalar expression as the same
        // value, so `(MAX(age))` projects the column type exactly like the bare
        // `MAX(age)`. The engine models BOTH identically (Pg::Ok). A min/max
        // COMPOSED into a larger expression (COALESCE, arithmetic, an
        // unmodelled function) has a result type the engine does not model, so
        // it is loud-accurate with OR without a redundant wrapper — PG accepts
        // those forms, the engine deliberately demands a cast (UnmodelableLoud).
        // Each verdict re-verified live via PREPARE, in BOTH directions. ──
        (Pg::Ok,   "max(age) bare", "SELECT max(age) FROM s10k_users"),
        (Pg::Ok,   "(max(age)) paren", "SELECT (max(age)) FROM s10k_users"),
        (Pg::Ok,   "((max(age))) double paren", "SELECT ((max(age))) FROM s10k_users"),
        (Pg::Ok,   "(min(age)) paren", "SELECT (min(age)) FROM s10k_users"),
        (Pg::Ok,   "(max(qualified)) paren", "SELECT (max(s10k_users.age)) FROM s10k_users"),
        (Pg::Ok,   "(min(NOT NULL col)) paren", "SELECT (min(balance)) FROM s10k_accounts"),
        (Pg::Ok,   "(max(age)) AS m", "SELECT (max(age)) AS m FROM s10k_users"),
        (Pg::Ok,   "name,(max(age)) GROUP BY", "SELECT name, (max(age)) FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "(max OVER) windowed paren", "SELECT (max(age) OVER (PARTITION BY name)) FROM s10k_users"),
        (Pg::Ok,   "(count(*)) paren", "SELECT (count(*)) FROM s10k_users"),
        (Pg::UnmodelableLoud, "COALESCE(max,0) composed", "SELECT COALESCE(max(age),0) FROM s10k_users"),
        (Pg::UnmodelableLoud, "(COALESCE(max,0)) composed paren", "SELECT (COALESCE(max(age),0)) FROM s10k_users"),
        (Pg::UnmodelableLoud, "COALESCE((max),0) inner paren", "SELECT COALESCE((max(age)),0) FROM s10k_users"),
        (Pg::UnmodelableLoud, "age+1 arithmetic", "SELECT age+1 FROM s10k_users"),
        (Pg::UnmodelableLoud, "abs(max(age)) wrapping fn", "SELECT abs(max(age)) FROM s10k_users"),
        (Pg::Ok,   "GROUP BY PK covers cols", "SELECT id, name, age FROM s10k_users GROUP BY id"),
        (Pg::Loud, "GROUP BY non-PK uncovered", "SELECT id, name FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "GROUP BY name selects name", "SELECT name, COUNT(*) FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "GROUP BY ordinal", "SELECT name, COUNT(*) FROM s10k_users GROUP BY 1"),
        (Pg::Ok,   "GROUP BY alias", "SELECT name AS n, COUNT(*) FROM s10k_users GROUP BY n"),
        (Pg::Ok,   "COALESCE over grouped", "SELECT COALESCE(name, 'x') FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "composite PK fully grouped", "SELECT a, b, c FROM s10k_comp GROUP BY a, b"),
        (Pg::Loud, "composite PK partly grouped", "SELECT a, b, c FROM s10k_comp GROUP BY a"),
        (Pg::Loud, "MAX + ungrouped column", "SELECT MAX(age)::int4, name FROM s10k_users"),
        (Pg::Loud, "HAVING ungrouped column", "SELECT name FROM s10k_users GROUP BY name HAVING age > 0"),
        (Pg::Loud, "ORDER BY ungrouped column", "SELECT name FROM s10k_users GROUP BY name ORDER BY age"),
        (Pg::Ok,   "ORDER BY aggregate", "SELECT name FROM s10k_users GROUP BY name ORDER BY COUNT(*)"),
        (Pg::Ok,   "ROLLUP column grouped", "SELECT name FROM s10k_users GROUP BY ROLLUP(name)"),
        (Pg::Loud, "ROLLUP leaves other uncovered", "SELECT name, age FROM s10k_users GROUP BY ROLLUP(name)"),
        (Pg::Loud, "JOIN one relation grouped", "SELECT u.name, a.balance FROM s10k_users u JOIN s10k_accounts a ON a.user_id=u.id GROUP BY u.name"),
        (Pg::Loud, "no FD through derived table", "SELECT g.id, g.name FROM (SELECT id, name FROM s10k_users) g GROUP BY g.id"),
        (Pg::Ok,   "derived all columns grouped", "SELECT g.id, g.name FROM (SELECT id, name FROM s10k_users) g GROUP BY g.id, g.name"),
        // A USING / NATURAL merged column is covered by grouping the qualified
        // component its value is DRAWN FROM: the merge expands to the preserved
        // (left) side for INNER / LEFT, the right side for RIGHT, a COALESCE of
        // both for FULL — so PG accepts grouping by exactly that source side and
        // rejects grouping by a non-source side. (Standard fixtures share the PK
        // `id`; users is the left side, accounts the right.) All live-verified.
        (Pg::Ok,   "merged id covered by left source", "SELECT id, COUNT(*) FROM s10k_users JOIN s10k_accounts USING (id) GROUP BY s10k_users.id"),
        (Pg::Loud, "merged id NOT covered by right side (INNER)", "SELECT id, COUNT(*) FROM s10k_users JOIN s10k_accounts USING (id) GROUP BY s10k_accounts.id"),
        (Pg::Ok,   "merged id covered by left source (LEFT)", "SELECT id, COUNT(*) FROM s10k_users LEFT JOIN s10k_accounts USING (id) GROUP BY s10k_users.id"),
        (Pg::Loud, "merged id not covered by right side (LEFT)", "SELECT id, COUNT(*) FROM s10k_users LEFT JOIN s10k_accounts USING (id) GROUP BY s10k_accounts.id"),
        (Pg::Ok,   "merged id covered by right source (RIGHT)", "SELECT id, COUNT(*) FROM s10k_users RIGHT JOIN s10k_accounts USING (id) GROUP BY s10k_accounts.id"),
        (Pg::Loud, "merged id not covered by left side (RIGHT)", "SELECT id, COUNT(*) FROM s10k_users RIGHT JOIN s10k_accounts USING (id) GROUP BY s10k_users.id"),
        (Pg::Loud, "merged id one side under FULL is loud", "SELECT id, COUNT(*) FROM s10k_users FULL JOIN s10k_accounts USING (id) GROUP BY s10k_users.id"),
        (Pg::Ok,   "merged id both sides under FULL", "SELECT id, COUNT(*) FROM s10k_users FULL JOIN s10k_accounts USING (id) GROUP BY s10k_users.id, s10k_accounts.id"),
        (Pg::Ok,   "merged id grouped as merge expr", "SELECT id, COUNT(*) FROM s10k_users FULL JOIN s10k_accounts USING (id) GROUP BY id"),
        (Pg::Ok,   "merged id in ORDER BY by left source", "SELECT COUNT(*) FROM s10k_users JOIN s10k_accounts USING (id) GROUP BY s10k_users.id ORDER BY id"),
        (Pg::Loud, "merged id in ORDER BY one side FULL", "SELECT COUNT(*) FROM s10k_users FULL JOIN s10k_accounts USING (id) GROUP BY s10k_users.id ORDER BY id"),
        (Pg::Ok,   "NATURAL merged id covered by left", "SELECT id, COUNT(*) FROM s10k_users NATURAL JOIN s10k_accounts GROUP BY s10k_users.id"),
        (Pg::Loud, "NATURAL FULL merged id one side", "SELECT id, COUNT(*) FROM s10k_users NATURAL FULL JOIN s10k_accounts GROUP BY s10k_users.id"),
        // A HAVING clause ALWAYS makes the query an aggregate query (even with
        // no aggregate inside it), so a bare selected column is uncovered;
        // verified live both directions.
        (Pg::Loud, "HAVING alone bare column", "SELECT name FROM s10k_users HAVING name > 'a'"),
        (Pg::Ok,   "aggregate-only HAVING", "SELECT COUNT(*) FROM s10k_users HAVING COUNT(*) > 0"),

        // Aggregate ARGUMENTS may reference ungrouped columns (the column is
        // inside the aggregate, so it need not be grouped); DISTINCT / FILTER
        // forms likewise. A WINDOW over a grouped column in an aggregate query
        // is fine; over an UNGROUPED column it is loud. All live-verified.
        (Pg::Ok,   "agg arg ungrouped", "SELECT name, SUM(age)::int8 FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "agg DISTINCT ungrouped", "SELECT name, COUNT(DISTINCT age) FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "agg FILTER ungrouped", "SELECT name, COUNT(*) FILTER (WHERE age > 0) FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "window over grouped in agg query", "SELECT name, (ROW_NUMBER() OVER (ORDER BY name))::int8 FROM s10k_users GROUP BY name"),
        (Pg::Loud, "window over ungrouped in agg query", "SELECT name, (ROW_NUMBER() OVER (ORDER BY age))::int8 FROM s10k_users GROUP BY name"),
        // An uncorrelated subquery (even one with an aggregate) in an aggregate
        // query's SELECT / WHERE is its own level: it does not break coverage.
        (Pg::Ok,   "uncorrelated subq in agg select", "SELECT name, (SELECT COUNT(*) FROM s10k_accounts) FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "subq aggregate in WHERE of agg query", "SELECT name FROM s10k_users WHERE age > (SELECT AVG(age)::float8 FROM s10k_users) GROUP BY name"),
        // Outer-join nullable-side primary key still functionally determines its
        // columns (PostgreSQL accepts), so grouping its PK covers them.
        (Pg::Ok,   "outer-join nullable-side PK FD", "SELECT a.balance FROM s10k_users u LEFT JOIN s10k_accounts a ON a.user_id=u.id GROUP BY a.id"),
        (Pg::Ok,   "outer-join both PKs grouped", "SELECT u.name, a.balance FROM s10k_users u LEFT JOIN s10k_accounts a ON a.user_id=u.id GROUP BY u.id, a.id"),

        // Grouping-set functional dependency: the primary key grants a
        // functional dependency only when it is grouped in EVERY generated
        // grouping set (the always-grouped set). ROLLUP/CUBE generate the
        // grand-total set that groups none of their members, so they grant no
        // FD; a GROUPING SETS whose listed sets all include the whole PK does.
        // A column listed inside a grouping construct is still DIRECTLY covered.
        // All verdicts live-verified, BOTH directions.
        (Pg::Loud, "ROLLUP(a,b) c uncovered", "SELECT a,b,c FROM s10k_comp GROUP BY ROLLUP(a,b)"),
        (Pg::Loud, "CUBE(a,b) c uncovered", "SELECT a,b,c FROM s10k_comp GROUP BY CUBE(a,b)"),
        (Pg::Ok,   "GROUPING SETS((a,b)) c covered", "SELECT a,b,c FROM s10k_comp GROUP BY GROUPING SETS((a,b))"),
        (Pg::Loud, "GROUPING SETS((a,b),(a)) c uncovered", "SELECT a,b,c FROM s10k_comp GROUP BY GROUPING SETS((a,b),(a))"),
        (Pg::Ok,   "id PK plain beside ROLLUP", "SELECT id, bio FROM s10k_users GROUP BY id, ROLLUP(name)"),
        (Pg::Ok,   "c inside ROLLUP directly covered", "SELECT a,b,c FROM s10k_comp GROUP BY a, b, ROLLUP(c)"),
        (Pg::Loud, "GROUPING SETS((id),()) PK not always grouped", "SELECT id, bio FROM s10k_users GROUP BY GROUPING SETS((id),())"),

        // Subquery LIMIT/OFFSET/FETCH parameter: a `$N` in a subquery / derived
        // table / CTE body's own count clause is a bigint, exactly as on the
        // top-level query (PG parameter_types {bigint}). All Ok, live-verified.
        (Pg::Ok,   "derived LIMIT $1", "SELECT id FROM (SELECT id FROM s10k_users LIMIT $1) q"),
        (Pg::Ok,   "derived OFFSET $1", "SELECT id FROM (SELECT id FROM s10k_users OFFSET $1) q"),
        (Pg::Ok,   "derived FETCH FIRST $1", "SELECT id FROM (SELECT id FROM s10k_users FETCH FIRST $1 ROWS ONLY) q"),
        (Pg::Ok,   "IN subquery LIMIT $1", "SELECT id FROM s10k_users WHERE id IN (SELECT user_id FROM s10k_accounts LIMIT $1)"),
        (Pg::Ok,   "CTE body LIMIT $1", "WITH t AS (SELECT id FROM s10k_users LIMIT $1) SELECT id FROM t"),

        // Correlated-ungrouped-outer subquery: a correlated reference inside a
        // subquery to an UNGROUPED column of the enclosing aggregate query is
        // rejected ("subquery uses ungrouped column ... from outer query"); a
        // grouped / PK-determined / uncorrelated reference is accepted, and a
        // pure-outer aggregate over the outer column (an aggregate of the
        // enclosing level) is accepted. All verdicts live-verified, BOTH
        // directions.
        (Pg::Loud, "correlated ungrouped outer", "SELECT name,(SELECT COUNT(*) FROM s10k_accounts a WHERE a.balance=s10k_users.age) FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "correlated grouped outer", "SELECT name,(SELECT COUNT(*) FROM s10k_accounts a WHERE a.balance=LENGTH(s10k_users.name)) FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "correlated PK-determined outer", "SELECT id,(SELECT COUNT(*) FROM s10k_accounts a WHERE a.balance=s10k_users.age) FROM s10k_users GROUP BY id"),
        (Pg::Ok,   "uncorrelated subquery agg query", "SELECT name,(SELECT COUNT(*) FROM s10k_accounts a WHERE a.balance=0) FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "pure-outer aggregate in subquery", "SELECT name,(SELECT SUM(s10k_users.age)::int8 FROM s10k_accounts a) FROM s10k_users GROUP BY name"),
        (Pg::Loud, "inner aggregate mixes ungrouped outer", "SELECT name,(SELECT SUM(a.balance + s10k_users.age)::int8 FROM s10k_accounts a) FROM s10k_users GROUP BY name"),
        (Pg::Loud, "EXISTS correlated ungrouped outer", "SELECT name FROM s10k_users GROUP BY name HAVING EXISTS (SELECT 1 FROM s10k_accounts a WHERE a.balance=s10k_users.age)"),

        // Correlated outer reference inside an INNER aggregate subquery: when the
        // inner subquery is itself an aggregate (its OWN GROUP BY / HAVING), a
        // reference to an OUTER relation is a per-outer-row CONSTANT PG exempts
        // from the inner query's grouping — exactly as it exempts a literal — so
        // it need not appear in the inner GROUP BY. Only a reference to the inner
        // query's OWN ungrouped relation is loud. The own-relation ungrouped form
        // (a top-level aggregate query whose HAVING names an ungrouped own column)
        // stays loud. All verdicts live-verified via PREPARE, BOTH directions.
        (Pg::Ok,   "EXISTS inner-agg HAVING outer ref", "SELECT id FROM s10k_users u WHERE EXISTS (SELECT 1 FROM s10k_accounts a GROUP BY a.user_id HAVING a.user_id = u.id)"),
        (Pg::Ok,   "EXISTS inner-agg projection outer ref", "SELECT u.name FROM s10k_users u WHERE EXISTS (SELECT u.name FROM s10k_accounts a GROUP BY a.user_id)"),
        (Pg::Ok,   "IN inner-agg HAVING outer ref", "SELECT id FROM s10k_users u WHERE u.id IN (SELECT a.user_id FROM s10k_accounts a GROUP BY a.user_id HAVING SUM(a.balance)::int8 > u.age)"),
        (Pg::Ok,   "scalar inner-agg HAVING outer ref", "SELECT u.id,(SELECT SUM(a.balance)::int8 FROM s10k_accounts a GROUP BY a.user_id HAVING a.user_id=u.id LIMIT 1) FROM s10k_users u"),
        (Pg::Loud, "own ungrouped relation in HAVING", "SELECT u.age,COUNT(*) FROM s10k_users u GROUP BY u.age HAVING u.id>0"),
        (Pg::Loud, "inner-agg own ungrouped relation in HAVING", "SELECT id FROM s10k_users u WHERE EXISTS (SELECT 1 FROM s10k_accounts a GROUP BY a.user_id HAVING a.balance = u.id)"),

        // Nested aggregates: an aggregate whose argument transitively contains
        // another non-windowed aggregate is rejected ("aggregate function calls
        // cannot be nested"); a WINDOW outer over an aggregate, and an aggregate
        // over a subquery with its own aggregate, are NOT nesting. All verdicts
        // live-verified, BOTH directions.
        (Pg::Loud, "SUM(COUNT(*)) nested", "SELECT SUM(COUNT(*))::int8 FROM s10k_users"),
        (Pg::Loud, "COUNT(SUM(age)) nested", "SELECT COUNT(SUM(age)) FROM s10k_users"),
        (Pg::Loud, "AVG(SUM(age)) nested", "SELECT AVG(SUM(age))::float8 FROM s10k_users"),
        (Pg::Loud, "MAX(AVG(age)) nested", "SELECT MAX(AVG(age))::float8 FROM s10k_users"),
        (Pg::Loud, "COUNT(COUNT(*)) nested", "SELECT COUNT(COUNT(*)) FROM s10k_users"),
        (Pg::Loud, "BOOL_AND(COUNT(*)>0) nested", "SELECT BOOL_AND(COUNT(*)>0) FROM s10k_users"),
        (Pg::Ok,   "SUM(COUNT(*)) OVER () windowed", "SELECT name, (SUM(COUNT(*)) OVER ())::int8 FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "agg over subquery agg", "SELECT SUM((SELECT COUNT(*) FROM s10k_accounts))::int8 FROM s10k_users"),

        // Window inside an aggregate ARGUMENT: PG rejects ("aggregate function
        // calls cannot contain window function calls") — a window runs after
        // grouping, so it cannot sit inside an aggregate's per-row argument. A
        // WINDOW wrapping an aggregate (`SUM(SUM(age)) OVER ()`), and an
        // aggregate over a SUBQUERY that windows, are NOT this error. All
        // verdicts live-verified, BOTH directions.
        (Pg::Loud, "SUM(ROW_NUMBER() OVER ())", "SELECT SUM(ROW_NUMBER() OVER ())::int8 FROM s10k_users"),
        (Pg::Loud, "MAX(RANK() OVER (ORDER BY age))", "SELECT MAX(RANK() OVER (ORDER BY age))::int8 FROM s10k_users"),
        (Pg::Loud, "COUNT(SUM(age) OVER ())", "SELECT COUNT(SUM(age) OVER ()) FROM s10k_users"),
        (Pg::Loud, "SUM(abs(ROW_NUMBER() OVER ())) nested scalar", "SELECT SUM(abs(ROW_NUMBER() OVER ()))::int8 FROM s10k_users"),
        (Pg::Ok,   "SUM(SUM(age)) OVER () windowed-outer", "SELECT name, (SUM(SUM(age)) OVER ())::int8 FROM s10k_users GROUP BY name"),
        (Pg::Ok,   "agg over subquery window", "SELECT SUM((SELECT ROW_NUMBER() OVER () FROM s10k_accounts LIMIT 1))::int8 FROM s10k_users"),

        // Aggregate in the top-level ORDER BY or in DISTINCT ON makes the WHOLE
        // query an aggregate query, so a bare projected column must then be
        // grouped — PG rejects an ungrouped one, accepts a grouped one. An
        // aggregate-only projection ordered by another aggregate stays Ok. An
        // ORDER BY aggregate inside a subquery/derived body is owned by that
        // level and likewise made-aggregate there. All verdicts live-verified,
        // BOTH directions.
        (Pg::Loud, "ORDER BY COUNT(*) bare col", "SELECT age FROM s10k_users ORDER BY COUNT(*)"),
        (Pg::Loud, "DISTINCT ON (COUNT(*)) bare col", "SELECT DISTINCT ON (COUNT(*)) age FROM s10k_users"),
        (Pg::Ok,   "COUNT(*) ORDER BY SUM(age)", "SELECT COUNT(*) FROM s10k_users ORDER BY SUM(age)"),
        (Pg::Ok,   "GROUP BY name ORDER BY COUNT(*)", "SELECT name FROM s10k_users GROUP BY name ORDER BY COUNT(*)"),
        (Pg::Ok,   "DISTINCT ON (COUNT(*)) name GROUP BY name", "SELECT DISTINCT ON (COUNT(*)) name FROM s10k_users GROUP BY name"),
        (Pg::Loud, "DISTINCT ON (age) name GROUP BY name ungrouped distincton", "SELECT DISTINCT ON (age) name FROM s10k_users GROUP BY name"),
        (Pg::Loud, "ORDER BY agg in derived body", "SELECT q.age FROM (SELECT age FROM s10k_users ORDER BY COUNT(*)) q"),
        (Pg::Loud, "ORDER BY agg in IN-subquery body", "SELECT id FROM s10k_users WHERE id IN (SELECT user_id FROM s10k_accounts ORDER BY COUNT(*))"),
        (Pg::Loud, "ORDER BY SUM(COUNT(*)) nested", "SELECT name FROM s10k_users GROUP BY name ORDER BY SUM(COUNT(*))"),

        // Aggregate / window in LIMIT / OFFSET / FETCH count: PG forbids BOTH
        // ("aggregate functions are not allowed in LIMIT", "window functions are
        // not allowed in OFFSET", …). A subquery in LIMIT (its aggregate owned by
        // that level) and a `$N` / constant count stay Ok. All live-verified.
        (Pg::Loud, "LIMIT SUM(age)", "SELECT id FROM s10k_users LIMIT SUM(age)"),
        (Pg::Loud, "OFFSET MAX(age)", "SELECT id FROM s10k_users OFFSET MAX(age)"),
        (Pg::Loud, "LIMIT ROW_NUMBER() OVER ()", "SELECT id FROM s10k_users LIMIT ROW_NUMBER() OVER ()"),
        (Pg::Loud, "OFFSET ROW_NUMBER() OVER ()", "SELECT id FROM s10k_users OFFSET ROW_NUMBER() OVER ()"),
        (Pg::Ok,   "LIMIT subquery agg", "SELECT id FROM s10k_users LIMIT (SELECT COUNT(*) FROM s10k_accounts)"),
        (Pg::Ok,   "LIMIT constant", "SELECT id FROM s10k_users LIMIT 5"),

        // WITHIN GROUP / FILTER targeting: WITHIN GROUP is valid ONLY on an
        // ordered-set / hypothetical-set aggregate; on COUNT(*)/MAX it is loud
        // ("<f> is not an ordered-set aggregate"). FILTER is valid ONLY on an
        // aggregate; on a scalar function it is loud ("FILTER specified, but <f>
        // is not an aggregate function"). MAX(...) FILTER stays Ok. All verdicts
        // live-verified, BOTH directions.
        (Pg::Loud, "COUNT(*) WITHIN GROUP", "SELECT COUNT(*) WITHIN GROUP (ORDER BY age) FROM s10k_users"),
        (Pg::Loud, "MAX(age) WITHIN GROUP", "SELECT MAX(age) WITHIN GROUP (ORDER BY age) FROM s10k_users"),
        (Pg::Loud, "length(name) FILTER not aggregate", "SELECT length(name) FILTER (WHERE age>0) FROM s10k_users"),
        (Pg::Ok,   "MAX(age) FILTER", "SELECT MAX(age) FILTER (WHERE age>0) FROM s10k_users"),

        // Ordered-set / hypothetical-set aggregates WITH a valid `WITHIN GROUP`:
        // PG ACCEPTS these (the WITHIN GROUP clause is well-placed), and the
        // engine PERMITS the clause too — it does NOT raise the
        // not-ordered-set error. The verdict now splits on the OUTPUT type:
        //   * When the aggregate is wrapped in an explicit `::float8` cast, the
        //     cast PINS the output column type to `float8` — a type the catalog
        //     now models (`f64`). The inner clause is still fully validated
        //     (columns resolved, WITHIN GROUP placement checked), and the cast
        //     is the documented "materialise the unmodelable return with an
        //     explicit cast" escape hatch — so PG and the engine now AGREE (Ok).
        //   * The BARE forms (`percentile_disc`/`mode` -> the ORDER BY column's
        //     type, `rank`/`dense_rank` -> bigint) carry a return type the
        //     engine does not infer for these names, so with no cast to pin it
        //     they stay engine-loud at the unsupported-return boundary
        //     (a focused unit test asserts this is NOT `WithinGroupNotOrderedSet`).
        (Pg::Ok, "percentile_cont WITHIN GROUP ::float8", "SELECT (percentile_cont(0.5) WITHIN GROUP (ORDER BY age))::float8 FROM s10k_users"),
        (Pg::UnmodelableLoud, "percentile_disc WITHIN GROUP", "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY age) FROM s10k_users"),
        (Pg::UnmodelableLoud, "mode WITHIN GROUP", "SELECT mode() WITHIN GROUP (ORDER BY age) FROM s10k_users"),
        (Pg::UnmodelableLoud, "rank hypothetical WITHIN GROUP", "SELECT rank(5) WITHIN GROUP (ORDER BY age) FROM s10k_users"),
        (Pg::UnmodelableLoud, "dense_rank hypothetical WITHIN GROUP", "SELECT dense_rank(5) WITHIN GROUP (ORDER BY age) FROM s10k_users"),
        (Pg::Ok, "percent_rank hypothetical WITHIN GROUP ::float8", "SELECT (percent_rank(5) WITHIN GROUP (ORDER BY age))::float8 FROM s10k_users"),
        (Pg::Ok, "cume_dist hypothetical WITHIN GROUP ::float8", "SELECT (cume_dist(5) WITHIN GROUP (ORDER BY age))::float8 FROM s10k_users"),

        // No-column-list INSERT $N: PG types each position from declaration
        // order, but the catalog has no positional order, so the engine is
        // loud-accurate (the unordered-catalog/never-guess boundary). An
        // explicit column list (typed by name) or a cast resolves it.
        (Pg::UnmodelableLoud, "INSERT no col list $N", "INSERT INTO s10k_users VALUES ($1,$2,$3,$4) RETURNING id"),
        (Pg::Ok,   "INSERT no col list cast", "INSERT INTO s10k_users VALUES ($1::int8,$2::text,$3::text,$4::int4) RETURNING id"),
        (Pg::Ok,   "INSERT explicit col list $N", "INSERT INTO s10k_users (id,name,bio,age) VALUES ($1,$2,$3,$4) RETURNING id"),

        // Aggregate / window in a DML EXPRESSION POSITION: a data-modifying
        // statement summarises no group, so PG forbids both in an UPDATE / ON
        // CONFLICT DO UPDATE SET value, an INSERT VALUES cell, a DML WHERE, an
        // ON CONFLICT WHERE, and a RETURNING projection ("... not allowed in
        // UPDATE / VALUES / WHERE / RETURNING"). A subquery in those positions
        // (its aggregate / window owned by that level) and a $N / constant stay
        // Ok. All verdicts live-verified via PREPARE, BOTH directions.
        (Pg::Loud, "UPDATE SET = SUM(age)", "UPDATE s10k_users SET age = SUM(age) WHERE id=$1 RETURNING id"),
        (Pg::Loud, "UPDATE SET = ROW_NUMBER OVER", "UPDATE s10k_users SET age=(ROW_NUMBER() OVER ())::int4 WHERE id=$1 RETURNING id"),
        (Pg::Loud, "UPDATE tuple SET cell SUM", "UPDATE s10k_users SET (name,age)=('x', SUM(age)) WHERE id=$1 RETURNING id"),
        (Pg::Loud, "UPDATE tuple SET ROW cell SUM", "UPDATE s10k_users SET (name,age)=ROW('x', SUM(age)) WHERE id=$1 RETURNING id"),
        (Pg::Loud, "INSERT VALUES cell COUNT", "INSERT INTO s10k_users (id,age) VALUES ($1, COUNT(*)) RETURNING id"),
        (Pg::Loud, "INSERT multirow VALUES cell COUNT", "INSERT INTO s10k_users (id,age) VALUES ($1, 1),($2, COUNT(*)) RETURNING id"),
        (Pg::Loud, "INSERT VALUES cell ROW_NUMBER OVER", "INSERT INTO s10k_users (id,age) VALUES ($1, (ROW_NUMBER() OVER ())::int4) RETURNING id"),
        (Pg::Loud, "UPDATE WHERE SUM", "UPDATE s10k_users SET age=1 WHERE age > SUM(age) RETURNING id"),
        (Pg::Loud, "DELETE WHERE ROW_NUMBER OVER", "DELETE FROM s10k_users WHERE ROW_NUMBER() OVER () > 0 RETURNING id"),
        (Pg::Loud, "OC DO UPDATE SET SUM", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET age = SUM(s10k_users.age) RETURNING id"),
        (Pg::Loud, "OC WHERE SUM", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET age=1 WHERE s10k_users.age > SUM(s10k_users.age) RETURNING id"),
        (Pg::Loud, "INSERT RETURNING SUM", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) RETURNING SUM(age)::int8"),
        (Pg::Loud, "UPDATE RETURNING SUM", "UPDATE s10k_users SET age=1 WHERE id=$1 RETURNING SUM(age)::int8"),
        (Pg::Loud, "DELETE RETURNING ROW_NUMBER OVER", "DELETE FROM s10k_users WHERE id=$1 RETURNING (ROW_NUMBER() OVER ())::int8"),
        (Pg::Loud, "UPDATE RETURNING ROW_NUMBER OVER", "UPDATE s10k_users SET age=1 WHERE id=$1 RETURNING (ROW_NUMBER() OVER ())::int8"),
        // DML expression-position OK controls.
        (Pg::Ok,   "UPDATE SET col=$1 WHERE id=$2", "UPDATE s10k_users SET age=$1 WHERE id=$2 RETURNING id"),
        (Pg::Ok,   "INSERT VALUES ($1,$2)", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) RETURNING id"),
        (Pg::Ok,   "UPDATE RETURNING col", "UPDATE s10k_users SET age=1 WHERE id=$1 RETURNING age"),
        (Pg::Ok,   "DELETE WHERE col=$1", "DELETE FROM s10k_users WHERE id=$1 RETURNING id"),
        (Pg::Ok,   "UPDATE SET = subquery agg", "UPDATE s10k_users SET age=(SELECT COUNT(*)::int4 FROM s10k_accounts) WHERE id=$1 RETURNING id"),
        (Pg::Ok,   "INSERT VALUES cell subquery agg", "INSERT INTO s10k_users (id,age) VALUES ($1, (SELECT COUNT(*)::int4 FROM s10k_accounts)) RETURNING id"),
        (Pg::Ok,   "UPDATE WHERE subquery agg", "UPDATE s10k_users SET age=1 WHERE age > (SELECT AVG(balance)::int4 FROM s10k_accounts) RETURNING id"),
        (Pg::Ok,   "UPDATE RETURNING subquery agg", "UPDATE s10k_users SET age=1 WHERE id=$1 RETURNING (SELECT COUNT(*)::int8 FROM s10k_accounts)"),
        (Pg::Ok,   "DELETE WHERE subquery window", "DELETE FROM s10k_users WHERE id IN (SELECT (ROW_NUMBER() OVER ())::int8 FROM s10k_accounts) RETURNING id"),
        (Pg::Ok,   "UPDATE tuple SET = subquery agg-in-projection", "UPDATE s10k_users SET (name,age)=(SELECT name, COUNT(*)::int FROM s10k_accounts GROUP BY name LIMIT 1) WHERE id=$1 RETURNING id"),

        // Window inside ANOTHER window's OVER definition: a window definition
        // partitions / orders the base rows (not yet reduced by any window), so
        // PG forbids a window in the PARTITION BY / ORDER BY ("window functions
        // are not allowed in window definitions"). A window inside a SUBQUERY in
        // the OVER spec is its own level, and a plain window in a legal
        // projection position is fine. All verdicts live-verified, BOTH
        // directions.
        (Pg::Loud, "window in OVER ORDER BY", "SELECT (ROW_NUMBER() OVER (ORDER BY (SUM(age) OVER ())))::int8 FROM s10k_users"),
        (Pg::Loud, "window in OVER PARTITION BY", "SELECT (SUM(age) OVER (PARTITION BY (RANK() OVER ())))::int8 FROM s10k_users"),
        (Pg::Loud, "window in scalar in OVER ORDER BY", "SELECT (ROW_NUMBER() OVER (ORDER BY abs((SUM(age) OVER ())::int4)))::int8 FROM s10k_users"),
        (Pg::Ok,   "plain window ORDER BY col", "SELECT (ROW_NUMBER() OVER (ORDER BY age))::int8 FROM s10k_users"),
        (Pg::Ok,   "plain window PARTITION + ORDER", "SELECT (RANK() OVER (PARTITION BY name ORDER BY age))::int8 FROM s10k_users"),
        (Pg::Ok,   "window inside subquery in OVER spec", "SELECT (ROW_NUMBER() OVER (ORDER BY (SELECT (ROW_NUMBER() OVER ())::int8 FROM s10k_accounts LIMIT 1)))::int8 FROM s10k_users"),

        // Window inside a NAMED window's definition (`WINDOW w AS (...)`): the
        // same rule as the inline OVER spec — a window definition partitions /
        // orders the base rows, so PG forbids a window in its PARTITION BY /
        // ORDER BY ("window functions are not allowed in window definitions"). A
        // window inside a SUBQUERY in the definition is its own level, and an
        // AGGREGATE (not a window) in the definition is fine. BOTH directions.
        (Pg::Loud, "named-window PARTITION BY window", "SELECT (SUM(age) OVER w)::int8 FROM s10k_users WINDOW w AS (PARTITION BY (RANK() OVER ()))"),
        (Pg::Loud, "named-window ORDER BY window", "SELECT (SUM(age) OVER w)::int8 FROM s10k_users WINDOW w AS (ORDER BY (RANK() OVER ()))"),
        (Pg::Ok,   "named-window window-in-subquery def", "SELECT (SUM(age) OVER w)::int8 FROM s10k_users WINDOW w AS (PARTITION BY (SELECT (RANK() OVER ())::int8 FROM s10k_accounts LIMIT 1))"),
        (Pg::Ok,   "named-window aggregate in def (grouped)", "SELECT (age)::int8 FROM s10k_users GROUP BY age WINDOW w AS (PARTITION BY COUNT(age))"),

        // Aggregate INSIDE a named WINDOW definition flips the whole query to
        // aggregate-mode (PG: a non-aggregated selected column must then be
        // grouped). And under aggregate-mode the WINDOW definition's own
        // PARTITION BY / ORDER BY columns must be grouped or aggregated too. An
        // aggregate (`COUNT(age)`) in the definition is itself covered; a window
        // aggregate (`SUM(age) OVER w`) does NOT make the query aggregate, so
        // its definition's `age` is fine. BOTH directions, live-verified.
        (Pg::Loud, "agg in WINDOW def ORDER BY -> ungrouped id loud", "SELECT id FROM s10k_users WINDOW w AS (ORDER BY SUM(age))"),
        (Pg::Loud, "agg in WINDOW def PARTITION BY -> ungrouped id loud", "SELECT id FROM s10k_users WINDOW w AS (PARTITION BY COUNT(age))"),
        (Pg::Ok,   "agg in WINDOW def + grouped age", "SELECT age FROM s10k_users GROUP BY age WINDOW w AS (ORDER BY SUM(age))"),
        (Pg::Loud, "aggregate query: ungrouped col in WINDOW def loud", "SELECT (SUM(age))::int8 FROM s10k_users WINDOW w AS (PARTITION BY id)"),
        (Pg::Ok,   "aggregate query: grouped col in WINDOW def ok", "SELECT (COUNT(*))::int8 FROM s10k_users GROUP BY age WINDOW w AS (ORDER BY age)"),
        (Pg::Ok,   "aggregate query: aggregate in WINDOW def ok", "SELECT (SUM(age))::int8 FROM s10k_users WINDOW w AS (ORDER BY COUNT(age))"),
        (Pg::Ok,   "windowed agg over w does not make query aggregate", "SELECT (SUM(age) OVER w)::int8 FROM s10k_users WINDOW w AS (ORDER BY age)"),

        // Window-NAME reference existence: `OVER w`, an inline `OVER (base ...)`
        // base, and a WINDOW definition's base must name a declared window. A
        // use site may name any declared label regardless of WINDOW-list order;
        // a definition's base may name only a label declared EARLIER in the list
        // (a forward / self reference points at no declared window). The
        // top-level ORDER BY's window references resolve against the body
        // SELECT's labels, and a subquery declares its own labels. BOTH
        // directions, live-verified ("window X does not exist").
        (Pg::Loud, "OVER undefined window", "SELECT (ROW_NUMBER() OVER undefined_win)::int8 FROM s10k_users"),
        (Pg::Loud, "OVER mismatched window (only v defined)", "SELECT (ROW_NUMBER() OVER w)::int8 FROM s10k_users WINDOW v AS (ORDER BY age)"),
        (Pg::Loud, "OVER nope window", "SELECT (ROW_NUMBER() OVER nope)::int8 FROM s10k_users"),
        (Pg::Ok,   "OVER defined window", "SELECT (ROW_NUMBER() OVER w)::int8 FROM s10k_users WINDOW w AS (ORDER BY age)"),
        (Pg::Loud, "inline OVER undefined base window", "SELECT (ROW_NUMBER() OVER (undefbase ORDER BY age))::int8 FROM s10k_users"),
        (Pg::Ok,   "inline OVER defined base window", "SELECT (ROW_NUMBER() OVER (w0 ORDER BY age))::int8 FROM s10k_users WINDOW w0 AS (PARTITION BY name)"),
        (Pg::Loud, "WINDOW def forward base reference", "SELECT (ROW_NUMBER() OVER w)::int8 FROM s10k_users WINDOW w AS (w0 ORDER BY age), w0 AS (PARTITION BY name)"),
        (Pg::Ok,   "WINDOW def backward base reference", "SELECT (ROW_NUMBER() OVER w)::int8 FROM s10k_users WINDOW w0 AS (PARTITION BY name), w AS (w0 ORDER BY age)"),
        (Pg::Ok,   "OVER window used before declared in list", "SELECT (ROW_NUMBER() OVER w)::int8 FROM s10k_users WINDOW v AS (ORDER BY age), w AS (PARTITION BY name)"),
        (Pg::Loud, "ORDER BY OVER undefined window", "SELECT id FROM s10k_users WINDOW w AS (ORDER BY age) ORDER BY ROW_NUMBER() OVER nope"),
        (Pg::Ok,   "ORDER BY OVER defined window", "SELECT id FROM s10k_users WINDOW w AS (ORDER BY age) ORDER BY ROW_NUMBER() OVER w"),
        (Pg::Ok,   "subquery-local window label", "SELECT (SELECT (ROW_NUMBER() OVER sw)::int8 FROM s10k_accounts WINDOW sw AS (ORDER BY balance) LIMIT 1) AS x FROM s10k_users"),
        (Pg::Loud, "subquery undefined window label", "SELECT (SELECT (ROW_NUMBER() OVER nope)::int8 FROM s10k_accounts LIMIT 1) AS x FROM s10k_users"),
        (Pg::Loud, "DISTINCT ON undefined window", "SELECT DISTINCT ON ((ROW_NUMBER() OVER nope)::int8) id FROM s10k_users"),
        (Pg::Ok,   "DISTINCT ON defined window", "SELECT DISTINCT ON ((ROW_NUMBER() OVER w)::int8) id FROM s10k_users WINDOW w AS (ORDER BY age)"),

        // INSERT ... SELECT source parameters: the source is a standalone query,
        // so a `::cast` on a projected `$N` types it, a `$N` in the inner WHERE
        // types from its column, and a genuinely-uncast bare projected `$N`
        // stays loud (the never-guess posture; PG types it from the target but
        // the engine declines without a cast). The source's trailing ORDER BY
        // typo is loud. BOTH directions.
        (Pg::Ok,   "insert-select cast projection", "INSERT INTO s10k_users (id,age) SELECT $1::int8, $2::int4 FROM s10k_accounts RETURNING id"),
        (Pg::Ok,   "insert-select inner WHERE param", "INSERT INTO s10k_users (id,age) SELECT user_id, balance FROM s10k_accounts WHERE balance = $1 RETURNING id"),
        (Pg::Ok,   "insert-select mixed cast + WHERE", "INSERT INTO s10k_users (id,age) SELECT $1::int8, balance FROM s10k_accounts WHERE balance = $2 RETURNING id"),
        (Pg::Loud, "insert-select source ORDER BY typo", "INSERT INTO s10k_users (id) SELECT user_id FROM s10k_accounts ORDER BY typo RETURNING id"),
        // The source's trailing ORDER BY resolves against the source SELECT's
        // OWN FROM scope: a non-output FROM column, an expression over it, or a
        // qualified reference is valid (PG accepts), while a typo names the
        // source relation. BOTH directions, live-verified.
        (Pg::Ok,   "insert-select source ORDER BY non-output FROM col", "INSERT INTO s10k_users (id) SELECT user_id FROM s10k_accounts ORDER BY balance RETURNING id"),
        (Pg::Ok,   "insert-select source ORDER BY expression", "INSERT INTO s10k_users (id) SELECT user_id FROM s10k_accounts ORDER BY balance + 1 RETURNING id"),
        (Pg::Ok,   "insert-select source ORDER BY qualified col", "INSERT INTO s10k_users (id) SELECT user_id FROM s10k_accounts a ORDER BY a.balance RETURNING id"),
        (Pg::Ok,   "insert-select source ORDER BY output alias", "INSERT INTO s10k_users (id) SELECT user_id AS x FROM s10k_accounts ORDER BY x RETURNING id"),

        // Set-operation trailing ORDER BY / LIMIT / OFFSET / FETCH: the ORDER BY
        // of a UNION / INTERSECT / EXCEPT may name ONLY an output column or an
        // in-range positional ordinal — never a bare non-output input column, an
        // arbitrary expression, an aggregate / window, or a qualified reference.
        // LIMIT / OFFSET / FETCH forbid an aggregate / window and a bogus column,
        // but accept a param / constant / subquery. The output name follows the
        // LEFT arm's projection alias. The set-op restriction does NOT leak to a
        // plain SELECT ORDER BY (which legally allows an expression over the FROM
        // scope). All verdicts live-verified, BOTH directions.
        (Pg::Ok,   "setop ORDER BY output name", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts ORDER BY id"),
        (Pg::Ok,   "setop ORDER BY ordinal 1", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts ORDER BY 1"),
        (Pg::Loud, "setop ORDER BY typo", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts ORDER BY typo"),
        (Pg::Loud, "setop ORDER BY non-output column", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts ORDER BY age"),
        (Pg::Loud, "setop ORDER BY ordinal out of range", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts ORDER BY 5"),
        (Pg::Loud, "setop ORDER BY expression", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts ORDER BY id + 1"),
        (Pg::Loud, "setop ORDER BY aggregate", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts ORDER BY COUNT(*)"),
        (Pg::Loud, "setop ORDER BY qualified ref", "SELECT id FROM s10k_users u UNION SELECT id FROM s10k_accounts ORDER BY u.id"),
        (Pg::Ok,   "setop ORDER BY left-arm alias", "SELECT id AS x FROM s10k_users UNION SELECT id FROM s10k_accounts ORDER BY x"),
        (Pg::Loud, "setop ORDER BY original name post-alias", "SELECT id AS x FROM s10k_users UNION SELECT id FROM s10k_accounts ORDER BY id"),
        (Pg::Loud, "setop LIMIT aggregate", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts LIMIT SUM(id)"),
        (Pg::Loud, "setop OFFSET aggregate", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts OFFSET COUNT(*)"),
        (Pg::Ok,   "setop LIMIT param", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts LIMIT $1"),
        (Pg::Ok,   "setop LIMIT constant", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts LIMIT 5"),
        (Pg::Ok,   "setop LIMIT subquery", "SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts LIMIT (SELECT COUNT(*) FROM s10k_users)"),
        (Pg::Ok,   "setop parenthesized arms ORDER BY id", "(SELECT id FROM s10k_users) UNION (SELECT id FROM s10k_accounts) ORDER BY id"),
        (Pg::Loud, "setop parenthesized arms ORDER BY typo", "(SELECT id FROM s10k_users) UNION (SELECT id FROM s10k_accounts) ORDER BY typo"),
        (Pg::Ok,   "setop parenthesized whole ORDER BY id", "(SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts) ORDER BY id"),
        (Pg::Loud, "setop parenthesized whole ORDER BY expr", "(SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts) ORDER BY id + 1"),
        (Pg::Loud, "setop parenthesized whole ORDER BY typo", "(SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts) ORDER BY typo"),
        (Pg::Loud, "except ORDER BY typo", "SELECT id FROM s10k_users EXCEPT SELECT id FROM s10k_accounts ORDER BY typo"),
        (Pg::Loud, "intersect ORDER BY ordinal out of range", "SELECT id FROM s10k_users INTERSECT SELECT id FROM s10k_accounts ORDER BY 2"),
        // The set-op restriction must NOT leak to a plain SELECT ORDER BY.
        (Pg::Ok,   "plain SELECT ORDER BY expression (no leak)", "SELECT id FROM s10k_users ORDER BY age + 1"),
        (Pg::Ok,   "plain SELECT ORDER BY non-output FROM col (no leak)", "SELECT id FROM s10k_users ORDER BY bio"),
        (Pg::Ok,   "parenthesized plain SELECT ORDER BY expression (no leak)", "(SELECT id FROM s10k_users) ORDER BY age + 1"),
        (Pg::Loud, "parenthesized plain SELECT ORDER BY typo", "(SELECT id FROM s10k_users) ORDER BY typo"),

        // ── structural-integrity gaps: each verdict is the TRUE live-PG verdict,
        // re-verified via PREPARE, in BOTH directions. ──

        // Nested aggregate inside a named WINDOW definition's ORDER BY /
        // PARTITION BY: PG "aggregate function calls cannot be nested".
        (Pg::Loud, "WINDOW def ORDER BY SUM(COUNT(*))", "SELECT SUM(age)::int8 FROM s10k_users GROUP BY id WINDOW w AS (ORDER BY SUM(COUNT(*)))"),
        (Pg::Loud, "WINDOW def PARTITION BY SUM(COUNT(*))", "SELECT SUM(age)::int8 FROM s10k_users GROUP BY id WINDOW w AS (PARTITION BY SUM(COUNT(*)))"),
        (Pg::Loud, "WINDOW def ORDER BY MAX(AVG(age))", "SELECT SUM(age)::int8 FROM s10k_users GROUP BY id WINDOW w AS (ORDER BY MAX(AVG(age)))"),
        // A non-nested aggregate in the definition flips aggregate mode (ok); a
        // plain column in the definition stays ok.
        (Pg::Ok,   "WINDOW def ORDER BY COUNT(*)", "SELECT SUM(age)::int8 FROM s10k_users GROUP BY id WINDOW w AS (ORDER BY COUNT(*))"),
        (Pg::Ok,   "WINDOW def ORDER BY age", "SELECT id FROM s10k_users WINDOW w AS (ORDER BY age)"),

        // Duplicate INSERT target column: PG "column \"id\" specified more than once".
        (Pg::Loud, "INSERT (id,name,id)", "INSERT INTO s10k_users (id,name,id) VALUES (1,'a',2) RETURNING id"),
        (Pg::Ok,   "INSERT (id,name) distinct", "INSERT INTO s10k_users (id,name) VALUES (1,'a') RETURNING id"),

        // Duplicate UPDATE SET / ON CONFLICT DO UPDATE SET target: PG "multiple
        // assignments to same column \"...\"".
        (Pg::Loud, "UPDATE SET id=1,id=2", "UPDATE s10k_users SET id=1,id=2 WHERE id=$1 RETURNING id"),
        (Pg::Loud, "UPDATE SET (name,name)=($1,$2)", "UPDATE s10k_users SET (name,name)=($1,$2) WHERE id=$3 RETURNING id"),
        (Pg::Loud, "UPDATE SET name=$1,bio=$2,name=$3", "UPDATE s10k_users SET name=$1,bio=$2,name=$3 WHERE id=$4 RETURNING id"),
        (Pg::Loud, "UPDATE SET (name,bio)=($1,$2),name=$3", "UPDATE s10k_users SET (name,bio)=($1,$2),name=$3 WHERE id=$4 RETURNING id"),
        (Pg::Ok,   "UPDATE SET name=$1,bio=$2 distinct", "UPDATE s10k_users SET name=$1,bio=$2 WHERE id=$3 RETURNING id"),
        (Pg::Loud, "OC SET name=$3,name=$4", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET name=$3,name=$4 RETURNING id"),
        (Pg::Loud, "OC SET (name,name)=($3,$4)", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET (name,name)=($3,$4) RETURNING id"),
        // The ON CONFLICT (...) conflict-target spec accepts a repeated column.
        (Pg::Ok,   "OC (id,id) conflict target", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id,id) DO NOTHING RETURNING id"),

        // Duplicate FROM relation alias: PG "table name \"...\" specified more
        // than once" — even a qualified ref to the dup is loud.
        (Pg::Loud, "FROM users u, accounts u", "SELECT u.id FROM s10k_users u, s10k_accounts u"),
        (Pg::Loud, "FROM users, users bare", "SELECT id FROM s10k_users, s10k_users"),
        (Pg::Loud, "FROM users u JOIN accounts u", "SELECT id FROM s10k_users u JOIN s10k_accounts u ON true"),
        (Pg::Loud, "UPDATE FROM reuses target alias", "UPDATE s10k_users u SET age=1 FROM s10k_accounts u WHERE u.id=1 RETURNING u.id"),
        (Pg::Ok,   "FROM users u, accounts a distinct", "SELECT u.id AS uid, a.id AS aid FROM s10k_users u, s10k_accounts a"),
        (Pg::Ok,   "self-join distinct aliases", "SELECT a.id AS aid, b.id AS bid FROM s10k_users a JOIN s10k_users b ON a.id=b.id"),
        (Pg::Ok,   "FROM users, accounts bare distinct", "SELECT s10k_users.id AS uid, s10k_accounts.id AS aid FROM s10k_users, s10k_accounts"),
        // A correlated subquery is its own scope; an inner alias may shadow outer.
        (Pg::Ok,   "subquery alias shadows outer", "SELECT u.id FROM s10k_users u WHERE EXISTS (SELECT 1 FROM s10k_accounts u WHERE u.balance > 0)"),

        // Position-dependent subquery arity. Scalar value / scalar comparison
        // operand: PG "subquery must return only one column".
        (Pg::Loud, "id = (SELECT id,name)", "SELECT id FROM s10k_users WHERE id = (SELECT id,name FROM s10k_users)"),
        (Pg::Loud, "(SELECT id,name) > 5", "SELECT id FROM s10k_users WHERE (SELECT id,name FROM s10k_users LIMIT 1) > 5"),
        (Pg::Loud, "scalar subq projection 2-col", "SELECT (SELECT id,name FROM s10k_users) FROM s10k_users"),
        (Pg::Loud, "SET age=(SELECT id,user_id)", "UPDATE s10k_users SET age=(SELECT id,user_id FROM s10k_accounts) WHERE id=$1 RETURNING id"),
        (Pg::Ok,   "id = (SELECT id)", "SELECT id FROM s10k_users WHERE id = (SELECT id FROM s10k_users)"),
        // IN / ANY / ALL / row comparison: PG "subquery has too many/few columns".
        (Pg::Loud, "id IN (SELECT id,user_id)", "SELECT id FROM s10k_users WHERE id IN (SELECT id,user_id FROM s10k_accounts)"),
        (Pg::Loud, "(id,age) IN (SELECT user_id)", "SELECT id FROM s10k_users WHERE (id,age) IN (SELECT user_id FROM s10k_accounts)"),
        (Pg::Loud, "id = ANY (SELECT id,name)", "SELECT id FROM s10k_users WHERE id = ANY (SELECT id,name FROM s10k_users)"),
        (Pg::Loud, "id NOT IN (SELECT id,name)", "SELECT id FROM s10k_users WHERE id NOT IN (SELECT id,name FROM s10k_users)"),
        (Pg::Loud, "(id,age) = (SELECT id)", "SELECT id FROM s10k_users WHERE (id,age) = (SELECT id FROM s10k_users LIMIT 1)"),
        (Pg::Ok,   "id IN (SELECT user_id)", "SELECT id FROM s10k_users WHERE id IN (SELECT user_id FROM s10k_accounts)"),
        (Pg::Ok,   "(id,age) IN (SELECT id,user_id)", "SELECT id FROM s10k_users WHERE (id,age) IN (SELECT id,user_id FROM s10k_accounts)"),
        (Pg::Ok,   "id = ANY (SELECT id)", "SELECT id FROM s10k_users WHERE id = ANY (SELECT id FROM s10k_users)"),
        (Pg::Ok,   "(id,age) = (SELECT id,age)", "SELECT id FROM s10k_users WHERE (id,age) = (SELECT id,age FROM s10k_users LIMIT 1)"),
        // EXISTS is arity-agnostic.
        (Pg::Ok,   "EXISTS (SELECT 1,2)", "SELECT id FROM s10k_users WHERE EXISTS (SELECT 1, 2 FROM s10k_accounts)"),
        // A multi-column SET tuple subquery row source is NOT a scalar position.
        (Pg::Ok,   "SET (name,bio)=(SELECT name,bio) subq", "UPDATE s10k_users SET (name,bio)=(SELECT name,bio FROM s10k_users) WHERE id=$1 RETURNING id"),

        // INSERT INTO t AS alias: the alias resolves, the bare name is hidden.
        (Pg::Ok,   "INSERT AS u RETURNING u.id", "INSERT INTO s10k_users AS u (id,name) VALUES ($1,$2) RETURNING u.id"),
        (Pg::Ok,   "INSERT AS u OC SET name=u.name WHERE u.age>0", "INSERT INTO s10k_users AS u (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET name=u.name WHERE u.age>0 RETURNING u.id"),
        (Pg::Ok,   "INSERT AS u OC SET name=excluded.name", "INSERT INTO s10k_users AS u (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET name=excluded.name RETURNING u.id"),
        (Pg::Loud, "INSERT AS u RETURNING u.bogus", "INSERT INTO s10k_users AS u (id,name) VALUES ($1,$2) RETURNING u.bogus"),
        (Pg::Loud, "INSERT AS u RETURNING bare table.id", "INSERT INTO s10k_users AS u (id,name) VALUES ($1,$2) RETURNING s10k_users.id"),

        // ── structural-integrity: WILDCARD subquery arity. A `SELECT *` /
        // `SELECT t.*` subquery in a scalar / IN / ANY / ALL / row-comparison
        // position has its TRUE width resolved by expanding the wildcard against
        // the subquery's OWN FROM scope (s10k_accounts has 3 columns), so a
        // mismatch is loud rather than silently accepted. PG rejects each: a
        // scalar / single-value position with "subquery must return only one
        // column", an IN / ANY / ALL / 2-col-row position with "subquery has
        // too many/few columns". Each verdict re-verified live via PREPARE. ──
        (Pg::Loud, "id IN (SELECT * FROM accounts)", "SELECT id FROM s10k_users WHERE id IN (SELECT * FROM s10k_accounts)"),
        (Pg::Loud, "(id,name) IN (SELECT * FROM accounts)", "SELECT id FROM s10k_users WHERE (id,name) IN (SELECT * FROM s10k_accounts)"),
        (Pg::Loud, "id = ANY (SELECT * FROM accounts)", "SELECT id FROM s10k_users WHERE id = ANY (SELECT * FROM s10k_accounts)"),
        (Pg::Loud, "id > ALL (SELECT * FROM accounts)", "SELECT id FROM s10k_users WHERE id > ALL (SELECT * FROM s10k_accounts)"),
        (Pg::Loud, "(id,name) = (SELECT * FROM accounts)", "SELECT id FROM s10k_users WHERE (id,name) = (SELECT * FROM s10k_accounts LIMIT 1)"),
        (Pg::Loud, "id IN (SELECT a.* FROM accounts a)", "SELECT id FROM s10k_users WHERE id IN (SELECT a.* FROM s10k_accounts a)"),
        (Pg::Loud, "id IN (SELECT * UNION SELECT *)", "SELECT id FROM s10k_users WHERE id IN (SELECT * FROM s10k_accounts UNION SELECT * FROM s10k_accounts)"),
        (Pg::Loud, "id = (SELECT * FROM accounts)", "SELECT id FROM s10k_users WHERE id = (SELECT * FROM s10k_accounts LIMIT 1)"),
        (Pg::Loud, "SET name=(SELECT * FROM accounts)", "UPDATE s10k_users SET name=(SELECT * FROM s10k_accounts LIMIT 1) WHERE id=$1 RETURNING id"),
        (Pg::Loud, "DELETE id IN (SELECT * FROM accounts)", "DELETE FROM s10k_users WHERE id IN (SELECT * FROM s10k_accounts) RETURNING id"),
        // A `t.*` qualified wildcard resolves to that one relation's full column
        // count (s10k_users has 4): a 4-col row LHS matches (Ok), a scalar LHS
        // does not (Loud) — exercising the qualified-wildcard width path BOTH
        // directions.
        (Pg::Loud, "id IN (SELECT u.* FROM users u)", "SELECT id FROM s10k_users WHERE id IN (SELECT u.* FROM s10k_users u)"),
        (Pg::Ok,   "(4-col) IN (SELECT u.* FROM users u)", "SELECT id FROM s10k_users WHERE (id,name,bio,age) IN (SELECT u.* FROM s10k_users u)"),
        // Matching-width controls: a 3-col row LHS against a `SELECT *` over the
        // 3-col composite table; a scalar LHS against a single-column `*`.
        (Pg::Ok,   "(a,b,c) IN (SELECT * FROM comp)", "SELECT a FROM s10k_comp WHERE (a,b,c) IN (SELECT * FROM s10k_comp)"),
        (Pg::Ok,   "id IN (SELECT * FROM (SELECT id) s)", "SELECT id FROM s10k_users WHERE id IN (SELECT * FROM (SELECT id FROM s10k_users) s)"),

        // ── structural-integrity: ROW-CONSTRUCTOR-vs-ROW-CONSTRUCTOR arity.
        // When BOTH operands of a comparison (=, <>, !=, <, <=, >, >=, IS [NOT]
        // DISTINCT FROM, BETWEEN, IN) are countable row constructors of
        // differing width, PG is loud ("unequal number of entries in row
        // expressions"); the engine raises a dedicated RowArityMismatch (NOT the
        // subquery form). A nested row compares its OUTER widths only (PG
        // accepts), and an `= ANY(ARRAY[...])` is an element comparison PG does
        // not arity-check (PG accepts). A scalar-vs-row pair (`id = (1, 2)`) is
        // a DIFFERENT failure — an operator-resolution TYPE error ("operator
        // does not exist: bigint = record") in the predicate-operand type
        // dimension this build-time engine does not model at all (a plain
        // `WHERE name = 5` text-vs-int mismatch is likewise not flagged here);
        // so the row-arity check deliberately does NOT fire on it, and it is not
        // in this corpus (it belongs to the future predicate-type dimension, not
        // the arity dimension). Each verdict re-verified live via PREPARE. ──
        (Pg::Loud, "(id,age)=(1,2,3)", "SELECT id FROM s10k_users WHERE (id,age)=(1,2,3)"),
        (Pg::Loud, "(id,age)=ROW(1,2,3)", "SELECT id FROM s10k_users WHERE (id,age)=ROW(1,2,3)"),
        (Pg::Loud, "ROW(id,age)=ROW(1,2,3)", "SELECT id FROM s10k_users WHERE ROW(id,age)=ROW(1,2,3)"),
        (Pg::Loud, "(id,age)<(1,2,3)", "SELECT id FROM s10k_users WHERE (id,age)<(1,2,3)"),
        (Pg::Loud, "(id,age)<=(1,2,3)", "SELECT id FROM s10k_users WHERE (id,age)<=(1,2,3)"),
        (Pg::Loud, "(id,age)>(1,2,3)", "SELECT id FROM s10k_users WHERE (id,age)>(1,2,3)"),
        (Pg::Loud, "(id,age)>=(1,2,3)", "SELECT id FROM s10k_users WHERE (id,age)>=(1,2,3)"),
        (Pg::Loud, "(id,age)<>(1,2,3)", "SELECT id FROM s10k_users WHERE (id,age)<>(1,2,3)"),
        (Pg::Loud, "(id,age)!=(1,2,3)", "SELECT id FROM s10k_users WHERE (id,age)!=(1,2,3)"),
        (Pg::Loud, "(id,age) IS DISTINCT FROM (1,2,3)", "SELECT id FROM s10k_users WHERE (id,age) IS DISTINCT FROM (1,2,3)"),
        (Pg::Loud, "(id,age) IS NOT DISTINCT FROM (1,2,3)", "SELECT id FROM s10k_users WHERE (id,age) IS NOT DISTINCT FROM (1,2,3)"),
        (Pg::Loud, "(id,age) BETWEEN (1,2,3) AND (4,5)", "SELECT id FROM s10k_users WHERE (id,age) BETWEEN (1,2,3) AND (4,5)"),
        (Pg::Loud, "(id,age) BETWEEN (1,2) AND (3,4,5)", "SELECT id FROM s10k_users WHERE (id,age) BETWEEN (1,2) AND (3,4,5)"),
        (Pg::Loud, "(id,age) IN ((1,2,3))", "SELECT id FROM s10k_users WHERE (id,age) IN ((1,2,3))"),
        (Pg::Loud, "(id,age) IN ((1,2),(3,4,5))", "SELECT id FROM s10k_users WHERE (id,age) IN ((1,2),(3,4,5))"),
        (Pg::Loud, "(id,age) NOT IN ((1,2),(3,4,5))", "SELECT id FROM s10k_users WHERE (id,age) NOT IN ((1,2),(3,4,5))"),
        (Pg::Loud, "(id,age,name)=(1,2)", "SELECT id FROM s10k_users WHERE (id,age,name)=(1,2)"),
        (Pg::Loud, "NOT ((id,age)=(1,2,3))", "SELECT id FROM s10k_users WHERE NOT ((id,age)=(1,2,3))"),
        // `ROW(1)` is a genuine ONE-entry row (not a scalar), so a 2-entry LHS
        // against it is a row-width mismatch, NOT the scalar-vs-row type error.
        (Pg::Loud, "(id,age)=ROW(1)", "SELECT id FROM s10k_users WHERE (id,age)=ROW(1)"),
        // Matching-width controls + the deliberately-untouched forms.
        (Pg::Ok,   "(id,age)=(1,2)", "SELECT id FROM s10k_users WHERE (id,age)=(1,2)"),
        (Pg::Ok,   "ROW(id,age)=ROW(1,2)", "SELECT id FROM s10k_users WHERE ROW(id,age)=ROW(1,2)"),
        (Pg::Ok,   "(id,age) IN ((1,2),(3,4))", "SELECT id FROM s10k_users WHERE (id,age) IN ((1,2),(3,4))"),
        (Pg::Ok,   "(id,age) BETWEEN (1,2) AND (3,4)", "SELECT id FROM s10k_users WHERE (id,age) BETWEEN (1,2) AND (3,4)"),
        (Pg::Ok,   "(id,age)=ANY(ARRAY[ROW(1,2),ROW(3,4,5)])", "SELECT id FROM s10k_users WHERE (id,age)=ANY(ARRAY[ROW(1,2),ROW(3,4,5)])"),
        (Pg::Ok,   "nested-row outer widths match", "SELECT id FROM s10k_users WHERE ((id,age),bio)=((1,2),'y')"),

        // ── structural-integrity: SET-OPERATION ARM-COUNT inside a subquery.
        // A `UNION` / `INTERSECT` / `EXCEPT` used as the body of an IN / = / ANY
        // / ALL / EXISTS / scalar subquery must have arms of equal width — PG is
        // loud ("each UNION/INTERSECT/EXCEPT query must have the same number of
        // columns") regardless of position, because the set operation itself is
        // malformed. The engine checks arm-count consistency at the body, so a
        // left-matches-but-right-disagrees set op is loud in EVERY position (the
        // older count rule only ran when the set op's columns were typed). A
        // consistent-arm set op stays Ok. Each verdict re-verified live. ──
        (Pg::Loud, "id IN (1col UNION 2col)", "SELECT id FROM s10k_users WHERE id IN (SELECT id FROM s10k_accounts UNION SELECT id, user_id FROM s10k_accounts)"),
        (Pg::Ok,   "id IN (1col UNION 1col)", "SELECT id FROM s10k_users WHERE id IN (SELECT id FROM s10k_accounts UNION SELECT id FROM s10k_accounts)"),
        (Pg::Loud, "id IN (1col INTERSECT 2col)", "SELECT id FROM s10k_users WHERE id IN (SELECT id FROM s10k_accounts INTERSECT SELECT id, user_id FROM s10k_accounts)"),
        (Pg::Loud, "id IN (1col EXCEPT 2col)", "SELECT id FROM s10k_users WHERE id IN (SELECT id FROM s10k_accounts EXCEPT SELECT id, user_id FROM s10k_accounts)"),
        (Pg::Loud, "id = (1col UNION 2col)", "SELECT id FROM s10k_users WHERE id = (SELECT id FROM s10k_accounts UNION SELECT id, user_id FROM s10k_accounts)"),
        (Pg::Loud, "id = ANY (1col UNION 2col)", "SELECT id FROM s10k_users WHERE id = ANY (SELECT id FROM s10k_accounts UNION SELECT id, user_id FROM s10k_accounts)"),
        (Pg::Loud, "id > ALL (1col UNION 2col)", "SELECT id FROM s10k_users WHERE id > ALL (SELECT id FROM s10k_accounts UNION SELECT id, user_id FROM s10k_accounts)"),
        (Pg::Loud, "EXISTS (1col UNION 2col)", "SELECT id FROM s10k_users WHERE EXISTS (SELECT id FROM s10k_accounts UNION SELECT id, user_id FROM s10k_accounts)"),
        (Pg::Loud, "nested 3-arm last disagrees", "SELECT id FROM s10k_users WHERE id IN (SELECT id FROM s10k_accounts UNION SELECT id FROM s10k_accounts UNION SELECT id, user_id FROM s10k_accounts)"),
        (Pg::Loud, "scalar subq (1col UNION 2col)", "SELECT (SELECT id FROM s10k_accounts UNION SELECT id, user_id FROM s10k_accounts) FROM s10k_users"),
        (Pg::Ok,   "scalar subq (1col UNION 1col)", "SELECT (SELECT id FROM s10k_accounts UNION SELECT id FROM s10k_accounts LIMIT 1) FROM s10k_users"),

        // ── structural-integrity: VALUES-as-FUNCTION arity. `sqlparser` parses
        // `(VALUES (...))` / `ANY(VALUES (...))` in a comparison / ANY / ALL /
        // scalar position as a `values`-named function, escaping the subquery /
        // row arity checks. The engine recognizes it as a row source whose width
        // is the row's cell count: a multi-cell one in a scalar position is loud
        // ("subquery must return only one column"), in an ANY / ALL / row
        // position it must match the LHS width ("subquery has too many/few
        // columns"). A single-cell one in a scalar position, and a matching-width
        // one in a row position, are Ok. Each verdict re-verified live. ──
        (Pg::Loud, "id = (VALUES (1,2))", "SELECT id FROM s10k_users WHERE id = (VALUES (1,2))"),
        (Pg::Loud, "id = ANY(VALUES (1,2))", "SELECT id FROM s10k_users WHERE id = ANY(VALUES (1,2))"),
        (Pg::Loud, "id > ALL(VALUES (1,2))", "SELECT id FROM s10k_users WHERE id > ALL(VALUES (1,2))"),
        (Pg::Loud, "(id,age) = (VALUES (1,2,3))", "SELECT id FROM s10k_users WHERE (id,age) = (VALUES (1,2,3))"),
        (Pg::Loud, "projected (VALUES (1,2))", "SELECT (VALUES (1,2)) FROM s10k_users"),
        (Pg::Ok,   "id = (VALUES (1))", "SELECT id FROM s10k_users WHERE id = (VALUES (1))"),
        (Pg::Ok,   "id = ANY(VALUES (1))", "SELECT id FROM s10k_users WHERE id = ANY(VALUES (1))"),
        (Pg::Ok,   "(id,age) = (VALUES (1,2))", "SELECT id FROM s10k_users WHERE (id,age) = (VALUES (1,2))"),
        // A genuine IN-subquery `VALUES` (NOT a function) is already a subquery
        // body; its arity is checked there. Loud on a 2-col body for a scalar
        // LHS, Ok on a single-col body.
        (Pg::Loud, "id IN (VALUES (1,2))", "SELECT id FROM s10k_users WHERE id IN (VALUES (1,2))"),
        (Pg::Ok,   "id IN (VALUES (1))", "SELECT id FROM s10k_users WHERE id IN (VALUES (1))"),
        (Pg::Ok,   "id IN (VALUES (1),(2))", "SELECT id FROM s10k_users WHERE id IN (VALUES (1),(2))"),

        // ── structural-integrity: RAGGED VALUES in a predicate-subquery body.
        // A multi-row VALUES whose rows differ in length is malformed regardless
        // of position — PG is loud ("VALUES lists must all be the same length").
        // In an IN / EXISTS predicate body the body's columns are never typed, so
        // the same-length rule runs in the reference-validation pass: the body is
        // the single authority every predicate subquery routes through. An
        // EQUAL-WIDTH multi-row body (single-column for a scalar LHS, matching
        // width for a row LHS, any equal width for EXISTS) stays Ok. Each verdict
        // re-verified live via PREPARE, BOTH directions. ──
        (Pg::Loud, "id IN (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users WHERE id IN (VALUES (1),(2,3))"),
        (Pg::Ok,   "id IN (VALUES (1),(2),(3)) equal", "SELECT id FROM s10k_users WHERE id IN (VALUES (1),(2),(3))"),
        (Pg::Loud, "EXISTS (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users WHERE EXISTS (VALUES (1),(2,3))"),
        (Pg::Ok,   "EXISTS (VALUES (1),(2)) equal", "SELECT id FROM s10k_users WHERE EXISTS (VALUES (1),(2))"),
        (Pg::Loud, "(id,age) IN (VALUES (1,2),(3)) ragged", "SELECT id FROM s10k_users WHERE (id,age) IN (VALUES (1,2),(3))"),
        (Pg::Ok,   "(id,age) IN (VALUES (1,2),(3,4),(5,6)) equal", "SELECT id FROM s10k_users WHERE (id,age) IN (VALUES (1,2),(3,4),(5,6))"),
        (Pg::Loud, "id IN (VALUES (1),(2),(3,4)) last ragged", "SELECT id FROM s10k_users WHERE id IN (VALUES (1),(2),(3,4))"),

        // ── structural-integrity: RAGGED VALUES in a COMPARISON-OPERAND / VALUE
        // position. In a scalar value / comparison-operand position `sqlparser`
        // parses `(VALUES r1, r2, ...)` NOT as a subquery body but as a tuple
        // whose first element is a `values`-named function (the first row) and
        // whose later elements are the trailing rows; the same-length rule is
        // enforced at that values-constructor tuple site. PG rejects every ragged
        // one ("VALUES lists must all be the same length"); an equal-width one in
        // a scalar `=` / value position is accepted (a single-column multi-row
        // VALUES is a scalar subquery at PREPARE). Covers `=`, `<>`, `<`, `<=`,
        // `>`, `>=`, IS [NOT] DISTINCT FROM, BETWEEN (both bounds), CASE (WHEN and
        // result), arithmetic-nested, HAVING, ORDER BY, LIMIT, and the left side.
        // Each verdict re-verified live via PREPARE, BOTH directions. ──
        (Pg::Loud, "id = (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users WHERE id = (VALUES (1),(2,3))"),
        (Pg::Loud, "(VALUES (1),(2,3)) = id ragged lhs", "SELECT id FROM s10k_users WHERE (VALUES (1),(2,3)) = id"),
        (Pg::Loud, "id <> (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users WHERE id <> (VALUES (1),(2,3))"),
        (Pg::Loud, "id < (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users WHERE id < (VALUES (1),(2,3))"),
        (Pg::Loud, "id <= (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users WHERE id <= (VALUES (1),(2,3))"),
        (Pg::Loud, "id > (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users WHERE id > (VALUES (1),(2,3))"),
        (Pg::Loud, "id >= (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users WHERE id >= (VALUES (1),(2,3))"),
        (Pg::Loud, "id IS DISTINCT FROM (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users WHERE id IS DISTINCT FROM (VALUES (1),(2,3))"),
        (Pg::Loud, "id IS NOT DISTINCT FROM (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users WHERE id IS NOT DISTINCT FROM (VALUES (1),(2,3))"),
        (Pg::Loud, "id BETWEEN (VALUES (1),(2,3)) AND 5 ragged low", "SELECT id FROM s10k_users WHERE id BETWEEN (VALUES (1),(2,3)) AND 5"),
        (Pg::Loud, "id BETWEEN 1 AND (VALUES (1),(2,3)) ragged high", "SELECT id FROM s10k_users WHERE id BETWEEN 1 AND (VALUES (1),(2,3))"),
        (Pg::Loud, "CASE WHEN id=(VALUES (1),(2,3)) ragged", "SELECT (CASE WHEN id=(VALUES (1),(2,3)) THEN 1 ELSE 0 END) FROM s10k_users"),
        (Pg::Loud, "CASE THEN (VALUES (1),(2,3)) ragged", "SELECT (CASE WHEN id=1 THEN (VALUES (1),(2,3)) ELSE 0 END) FROM s10k_users"),
        (Pg::Loud, "id = ((VALUES (1),(2,3)) + 0) ragged arith", "SELECT id FROM s10k_users WHERE id = ((VALUES (1),(2,3)) + 0)"),
        (Pg::Loud, "HAVING id=(VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users GROUP BY id HAVING id = (VALUES (1),(2,3))"),
        (Pg::Loud, "ORDER BY (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users ORDER BY (VALUES (1),(2,3))"),
        (Pg::Loud, "LIMIT (VALUES (1),(2,3)) ragged", "SELECT id FROM s10k_users LIMIT (VALUES (1),(2,3))"),
        (Pg::Loud, "id = (VALUES (1,2),(3)) wider first ragged", "SELECT id FROM s10k_users WHERE id = (VALUES (1,2),(3))"),
        (Pg::Loud, "(id,age) = (VALUES (1,2),(3)) ragged row lhs", "SELECT id FROM s10k_users WHERE (id,age) = (VALUES (1,2),(3))"),
        (Pg::Loud, "id = (VALUES (1),(2),(3,4)) middle ragged", "SELECT id FROM s10k_users WHERE id = (VALUES (1),(2),(3,4))"),
        // Equal-width multi-row VALUES in a value position is NOT ragged — the
        // rule does not fire and the form stays accepted.
        (Pg::Ok,   "id = (VALUES (1),(2)) equal", "SELECT id FROM s10k_users WHERE id = (VALUES (1),(2))"),
        (Pg::Ok,   "id <> (VALUES (1),(2)) equal", "SELECT id FROM s10k_users WHERE id <> (VALUES (1),(2))"),
        (Pg::Ok,   "id BETWEEN (VALUES (1),(2)) AND 5 equal", "SELECT id FROM s10k_users WHERE id BETWEEN (VALUES (1),(2)) AND 5"),
        (Pg::Ok,   "ORDER BY (VALUES (1),(2)) equal", "SELECT id FROM s10k_users ORDER BY (VALUES (1),(2))"),
        (Pg::Ok,   "LIMIT (VALUES (1),(2)) equal", "SELECT id FROM s10k_users LIMIT (VALUES (1),(2))"),
        (Pg::Ok,   "id = ((VALUES (1),(2)) + 0) equal arith", "SELECT id FROM s10k_users WHERE id = ((VALUES (1),(2)) + 0)"),
        // The values-constructor tuple site is reached from every value position,
        // so a ragged one is loud however deeply it nests — projected directly,
        // double-parenthesised, a function argument, under NOT, behind a cast, a
        // tuple element, or under a unary operator. Each verdict re-verified live.
        (Pg::Loud, "projected (VALUES (1),(2,3)) ragged", "SELECT (VALUES (1),(2,3)) FROM s10k_users"),
        (Pg::Loud, "id IN ((VALUES (1),(2,3))) ragged paren", "SELECT id FROM s10k_users WHERE id IN ((VALUES (1),(2,3)))"),
        (Pg::Loud, "COALESCE((VALUES (1),(2,3)),0) ragged arg", "SELECT id FROM s10k_users WHERE id = COALESCE((VALUES (1),(2,3)), 0)"),
        (Pg::Loud, "NOT (id=(VALUES (1),(2,3))) ragged", "SELECT id FROM s10k_users WHERE NOT (id = (VALUES (1),(2,3)))"),
        (Pg::Loud, "(VALUES (1),(2,3))::int ragged cast", "SELECT id FROM s10k_users WHERE id = (VALUES (1),(2,3))::int"),
        (Pg::Loud, "(id,(VALUES (1),(2,3)))=(1,2) ragged elem", "SELECT id FROM s10k_users WHERE (id, (VALUES (1),(2,3))) = (1, 2)"),
        (Pg::Loud, "-(VALUES (1),(2,3)) ragged unary", "SELECT id FROM s10k_users WHERE id = -(VALUES (1),(2,3))"),

        // ── structural-integrity: MULTI-ROW (VALUES ...)-tuple ARITY as a row
        // source. A multi-row `(VALUES r1, r2, ...)` in a comparison / value
        // position parses as a tuple whose FIRST element is a `values`-named
        // function (the first row) and whose later elements are the trailing rows.
        // Its TRUE shape is a `VALUES` ROW SOURCE whose COLUMN width is the FIRST
        // row's cell count — NEVER the outer tuple's element count, which is the
        // ROW count — and it is treated identically to a `VALUES` subquery. A row
        // LHS of arity N matches column-width N (accept) and mismatches loudly
        // ("subquery has too few/many columns"); a SCALAR context (the right side
        // of a scalar `=` family operator, the LEFT side of any comparison — which
        // PG parses as a scalar subquery — IS [NOT] DISTINCT FROM on either side,
        // and the BETWEEN subject) requires column-width 1 ("subquery must return
        // only one column"). Each verdict re-verified live via PREPARE, BOTH
        // directions. ──
        // Row LHS arity N == VALUES column-width N: accept.
        (Pg::Ok,   "(id,age) = (VALUES (1,2),(3,4)) row eq", "SELECT id FROM s10k_users WHERE (id,age) = (VALUES (1,2),(3,4))"),
        (Pg::Ok,   "(id,name,age) = (VALUES (1,'x',2),(3,'y',4)) row3 eq", "SELECT id FROM s10k_users WHERE (id,name,age) = (VALUES (1,'x',2),(3,'y',4))"),
        // Row LHS arity N != VALUES column-width M: loud row form.
        (Pg::Loud, "(id,age) = (VALUES (1),(2)) too few", "SELECT id FROM s10k_users WHERE (id,age) = (VALUES (1),(2))"),
        (Pg::Loud, "(id,name,age) = (VALUES (1),(2)) too few", "SELECT id FROM s10k_users WHERE (id,name,age) = (VALUES (1),(2))"),
        (Pg::Loud, "(id,age) = (VALUES (1,2,3),(4,5,6)) too many", "SELECT id FROM s10k_users WHERE (id,age) = (VALUES (1,2,3),(4,5,6))"),
        // Scalar LHS against a multi-column multi-row VALUES: loud scalar form.
        (Pg::Loud, "id = (VALUES (1,2),(3,4)) scalar multicol", "SELECT id FROM s10k_users WHERE id = (VALUES (1,2),(3,4))"),
        (Pg::Loud, "id <> (VALUES (1,2),(3,4)) scalar multicol", "SELECT id FROM s10k_users WHERE id <> (VALUES (1,2),(3,4))"),
        // A multi-column VALUES on the LEFT is a scalar subquery (one column).
        (Pg::Loud, "(VALUES (1,2),(3,4)) = id lhs multicol", "SELECT id FROM s10k_users WHERE (VALUES (1,2),(3,4)) = id"),
        (Pg::Loud, "(VALUES (1,2),(3,4)) = (id,age) lhs multicol", "SELECT id FROM s10k_users WHERE (VALUES (1,2),(3,4)) = (id,age)"),
        (Pg::Loud, "(VALUES (1,2)) = (id,age) single-row lhs multicol", "SELECT id FROM s10k_users WHERE (VALUES (1,2)) = (id,age)"),
        // A single-column multi-row VALUES against a scalar side: accept.
        (Pg::Ok,   "id = (VALUES (1),(2)) scalar 1col", "SELECT id FROM s10k_users WHERE id = (VALUES (1),(2))"),
        (Pg::Ok,   "id <> (VALUES (1),(2),(3)) scalar 1col", "SELECT id FROM s10k_users WHERE id <> (VALUES (1),(2),(3))"),
        (Pg::Ok,   "(VALUES (1),(2)) = id 1col lhs", "SELECT id FROM s10k_users WHERE (VALUES (1),(2)) = id"),
        // IS [NOT] DISTINCT FROM: a VALUES source on EITHER side is scalar.
        (Pg::Loud, "(id,age) IS DISTINCT FROM (VALUES (1,2)) multicol", "SELECT id FROM s10k_users WHERE (id,age) IS DISTINCT FROM (VALUES (1,2))"),
        (Pg::Loud, "(id,age) IS DISTINCT FROM (VALUES (1,2),(3,4)) multicol", "SELECT id FROM s10k_users WHERE (id,age) IS DISTINCT FROM (VALUES (1,2),(3,4))"),
        (Pg::Loud, "id IS NOT DISTINCT FROM (VALUES (1,2),(3,4)) multicol", "SELECT id FROM s10k_users WHERE id IS NOT DISTINCT FROM (VALUES (1,2),(3,4))"),
        (Pg::Loud, "(VALUES (1,2),(3,4)) IS DISTINCT FROM (id,age) lhs multicol", "SELECT id FROM s10k_users WHERE (VALUES (1,2),(3,4)) IS DISTINCT FROM (id,age)"),
        (Pg::Ok,   "id IS DISTINCT FROM (VALUES (1),(2)) 1col", "SELECT id FROM s10k_users WHERE id IS DISTINCT FROM (VALUES (1),(2))"),
        // BETWEEN: the subject is scalar; each bound is a row source matched
        // against the subject's arity.
        (Pg::Ok,   "(id,age) BETWEEN (VALUES (1,2),(3,4)) AND (5,6) row low eq", "SELECT id FROM s10k_users WHERE (id,age) BETWEEN (VALUES (1,2),(3,4)) AND (5,6)"),
        (Pg::Ok,   "(id,age) BETWEEN (1,2) AND (VALUES (1,2),(3,4)) row high eq", "SELECT id FROM s10k_users WHERE (id,age) BETWEEN (1,2) AND (VALUES (1,2),(3,4))"),
        (Pg::Loud, "(id,age) BETWEEN (VALUES (1),(2)) AND (5,6) low too few", "SELECT id FROM s10k_users WHERE (id,age) BETWEEN (VALUES (1),(2)) AND (5,6)"),
        (Pg::Ok,   "id BETWEEN (VALUES (1),(2)) AND 5 scalar 1col bound", "SELECT id FROM s10k_users WHERE id BETWEEN (VALUES (1),(2)) AND 5"),
        (Pg::Loud, "(VALUES (1,2),(3,4)) BETWEEN 1 AND 5 subject multicol", "SELECT id FROM s10k_users WHERE (VALUES (1,2),(3,4)) BETWEEN 1 AND 5"),
        // Ragged multi-row VALUES wins over arity (PG reports it first).
        (Pg::Loud, "(id,age) = (VALUES (1,2),(3)) ragged row lhs2", "SELECT id FROM s10k_users WHERE (id,age) = (VALUES (1,2),(3))"),
        (Pg::Loud, "(id,age) BETWEEN (1,2) AND (VALUES (1),(2,3)) ragged high", "SELECT id FROM s10k_users WHERE (id,age) BETWEEN (1,2) AND (VALUES (1),(2,3))"),
        // A legitimate row constructor (first element NOT a `values` function) is
        // untouched — equal width accepts, unequal width is the row-constructor
        // form, never the VALUES-source rule.
        (Pg::Ok,   "(id,age) = (1,2) plain row", "SELECT id FROM s10k_users WHERE (id,age) = (1,2)"),
        (Pg::Loud, "(id,age) = (1,2,3) plain row mismatch", "SELECT id FROM s10k_users WHERE (id,age) = (1,2,3)"),

        // ── structural-integrity: MULTI-ROW (VALUES ...)-tuple in ANY / ALL.
        // `<lhs> <op> ANY ((VALUES ...))` / `ALL ((VALUES ...))` parses the right
        // side as a tuple led by a `values` function (the first row) followed by the
        // later rows — the SAME values-led shape every other comparison position
        // takes (the inner double parens are needed because single-paren
        // `ANY (VALUES ...)` is a `sqlparser` grammar limit). The source's column
        // width is the FIRST row's cell count; PG always reports the ROW form here
        // ("subquery has too many/few columns"). Each verdict re-verified live via
        // PREPARE, BOTH directions. ──
        // Scalar LHS (arity 1) against a multi-column multi-row VALUES: loud row form.
        (Pg::Loud, "id = ANY ((VALUES (1,2),(3,4))) scalar multicol", "SELECT id FROM s10k_users WHERE id = ANY ((VALUES (1,2),(3,4)))"),
        (Pg::Loud, "id > ALL ((VALUES (1,2),(3,4))) scalar multicol", "SELECT id FROM s10k_users WHERE id > ALL ((VALUES (1,2),(3,4)))"),
        (Pg::Loud, "id = ANY ((VALUES (1,2,3),(4,5,6))) scalar 3col", "SELECT id FROM s10k_users WHERE id = ANY ((VALUES (1,2,3),(4,5,6)))"),
        // Row LHS arity N against a multi-row VALUES of column-width M != N: loud.
        (Pg::Loud, "(id,age) = ANY ((VALUES (1),(2))) row too few", "SELECT id FROM s10k_users WHERE (id,age) = ANY ((VALUES (1),(2)))"),
        (Pg::Loud, "(id,name,age) = ANY ((VALUES (1),(2))) row too few", "SELECT id FROM s10k_users WHERE (id,name,age) = ANY ((VALUES (1),(2)))"),
        (Pg::Loud, "(id,age) = ALL ((VALUES (1,2,3),(4,5,6))) row too many", "SELECT id FROM s10k_users WHERE (id,age) = ALL ((VALUES (1,2,3),(4,5,6)))"),
        // Scalar LHS against a single-column multi-row VALUES: accept (cardinality
        // is a runtime-only concern).
        (Pg::Ok,   "id = ANY ((VALUES (1),(2))) scalar 1col", "SELECT id FROM s10k_users WHERE id = ANY ((VALUES (1),(2)))"),
        (Pg::Ok,   "id = ANY ((VALUES (1),(2),(3))) scalar 1col", "SELECT id FROM s10k_users WHERE id = ANY ((VALUES (1),(2),(3)))"),
        (Pg::Ok,   "id > ALL ((VALUES (1),(2))) scalar 1col", "SELECT id FROM s10k_users WHERE id > ALL ((VALUES (1),(2)))"),
        // Row LHS arity N == VALUES column-width N: accept.
        (Pg::Ok,   "(id,age) = ANY ((VALUES (1,2),(3,4))) row eq", "SELECT id FROM s10k_users WHERE (id,age) = ANY ((VALUES (1,2),(3,4)))"),
        (Pg::Ok,   "(id,age) = ALL ((VALUES (1,2),(3,4))) row eq", "SELECT id FROM s10k_users WHERE (id,age) = ALL ((VALUES (1,2),(3,4)))"),
        // Single-row (VALUES (...)) ANY / ALL keeps its prior verdict.
        (Pg::Loud, "id = ANY (VALUES (1,2)) single multicol", "SELECT id FROM s10k_users WHERE id = ANY (VALUES (1,2))"),
        (Pg::Ok,   "id = ANY (VALUES (1)) single 1col", "SELECT id FROM s10k_users WHERE id = ANY (VALUES (1))"),
        // Ragged multi-row VALUES in ANY / ALL: loud ("same length"), before arity.
        (Pg::Loud, "id = ANY ((VALUES (1,2),(3))) ragged", "SELECT id FROM s10k_users WHERE id = ANY ((VALUES (1,2),(3)))"),
        (Pg::Loud, "(id,age) = ALL ((VALUES (1),(2,3))) ragged", "SELECT id FROM s10k_users WHERE (id,age) = ALL ((VALUES (1),(2,3)))"),

        // ── structural-integrity: bare non-parenthesised later VALUES row. PG
        // requires every VALUES row after the first to be a parenthesised list;
        // `sqlparser` leniently parses a bare later row (`2`, `name`) as an ordinary
        // expression — a comparison / value operand becomes a tuple
        // `[values_fn, bare]`, and an `IN` operand degrades to an `InList`
        // `[values_fn, bare]`. The engine rejects it loudly (a syntax-level error)
        // in EVERY position, matching PG's hard `syntax error at or near "<value>"`.
        // Each verdict re-verified live via PREPARE, BOTH directions. ──
        // BinaryOp comparison, either side and every operator.
        (Pg::Loud, "id = (VALUES (1),2) bare", "SELECT id FROM s10k_users WHERE id = (VALUES (1),2)"),
        (Pg::Loud, "id <> (VALUES (1),2) bare", "SELECT id FROM s10k_users WHERE id <> (VALUES (1),2)"),
        (Pg::Loud, "id < (VALUES (1),name) bare ident", "SELECT id FROM s10k_users WHERE id < (VALUES (1),name)"),
        (Pg::Loud, "(VALUES (1),2) = id bare lhs", "SELECT id FROM s10k_users WHERE (VALUES (1),2) = id"),
        // IS [NOT] DISTINCT FROM, either side.
        (Pg::Loud, "id IS DISTINCT FROM (VALUES (1),2) bare", "SELECT id FROM s10k_users WHERE id IS DISTINCT FROM (VALUES (1),2)"),
        (Pg::Loud, "id IS NOT DISTINCT FROM (VALUES (1),2) bare", "SELECT id FROM s10k_users WHERE id IS NOT DISTINCT FROM (VALUES (1),2)"),
        (Pg::Loud, "(VALUES (1),2) IS DISTINCT FROM id bare lhs", "SELECT id FROM s10k_users WHERE (VALUES (1),2) IS DISTINCT FROM id"),
        // BETWEEN subject and both bounds.
        (Pg::Loud, "(VALUES (1),2) BETWEEN 1 AND 5 bare subj", "SELECT id FROM s10k_users WHERE (VALUES (1),2) BETWEEN 1 AND 5"),
        (Pg::Loud, "id BETWEEN (VALUES (1),2) AND 5 bare low", "SELECT id FROM s10k_users WHERE id BETWEEN (VALUES (1),2) AND 5"),
        (Pg::Loud, "id BETWEEN 1 AND (VALUES (1),2) bare high", "SELECT id FROM s10k_users WHERE id BETWEEN 1 AND (VALUES (1),2)"),
        // ANY / ALL.
        (Pg::Loud, "id = ANY ((VALUES (1),2)) bare", "SELECT id FROM s10k_users WHERE id = ANY ((VALUES (1),2))"),
        (Pg::Loud, "id > ALL ((VALUES (1),2)) bare", "SELECT id FROM s10k_users WHERE id > ALL ((VALUES (1),2))"),
        // IN-list (a bare later row degrades the parse to an InList).
        (Pg::Loud, "id IN (VALUES (1),2) bare", "SELECT id FROM s10k_users WHERE id IN (VALUES (1),2)"),
        (Pg::Loud, "id IN (VALUES (1,2),3) bare wide first", "SELECT id FROM s10k_users WHERE id IN (VALUES (1,2),3)"),
        (Pg::Loud, "id IN (VALUES (1),(2),3) bare last", "SELECT id FROM s10k_users WHERE id IN (VALUES (1),(2),3)"),
        // A multi-cell first row with a bare later row.
        (Pg::Loud, "(id,age) = (VALUES (1,2),3) bare wide first", "SELECT id FROM s10k_users WHERE (id,age) = (VALUES (1,2),3)"),
        // The legal parenthesised siblings stay accepted (the bare-row rule does
        // not touch them).
        (Pg::Ok,   "id = (VALUES (1),(2)) paren ok", "SELECT id FROM s10k_users WHERE id = (VALUES (1),(2))"),
        (Pg::Ok,   "id IN (VALUES (1),(2)) paren ok", "SELECT id FROM s10k_users WHERE id IN (VALUES (1),(2))"),
        (Pg::Ok,   "id = ANY ((VALUES (1),(2))) paren ok", "SELECT id FROM s10k_users WHERE id = ANY ((VALUES (1),(2)))"),
        (Pg::Ok,   "(id,age) = (VALUES (1,2),(3,4)) paren ok", "SELECT id FROM s10k_users WHERE (id,age) = (VALUES (1,2),(3,4))"),

        // ── INSERT ... SELECT * / t.* source arity. The `*` source width is
        // resolved against the source SELECT's OWN FROM scope, so a matching
        // width is accepted and a mismatch is the accurate count error — never a
        // misleading "not modeled". Only a genuinely-unenumerable source (a
        // table function) is loud-unsupported. Each verdict re-verified live. ──
        (Pg::Ok,   "INSERT acc(3) SELECT * acc", "INSERT INTO s10k_accounts (id,user_id,balance) SELECT * FROM s10k_accounts RETURNING id"),
        (Pg::Loud, "INSERT acc(2) SELECT * acc", "INSERT INTO s10k_accounts (id,user_id) SELECT * FROM s10k_accounts RETURNING id"),
        (Pg::Ok,   "INSERT acc no-list SELECT * acc", "INSERT INTO s10k_accounts SELECT * FROM s10k_accounts RETURNING id"),
        (Pg::Ok,   "INSERT acc(3) SELECT a.* acc", "INSERT INTO s10k_accounts (id,user_id,balance) SELECT a.* FROM s10k_accounts a RETURNING id"),
        (Pg::Loud, "INSERT acc(1) SELECT * comp", "INSERT INTO s10k_accounts (id) SELECT * FROM s10k_comp RETURNING id"),
        (Pg::UnmodelableLoud, "INSERT acc(1) SELECT * generate_series", "INSERT INTO s10k_accounts (id) SELECT * FROM generate_series(1,3) RETURNING id"),

        // ── UPDATE / ON CONFLICT DO UPDATE SET (a,b,c) = (SELECT *) source arity.
        // The sub-SELECT `*` source width is expanded against its own FROM scope,
        // so a matching-width assignment is accepted and a mismatch is the
        // accurate UpdateSetArityMismatch. Each verdict re-verified live. ──
        (Pg::Ok,   "SET (3) = (SELECT * acc)", "UPDATE s10k_users SET (name,bio,age)=(SELECT * FROM s10k_accounts LIMIT 1) WHERE id=$1 RETURNING id"),
        (Pg::Loud, "SET (2) = (SELECT * acc)", "UPDATE s10k_users SET (name,bio)=(SELECT * FROM s10k_accounts LIMIT 1) WHERE id=$1 RETURNING id"),
        (Pg::Ok,   "OC SET (3) = (SELECT * acc)", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET (name,bio,age)=(SELECT * FROM s10k_accounts LIMIT 1) RETURNING id"),
        (Pg::Loud, "OC SET (2) = (SELECT * acc)", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET (name,bio)=(SELECT * FROM s10k_accounts LIMIT 1) RETURNING id"),
        (Pg::Loud, "SET (2) = (SELECT * comp)", "UPDATE s10k_users SET (name,bio)=(SELECT * FROM s10k_comp LIMIT 1) WHERE id=$1 RETURNING id"),

        // ── DELETE ... USING — the exact analog of UPDATE ... FROM. The USING
        // relations thread into the DELETE scope, so the WHERE / RETURNING
        // resolve their columns, params type, and a cross-relation typo or a
        // nonexistent USING relation is loud. Each verdict re-verified live. ──
        (Pg::Ok,   "DELETE u USING a WHERE join RETURNING u.id", "DELETE FROM s10k_users u USING s10k_accounts a WHERE a.user_id=u.id RETURNING u.id"),
        (Pg::Loud, "DELETE u USING a WHERE a.bogus", "DELETE FROM s10k_users u USING s10k_accounts a WHERE a.bogus=u.id RETURNING u.id"),
        (Pg::Loud, "DELETE u USING nonexistent", "DELETE FROM s10k_users u USING s10k_nonexistent a WHERE a.x=u.id RETURNING u.id"),
        (Pg::Ok,   "DELETE no-alias USING a qualified", "DELETE FROM s10k_users USING s10k_accounts a WHERE a.user_id=s10k_users.id RETURNING s10k_users.id"),
        (Pg::Loud, "DELETE u USING a WHERE u.bogus", "DELETE FROM s10k_users u USING s10k_accounts a WHERE u.bogus=a.id RETURNING u.id"),
        (Pg::Ok,   "DELETE u USING a param in WHERE", "DELETE FROM s10k_users u USING s10k_accounts a WHERE a.user_id=u.id AND a.balance=$1 RETURNING u.id"),
        (Pg::Ok,   "DELETE u USING a RETURNING a.balance", "DELETE FROM s10k_users u USING s10k_accounts a WHERE a.user_id=u.id RETURNING a.balance"),
        (Pg::Ok,   "DELETE u USING a, comp c", "DELETE FROM s10k_users u USING s10k_accounts a, s10k_comp c WHERE a.user_id=u.id AND c.a=a.balance RETURNING u.id"),
        (Pg::Loud, "DELETE USING unqualified ambiguous id", "DELETE FROM s10k_users USING s10k_accounts WHERE s10k_accounts.user_id=s10k_users.id RETURNING id"),

        // ── ON CONFLICT DO UPDATE arbiter requirement. A `DO UPDATE` needs an
        // arbiter to decide which conflicting row to rewrite; with NO conflict
        // target PG rejects it ("ON CONFLICT DO UPDATE requires inference
        // specification or constraint name") — but `DO NOTHING` with no target
        // is valid (PG infers the arbiter and skips), and any column-list target
        // makes either action valid. Each verdict re-verified live via PREPARE,
        // BOTH directions. ──
        (Pg::Loud, "OC no-target DO UPDATE", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT DO UPDATE SET name='z' RETURNING id"),
        (Pg::Ok,   "OC no-target DO NOTHING", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT DO NOTHING RETURNING id"),
        (Pg::Ok,   "OC (id) DO UPDATE", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET name='z' RETURNING id"),
        (Pg::Ok,   "OC (id) DO NOTHING", "INSERT INTO s10k_users (id,name) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING RETURNING id"),

        // ── DISTINCT ON must be a leading prefix of ORDER BY. PG keeps the first
        // row of each distinct group under the ORDER BY, so every leading ORDER
        // BY key (up to the count of DISTINCT ON expressions) must be a DISTINCT
        // ON expression — matched by sort-key identity, NOT positionally and NOT
        // by raw text (bare/qualified/alias/ordinal spellings of one column all
        // match; an arbitrary expression matches by canonical text), and the
        // distinct list may be LONGER than ORDER BY. No ORDER BY at all is
        // valid. Each verdict re-verified live via PREPARE, BOTH directions. ──
        (Pg::Loud, "DON (name) OB age", "SELECT DISTINCT ON (name) id FROM s10k_users ORDER BY age"),
        (Pg::Loud, "DON (age) OB name,age", "SELECT DISTINCT ON (age) id FROM s10k_users ORDER BY name, age"),
        (Pg::Ok,   "DON (name) OB name,age prefix", "SELECT DISTINCT ON (name) id FROM s10k_users ORDER BY name, age"),
        (Pg::Ok,   "DON (name,age) OB name,age exact", "SELECT DISTINCT ON (name, age) id FROM s10k_users ORDER BY name, age"),
        (Pg::Ok,   "DON (name) no OB", "SELECT DISTINCT ON (name) id FROM s10k_users"),
        (Pg::Ok,   "DON (name,age) OB age,name reordered set", "SELECT DISTINCT ON (name, age) id FROM s10k_users ORDER BY age, name"),
        (Pg::Ok,   "DON (name,age) OB name longer-distinct", "SELECT DISTINCT ON (name, age) id FROM s10k_users ORDER BY name"),
        (Pg::Ok,   "DON (qualified name) OB name", "SELECT DISTINCT ON (s10k_users.name) id FROM s10k_users ORDER BY name"),
        (Pg::Ok,   "DON (u.name) FROM users u OB name", "SELECT DISTINCT ON (u.name) id FROM s10k_users u ORDER BY name"),
        (Pg::Ok,   "DON (qualified name) OB ordinal 1", "SELECT DISTINCT ON (s10k_users.name) name, age FROM s10k_users ORDER BY 1"),
        (Pg::Ok,   "DON (age) alias name OB name", "SELECT DISTINCT ON (age) age AS name FROM s10k_users ORDER BY name"),
        (Pg::Ok,   "DON (age+1) OB age+1", "SELECT DISTINCT ON (age + 1) id FROM s10k_users ORDER BY age + 1"),
        (Pg::Loud, "DON (age+1) OB age+2", "SELECT DISTINCT ON (age + 1) id FROM s10k_users ORDER BY age + 2"),
        (Pg::Loud, "DON mismatch in subquery", "SELECT * FROM (SELECT DISTINCT ON (name) id FROM s10k_users ORDER BY age) sub"),
        (Pg::Loud, "DON outer-paren OB mismatch", "(SELECT DISTINCT ON (name) id FROM s10k_users) ORDER BY id"),
        (Pg::Ok,   "DON outer-paren OB match", "(SELECT DISTINCT ON (name) id, name FROM s10k_users) ORDER BY name"),
        (Pg::Loud, "DON mismatch in UNION arm", "(SELECT DISTINCT ON (name) id, name FROM s10k_users ORDER BY age) UNION (SELECT id, name FROM s10k_users)"),

        // ── Plain SELECT DISTINCT: ORDER BY must appear in the select list. A
        // plain DISTINCT de-duplicates the projected rows, so PG can sort only by
        // a projected value ("for SELECT DISTINCT, ORDER BY expressions must
        // appear in select list"). A key matches a projected expression by
        // sort-key identity (bare / qualified / alias / ordinal of the projected
        // column, or an arbitrary expression by canonical text); a non-projected
        // key is loud. Without DISTINCT the rule does not apply. Each verdict
        // re-verified live via PREPARE, BOTH directions. ──
        (Pg::Loud, "DISTINCT id OB age", "SELECT DISTINCT id FROM s10k_users ORDER BY age"),
        (Pg::Ok,   "DISTINCT id OB id", "SELECT DISTINCT id FROM s10k_users ORDER BY id"),
        (Pg::Ok,   "DISTINCT id OB qualified id", "SELECT DISTINCT id FROM s10k_users ORDER BY s10k_users.id"),
        (Pg::Ok,   "DISTINCT age AS a OB a", "SELECT DISTINCT age AS a FROM s10k_users ORDER BY a"),
        (Pg::Ok,   "DISTINCT id OB ordinal 1", "SELECT DISTINCT id FROM s10k_users ORDER BY 1"),
        (Pg::Ok,   "DISTINCT (id+1) cast OB same", "SELECT DISTINCT (id + 1)::int8 AS x FROM s10k_users ORDER BY (id + 1)::int8"),
        (Pg::Loud, "DISTINCT id OB id+1", "SELECT DISTINCT id FROM s10k_users ORDER BY id + 1"),
        (Pg::Ok,   "DISTINCT id no OB", "SELECT DISTINCT id FROM s10k_users"),
        (Pg::Ok,   "non-DISTINCT OB non-projected", "SELECT id FROM s10k_users ORDER BY age"),
        (Pg::Ok,   "DISTINCT id,name OB name,id", "SELECT DISTINCT id, name FROM s10k_users ORDER BY name, id"),
        (Pg::Loud, "DISTINCT id,name OB name,age", "SELECT DISTINCT id, name FROM s10k_users ORDER BY name, age"),
        // A `DISTINCT *` projects every column, so PG accepts `ORDER BY <any
        // column>`; the engine cannot type a `*` at all, so it stays loud-
        // accurate (the Wildcard error, NOT a misleading DISTINCT/ORDER BY one).
        (Pg::UnmodelableLoud, "DISTINCT * OB age", "SELECT DISTINCT * FROM s10k_users ORDER BY age"),
        // A DISTINCT aggregate matches an ORDER BY of the same aggregate by text.
        (Pg::Ok,   "DISTINCT count(*) OB count(*)", "SELECT DISTINCT count(*) FROM s10k_users ORDER BY count(*)"),

        // ── Redundant parentheses in a sort key. PG strips them at every level
        // before comparing the parsed trees, so a parenthesised key equals its
        // un-parenthesised twin (Ok), while a precedence-significant parenthesis
        // (which changes the tree shape) stays a distinct key (Loud) and operand
        // order is never reassociated. Each verdict re-verified live via PREPARE,
        // BOTH directions. ──
        (Pg::Ok,   "DON ((age)) OB age", "SELECT DISTINCT ON ((age)) id FROM s10k_users ORDER BY age"),
        (Pg::Ok,   "DON (age) OB (age)", "SELECT DISTINCT ON (age) id FROM s10k_users ORDER BY (age)"),
        (Pg::Ok,   "DON ((age+1)) OB age+1", "SELECT DISTINCT ON ((age + 1)) id FROM s10k_users ORDER BY age + 1"),
        (Pg::Ok,   "DON ((age)+1) OB age+1", "SELECT DISTINCT ON ((age) + 1) id FROM s10k_users ORDER BY age + 1"),
        (Pg::Ok,   "DON ((users.age)) OB (age)", "SELECT DISTINCT ON ((s10k_users.age)) id FROM s10k_users ORDER BY (age)"),
        (Pg::Ok,   "DISTINCT (age) OB age", "SELECT DISTINCT (age) FROM s10k_users ORDER BY age"),
        (Pg::Ok,   "DON (age+1) OB (age)+1", "SELECT DISTINCT ON (age + 1) id FROM s10k_users ORDER BY (age) + 1"),
        (Pg::Ok,   "DON (lower((name))) OB lower(name)", "SELECT DISTINCT ON (lower((name))) id FROM s10k_users ORDER BY lower(name)"),
        (Pg::Loud, "DON (age*2+1) OB age*(2+1) precedence", "SELECT DISTINCT ON (age * 2 + 1) id FROM s10k_users ORDER BY age * (2 + 1)"),
        (Pg::Loud, "DON (age+1) OB 1+age no-reassoc", "SELECT DISTINCT ON (age + 1) id FROM s10k_users ORDER BY 1 + age"),

        // ── Empty DISTINCT ON () — a PG syntax error the lenient parser accepts;
        // the engine rejects it loudly. Re-verified live via PREPARE. ──
        (Pg::Loud, "DON () empty", "SELECT DISTINCT ON () id FROM s10k_users"),
        (Pg::Loud, "DON () empty in subquery", "SELECT * FROM (SELECT DISTINCT ON () id FROM s10k_users) sub"),

        // ── Zero-length row comparison `ROW() = ROW()` — silently arity-equal
        // (0 == 0) but a PG error on the comparison operators / BETWEEN / IN;
        // `IS [NOT] DISTINCT FROM` of two empty rows PG ACCEPTS. A non-empty pair
        // is unaffected. Each verdict re-verified live via PREPARE. ──
        (Pg::Loud, "ROW()=ROW() zero-length", "SELECT 1 WHERE ROW() = ROW()"),
        (Pg::Loud, "ROW()<>ROW() zero-length", "SELECT 1 WHERE ROW() <> ROW()"),
        (Pg::Loud, "ROW() BETWEEN ROW() AND ROW()", "SELECT 1 WHERE ROW() BETWEEN ROW() AND ROW()"),
        (Pg::Loud, "ROW() IN (ROW())", "SELECT 1 WHERE ROW() IN (ROW())"),
        (Pg::Ok,   "ROW() IS DISTINCT FROM ROW()", "SELECT 1 WHERE ROW() IS DISTINCT FROM ROW()"),
        (Pg::Ok,   "ROW(1)=ROW(2) non-empty", "SELECT 1 WHERE ROW(1) = ROW(2)"),

        // ── A top-level `WITH` CTE name is in scope for a NESTED subquery's FROM
        // (an IN / EXISTS / scalar / projection-scalar subquery, and a
        // FROM-derived body — at any depth, including a nested `WITH`), so a
        // reference to the outer CTE resolves; a typo inside such a nested
        // subquery stays loud. Direct-FROM / qualified / sibling CTE remain Ok.
        // Each verdict re-verified live via PREPARE. ──
        (Pg::Ok,   "CTE in nested IN-subquery", "WITH a AS (SELECT id FROM s10k_users) SELECT id FROM a WHERE id IN (SELECT id FROM a)"),
        (Pg::Ok,   "CTE in nested EXISTS-subquery", "WITH a AS (SELECT id FROM s10k_users) SELECT id FROM a WHERE EXISTS (SELECT 1 FROM a)"),
        (Pg::Ok,   "CTE in projection scalar subquery", "WITH a AS (SELECT id FROM s10k_users) SELECT (SELECT count(*) FROM a) AS n"),
        (Pg::Ok,   "CTE in scalar comparison subquery", "WITH a AS (SELECT id FROM s10k_users) SELECT id FROM a WHERE id = (SELECT max(id) FROM a)"),
        (Pg::Ok,   "CTE in FROM-derived inside IN-subquery", "WITH a AS (SELECT id FROM s10k_users) SELECT id FROM a WHERE id IN (SELECT x FROM (SELECT id AS x FROM a) d)"),
        (Pg::Ok,   "CTE in direct FROM-derived body", "WITH a AS (SELECT id FROM s10k_users) SELECT x FROM (SELECT id AS x FROM a) d"),
        (Pg::Ok,   "CTE via nested WITH inside derived", "WITH a AS (SELECT id FROM s10k_users) SELECT y FROM (WITH b AS (SELECT id FROM a) SELECT id AS y FROM b) d"),
        (Pg::Ok,   "CTE two subquery levels deep", "WITH a AS (SELECT id FROM s10k_users) SELECT id FROM a WHERE id IN (SELECT id FROM a WHERE id IN (SELECT id FROM a))"),
        (Pg::Ok,   "direct FROM CTE still Ok", "WITH a AS (SELECT id FROM s10k_users) SELECT id FROM a"),
        (Pg::Ok,   "qualified FROM CTE still Ok", "WITH a AS (SELECT id FROM s10k_users) SELECT a.id FROM a"),
        (Pg::Ok,   "sibling CTE still Ok", "WITH a AS (SELECT id FROM s10k_users), b AS (SELECT id FROM a) SELECT id FROM b"),
        (Pg::Ok,   "sibling CTE in nested subquery", "WITH a AS (SELECT id FROM s10k_users), b AS (SELECT id FROM s10k_users WHERE id IN (SELECT id FROM a)) SELECT id FROM b"),
        (Pg::Loud, "typo column in nested CTE subquery", "WITH a AS (SELECT id FROM s10k_users) SELECT id FROM a WHERE id IN (SELECT nope FROM a)"),
        (Pg::Loud, "typo relation in nested CTE subquery", "WITH a AS (SELECT id FROM s10k_users) SELECT id FROM a WHERE id IN (SELECT id FROM nope_rel)"),

        // ── A leading `WITH` over a data-modifying statement (`WITH … DELETE /
        // UPDATE / INSERT … RETURNING`) parses as a `Query` with a DML body; PG
        // accepts it and the WITH belongs to the same statement, so it is routed
        // to the DML inference with the CTEs in scope (FROM / USING / WHERE /
        // RETURNING resolve the CTE). A typo inside stays loud; a DML nested as a
        // real subquery body is a PG syntax error and stays loud. Each verdict
        // re-verified live via PREPARE. ──
        (Pg::Ok,   "WITH + DELETE RETURNING", "WITH a AS (SELECT id FROM s10k_users) DELETE FROM s10k_users WHERE id IN (SELECT id FROM a) RETURNING id"),
        (Pg::Ok,   "WITH + UPDATE RETURNING", "WITH a AS (SELECT id FROM s10k_users) UPDATE s10k_users SET age = age + 1 WHERE id IN (SELECT id FROM a) RETURNING id"),
        (Pg::Ok,   "WITH + INSERT SELECT RETURNING", "WITH a AS (SELECT 99 AS v) INSERT INTO s10k_accounts (id, user_id, balance) SELECT v, 1, 0 FROM a RETURNING id"),
        (Pg::Ok,   "WITH + DELETE USING CTE", "WITH a AS (SELECT id FROM s10k_users WHERE age > 18) DELETE FROM s10k_users u USING a WHERE u.id = a.id RETURNING u.id"),
        (Pg::Ok,   "WITH + UPDATE FROM CTE", "WITH a AS (SELECT id, balance FROM s10k_accounts) UPDATE s10k_users SET age = a.balance FROM a WHERE s10k_users.id = a.id RETURNING s10k_users.id"),
        (Pg::Ok,   "WITH + DELETE RETURNING scalar-sub CTE", "WITH a AS (SELECT count(*) AS c FROM s10k_users) DELETE FROM s10k_users WHERE id = $1 RETURNING id, (SELECT c FROM a) AS total"),
        (Pg::Ok,   "WITH + DELETE param in CTE body", "WITH a AS (SELECT id FROM s10k_users WHERE age = $1) DELETE FROM s10k_users WHERE id IN (SELECT id FROM a) RETURNING id"),
        (Pg::Loud, "WITH + DELETE typo in subquery", "WITH a AS (SELECT id FROM s10k_users) DELETE FROM s10k_users WHERE id IN (SELECT nope FROM a) RETURNING id"),
        (Pg::Loud, "DML nested as subquery body", "SELECT * FROM (DELETE FROM s10k_users RETURNING id) d"),

        // ── An aggregate or window function in a VALUES cell is rejected by PG
        // ("aggregate/window functions are not allowed in VALUES") at every
        // nesting — a top-level VALUES, a `(VALUES …) AS t(c)` derived table, and
        // a `WITH c AS (VALUES …)` CTE body. A literal VALUES stays Ok; a subquery
        // cell is its own level (an aggregate inside it is valid). Each verdict
        // re-verified live via PREPARE. ──
        (Pg::Loud, "max() in derived VALUES", "SELECT m FROM (VALUES (max(1))) AS v(m)"),
        (Pg::Loud, "count(*) in top-level VALUES", "VALUES (count(*))"),
        (Pg::Loud, "bool_and in VALUES", "VALUES (bool_and(true))"),
        (Pg::Loud, "every in VALUES", "VALUES (every(true))"),
        (Pg::Loud, "sum in VALUES", "VALUES (sum(1))"),
        (Pg::Loud, "windowed max in VALUES", "VALUES (max(1) OVER ())"),
        (Pg::Loud, "max() in CTE VALUES", "WITH c AS (VALUES (max(1))) SELECT * FROM c"),
        (Pg::Loud, "typo inside VALUES aggregate", "SELECT m FROM (VALUES (max(bio))) AS v(m)"),
        (Pg::Ok,   "literal top-level VALUES", "VALUES (1), (2)"),
        (Pg::Ok,   "literal derived VALUES", "SELECT m FROM (VALUES (1), (2)) AS v(m)"),
        (Pg::Ok,   "literal CTE VALUES", "WITH c AS (VALUES (1)) SELECT column1 FROM c"),
        (Pg::Ok,   "subquery-cell aggregate in VALUES", "VALUES ((SELECT max(age) FROM s10k_users))"),

        // ── A DELETE TARGET resolves against the CATALOG ONLY, never the
        // enclosing `WITH` set. A CTE name is never a valid DELETE target (PG:
        // "relation does not exist"), and a CTE never shadows a base table as a
        // DELETE target (PG deletes from the base table). A `USING` relation,
        // unlike the target, CAN reference a CTE. Each verdict re-verified live
        // via PREPARE. ──
        (Pg::Loud, "DELETE FROM a CTE target", "WITH a AS (SELECT id FROM s10k_users) DELETE FROM a RETURNING id"),
        (Pg::Ok,   "DELETE FROM same-named CTE hits base table", "WITH s10k_users AS (SELECT id FROM s10k_accounts) DELETE FROM s10k_users RETURNING bio"),
        (Pg::Ok,   "DELETE USING a CTE", "WITH a AS (SELECT id FROM s10k_users WHERE age > 18) DELETE FROM s10k_users u USING a WHERE u.id = a.id RETURNING u.id"),

        // ── An aggregate / window function on the LEFT-HAND side of `x IN
        // (subquery)` in a pre-grouping position is rejected by PG ("aggregate /
        // window functions are not allowed in WHERE"); on the LHS in a
        // projection over an aggregating query, the ungrouped column is the
        // GROUP-BY error. A plain column on the LHS is unaffected. Each verdict
        // re-verified live via PREPARE. ──
        (Pg::Loud, "aggregate on IN-subquery LHS in WHERE", "SELECT id FROM s10k_users WHERE COUNT(*) IN (SELECT id FROM s10k_accounts)"),
        (Pg::Loud, "window on IN-subquery LHS in WHERE", "SELECT id FROM s10k_users WHERE row_number() OVER () IN (SELECT id FROM s10k_accounts)"),
        (Pg::Loud, "ungrouped col with aggregate IN-subquery LHS", "SELECT name, (COUNT(*) IN (SELECT id FROM s10k_accounts)) FROM s10k_users"),
        (Pg::Ok,   "plain column IN-subquery LHS", "SELECT id FROM s10k_users WHERE id IN (SELECT id FROM s10k_accounts)"),

        // ── `FETCH ... WITH TIES` requires an `ORDER BY` (PG: "WITH TIES cannot
        // be specified without ORDER BY clause"), and `FETCH ... PERCENT` is
        // never implemented (PG: syntax error). Both are loud at top level AND in
        // a derived / CTE tail; `WITH TIES` + `ORDER BY` and plain `FETCH ...
        // ROWS ONLY` stay Ok. Each verdict re-verified live via PREPARE. ──
        (Pg::Loud, "FETCH WITH TIES no ORDER BY", "SELECT id FROM s10k_users FETCH FIRST 1 ROWS WITH TIES"),
        (Pg::Ok,   "FETCH WITH TIES + ORDER BY", "SELECT id FROM s10k_users ORDER BY id FETCH FIRST 1 ROWS WITH TIES"),
        (Pg::Loud, "FETCH PERCENT", "SELECT id FROM s10k_users FETCH FIRST 10 PERCENT ROWS ONLY"),
        (Pg::Loud, "FETCH PERCENT + ORDER BY", "SELECT id FROM s10k_users ORDER BY id FETCH FIRST 10 PERCENT ROWS ONLY"),
        (Pg::Ok,   "FETCH FIRST n ROWS ONLY", "SELECT id FROM s10k_users FETCH FIRST 1 ROWS ONLY"),
        (Pg::Loud, "FETCH WITH TIES in derived tail", "SELECT x FROM (SELECT id AS x FROM s10k_users FETCH FIRST 1 ROWS WITH TIES) d"),
        (Pg::Loud, "FETCH WITH TIES in CTE tail", "WITH c AS (SELECT id FROM s10k_users FETCH FIRST 1 ROWS WITH TIES) SELECT id FROM c"),
        (Pg::Ok,   "FETCH WITH TIES + ORDER BY in derived tail", "SELECT x FROM (SELECT id AS x FROM s10k_users ORDER BY id FETCH FIRST 1 ROWS WITH TIES) d"),
        // A `FETCH ... WITH TIES` on a query whose body is a parenthesized inner
        // query is satisfied by the INNER (effective) `ORDER BY` — PG accepts it
        // (the inner sort establishes the order the WITH TIES extends), through
        // any nesting depth and across a parenthesized set operation whose ORDER
        // BY is inside the parentheses. The error stays for the case where NO
        // ORDER BY exists anywhere in the effective chain. Each re-verified live.
        (Pg::Ok,   "parenthesized inner ORDER BY satisfies WITH TIES", "(SELECT id FROM s10k_users ORDER BY id) FETCH FIRST 1 ROWS WITH TIES"),
        (Pg::Ok,   "double-parenthesized inner ORDER BY satisfies WITH TIES", "((SELECT id FROM s10k_users ORDER BY id)) FETCH FIRST 1 ROWS WITH TIES"),
        (Pg::Ok,   "parenthesized inner ORDER BY + OFFSET WITH TIES", "(SELECT id FROM s10k_users ORDER BY id) OFFSET 2 FETCH FIRST 1 ROWS WITH TIES"),
        (Pg::Ok,   "parenthesized set-op inner ORDER BY satisfies WITH TIES", "(SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts ORDER BY id) FETCH FIRST 1 ROWS WITH TIES"),
        (Pg::Loud, "parenthesized no ORDER BY WITH TIES", "(SELECT id FROM s10k_users) FETCH FIRST 1 ROWS WITH TIES"),
        (Pg::Loud, "parenthesized set-op no ORDER BY WITH TIES", "(SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts) FETCH FIRST 1 ROWS WITH TIES"),

        // ── Outer-correlated aggregate ASSOCIATION: SQL binds an aggregate to the
        // query level that OWNS the columns in its arguments, not to the syntactic
        // position it is written in. An aggregate nested in a subquery of an
        // aggregate-FORBIDDING clause (WHERE / JOIN-ON / DML WHERE) whose argument
        // columns belong to the ENCLOSING level associates back to that level and
        // is loud there (PG: "aggregate functions are not allowed in WHERE / JOIN
        // conditions"). An aggregate with NO column arguments (`count(*)`) or one
        // touching an INNER column is the subquery's own and stays Ok. An aggregate
        // associating to an enclosing SELECT's projection / ORDER BY level makes
        // THAT query aggregate, so an ungrouped selected column is the loud
        // UngroupedColumn — while a sole projected subquery aggregate (no ungrouped
        // column to flag) stays Ok. Each verdict re-verified live via PREPARE. ──
        (Pg::Loud, "pure-outer aggregate in WHERE", "SELECT id FROM s10k_users WHERE age > (SELECT min(age) FROM s10k_accounts)"),
        (Pg::Ok,   "own-column aggregate in WHERE subquery", "SELECT id FROM s10k_users WHERE age > (SELECT min(balance) FROM s10k_accounts)"),
        (Pg::Ok,   "count(*) in WHERE subquery", "SELECT id FROM s10k_users WHERE age > (SELECT count(*) FROM s10k_accounts)"),
        (Pg::Loud, "pure-outer aggregate via ALL", "SELECT id FROM s10k_users WHERE age > ALL (SELECT min(age) FROM s10k_accounts)"),
        (Pg::Loud, "pure-outer aggregate via BETWEEN", "SELECT id FROM s10k_users WHERE age BETWEEN (SELECT min(age) FROM s10k_accounts) AND 5"),
        (Pg::Loud, "pure-outer aggregate in JOIN-ON", "SELECT u.id FROM s10k_users u JOIN s10k_accounts a ON a.user_id = (SELECT min(u.age) FROM s10k_accounts b)"),
        (Pg::Ok,   "own-column aggregate in JOIN-ON subquery", "SELECT u.id FROM s10k_users u JOIN s10k_accounts a ON a.user_id = (SELECT min(b.balance) FROM s10k_accounts b)"),
        (Pg::Ok,   "count(*) in JOIN-ON subquery", "SELECT u.id FROM s10k_users u JOIN s10k_accounts a ON a.user_id = (SELECT count(*) FROM s10k_accounts b)"),
        (Pg::Loud, "pure-outer aggregate in DELETE WHERE", "DELETE FROM s10k_users WHERE age > (SELECT min(age) FROM s10k_accounts) RETURNING id"),
        (Pg::Loud, "pure-outer aggregate in UPDATE WHERE", "UPDATE s10k_users SET name='x' WHERE age > (SELECT min(age) FROM s10k_accounts) RETURNING id"),
        (Pg::Loud, "count(outer col) in WHERE subquery", "SELECT id FROM s10k_users u WHERE age > (SELECT count(u.age) FROM s10k_accounts)"),
        (Pg::Loud, "pure-outer aggregate nested two levels in WHERE", "SELECT id FROM s10k_users u WHERE age > (SELECT (SELECT min(u.age) FROM s10k_accounts a3) FROM s10k_accounts a2)"),
        (Pg::Ok,   "mixed inner+outer aggregate in WHERE subquery", "SELECT id FROM s10k_users u WHERE age > (SELECT min(u.age + a2.balance)::int8 FROM s10k_accounts a2)"),
        (Pg::Ok,   "pure-outer aggregate in HAVING subquery (allowed clause)", "SELECT id FROM s10k_users GROUP BY id HAVING id > (SELECT min(age) FROM s10k_accounts)"),
        (Pg::Loud, "promote: projected pure-outer aggregate ungroups id", "SELECT id, (SELECT min(age) FROM s10k_accounts) FROM s10k_users"),
        (Pg::Loud, "promote: two columns one ungrouped", "SELECT id, name, (SELECT min(age) FROM s10k_accounts) FROM s10k_users"),
        (Pg::Ok,   "promote: sole projected pure-outer aggregate", "SELECT (SELECT min(age) FROM s10k_accounts) FROM s10k_users"),
        (Pg::Ok,   "promote: projected own-column aggregate stays row-wise", "SELECT id, (SELECT min(balance) FROM s10k_accounts) FROM s10k_users"),
        (Pg::Ok,   "promote: projected count(*) stays row-wise", "SELECT id, (SELECT count(*) FROM s10k_accounts) FROM s10k_users"),
        (Pg::Ok,   "promote: grouped id with projected pure-outer aggregate", "SELECT id, (SELECT min(age) FROM s10k_accounts) FROM s10k_users GROUP BY id"),
        (Pg::Loud, "promote: ORDER BY pure-outer aggregate ungroups id", "SELECT id FROM s10k_users ORDER BY (SELECT min(age) FROM s10k_accounts)"),

        // ── A data-modifying CTE body (`WITH c AS (DELETE/UPDATE/INSERT ...
        // RETURNING ...)`) is valid PG: the CTE exposes the DML's RETURNING
        // columns, consumed by the outer query. A DML body WITHOUT RETURNING is
        // loud (PG: "WITH query does not have a RETURNING clause"); a typo inside
        // stays loud; a DML nested as a FROM-derived table is a PG syntax error
        // and stays loud. Each verdict re-verified live via PREPARE. ──
        (Pg::Ok,   "DELETE CTE body consumed by SELECT", "WITH c AS (DELETE FROM s10k_accounts WHERE id = 1 RETURNING id) SELECT id FROM c"),
        (Pg::Ok,   "UPDATE CTE body consumed by SELECT", "WITH c AS (UPDATE s10k_accounts SET balance = 0 WHERE id = 1 RETURNING id, balance) SELECT id, balance FROM c"),
        (Pg::Ok,   "INSERT CTE body consumed by SELECT", "WITH c AS (INSERT INTO s10k_accounts (id, user_id, balance) VALUES (1, 1, 0) RETURNING id) SELECT id FROM c"),
        (Pg::Ok,   "DML CTE body param across statement", "WITH c AS (DELETE FROM s10k_accounts WHERE id = $1 RETURNING id) SELECT id FROM c WHERE id > $2"),
        (Pg::Loud, "DML CTE body typo inside", "WITH c AS (DELETE FROM s10k_accounts WHERE nope = 1 RETURNING id) SELECT id FROM c"),
        (Pg::Loud, "DML CTE body without RETURNING", "WITH c AS (DELETE FROM s10k_accounts WHERE id = 1) SELECT id FROM c"),
        (Pg::Loud, "DML nested as FROM-derived table", "SELECT id FROM (DELETE FROM s10k_accounts RETURNING id) d"),

        // ── Multiple LIMIT / OFFSET clauses across a parenthesized-query stack:
        // PostgreSQL treats `LIMIT` and `FETCH FIRST … ROWS` as ONE row-count
        // slot and `OFFSET` as a separate one, each allowed at only ONE level. An
        // outer count over a parenthesized inner query that itself carries the
        // SAME slot is "multiple LIMIT/OFFSET clauses not allowed" — for every
        // spelling, through any nesting depth, and even when the inner LIMIT sits
        // on an inner query whose body is a set operation. The two slots are
        // INDEPENDENT (outer LIMIT over inner OFFSET, and the reverse, are Ok), a
        // count at one level only is Ok, an inner-ORDER-BY-only body still
        // satisfies an outer WITH TIES, and a LIMIT on a set-op ARM does not
        // collide with the outer LIMIT. Each verdict re-verified live via PREPARE.
        (Pg::Loud, "inner LIMIT + outer WITH TIES", "(SELECT id FROM s10k_users ORDER BY id LIMIT 5) FETCH FIRST 1 ROWS WITH TIES"),
        (Pg::Loud, "inner LIMIT + outer LIMIT", "(SELECT id FROM s10k_users LIMIT 5) LIMIT 2"),
        (Pg::Loud, "inner LIMIT + outer FETCH ONLY", "(SELECT id FROM s10k_users LIMIT 5) FETCH FIRST 1 ROWS ONLY"),
        (Pg::Loud, "double-paren inner LIMIT + outer LIMIT", "((SELECT id FROM s10k_users LIMIT 5)) LIMIT 2"),
        (Pg::Loud, "inner OFFSET + outer OFFSET", "(SELECT id FROM s10k_users OFFSET 5) OFFSET 2"),
        (Pg::Loud, "deep LIMIT through OFFSET layer + outer LIMIT", "((SELECT id FROM s10k_users LIMIT 5) OFFSET 1) LIMIT 2"),
        (Pg::Loud, "inner set-op-query LIMIT + outer LIMIT", "(SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts LIMIT 5) LIMIT 2"),
        (Pg::Loud, "multiple LIMIT inside a derived table", "SELECT id FROM ((SELECT id FROM s10k_users LIMIT 5) LIMIT 2) t"),
        (Pg::Ok,   "inner ORDER BY only + outer WITH TIES", "(SELECT id FROM s10k_users ORDER BY id) FETCH FIRST 1 ROWS WITH TIES"),
        (Pg::Ok,   "inner LIMIT + outer OFFSET (separate slots)", "(SELECT id FROM s10k_users LIMIT 5) OFFSET 2"),
        (Pg::Ok,   "inner OFFSET + outer LIMIT (separate slots)", "(SELECT id FROM s10k_users OFFSET 2) LIMIT 5"),
        (Pg::Ok,   "set-op arm LIMIT + outer LIMIT (no collision)", "((SELECT id FROM s10k_users LIMIT 5) UNION SELECT id FROM s10k_accounts) LIMIT 2"),

        // ── Pure-outer aggregate ASSOCIATION (the subquery's LOCAL checks): an
        // aggregate is OWNED by the query level whose columns its arguments
        // reference. An aggregate written in a subquery clause whose argument
        // columns are PURE-OUTER (they reference ONLY an enclosing level) is the
        // enclosing level's own — PostgreSQL validates it THERE, where it may be
        // legal (an outer HAVING) — so the subquery's local placement / nesting
        // checks must IGNORE it. An aggregate touching the subquery's OWN column
        // is the subquery's own and stays loud where the subquery forbids / nests
        // it. Each verdict re-verified live via PREPARE.
        (Pg::Ok,   "pure-outer aggregate in subquery WHERE (outer HAVING owns it)", "SELECT u.id FROM s10k_users u GROUP BY u.id HAVING (SELECT count(*) FROM s10k_accounts a WHERE a.balance > sum(u.age)) > 0"),
        (Pg::Ok,   "pure-outer aggregate not nested in inner aggregate", "SELECT u.id FROM s10k_users u GROUP BY u.id HAVING (SELECT bool_or(a.balance > min(u.age)) FROM s10k_accounts a)"),
        (Pg::Ok,   "pure-outer aggregate sole subquery projection", "SELECT u.id FROM s10k_users u GROUP BY u.id HAVING (SELECT min(u.age) FROM s10k_accounts a WHERE a.balance > 0) > 0"),
        (Pg::Loud, "own-column aggregate in subquery WHERE", "SELECT u.id FROM s10k_users u WHERE u.age > (SELECT sum(a.balance) FROM s10k_accounts a WHERE a.balance > sum(a.balance))"),
        (Pg::Loud, "genuinely-nested own aggregate in subquery", "SELECT u.id FROM s10k_users u GROUP BY u.id HAVING (SELECT bool_or(min(a.balance) > 0) FROM s10k_accounts a)"),
        (Pg::Loud, "window inside subquery's own aggregate", "SELECT u.id FROM s10k_users u GROUP BY u.id HAVING (SELECT sum(count(*) OVER ()) FROM s10k_accounts a) > 0"),

        // ── Co-owned NESTING: two aggregates written one inside the other that
        // BOTH float to the SAME enclosing query level (their column arguments are
        // pure-outer to that level, none touching the subquery's own column) are a
        // real nesting at THAT level — PostgreSQL "aggregate function calls cannot
        // be nested" — even though the subquery's own per-level walk skips both as
        // belonging to an enclosing level. A window inside such a pure-outer
        // aggregate is the contained-window error. The boundary STAYS Ok: when the
        // OUTER aggregate touches a LOCAL column it is the subquery's own and the
        // inner pure-outer aggregate is a DIFFERENT level (not nested). Each
        // verdict re-verified live via EXPLAIN.
        (Pg::Loud, "co-owned min(max(u)) in HAVING subquery", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT min(max(u.age)) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "co-owned count(sum(u)) in HAVING subquery", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT count(sum(u.age)) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "co-owned bool_or(min(u)>0) in HAVING subquery", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT bool_or(min(u.age) > 0) FROM s10k_accounts a)"),
        (Pg::Loud, "co-owned bool_or(min(u)>0) in projection subquery", "SELECT name,(SELECT bool_or(min(u.age) > 0) FROM s10k_accounts a) FROM s10k_users u GROUP BY name"),
        (Pg::Loud, "co-owned min(max(u)) in ORDER BY subquery", "SELECT name FROM s10k_users u GROUP BY name ORDER BY (SELECT min(max(u.age)) FROM s10k_accounts a)"),
        (Pg::Loud, "co-owned min over FILTERed agg in HAVING subquery", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT count(*) FILTER (WHERE min(u.age) > 0) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "co-owned min(abs(max(u))) scalar wrapper", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT min(abs(max(u.age))) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "co-owned nest inside a local aggregate", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT bool_or(a.balance > min(max(u.age))) FROM s10k_accounts a)"),
        (Pg::Loud, "co-owned pure-outer agg contains window", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT sum(count(u.age) OVER ())::int8 FROM s10k_accounts a) > 0"),
        (Pg::Loud, "co-owned pure-outer agg contains plain window", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT min(u.age + (row_number() OVER ())::int) FROM s10k_accounts a) > 0"),
        (Pg::Ok,   "boundary local-outer over pure-outer inner (different levels)", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT bool_or(a.balance > min(u.age)) FROM s10k_accounts a)"),
        (Pg::Ok,   "boundary single pure-outer aggregate (no nest)", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT min(u.age) FROM s10k_accounts a) > 0"),
        (Pg::Ok,   "boundary local aggregate over local column (no nest)", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT sum(a.balance) FROM s10k_accounts a) > 0"),

        // SUBQUERY-CROSSING nesting: the inner aggregate is written in a scalar
        // SUBQUERY of the outer aggregate's argument, yet associates with the
        // outer aggregate's OWN writing level (or a level enclosing it). The
        // per-level walks stop at the subquery boundary, so neither level sees
        // the nesting; it is caught across the boundary. PostgreSQL rejects each
        // ("aggregate function calls cannot be nested"). The BOUNDARY stays Ok:
        // when the inner aggregate reaches its OWN subquery's level (inner of the
        // outer aggregate's level), the two associate with DIFFERENT levels and
        // the nesting is legal. Each verdict re-verified live via PREPARE.
        (Pg::Loud, "crossing max((SELECT min(u))) reaches outer u", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max((SELECT min(u.age) FROM s10k_accounts b)) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "crossing max((SELECT min(a))) reaches max level a", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max((SELECT min(a.balance) FROM s10k_accounts b)) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "crossing sum((SELECT count(u)))", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT sum((SELECT count(u.age) FROM s10k_accounts b)) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "crossing max(u.age + (SELECT min(a)))", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max(u.age + (SELECT min(a.balance) FROM s10k_accounts b)) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "crossing three-level deep max(count(min(u)))", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max((SELECT count((SELECT min(u.age) FROM s10k_accounts c)) FROM s10k_accounts b)) FROM s10k_accounts a) > 0"),
        (Pg::Ok,   "boundary crossing max((SELECT min(b))) inner own level", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max((SELECT min(b.balance) FROM s10k_accounts b)) FROM s10k_accounts a) > 0"),
        (Pg::Ok,   "boundary crossing inner min over both b and a (innermost b)", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max((SELECT min(b.balance + a.balance) FROM s10k_accounts b)) FROM s10k_accounts a) > 0"),
        (Pg::Ok,   "boundary crossing outer max has direct local column", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max((SELECT min(b.balance) FROM s10k_accounts b) + a.balance) FROM s10k_accounts a) > 0"),

        // ── `<col> <op> ANY/ALL/SOME(ARRAY[...])` element parameters: PostgreSQL
        // types `<col>` against the array's ELEMENT type. An array CONSTRUCTOR of
        // bare placeholders resolves to `text`, so the comparison — and the
        // element params typing to `text` — succeeds only when the column is a text
        // column; a non-text column has no `int = text` member operator and PG
        // rejects it (so the engine stays loud, cast-required). An `ARRAY[...]::T[]`
        // cast pins the element type to `T`. The reversed `$1 = ANY(ARRAY[col])`
        // fixes the element type to the COLUMN's type, so the left param types to
        // it for any column type. A bare ARRAY-typed parameter `col = ANY($1)` is
        // accepted by PG (it types `$1` to `bigint[]`), but that array type is
        // outside the scalar set this engine types, so the engine deliberately
        // rejects it loud-accurately (UnmodelableLoud). Each parameter set
        // re-verified live via PREPARE + parameter_types.
        (Pg::Ok,   "name = ANY(ARRAY[$1,$2]) text", "SELECT id FROM s10k_users WHERE name = ANY(ARRAY[$1, $2])"),
        (Pg::Ok,   "name = ALL(ARRAY[$1,$2]) text", "SELECT id FROM s10k_users WHERE name = ALL(ARRAY[$1, $2])"),
        (Pg::Loud, "id = ANY(ARRAY[$1]) bigint col rejects text element", "SELECT id FROM s10k_users WHERE id = ANY(ARRAY[$1])"),
        (Pg::Loud, "balance = ANY(ARRAY[$1,$2]) int col rejects text element", "SELECT id FROM s10k_accounts WHERE balance = ANY(ARRAY[$1, $2])"),
        (Pg::Ok,   "name = ANY(ARRAY[$1,$2]::text[]) cast", "SELECT id FROM s10k_users WHERE name = ANY(ARRAY[$1, $2]::text[])"),
        (Pg::Ok,   "id = ANY(ARRAY[$1,$2]::int8[]) cast", "SELECT id FROM s10k_users WHERE id = ANY(ARRAY[$1, $2]::int8[])"),
        (Pg::Ok,   "balance = ANY(ARRAY[$1]::int4[]) cast", "SELECT id FROM s10k_accounts WHERE balance = ANY(ARRAY[$1]::int4[])"),
        (Pg::Ok,   "$1 = ANY(ARRAY[name]) reversed text member", "SELECT id FROM s10k_users WHERE $1 = ANY(ARRAY[name])"),
        (Pg::Ok,   "$1 = ANY(ARRAY[id]) reversed bigint member", "SELECT id FROM s10k_users WHERE $1 = ANY(ARRAY[id])"),
        (Pg::Ok,   "$1 = ANY(ARRAY[balance]) reversed int member", "SELECT id FROM s10k_accounts WHERE $1 = ANY(ARRAY[balance])"),
        (Pg::Ok,   "$1 = ANY(ARRAY[id,$2]) both from column member", "SELECT id FROM s10k_users WHERE $1 = ANY(ARRAY[id, $2])"),
        (Pg::Ok,   "name = ANY(ARRAY[bio,$1]) element from text member", "SELECT id FROM s10k_users WHERE name = ANY(ARRAY[bio, $1])"),
        (Pg::UnmodelableLoud, "id = ANY($1) bare array param (PG types $1 bigint[], outside the scalar set)", "SELECT id FROM s10k_users WHERE id = ANY($1)"),
        // A LITERAL or CAST member fixes the element type even with no column
        // member, so each placeholder element types from it (PG parameter_types
        // verified: `{integer}` / `{bigint,bigint}` etc.). The reversed
        // `$1 = ANY(ARRAY[1,2])` types the left param from the literal-fixed
        // element type.
        (Pg::Ok,   "id = ANY(ARRAY[1,$1]) int literal member", "SELECT id FROM s10k_users WHERE id = ANY(ARRAY[1, $1])"),
        (Pg::Ok,   "id = ANY(ARRAY[1,$1,$2]) int literal member", "SELECT id FROM s10k_users WHERE id = ANY(ARRAY[1, $1, $2])"),
        (Pg::Ok,   "id = ANY(ARRAY[$1::int8,$2]) cast member", "SELECT id FROM s10k_users WHERE id = ANY(ARRAY[$1::int8, $2])"),
        (Pg::Ok,   "balance = ANY(ARRAY[$1,99]) int literal member", "SELECT id FROM s10k_accounts WHERE balance = ANY(ARRAY[$1, 99])"),
        (Pg::Ok,   "$1 = ANY(ARRAY[1,2]) left from literal element", "SELECT id FROM s10k_users WHERE $1 = ANY(ARRAY[1, 2])"),

        // ── FOR UPDATE / SHARE locking clauses + FOR XML / JSON: a `FOR { UPDATE
        // | SHARE } OF <relation>` target must be an UNQUALIFIED name of a base
        // table or derived subquery in the query's FROM clause (NOT a
        // schema-qualified name, NOT a CTE reference, NOT a name absent from
        // FROM). A bare lock is Ok on a plain row-wise SELECT, but PostgreSQL
        // cannot row-lock the result of a set operation, an aggregate / GROUP BY
        // query, or a DISTINCT query. A `FOR XML` / `FOR JSON` clause is a
        // SQL-Server form PostgreSQL does not have. Each verdict re-verified live.
        (Pg::Loud, "FOR UPDATE OF relation not in FROM", "SELECT id FROM s10k_users FOR UPDATE OF s10k_accounts"),
        (Pg::Loud, "FOR UPDATE OF aliased relation by bare name", "SELECT id FROM s10k_users u FOR UPDATE OF s10k_users"),
        (Pg::Loud, "FOR UPDATE OF schema-qualified relation", "SELECT id FROM s10k_users FOR UPDATE OF public.s10k_users"),
        (Pg::Loud, "FOR UPDATE OF a CTE reference", "WITH c AS (SELECT id FROM s10k_users) SELECT id FROM c FOR UPDATE OF c"),
        (Pg::Loud, "FOR UPDATE on a set operation", "(SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts) FOR UPDATE"),
        (Pg::Loud, "FOR UPDATE with GROUP BY", "SELECT id FROM s10k_users GROUP BY id FOR UPDATE"),
        (Pg::Loud, "FOR UPDATE with DISTINCT", "SELECT DISTINCT id FROM s10k_users FOR UPDATE"),
        (Pg::Loud, "FOR XML RAW", "SELECT id FROM s10k_users FOR XML RAW"),
        (Pg::Loud, "FOR JSON AUTO", "SELECT id FROM s10k_users FOR JSON AUTO"),
        (Pg::Ok,   "FOR UPDATE OF relation in FROM", "SELECT id FROM s10k_users FOR UPDATE OF s10k_users"),
        (Pg::Ok,   "FOR UPDATE OF aliased relation by alias", "SELECT id FROM s10k_users u FOR UPDATE OF u"),
        (Pg::Ok,   "bare FOR UPDATE", "SELECT id FROM s10k_users FOR UPDATE"),
        (Pg::Ok,   "bare FOR SHARE", "SELECT id FROM s10k_users FOR SHARE"),
        (Pg::Ok,   "FOR UPDATE NOWAIT", "SELECT id FROM s10k_users FOR UPDATE NOWAIT"),
        (Pg::Ok,   "FOR UPDATE OF a joined relation's alias", "SELECT u.id FROM s10k_users u JOIN s10k_accounts a ON a.user_id = u.id FOR UPDATE OF a"),
        (Pg::Ok,   "FOR UPDATE OF a derived subquery", "SELECT t.id FROM (SELECT id FROM s10k_users) t FOR UPDATE OF t"),
        (Pg::Ok,   "FOR UPDATE inside a derived body", "SELECT id FROM (SELECT id FROM s10k_users FOR UPDATE) t"),

        // ── FOR UPDATE / SHARE over a NON-LOCKABLE NESTED source: a lock binds
        // to the rows the query reads, so the LOCKED SOURCE must itself be a
        // plain row-wise SELECT. (1) A lock inside a set-operation ARM is loud
        // ("FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT") — the arm
        // would otherwise be validated as a standalone query, losing the set-op
        // context. (2) A bare lock locks ALL the FROM relations, so a DERIVED
        // subquery whose body groups / aggregates / applies HAVING / dedups /
        // windows / is a set operation makes the lock loud, naming the inner
        // clause. (3) An `OF <name>` lock whose named source is non-lockable is
        // loud, while an `OF <plain table>` lock stays Ok beside a non-lockable
        // derived sibling. (4) A `FOR UPDATE` inside a from-derived / IN / EXISTS
        // PLAIN subquery is a separate query level whose lock is legal. Each
        // verdict re-verified live via PREPARE, BOTH directions. ──
        (Pg::Loud, "lock inside a set-op arm", "(SELECT id FROM s10k_users FOR UPDATE) UNION (SELECT id FROM s10k_accounts)"),
        (Pg::Loud, "lock inside a nested set-op arm", "((SELECT id FROM s10k_users FOR UPDATE) UNION (SELECT id FROM s10k_accounts)) UNION (SELECT a FROM s10k_comp)"),
        (Pg::Loud, "lock inside the right set-op arm", "(SELECT id FROM s10k_accounts) EXCEPT (SELECT id FROM s10k_accounts FOR UPDATE)"),
        (Pg::Loud, "bare lock over derived GROUP BY", "SELECT id FROM (SELECT id FROM s10k_users GROUP BY id) x FOR UPDATE"),
        (Pg::Loud, "bare lock over derived DISTINCT", "SELECT id FROM (SELECT DISTINCT id FROM s10k_users) x FOR UPDATE"),
        (Pg::Loud, "bare lock comma derived UNION", "SELECT u.id FROM s10k_users u, (SELECT id FROM s10k_users UNION SELECT id FROM s10k_accounts) x FOR UPDATE"),
        (Pg::Loud, "bare lock comma derived aggregate", "SELECT u.id FROM s10k_users u, (SELECT count(*) AS c FROM s10k_users) x FOR UPDATE"),
        (Pg::Loud, "bare lock comma derived HAVING", "SELECT u.id FROM s10k_users u, (SELECT user_id FROM s10k_accounts GROUP BY user_id HAVING count(*) > 0) x FOR UPDATE"),
        (Pg::Loud, "bare lock comma derived window", "SELECT u.id FROM s10k_users u, (SELECT id, count(*) OVER () AS c FROM s10k_accounts) x FOR UPDATE"),
        (Pg::Loud, "top-level window lock", "SELECT id, count(*) OVER () AS c FROM s10k_users FOR UPDATE"),
        (Pg::Loud, "OF the non-lockable derived by name", "SELECT x.c FROM (SELECT count(*) AS c FROM s10k_users) x FOR UPDATE OF x"),
        (Pg::Ok,   "OF plain table beside non-lockable derived sibling", "SELECT u.id FROM s10k_users u, (SELECT count(*) AS c FROM s10k_users) x FOR UPDATE OF u"),
        (Pg::Ok,   "FOR UPDATE inside an IN plain subquery", "SELECT id FROM s10k_accounts WHERE user_id IN (SELECT id FROM s10k_users FOR UPDATE)"),
        (Pg::Ok,   "FOR UPDATE inside an EXISTS plain subquery", "SELECT id FROM s10k_accounts a WHERE EXISTS (SELECT 1 FROM s10k_users u WHERE u.id=a.user_id FOR UPDATE)"),
        (Pg::Ok,   "bare lock over a plain derived source", "SELECT t.id FROM (SELECT id FROM s10k_users) t FOR UPDATE"),
        (Pg::Ok,   "lock on the plain inner derived inside a set-op arm", "(SELECT id FROM (SELECT id FROM s10k_users FOR UPDATE) t) UNION (SELECT id FROM s10k_accounts)"),

        // ── A WINDOW function in the QUERY-LEVEL ORDER BY makes the result
        // non-lockable: the window lives on the `Query`'s ORDER BY (not the
        // `Select`), and PG rejects every combination with "FOR UPDATE is not
        // allowed with window functions" — including the parenthesized form (the
        // window's ORDER BY is inside the parentheses) and the windowed `OF`
        // form. The SAME ORDER BY window WITHOUT a lock is a valid sort PG
        // accepts. A window in a DIFFERENT query level than the lock (a deeper
        // sub-SELECT's own ORDER BY, or an outer ORDER BY beside a lock that
        // lives only on an inner subquery) stays Ok. Both directions live-PG
        // verified. ──
        (Pg::Loud, "FOR UPDATE + window in ORDER BY", "SELECT id FROM s10k_users ORDER BY row_number() OVER () FOR UPDATE"),
        (Pg::Loud, "FOR UPDATE OF + window in ORDER BY", "SELECT id FROM s10k_users ORDER BY row_number() OVER () FOR UPDATE OF s10k_users"),
        (Pg::Loud, "FOR UPDATE + window in ORDER BY (paren)", "(SELECT id FROM s10k_users ORDER BY row_number() OVER ()) FOR UPDATE"),
        (Pg::Loud, "FOR UPDATE + ranked ORDER BY window", "SELECT id FROM s10k_users ORDER BY rank() OVER (ORDER BY age) FOR UPDATE"),
        (Pg::Loud, "FOR SHARE + window in ORDER BY", "SELECT id FROM s10k_users ORDER BY row_number() OVER () FOR SHARE"),
        (Pg::Ok,   "window in ORDER BY, no lock", "SELECT id FROM s10k_users ORDER BY row_number() OVER ()"),
        (Pg::Ok,   "window in ORDER BY, no lock (paren)", "(SELECT id FROM s10k_users ORDER BY row_number() OVER ())"),
        (Pg::Ok,   "outer lock, window in deeper IN-subquery ORDER BY", "SELECT id FROM s10k_accounts WHERE user_id IN (SELECT id FROM s10k_users ORDER BY row_number() OVER ()) FOR UPDATE"),
        (Pg::Ok,   "inner-subquery lock, window in outer ORDER BY", "SELECT id FROM s10k_accounts WHERE user_id IN (SELECT id FROM s10k_users FOR UPDATE) ORDER BY row_number() OVER ()"),

        // ── CROSSING-NESTED-AGGREGATE upper bound: an aggregate written inside
        // another aggregate's argument is nested ONLY when the inner aggregate
        // associates with the outer aggregate's OWN binding level. The outer
        // aggregate binds to the query level of its direct columns; an inner
        // aggregate reaching a level STRICTLY beyond that binding level is a
        // DIFFERENT level — not nested, PG ACCEPTS. Within the band — from the
        // writing level up to the binding level — it IS the nesting PG rejects.
        // Each verdict re-verified live via PREPARE, BOTH directions. ──
        (Pg::Ok,   "outer max binds local a.balance, inner min reaches outer u (subq)", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max(a.balance + (SELECT min(u.age) FROM s10k_accounts b)) FROM s10k_accounts a) > 0"),
        (Pg::Ok,   "outer max binds local a.balance, inner min reaches outer u (direct)", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max(a.balance + min(u.age)) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "outer max no local col, inner min reaches outer u", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max(min(u.age)) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "outer max no local col, inner min reaches outer u (paren)", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max((min(u.age))) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "both aggregates at same local level a", "SELECT max(min(a.balance)) FROM s10k_accounts a"),
        (Pg::Loud, "outer max binds enclosing u.age, inner min reaches local a (in band)", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max(u.age + min(a.balance)) FROM s10k_accounts a) > 0"),
        (Pg::Loud, "outer max binds enclosing u.age, inner min reaches same u", "SELECT name FROM s10k_users u GROUP BY name HAVING (SELECT max(u.age + min(u.age)) FROM s10k_accounts a) > 0"),

        // ── MIN/MAX over a redundantly-parenthesised COLUMN argument: PG
        // evaluates `MAX((age))` as the maximum of the column `age`, the
        // parentheses redundant. The argument is paren-stripped, so the column
        // type is projected exactly as the bare `MAX(age)`. A non-column inner
        // (arithmetic, cast, scalar subquery) is a result type the engine does
        // not model: PG ACCEPTS it, but the engine stays loud-accurate
        // (UnmodelableLoud, cast-required), exactly as the unparenthesised form
        // does. Each verdict re-verified live via PREPARE + pg_typeof. ──
        (Pg::Ok,   "MAX((age)) paren column arg", "SELECT max((age)) FROM s10k_users"),
        (Pg::Ok,   "MIN((users.age)) paren qualified arg", "SELECT min((s10k_users.age)) FROM s10k_users"),
        (Pg::Ok,   "MAX(((age))) double-paren arg", "SELECT max(((age))) FROM s10k_users"),
        (Pg::Ok,   "MAX(age) bare arg unchanged", "SELECT max(age) FROM s10k_users"),
        (Pg::UnmodelableLoud, "MAX((age+1)) non-column inner stays cast-required", "SELECT max((age+1)) FROM s10k_users"),

        // Bare NULL typed by a sibling (CASE result / COALESCE arg / set-op arm
        // / VALUES row). PG types the construct from the typed sibling and only
        // rejects an ALL-NULL construct. Each verdict cross-checked live.
        (Pg::Ok,   "CASE ELSE NULL types from THEN", "SELECT CASE WHEN id>0 THEN name ELSE NULL END FROM s10k_users"),
        (Pg::Ok,   "simple CASE ELSE NULL", "SELECT CASE age WHEN 1 THEN name ELSE NULL END FROM s10k_users"),
        (Pg::Ok,   "COALESCE(name,NULL)", "SELECT COALESCE(name, NULL) FROM s10k_users"),
        (Pg::Ok,   "COALESCE(NULL,name)", "SELECT COALESCE(NULL, name) FROM s10k_users"),
        (Pg::Ok,   "COALESCE(age,NULL)", "SELECT COALESCE(age, NULL) FROM s10k_users"),
        (Pg::Ok,   "UNION SELECT NULL arm", "SELECT name FROM s10k_users UNION SELECT NULL"),
        (Pg::Ok,   "UNION leading NULL arm", "SELECT NULL UNION SELECT name FROM s10k_users"),
        (Pg::Ok,   "VALUES (1),(NULL) row sibling", "SELECT x FROM (VALUES (1),(NULL)) AS t(x)"),
        (Pg::Ok,   "COALESCE widen + NULL", "SELECT COALESCE(balance, user_id, NULL) FROM s10k_accounts"),
        // All-typeless constructs: PG ACCEPTS them, typing the unknown as `text`
        // (its default); the engine deliberately stays loud rather than guess
        // that default (UnmodelableLoud — verified live: each PG-types `text`).
        (Pg::UnmodelableLoud, "all-NULL CASE engine-loud", "SELECT CASE WHEN id>0 THEN NULL ELSE NULL END FROM s10k_users"),
        (Pg::UnmodelableLoud, "all-NULL COALESCE engine-loud", "SELECT COALESCE(NULL, NULL) FROM s10k_users"),
        (Pg::UnmodelableLoud, "both-NULL UNION engine-loud", "SELECT NULL FROM s10k_users UNION SELECT NULL"),
        (Pg::UnmodelableLoud, "bare NULL projection engine-loud", "SELECT NULL FROM s10k_users"),
        // text+int+NULL: PG genuinely REJECTS (no common type even with a NULL).
        (Pg::Loud, "text+int+NULL no common type", "SELECT COALESCE(name, age, NULL) FROM s10k_users"),

        // Reserved keyword as an UNQUOTED relation alias / CTE name: PG rejects
        // it as a parse-time syntax error in every relation-alias position; the
        // parser leniently accepts it, so the engine rejects it loudly. A QUOTED
        // alias, a NON-reserved keyword, and the `ONLY` modifier stay accepted.
        // Each verdict cross-checked live.
        (Pg::Loud, "bare table alias reserved", "SELECT name FROM s10k_users distinct"),
        (Pg::Loud, "AS table alias reserved", "SELECT name FROM s10k_users AS select"),
        (Pg::Loud, "derived-table alias reserved", "SELECT x FROM (SELECT 1 AS x) user"),
        (Pg::Loud, "JOIN alias reserved", "SELECT s10k_users.id FROM s10k_users JOIN s10k_accounts default ON default.user_id=s10k_users.id"),
        (Pg::Loud, "CTE name reserved", "WITH select AS (SELECT 1 AS x) SELECT x FROM select"),
        (Pg::Loud, "INSERT target alias reserved", "INSERT INTO s10k_users AS distinct (id,name) VALUES ($1,$2) RETURNING id"),
        (Pg::Loud, "UPDATE target alias reserved", "UPDATE s10k_users AS distinct SET name='z' WHERE id=$1 RETURNING id"),
        (Pg::Loud, "DELETE target alias reserved", "DELETE FROM s10k_users AS distinct WHERE id=$1 RETURNING id"),
        (Pg::Loud, "reserved keyword after table", "SELECT name FROM s10k_users only"),
        (Pg::Ok,   "quoted reserved alias OK", "SELECT name FROM s10k_users AS \"user\""),
        (Pg::Ok,   "non-reserved alias OK", "SELECT value.name FROM s10k_users value"),
        (Pg::Ok,   "ONLY modifier OK", "SELECT name FROM ONLY s10k_users"),
        (Pg::Ok,   "non-reserved CTE name OK", "WITH t AS (SELECT 1 AS x) SELECT x FROM t"),
        (Pg::Ok,   "AS reserved column alias permissive", "SELECT id AS select FROM s10k_users"),
        // A column-alias LIST forbids a reserved keyword by the same rule as a
        // relation alias (PG syntax error); a quoted / non-reserved one is OK.
        (Pg::Loud, "derived column-alias list reserved", "SELECT x FROM (SELECT 1 AS x) AS t(select)"),
        (Pg::Loud, "CTE column-alias list reserved", "WITH t(select) AS (SELECT 1 AS x) SELECT 1"),
        (Pg::Ok,   "derived column-alias list non-reserved", "SELECT p FROM (SELECT 1 AS x) AS t(p)"),

        // Column OUTPUT alias bare-label rule. PostgreSQL accepts ANY keyword as
        // a column label AFTER `AS` (`SELECT 1 AS select`), but a BARE trailing
        // label (no `AS`) must follow the stricter `BareColLabel` grammar, so a
        // bare-label-INVALID keyword there is a parse-time syntax error
        // (`SELECT age year` -> `syntax error at or near "year"`). The parser
        // drops the `AS`-presence (it parses `SELECT id on` the same as `SELECT
        // id AS on`), so the engine reconstructs the bare form from the source
        // and rejects it. Each verdict cross-checked live: the bare forms below
        // are a PG syntax error, the `AS`/quoted/bare-valid forms PG accepts.
        // The set spans all keyword categories — the unreserved `year` / `over`
        // / `filter` / `varying` are valid RELATION aliases yet invalid bare
        // column labels.
        (Pg::Loud, "bare col alias year",        "SELECT age year FROM s10k_users"),
        (Pg::Loud, "bare col alias on",          "SELECT age on FROM s10k_users"),
        (Pg::Loud, "bare col alias over",        "SELECT age over FROM s10k_users"),
        (Pg::Loud, "bare col alias window",      "SELECT age window FROM s10k_users"),
        (Pg::Loud, "bare col alias filter",      "SELECT age filter FROM s10k_users"),
        (Pg::Loud, "bare col alias within",      "SELECT age within FROM s10k_users"),
        (Pg::Loud, "bare col alias varying",     "SELECT age varying FROM s10k_users"),
        (Pg::Loud, "bare col alias precision",   "SELECT age precision FROM s10k_users"),
        (Pg::Loud, "bare col alias create",      "SELECT age create FROM s10k_users"),
        (Pg::Loud, "bare col alias char",        "SELECT age char FROM s10k_users"),
        (Pg::Ok,   "AS col alias year",          "SELECT age AS year FROM s10k_users"),
        (Pg::Ok,   "AS col alias over",          "SELECT age AS over FROM s10k_users"),
        (Pg::Ok,   "AS col alias window",        "SELECT age AS window FROM s10k_users"),
        (Pg::Ok,   "quoted col alias year",      "SELECT age \"year\" FROM s10k_users"),
        (Pg::Ok,   "bare-VALID col alias value", "SELECT age value FROM s10k_users"),
        (Pg::Ok,   "ordinary ident col alias",   "SELECT age yr FROM s10k_users"),
        // The alias literally spelled `as` is the `AS` keyword used as a label.
        // PG accepts it after `AS` (`SELECT age AS as` -> a column named `as`,
        // case-insensitive in `AS`), typed identically to the quoted `"as"`. A
        // bare `as` cannot exist (the lone `as` is consumed as the `AS` keyword,
        // a parse error), so `as` reaches the engine only AS-introduced. Each
        // verdict PREPARE-checked live.
        (Pg::Ok,   "AS col alias `as` keyword",  "SELECT age AS as FROM s10k_users"),
        (Pg::Ok,   "col alias `as` lowercased",  "SELECT age as as FROM s10k_users"),
        (Pg::Ok,   "quoted col alias `as`",      "SELECT age \"as\" FROM s10k_users"),
        // Bare alias rule applies in EVERY projection context, not only the top
        // level (cross-checked live: each is a PG syntax error).
        (Pg::Loud, "bare col alias in derived",  "SELECT y FROM (SELECT age year FROM s10k_users) t(y)"),
        (Pg::Loud, "bare col alias in CTE body", "WITH c AS (SELECT age year FROM s10k_users) SELECT y FROM c(y)"),
        (Pg::Loud, "bare col alias in set-op arm","SELECT id x FROM s10k_users UNION SELECT age year FROM s10k_users"),
        (Pg::Loud, "bare col alias in IN-subq",  "SELECT id FROM s10k_users WHERE id IN (SELECT age year FROM s10k_users)"),
        (Pg::Loud, "bare col alias in scalar subq","SELECT (SELECT age year FROM s10k_users) FROM s10k_users"),
        (Pg::Loud, "bare col alias in RETURNING","UPDATE s10k_users SET age=1 WHERE id=$1 RETURNING age year"),
        (Pg::Ok,   "AS col alias in RETURNING",  "UPDATE s10k_users SET age=1 WHERE id=$1 RETURNING age AS year"),
        (Pg::Ok,   "AS col alias `as` in RETURNING", "UPDATE s10k_users SET age=1 WHERE id=$1 RETURNING age AS as"),
        // `isnull` / `notnull` bare are NOT aliases: PostgreSQL reinterprets each
        // as the IS [NOT] NULL POSTFIX OPERATOR (a boolean), so the engine — which
        // does not model a bare projected boolean — is loud (cast-required) on the
        // operator, never a silently wrong-typed alias of the operand. PG ACCEPTS
        // them (as booleans), so they are the deliberate loud-accurate posture.
        // `AS isnull` / `AS notnull` stay real aliases (PG and engine accept).
        (Pg::UnmodelableLoud, "bare isnull operator",  "SELECT id isnull FROM s10k_users"),
        (Pg::UnmodelableLoud, "bare notnull operator", "SELECT id notnull FROM s10k_users"),
        (Pg::Ok,   "AS isnull alias",            "SELECT id AS isnull FROM s10k_users"),
        (Pg::Ok,   "AS notnull alias",           "SELECT id AS notnull FROM s10k_users"),
        (Pg::Ok,   "quoted isnull alias",        "SELECT id \"isnull\" FROM s10k_users"),
    ];

    let dir = std::env::temp_dir().join(format!("bsql_s10k_xcheck_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp migration dir");
    let mut file = std::fs::File::create(dir.join("001_schema.sql")).expect("create ddl file");
    file.write_all(USERS.as_bytes()).expect("write users ddl");
    file.write_all(b"\n").expect("write newline");
    file.write_all(ACCOUNTS.as_bytes()).expect("write accounts ddl");
    file.write_all(b"\n").expect("write newline");
    file.write_all(COMP.as_bytes()).expect("write comp ddl");
    drop(file);
    let catalog = catalog_from_dir(&dir).expect("catalog replay");
    std::fs::remove_dir_all(&dir).expect("clean temp dir");

    let mut mismatches = Vec::new();
    for (pg, name, sql) in cases {
        let got = infer_query(&catalog, sql);
        let engine_ok = got.is_ok();
        // The engine must ACCEPT only the `Ok` cases; it must reject `Loud` AND
        // `UnmodelableLoud` (the latter is the deliberate loud-accurate posture
        // on a construct the catalog cannot model, even though PG accepts it).
        let engine_should_accept = matches!(pg, Pg::Ok);
        let verdict = if engine_ok { "ENGINE OK  " } else { "ENGINE LOUD" };
        let detail = match &got {
            Ok(_) => String::new(),
            Err(e) => format!(" | {e:?}"),
        };
        eprintln!("{verdict} | {name}{detail}");
        if engine_ok != engine_should_accept {
            mismatches.push(format!(
                "{name}: engine {} but expected {}",
                if engine_ok { "Ok" } else { "Loud" },
                if engine_should_accept { "Ok" } else { "Loud" }
            ));
        }
    }
    assert!(mismatches.is_empty(), "engine/PG divergence: {mismatches:#?}");
}

/// A column grouped under ROLLUP / CUBE / a multi-set GROUPING SETS that is NOT
/// in EVERY generated grouping set is NULL in the super-aggregate rows
/// PostgreSQL emits (the grand-total row of `GROUP BY ROLLUP(id)` has
/// id = NULL), so the engine MUST type it `Option<T>` (nullable). A column
/// grouped in every set — a plain `GROUP BY` key, a single-set GROUPING SETS key
/// — is never NULL from grouping and stays `T` (NOT NULL). Decoding the NULL
/// super-aggregate row into a non-`Option` field is a runtime panic, so the
/// Option-ness is a correctness property, not a style choice. Each row shape was
/// cross-checked live against PostgreSQL by EXECUTING the query (not just
/// PREPARE) and observing the NULL super-aggregate row — recorded here as the
/// per-column `nullable` flag each query must produce.
#[test]
fn engine_grouping_set_nullability_matches_live_pg() {
    // (sql, expected per-column nullable flags in projection order).
    // Live-PG executions that established each NULL/NOT-NULL flag:
    //   ROLLUP(id)            -> grand-total row has id = NULL          (nullable)
    //   CUBE(a, b)            -> super rows have a = NULL and b = NULL  (both)
    //   GROUPING SETS((name),(bio)) -> each col NULL in the other set   (both)
    //   GROUPING SETS((name)) -> name grouped in the only set           (NOT NULL)
    //   GROUP BY id           -> id grouped in the only set             (NOT NULL)
    //   a, CUBE(b)            -> a never NULL, b NULL in CUBE grand rows
    //   id::int8 ROLLUP(id)   -> cast follows the column: NULL on grand-total row
    //   id::int8 GROUP BY id  -> plain key: cast stays NOT NULL
    let cases: &[(&str, &[bool])] = &[
        ("SELECT id, count(*) FROM s10k_accounts GROUP BY ROLLUP(id)", &[true, false]),
        ("SELECT a, b, count(*) FROM s10k_comp GROUP BY CUBE(a, b)", &[true, true, false]),
        ("SELECT name, bio, count(*) FROM s10k_users GROUP BY GROUPING SETS((name),(bio))", &[true, true, false]),
        ("SELECT name, count(*) FROM s10k_users GROUP BY GROUPING SETS((name))", &[false, false]),
        ("SELECT id, count(*) FROM s10k_accounts GROUP BY id", &[false, false]),
        ("SELECT a, b, count(*) FROM s10k_comp GROUP BY a, CUBE(b)", &[false, true, false]),
        ("SELECT id AS k, count(*) FROM s10k_accounts GROUP BY ROLLUP(id)", &[true, false]),
        ("SELECT id FROM (SELECT id, count(*) AS c FROM s10k_accounts GROUP BY ROLLUP(id)) t", &[true]),
        // A CAST over a super-aggregate-grouped column is NULL in exactly the
        // super-aggregate rows the bare column is — a cast neither adds nor
        // removes NULL — so it gets the same promotion. Live PG: `id::int8 IS
        // NULL` is TRUE on the ROLLUP grand-total row; a cast over a PLAIN
        // GROUP BY / single-set GROUPING SETS key stays NOT NULL.
        ("SELECT id::int8, count(*) FROM s10k_accounts GROUP BY ROLLUP(id)", &[true, false]),
        ("SELECT a::int8, b, count(*) FROM s10k_comp GROUP BY CUBE(a, b)", &[true, true, false]),
        ("SELECT id::int8, count(*) FROM s10k_accounts GROUP BY GROUPING SETS((id), ())", &[true, false]),
        ("SELECT (id)::int8, count(*) FROM s10k_accounts GROUP BY ROLLUP(id)", &[true, false]),
        ("SELECT id::int8, count(*) FROM s10k_accounts GROUP BY id", &[false, false]),
        ("SELECT id::int8, count(*) FROM s10k_accounts GROUP BY GROUPING SETS((id))", &[false, false]),
    ];

    let dir = std::env::temp_dir().join(format!("bsql_s10k_null_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp migration dir");
    let mut file = std::fs::File::create(dir.join("001_schema.sql")).expect("create ddl file");
    file.write_all(USERS.as_bytes()).expect("write users ddl");
    file.write_all(b"\n").expect("write newline");
    file.write_all(ACCOUNTS.as_bytes()).expect("write accounts ddl");
    file.write_all(b"\n").expect("write newline");
    file.write_all(COMP.as_bytes()).expect("write comp ddl");
    drop(file);
    let catalog = catalog_from_dir(&dir).expect("catalog replay");
    std::fs::remove_dir_all(&dir).expect("clean temp dir");

    let mut mismatches = Vec::new();
    for (sql, expected) in cases {
        let shape = match infer_query(&catalog, sql) {
            Ok(shape) => shape,
            Err(e) => {
                mismatches.push(format!("{sql}: engine rejected: {e:?}"));
                continue;
            }
        };
        let got: Vec<bool> = shape.columns.iter().map(|c| c.nullable).collect();
        if got.as_slice() != *expected {
            mismatches.push(format!("{sql}: nullability {got:?} but expected {expected:?}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "engine/PG nullability divergence: {mismatches:#?}"
    );
}

/// A `USING` / `NATURAL` merged column is `COALESCE(left.k, right.k)` over an
/// equi-join key NULL never satisfies, so its nullability follows each side's
/// BASE (own, pre-outer-join) nullability and the join type — NOT the
/// outer-join-promoted per-side nullability, and NOT a COALESCE-collapses-to-NOT
/// -NULL-when-either-side-is rule. The matrix verified live by EXECUTION across
/// all 12 (join x base-nullability) combos (each `bool_or(k IS NULL)` observed
/// over rows including a NULL-key row on a nullable side and unmatched rows on
/// each outer-join side): INNER -> NOT NULL always; LEFT -> nullable iff the
/// LEFT base column is nullable; RIGHT -> nullable iff the RIGHT base column is
/// nullable; FULL -> nullable iff EITHER base column is nullable. The two corners
/// the prior code mistyped (over-nullable) and live PG proves NOT NULL: INNER
/// with both base columns nullable, and FULL with both base columns NOT NULL.
#[test]
fn engine_merged_column_nullability_matches_live_pg() {
    // (sql, expected per-column nullable flags). `nn_*` share a NULLABLE `k`,
    // `pk_*` share a NOT NULL non-key `k`. Each flag is the live-PG
    // `bool_or(k IS NULL)` over the union of matched, left-only, right-only, and
    // NULL-key rows.
    let cases: &[(&str, &[bool])] = &[
        // Both base columns NULLABLE.
        ("SELECT k FROM s10k_nn_a INNER JOIN s10k_nn_b USING (k)", &[false]),
        ("SELECT k FROM s10k_nn_a LEFT JOIN s10k_nn_b USING (k)", &[true]),
        ("SELECT k FROM s10k_nn_a RIGHT JOIN s10k_nn_b USING (k)", &[true]),
        ("SELECT k FROM s10k_nn_a FULL JOIN s10k_nn_b USING (k)", &[true]),
        // Both base columns NOT NULL.
        ("SELECT k FROM s10k_pk_a INNER JOIN s10k_pk_b USING (k)", &[false]),
        ("SELECT k FROM s10k_pk_a LEFT JOIN s10k_pk_b USING (k)", &[false]),
        ("SELECT k FROM s10k_pk_a RIGHT JOIN s10k_pk_b USING (k)", &[false]),
        ("SELECT k FROM s10k_pk_a FULL JOIN s10k_pk_b USING (k)", &[false]),
        // Left NULLABLE, right NOT NULL.
        ("SELECT k FROM s10k_nn_a INNER JOIN s10k_pk_b USING (k)", &[false]),
        ("SELECT k FROM s10k_nn_a LEFT JOIN s10k_pk_b USING (k)", &[true]),
        ("SELECT k FROM s10k_nn_a RIGHT JOIN s10k_pk_b USING (k)", &[false]),
        ("SELECT k FROM s10k_nn_a FULL JOIN s10k_pk_b USING (k)", &[true]),
        // Left NOT NULL, right NULLABLE (the symmetric remainder of the 12).
        ("SELECT k FROM s10k_pk_a INNER JOIN s10k_nn_b USING (k)", &[false]),
        ("SELECT k FROM s10k_pk_a LEFT JOIN s10k_nn_b USING (k)", &[false]),
        ("SELECT k FROM s10k_pk_a RIGHT JOIN s10k_nn_b USING (k)", &[true]),
        ("SELECT k FROM s10k_pk_a FULL JOIN s10k_nn_b USING (k)", &[true]),
        // NATURAL follows the identical rule (`k` is the only common column).
        ("SELECT k FROM s10k_nn_a NATURAL INNER JOIN s10k_nn_b", &[false]),
        ("SELECT k FROM s10k_pk_a NATURAL FULL JOIN s10k_pk_b", &[false]),
        ("SELECT k FROM s10k_nn_a NATURAL LEFT JOIN s10k_pk_b", &[true]),
    ];

    let dir = std::env::temp_dir().join(format!("bsql_s10k_merged_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp migration dir");
    let mut file = std::fs::File::create(dir.join("001_schema.sql")).expect("create ddl file");
    for ddl in [NN_A, NN_B, PK_A, PK_B] {
        file.write_all(ddl.as_bytes()).expect("write merged ddl");
        file.write_all(b"\n").expect("write newline");
    }
    drop(file);
    let catalog = catalog_from_dir(&dir).expect("catalog replay");
    std::fs::remove_dir_all(&dir).expect("clean temp dir");

    let mut mismatches = Vec::new();
    for (sql, expected) in cases {
        let shape = match infer_query(&catalog, sql) {
            Ok(shape) => shape,
            Err(e) => {
                mismatches.push(format!("{sql}: engine rejected: {e:?}"));
                continue;
            }
        };
        let got: Vec<bool> = shape.columns.iter().map(|c| c.nullable).collect();
        if got.as_slice() != *expected {
            mismatches.push(format!("{sql}: nullability {got:?} but expected {expected:?}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "engine/PG merged-column nullability divergence: {mismatches:#?}"
    );
}

/// A `USING` / `NATURAL` merged column under `GROUP BY` is covered exactly when
/// the qualified component(s) its value is DRAWN FROM are themselves covered.
/// PostgreSQL expands the merged column to a fixed source expression set by the
/// join type at each step — the preserved (left) side for INNER / LEFT, the
/// right side for RIGHT, a `COALESCE` of both for FULL — and a side a later step
/// collapses drops out of that set. The merge is accepted under `GROUP BY` when
/// EVERY source relation is covered (its `k` grouped, or its whole primary key
/// grouped); grouping a NON-source side is the loud `UngroupedColumn`. Every
/// verdict below was cross-checked live against PostgreSQL by `EXPLAIN` (the
/// planner runs the grouping check), recorded here as Ok (PG accepts and the
/// engine accepts) / Loud (PG rejects and the engine rejects).
#[test]
fn engine_merged_column_grouping_coverage_matches_live_pg() {
    let cases: &[(Pg, &str, &str)] = &[
        // Two-table truth table. INNER / LEFT expand to the LEFT source (cja).
        (Pg::Ok,   "INNER GROUP BY left", "SELECT k, COUNT(*) FROM s10k_cja JOIN s10k_cjb USING (k) GROUP BY s10k_cja.k"),
        (Pg::Loud, "INNER GROUP BY right", "SELECT k, COUNT(*) FROM s10k_cja JOIN s10k_cjb USING (k) GROUP BY s10k_cjb.k"),
        (Pg::Ok,   "INNER GROUP BY both", "SELECT k, COUNT(*) FROM s10k_cja JOIN s10k_cjb USING (k) GROUP BY s10k_cja.k, s10k_cjb.k"),
        (Pg::Ok,   "LEFT GROUP BY left", "SELECT k, COUNT(*) FROM s10k_cja LEFT JOIN s10k_cjb USING (k) GROUP BY s10k_cja.k"),
        (Pg::Loud, "LEFT GROUP BY right (null-extended)", "SELECT k, COUNT(*) FROM s10k_cja LEFT JOIN s10k_cjb USING (k) GROUP BY s10k_cjb.k"),
        // RIGHT expands to the RIGHT source (cjb).
        (Pg::Loud, "RIGHT GROUP BY left (null-extended)", "SELECT k, COUNT(*) FROM s10k_cja RIGHT JOIN s10k_cjb USING (k) GROUP BY s10k_cja.k"),
        (Pg::Ok,   "RIGHT GROUP BY right", "SELECT k, COUNT(*) FROM s10k_cja RIGHT JOIN s10k_cjb USING (k) GROUP BY s10k_cjb.k"),
        // FULL expands to COALESCE(left, right) -> BOTH sources required.
        (Pg::Loud, "FULL GROUP BY left only", "SELECT k, COUNT(*) FROM s10k_cja FULL JOIN s10k_cjb USING (k) GROUP BY s10k_cja.k"),
        (Pg::Loud, "FULL GROUP BY right only", "SELECT k, COUNT(*) FROM s10k_cja FULL JOIN s10k_cjb USING (k) GROUP BY s10k_cjb.k"),
        (Pg::Ok,   "FULL GROUP BY both", "SELECT k, COUNT(*) FROM s10k_cja FULL JOIN s10k_cjb USING (k) GROUP BY s10k_cja.k, s10k_cjb.k"),
        // The merge expression itself grouped (GROUP BY k) covers every join type.
        (Pg::Ok,   "INNER GROUP BY merge expr", "SELECT k, COUNT(*) FROM s10k_cja JOIN s10k_cjb USING (k) GROUP BY k"),
        (Pg::Ok,   "FULL GROUP BY merge expr", "SELECT k, COUNT(*) FROM s10k_cja FULL JOIN s10k_cjb USING (k) GROUP BY k"),
        // NATURAL follows the same rule.
        (Pg::Ok,   "NATURAL GROUP BY left", "SELECT k, COUNT(*) FROM s10k_cja NATURAL JOIN s10k_cjb GROUP BY s10k_cja.k"),
        (Pg::Ok,   "NATURAL RIGHT GROUP BY right", "SELECT k, COUNT(*) FROM s10k_cja NATURAL RIGHT JOIN s10k_cjb GROUP BY s10k_cjb.k"),
        (Pg::Loud, "NATURAL FULL GROUP BY one side", "SELECT k, COUNT(*) FROM s10k_cja NATURAL FULL JOIN s10k_cjb GROUP BY s10k_cja.k"),
        // Chained / mixed folding: a side an INNER/RIGHT step collapses drops out
        // of the source set, a FULL step's new side joins it.
        (Pg::Ok,   "all-INNER chain source stays leftmost", "SELECT k FROM s10k_cja JOIN s10k_cjb USING (k) JOIN s10k_cjc USING (k) GROUP BY s10k_cja.k"),
        (Pg::Loud, "all-INNER chain non-source side", "SELECT k FROM s10k_cja JOIN s10k_cjb USING (k) JOIN s10k_cjc USING (k) GROUP BY s10k_cjc.k"),
        (Pg::Ok,   "INNER-then-FULL sources {cja,cjc}", "SELECT k FROM s10k_cja JOIN s10k_cjb USING (k) FULL JOIN s10k_cjc USING (k) GROUP BY s10k_cja.k, s10k_cjc.k"),
        (Pg::Loud, "INNER-then-FULL grouping collapsed cjb", "SELECT k FROM s10k_cja JOIN s10k_cjb USING (k) FULL JOIN s10k_cjc USING (k) GROUP BY s10k_cja.k, s10k_cjb.k"),
        (Pg::Ok,   "FULL-then-INNER sources {cja,cjb}", "SELECT k FROM s10k_cja FULL JOIN s10k_cjb USING (k) JOIN s10k_cjc USING (k) GROUP BY s10k_cja.k, s10k_cjb.k"),
        (Pg::Loud, "FULL-then-INNER grouping only cjc", "SELECT k FROM s10k_cja FULL JOIN s10k_cjb USING (k) JOIN s10k_cjc USING (k) GROUP BY s10k_cjc.k"),
        (Pg::Ok,   "all-FULL chain needs every side", "SELECT k FROM s10k_cja FULL JOIN s10k_cjb USING (k) FULL JOIN s10k_cjc USING (k) GROUP BY s10k_cja.k, s10k_cjb.k, s10k_cjc.k"),
        (Pg::Loud, "all-FULL chain missing a side", "SELECT k FROM s10k_cja FULL JOIN s10k_cjb USING (k) FULL JOIN s10k_cjc USING (k) GROUP BY s10k_cja.k, s10k_cjb.k"),
        (Pg::Ok,   "FULL-then-RIGHT collapses to cjc", "SELECT k FROM s10k_cja FULL JOIN s10k_cjb USING (k) RIGHT JOIN s10k_cjc USING (k) GROUP BY s10k_cjc.k"),
        (Pg::Loud, "FULL-then-RIGHT old sources insufficient", "SELECT k FROM s10k_cja FULL JOIN s10k_cjb USING (k) RIGHT JOIN s10k_cjc USING (k) GROUP BY s10k_cja.k, s10k_cjb.k"),
        // Primary-key functional dependency of the SOURCE side: grouping a source
        // relation's whole PK covers the merge without grouping `k` directly; the
        // NON-source side's PK does not.
        (Pg::Ok,   "INNER covered by left source PK", "SELECT k FROM s10k_cja JOIN s10k_cjb USING (k) GROUP BY s10k_cja.id"),
        (Pg::Loud, "INNER not covered by right PK", "SELECT k FROM s10k_cja JOIN s10k_cjb USING (k) GROUP BY s10k_cjb.id"),
        (Pg::Ok,   "RIGHT covered by right source PK", "SELECT k FROM s10k_cja RIGHT JOIN s10k_cjb USING (k) GROUP BY s10k_cjb.id"),
        (Pg::Ok,   "FULL covered by both source PKs", "SELECT k FROM s10k_cja FULL JOIN s10k_cjb USING (k) GROUP BY s10k_cja.id, s10k_cjb.id"),
        (Pg::Loud, "FULL not covered by one source PK", "SELECT k FROM s10k_cja FULL JOIN s10k_cjb USING (k) GROUP BY s10k_cja.id"),
    ];

    let dir = std::env::temp_dir().join(format!("bsql_s10k_mcov_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp migration dir");
    let mut file = std::fs::File::create(dir.join("001_schema.sql")).expect("create ddl file");
    for ddl in [CJA, CJB, CJC] {
        file.write_all(ddl.as_bytes()).expect("write coverage ddl");
        file.write_all(b"\n").expect("write newline");
    }
    drop(file);
    let catalog = catalog_from_dir(&dir).expect("catalog replay");
    std::fs::remove_dir_all(&dir).expect("clean temp dir");

    let mut mismatches = Vec::new();
    for (pg, name, sql) in cases {
        let engine_ok = infer_query(&catalog, sql).is_ok();
        let engine_should_accept = matches!(pg, Pg::Ok);
        if engine_ok != engine_should_accept {
            mismatches.push(format!(
                "{name}: engine {} but expected {}",
                if engine_ok { "Ok" } else { "Loud" },
                if engine_should_accept { "Ok" } else { "Loud" }
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "engine/PG merged-column grouping-coverage divergence: {mismatches:#?}"
    );
}

/// The SUPER-AGGREGATE nullability of a `USING` / `NATURAL` merged column,
/// isolated from base nullability by NOT NULL key columns (the base rule pins the
/// merge to NOT NULL, so any NULL result row is purely the grouping-set super-
/// aggregate). A merged column is NULL in a super-aggregate row exactly when SOME
/// source it draws from is omitted by some generated grouping set: a single-
/// source (INNER / LEFT / RIGHT) merge when its one source is omitted, and a FULL
/// merge — whose grouped sides are also null-extended in unmatched rows — when
/// ANY one of its sources is omitted. Each flag was established live by EXECUTING
/// the query and counting NULL result rows, so the Option-ness is asserted in
/// BOTH directions: never under-nullable (a decode panic on the NULL super-
/// aggregate row), never over-nullable (a useless Option on a plain GROUP BY).
#[test]
fn engine_merged_column_super_aggregate_nullability_matches_live_pg() {
    // (sql, expected per-column nullable flags). NOT NULL bases, so `false` is a
    // plain GROUP BY / single-source-always-grouped and `true` is a super-
    // aggregate row that leaves the merge NULL.
    let cases: &[(&str, &[bool])] = &[
        // Single source: nullable iff that source is omitted in some set.
        ("SELECT k FROM s10k_ms_a JOIN s10k_ms_b USING (k) GROUP BY s10k_ms_a.k", &[false]),
        ("SELECT k FROM s10k_ms_a JOIN s10k_ms_b USING (k) GROUP BY ROLLUP(s10k_ms_a.k)", &[true]),
        ("SELECT k FROM s10k_ms_a LEFT JOIN s10k_ms_b USING (k) GROUP BY s10k_ms_a.k", &[false]),
        ("SELECT k FROM s10k_ms_a RIGHT JOIN s10k_ms_b USING (k) GROUP BY ROLLUP(s10k_ms_b.k)", &[true]),
        // FULL: nullable iff ANY source is omitted in some generated set.
        ("SELECT k FROM s10k_ms_a FULL JOIN s10k_ms_b USING (k) GROUP BY s10k_ms_a.k, s10k_ms_b.k", &[false]),
        ("SELECT k FROM s10k_ms_a FULL JOIN s10k_ms_b USING (k) GROUP BY ROLLUP(s10k_ms_a.k, s10k_ms_b.k)", &[true]),
        ("SELECT k FROM s10k_ms_a FULL JOIN s10k_ms_b USING (k) GROUP BY ROLLUP(s10k_ms_a.k), s10k_ms_b.k", &[true]),
        ("SELECT k FROM s10k_ms_a FULL JOIN s10k_ms_b USING (k) GROUP BY s10k_ms_a.k, ROLLUP(s10k_ms_b.k)", &[true]),
        ("SELECT k FROM s10k_ms_a FULL JOIN s10k_ms_b USING (k) GROUP BY GROUPING SETS((s10k_ms_a.k),(s10k_ms_b.k))", &[true]),
        ("SELECT k FROM s10k_ms_a FULL JOIN s10k_ms_b USING (k) GROUP BY CUBE(s10k_ms_a.k, s10k_ms_b.k)", &[true]),
    ];

    let dir = std::env::temp_dir().join(format!("bsql_s10k_msagg_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp migration dir");
    let mut file = std::fs::File::create(dir.join("001_schema.sql")).expect("create ddl file");
    for ddl in [MS_A, MS_B] {
        file.write_all(ddl.as_bytes()).expect("write super-aggregate ddl");
        file.write_all(b"\n").expect("write newline");
    }
    drop(file);
    let catalog = catalog_from_dir(&dir).expect("catalog replay");
    std::fs::remove_dir_all(&dir).expect("clean temp dir");

    let mut mismatches = Vec::new();
    for (sql, expected) in cases {
        match infer_query(&catalog, sql) {
            Ok(shape) => {
                let got: Vec<bool> = shape.columns.iter().map(|c| c.nullable).collect();
                if got.as_slice() != *expected {
                    mismatches.push(format!("{sql}: nullability {got:?} but expected {expected:?}"));
                }
            }
            Err(e) => mismatches.push(format!("{sql}: engine rejected: {e:?}")),
        }
    }
    assert!(
        mismatches.is_empty(),
        "engine/PG merged-column super-aggregate nullability divergence: {mismatches:#?}"
    );
}

/// A cast NEVER introduces or removes NULL, so the nullability of a cast result
/// EQUALS the nullability of the value it casts. A cast over a resolvable column
/// reference therefore takes that reference's join-adjusted nullability — a
/// NOT NULL column is NOT NULL (no useless Option on a statically non-null
/// value), the same column on the null-extended side of an outer join is
/// nullable, a NULL literal is nullable, any other literal is NOT NULL, and a
/// non-resolvable inner (arithmetic, a scalar subquery) stays conservatively
/// nullable. Each per-column flag here was established by EXECUTING the query
/// against live PostgreSQL and observing `<cast> IS NULL` directly (not just
/// PREPARE), so the engine's Option-ness is asserted to match PG in BOTH
/// directions — never under-nullable (a panic) and never over-nullable (a
/// useless Option).
#[test]
fn engine_cast_nullability_matches_live_pg() {
    // (sql, expected per-column nullable flags). Live-PG executions that
    // established each flag (rows: one with all-present values, one with
    // age = NULL and bio = NULL):
    //   id::int8           IS NULL  -> always false  (NOT NULL primary key)
    //   name::text         IS NULL  -> always false  (NOT NULL text column)
    //   age::int8          IS NULL  -> true for NULL age          (nullable)
    //   bio::text          IS NULL  -> true for NULL bio          (nullable)
    //   1::int8            IS NULL  -> always false  (non-NULL literal)
    //   NULL::int8         IS NULL  -> always true               (nullable)
    //   (age + 1)::int8    IS NULL  -> true for NULL age (conservative-nullable)
    //   id::int4::int8     IS NULL  -> always false  (cast-of-cast of NOT NULL)
    //   age::int4::int8    IS NULL  -> true for NULL age          (nullable)
    //   LEFT JOIN a.balance::int8   -> NULL on an unmatched left row (nullable)
    //   COALESCE(bio::text, name::text) IS NULL -> always false (name not-null)
    //   CASE ... id::int8 ELSE 0::int8 END      -> always false (both not-null)
    //   (SELECT a.balance ...)::int8 IS NULL    -> NULL on no match (nullable)
    let cases: &[(&str, &[bool])] = &[
        ("SELECT id::int8 AS x FROM s10k_users", &[false]),
        ("SELECT name::text AS x FROM s10k_users", &[false]),
        ("SELECT age::int8 AS x FROM s10k_users", &[true]),
        ("SELECT bio::text AS x FROM s10k_users", &[true]),
        ("SELECT 1::int8 AS x FROM s10k_users", &[false]),
        ("SELECT NULL::int8 AS x FROM s10k_users", &[true]),
        ("SELECT (age + 1)::int8 AS x FROM s10k_users", &[true]),
        ("SELECT id::int4::int8 AS x FROM s10k_users", &[false]),
        ("SELECT age::int4::int8 AS x FROM s10k_users", &[true]),
        ("SELECT users.id::int8 AS x FROM s10k_users users", &[false]),
        ("SELECT (id)::int8 AS x FROM s10k_users", &[false]),
        (
            "SELECT a.balance::int8 AS x FROM s10k_users u \
             LEFT JOIN s10k_accounts a ON a.user_id = u.id",
            &[true],
        ),
        (
            "SELECT COALESCE(bio::text, name::text) AS x FROM s10k_users",
            &[false],
        ),
        (
            "SELECT CASE WHEN id > 0 THEN id::int8 ELSE 0::int8 END AS x FROM s10k_users",
            &[false],
        ),
        (
            "SELECT (SELECT a.balance FROM s10k_accounts a WHERE a.user_id = u.id)::int8 AS x \
             FROM s10k_users u",
            &[true],
        ),
    ];

    let dir = std::env::temp_dir().join(format!("bsql_s10k_castnull_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp migration dir");
    let mut file = std::fs::File::create(dir.join("001_schema.sql")).expect("create ddl file");
    file.write_all(USERS.as_bytes()).expect("write users ddl");
    file.write_all(b"\n").expect("write newline");
    file.write_all(ACCOUNTS.as_bytes()).expect("write accounts ddl");
    file.write_all(b"\n").expect("write newline");
    file.write_all(COMP.as_bytes()).expect("write comp ddl");
    drop(file);
    let catalog = catalog_from_dir(&dir).expect("catalog replay");
    std::fs::remove_dir_all(&dir).expect("clean temp dir");

    let mut mismatches = Vec::new();
    for (sql, expected) in cases {
        let shape = match infer_query(&catalog, sql) {
            Ok(shape) => shape,
            Err(e) => {
                mismatches.push(format!("{sql}: engine rejected: {e:?}"));
                continue;
            }
        };
        let got: Vec<bool> = shape.columns.iter().map(|c| c.nullable).collect();
        if got.as_slice() != *expected {
            mismatches.push(format!("{sql}: nullability {got:?} but expected {expected:?}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "engine/PG cast-nullability divergence: {mismatches:#?}"
    );
}
