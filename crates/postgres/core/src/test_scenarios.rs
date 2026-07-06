/// A shared library of SQL-mechanism scenarios, run by BOTH drivers.
///
/// Every scenario exercises one SQL mechanism — joins, CTEs, window functions,
/// aggregates, string/type ops, the error zoo, extreme values, transactions,
/// … — over a single connection, and the bodies are written in BLOCKING shape
/// (no `.await`). The sync driver runs them directly; the async driver runs the
/// IDENTICAL bodies through a thin blocking shim (a small adapter that drives
/// each async verb to completion on a per-test runtime). One scenario set, both
/// drivers: a fix to a scenario cannot silently cover only one of them.
///
/// The single argument is a zero-argument constructor returning a connection
/// whose inherent methods the scenarios call (`query_sql`, `execute_sql`,
/// `query_params`, `execute_params`, `simple_query`, `ping`, `transaction`,
/// `close`). Every generated test is `#[ignore]` — it needs a live PostgreSQL.
///
/// ```ignore
/// fn make_conn() -> bsql_postgres_sync::Connection { /* connect */ }
/// bsql_postgres_core::define_sql_scenario_tests!(make_conn);
/// ```
#[macro_export]
macro_rules! define_sql_scenario_tests {
    ($config_fn:expr) => {

#[test]
#[ignore = "requires local PG"]
fn sql_join_types() {
    let mut c = $config_fn();
    c.execute_sql("CREATE TEMP TABLE t1(id int, v text)").expect("t1");
    c.execute_sql("CREATE TEMP TABLE t2(id int, label text)").expect("t2");
    c.execute_sql("INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c')").expect("ins t1");
    c.execute_sql("INSERT INTO t2 VALUES (2,'x'),(3,'y'),(4,'z')").expect("ins t2");

    // INNER JOIN
    let r = c.query_sql("SELECT t1.v, t2.label FROM t1 INNER JOIN t2 ON t1.id = t2.id ORDER BY t1.id").expect("inner");
    assert_eq!(r.len(), 2);
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("b")));

    // LEFT JOIN
    let r = c.query_sql("SELECT t1.v, t2.label FROM t1 LEFT JOIN t2 ON t1.id = t2.id ORDER BY t1.id").expect("left");
    assert_eq!(r.len(), 3);
    assert!(r.get(0).expect("row 0").is_null(1)); // t1.id=1 has no match

    // RIGHT JOIN
    let r = c.query_sql("SELECT t1.v, t2.label FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY t2.id").expect("right");
    assert_eq!(r.len(), 3);

    // CROSS JOIN
    let r = c.query_sql("SELECT count(*) FROM t1 CROSS JOIN t2").expect("cross");
    assert_eq!(r.get(0).expect("row 0").get_i64(0), Ok(Some(9)));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_subqueries_and_cte() {
    let mut c = $config_fn();
    c.execute_sql("CREATE TEMP TABLE emp(id int, name text, dept_id int, salary int)").expect("create");
    c.execute_sql("INSERT INTO emp VALUES (1,'alice',1,100),(2,'bob',1,120),(3,'charlie',2,90),(4,'dave',2,110)").expect("insert");

    // Subquery in WHERE
    let r = c.query_sql("SELECT name FROM emp WHERE salary > (SELECT AVG(salary) FROM emp) ORDER BY name").expect("subq");
    assert_eq!(r.len(), 2); // bob(120), dave(110) > avg(105)

    // CTE (WITH)
    let r = c.query_sql("
        WITH dept_avg AS (
            SELECT dept_id, AVG(salary) as avg_sal FROM emp GROUP BY dept_id
        )
        SELECT e.name, d.avg_sal FROM emp e JOIN dept_avg d ON e.dept_id = d.dept_id
        WHERE e.salary > d.avg_sal ORDER BY e.name
    ").expect("cte");
    assert_eq!(r.len(), 2); // bob > dept1 avg, dave > dept2 avg

    // Correlated subquery
    let r = c.query_sql("
        SELECT name FROM emp e1
        WHERE salary = (SELECT MAX(salary) FROM emp e2 WHERE e2.dept_id = e1.dept_id)
        ORDER BY name
    ").expect("corr subq");
    assert_eq!(r.len(), 2); // bob(max dept1), dave(max dept2)

    // EXISTS
    let r = c.query_sql("SELECT name FROM emp WHERE EXISTS (SELECT 1 FROM emp e2 WHERE e2.dept_id = emp.dept_id AND e2.salary > 110) ORDER BY name").expect("exists");
    assert!(r.len() >= 1);

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_window_functions() {
    let mut c = $config_fn();
    c.execute_sql("CREATE TEMP TABLE sales(id int, region text, amount int)").expect("create");
    c.execute_sql("INSERT INTO sales VALUES (1,'east',100),(2,'east',200),(3,'west',150),(4,'west',300),(5,'east',50)").expect("ins");

    // ROW_NUMBER
    let r = c.query_sql("SELECT id, ROW_NUMBER() OVER (ORDER BY amount DESC) as rn FROM sales").expect("row_number");
    assert_eq!(r.len(), 5);

    // RANK + PARTITION BY
    let r = c.query_sql("SELECT region, amount, RANK() OVER (PARTITION BY region ORDER BY amount DESC) as rnk FROM sales ORDER BY region, rnk").expect("rank");
    assert_eq!(r.len(), 5);
    assert_eq!(r.get(0).expect("row 0").get_i64(2), Ok(Some(1))); // top of east partition

    // SUM OVER (running total)
    let r = c.query_sql("SELECT id, SUM(amount) OVER (ORDER BY id) as running FROM sales ORDER BY id").expect("running sum");
    assert_eq!(r.get(0).expect("row 0").get_i64(1), Ok(Some(100)));
    assert_eq!(r.get(1).expect("row 1").get_i64(1), Ok(Some(300)));

    // LAG / LEAD
    let r = c.query_sql("SELECT id, amount, LAG(amount) OVER (ORDER BY id) as prev FROM sales ORDER BY id").expect("lag");
    assert!(r.get(0).expect("row 0").is_null(2)); // first row has no prev

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_aggregates_and_grouping() {
    let mut c = $config_fn();
    c.execute_sql("CREATE TEMP TABLE agg_data(cat text, val int)").expect("create");
    c.execute_sql("INSERT INTO agg_data VALUES ('a',10),('a',20),('b',30),('b',40),('a',50)").expect("ins");

    // GROUP BY + HAVING
    let r = c.query_sql("SELECT cat, SUM(val) as s FROM agg_data GROUP BY cat HAVING SUM(val) > 50 ORDER BY cat").expect("having");
    assert_eq!(r.len(), 2); // 'a'(80) and 'b'(70) both > 50
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("a")));
    assert_eq!(r.get(0).expect("row 0").get_i64(1), Ok(Some(80)));
    assert_eq!(r.get(1).expect("row 1").get_str(0), Ok(Some("b")));
    assert_eq!(r.get(1).expect("row 1").get_i64(1), Ok(Some(70)));

    // COUNT, MIN, MAX, AVG
    let r = c.query_sql("SELECT COUNT(*), MIN(val), MAX(val), AVG(val)::int FROM agg_data").expect("agg");
    assert_eq!(r.get(0).expect("row 0").get_i64(0), Ok(Some(5)));
    assert_eq!(r.get(0).expect("row 0").get_i32(1), Ok(Some(10)));
    assert_eq!(r.get(0).expect("row 0").get_i32(2), Ok(Some(50)));

    // DISTINCT
    let r = c.query_sql("SELECT DISTINCT cat FROM agg_data ORDER BY cat").expect("distinct");
    assert_eq!(r.len(), 2);

    // GROUP BY ROLLUP (PG extension)
    let r = c.query_sql("SELECT cat, SUM(val) FROM agg_data GROUP BY ROLLUP(cat) ORDER BY cat NULLS LAST").expect("rollup");
    assert!(r.len() >= 2); // a, b (+ optional total row depending on PG version)

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_string_and_type_operations() {
    let mut c = $config_fn();

    // Long identifiers (63 chars = PG NAMEDATALEN-1)
    let long_name = "a".repeat(63);
    c.execute_sql(&format!("CREATE TEMP TABLE \"{long_name}\"(v int)")).expect("long table name");
    c.execute_sql(&format!("INSERT INTO \"{long_name}\" VALUES (1)")).expect("insert long");
    let r = c.query_sql(&format!("SELECT v FROM \"{long_name}\"")).expect("select long");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));

    // String functions
    let r = c.query_sql("SELECT LENGTH('hello'), UPPER('hello'), TRIM('  hi  '), CONCAT('a','b','c')").expect("str funcs");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(5)));
    assert_eq!(r.get(0).expect("row 0").get_str(1), Ok(Some("HELLO")));
    assert_eq!(r.get(0).expect("row 0").get_str(2), Ok(Some("hi")));
    assert_eq!(r.get(0).expect("row 0").get_str(3), Ok(Some("abc")));

    // Type casts
    let r = c.query_sql("SELECT '42'::int, 3.14::text, true::int, NULL::text").expect("casts");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(42)));
    assert_eq!(r.get(0).expect("row 0").get_str(1), Ok(Some("3.14")));
    assert_eq!(r.get(0).expect("row 0").get_i32(2), Ok(Some(1)));
    assert!(r.get(0).expect("row 0").is_null(3));

    // CASE WHEN
    let r = c.query_sql("SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END").expect("case");
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("yes")));

    // COALESCE + NULLIF
    let r = c.query_sql("SELECT COALESCE(NULL, NULL, 'found'), NULLIF(1, 1), NULLIF(1, 2)").expect("coalesce");
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("found")));
    assert!(r.get(0).expect("row 0").is_null(1));
    assert_eq!(r.get(0).expect("row 0").get_i32(2), Ok(Some(1)));

    // Array (PG-specific)
    let r = c.query_sql("SELECT ARRAY[1,2,3]::text").expect("array");
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("{1,2,3}")));

    // JSON (PG-specific)
    let r = c.query_sql("SELECT '{\"key\": \"value\"}'::json->>'key'").expect("json");
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("value")));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_where_and_ordering() {
    let mut c = $config_fn();
    c.execute_sql("CREATE TEMP TABLE wh_items(id int, name text, price int, active bool)").expect("create");
    c.execute_sql("INSERT INTO wh_items VALUES (1,'pen',10,true),(2,'book',50,true),(3,'eraser',5,false),(4,'ruler',15,true),(5,'pencil',8,false)").expect("ins");

    // WHERE with AND/OR
    let r = c.query_sql("SELECT name FROM wh_items WHERE active AND price > 10 ORDER BY name").expect("and");
    assert_eq!(r.len(), 2); // book(50), ruler(15)
    let r = c.query_sql("SELECT name FROM wh_items WHERE price < 10 OR price > 40 ORDER BY name").expect("or");
    assert!(r.len() >= 2); // eraser(5), pencil(8), book(50)

    // IN, NOT IN
    let r = c.query_sql("SELECT name FROM wh_items WHERE id IN (1, 3, 5) ORDER BY id").expect("in");
    assert_eq!(r.len(), 3);

    // BETWEEN
    let r = c.query_sql("SELECT name FROM wh_items WHERE price BETWEEN 8 AND 15 ORDER BY price").expect("between");
    assert_eq!(r.len(), 3); // pencil(8), pen(10), ruler(15)

    // LIKE / ILIKE
    let r = c.query_sql("SELECT name FROM wh_items WHERE name LIKE 'p%' ORDER BY name").expect("like");
    assert_eq!(r.len(), 2); // pen, pencil
    let r = c.query_sql("SELECT name FROM wh_items WHERE name ILIKE 'P%' ORDER BY name").expect("ilike");
    assert_eq!(r.len(), 2);

    // ORDER BY + LIMIT + OFFSET
    let r = c.query_sql("SELECT name FROM wh_items ORDER BY price DESC LIMIT 2 OFFSET 1").expect("limit offset");
    assert_eq!(r.len(), 2);

    // IS NULL / IS NOT NULL
    let r = c.query_sql("SELECT name FROM wh_items WHERE active IS NOT NULL").expect("is not null");
    assert_eq!(r.len(), 5);

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_keyset_pagination() {
    let mut c = $config_fn();
    c.execute_sql("CREATE TEMP TABLE pages(id int PRIMARY KEY, val text)").expect("create");
    for i in 1..=20 {
        c.execute_sql(&format!("INSERT INTO pages VALUES ({i}, 'row_{i}')")).expect("ins");
    }

    // Keyset pagination (seek method)
    let page1 = c.query_sql("SELECT id, val FROM pages ORDER BY id LIMIT 5").expect("page1");
    assert_eq!(page1.len(), 5);
    let last_id = page1.get(4).expect("row 4").get_i32(0).expect("last id decodes").expect("last id present");

    let page2 = c.query_params(
        "SELECT id, val FROM pages WHERE id > $1 ORDER BY id LIMIT 5",
        &(last_id,),
    ).expect("page2");
    assert_eq!(page2.len(), 5);
    assert_eq!(page2.get(0).expect("row 0").get_i32(0), Ok(Some(last_id + 1)));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_upsert_and_returning() {
    let mut c = $config_fn();
    c.execute_sql("CREATE TEMP TABLE kv(key text PRIMARY KEY, val int)").expect("create");
    c.execute_sql("INSERT INTO kv VALUES ('a', 1)").expect("ins");

    // ON CONFLICT (upsert)
    c.execute_sql("INSERT INTO kv VALUES ('a', 10) ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val").expect("upsert");
    let r = c.query_sql("SELECT val FROM kv WHERE key = 'a'").expect("check");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(10)));

    // INSERT ... RETURNING
    let r = c.query_sql("INSERT INTO kv VALUES ('b', 20) RETURNING key, val").expect("returning");
    assert_eq!(r.len(), 1);
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("b")));
    assert_eq!(r.get(0).expect("row 0").get_i32(1), Ok(Some(20)));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_generate_series_and_lateral() {
    let mut c = $config_fn();

    // generate_series
    let r = c.query_sql("SELECT * FROM generate_series(1, 5)").expect("series");
    assert_eq!(r.len(), 5);

    // LATERAL join
    c.execute_sql("CREATE TEMP TABLE depts(id int, name text)").expect("create");
    c.execute_sql("CREATE TEMP TABLE emps(id int, dept_id int, name text)").expect("create2");
    c.execute_sql("INSERT INTO depts VALUES (1,'eng'),(2,'sales')").expect("ins d");
    c.execute_sql("INSERT INTO emps VALUES (1,1,'a'),(2,1,'b'),(3,2,'c')").expect("ins e");

    let r = c.query_sql("
        SELECT d.name, e.name FROM depts d,
        LATERAL (SELECT name FROM emps WHERE dept_id = d.id ORDER BY name LIMIT 1) e
        ORDER BY d.name
    ").expect("lateral");
    assert_eq!(r.len(), 2);

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_system_and_meta_queries() {
    let mut c = $config_fn();

    // SELECT version()
    let r = c.query_sql("SELECT version()").expect("version");
    // Bind the row handle: `get_str` borrows it, so a `&str` cannot outlive it.
    let row = r.get(0).expect("row 0");
    let v = row.get_str(0).expect("version decodes").expect("version present");
    assert!(v.contains("PostgreSQL"));

    // current_database, current_user, current_timestamp
    let r = c.query_sql("SELECT current_database(), current_user, now()::text").expect("meta");
    assert!(r.get(0).expect("row 0").get_str(0).expect("meta col 0 decodes").is_some());
    assert!(r.get(0).expect("row 0").get_str(1).expect("meta col 1 decodes").is_some());
    assert!(r.get(0).expect("row 0").get_str(2).expect("meta col 2 decodes").is_some());

    // pg_catalog queries
    let r = c.query_sql("SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = 'pg_catalog'").expect("pg_tables");
    assert!(r.get(0).expect("row 0").get_i64(0).expect("count decodes").is_some_and(|n| n > 0));

    // information_schema
    let r = c.query_sql("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'pg_catalog'").expect("info_schema");
    assert!(r.get(0).expect("row 0").get_i64(0).expect("count decodes").is_some_and(|n| n > 0));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_edge_cases() {
    let mut c = $config_fn();

    // Empty string vs NULL
    c.execute_sql("CREATE TEMP TABLE edge(v text)").expect("create");
    c.execute_sql("INSERT INTO edge VALUES (''), (NULL), (' '), ('hello')").expect("ins");
    let r = c.query_sql("SELECT v FROM edge ORDER BY v NULLS FIRST").expect("q");
    assert!(r.get(0).expect("row 0").is_null(0)); // NULL first
    assert_eq!(r.get(1).expect("row 1").get_str(0), Ok(Some(""))); // empty string
    assert_eq!(r.get(2).expect("row 2").get_str(0), Ok(Some(" "))); // space

    // Long text value (within WriteBuf capacity)
    let long = "x".repeat(1_000);
    c.execute_sql("CREATE TEMP TABLE longval(v text)").expect("create");
    c.execute_params("INSERT INTO longval VALUES ($1)", &(long.as_str(),)).expect("ins long");
    let r = c.query_sql("SELECT LENGTH(v) FROM longval").expect("len");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1_000)));

    // Unicode edge cases
    let r = c.query_sql("SELECT '🦀🐘'::text, E'tab\\there'::text, E'new\\nline'::text").expect("unicode");
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("🦀🐘")));
    assert!(r.get(0).expect("row 0").get_str(1).expect("tab decodes").is_some_and(|s| s.contains('\t')));
    assert!(r.get(0).expect("row 0").get_str(2).expect("newline decodes").is_some_and(|s| s.contains('\n')));

    // Numeric edge cases
    let r = c.query_sql("SELECT 2147483647::int, (-2147483647-1)::int, 9223372036854775807::bigint").expect("nums");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(i32::MAX)));
    assert_eq!(r.get(0).expect("row 0").get_i32(1), Ok(Some(i32::MIN)));
    assert_eq!(r.get(0).expect("row 0").get_i64(2), Ok(Some(i64::MAX)));

    // Boolean
    let r = c.query_sql("SELECT true, false, NULL::bool").expect("bools");
    assert_eq!(r.get(0).expect("row 0").get_bool(0), Ok(Some(true)));
    assert_eq!(r.get(0).expect("row 0").get_bool(1), Ok(Some(false)));
    assert!(r.get(0).expect("row 0").is_null(2));

    // Empty result set
    let r = c.query_sql("SELECT 1 WHERE false").expect("empty");
    assert_eq!(r.len(), 0);

    // Multiple columns same name (PG allows this)
    let r = c.query_sql("SELECT 1 AS x, 2 AS x").expect("dup cols");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
    assert_eq!(r.get(0).expect("row 0").get_i32(1), Ok(Some(2)));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_date_and_interval() {
    let mut c = $config_fn();

    let r = c.query_sql("SELECT '2024-01-15'::date::text, '13:45:00'::time::text").expect("date/time");
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("2024-01-15")));
    assert_eq!(r.get(0).expect("row 0").get_str(1), Ok(Some("13:45:00")));

    let r = c.query_sql("SELECT '2024-01-15'::date + interval '1 month' + interval '2 days'").expect("interval");
    assert!(r.get(0).expect("row 0").get_str(0).expect("interval decodes").is_some());

    // EXTRACT
    let r = c.query_sql("SELECT EXTRACT(YEAR FROM '2024-06-15'::date)::int").expect("extract");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(2024)));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_error_zoo() {
    let mut c = $config_fn();

    // Syntax errors
    assert!(c.simple_query("SELCT").is_err());
    assert!(c.simple_query("SELECT FROM").is_err());
    assert!(c.simple_query("INSERT").is_err());
    assert!(c.simple_query("DROP TABLE").is_err());
    let _ = c.simple_query(""); // PG treats empty string as no-op — may succeed
    c.ping().expect("recover after syntax errors");

    // Missing objects
    assert!(c.query_sql("SELECT * FROM table_that_does_not_exist_xyz").is_err());
    assert!(c.query_sql("SELECT nonexistent_column FROM pg_class LIMIT 1").is_err());
    assert!(c.execute_sql("DROP TABLE table_that_does_not_exist_xyz").is_err());
    assert!(c.execute_sql("ALTER TABLE nonexistent_xyz ADD COLUMN x int").is_err());
    c.ping().expect("recover after missing objects");

    // Double-drop
    c.execute_sql("CREATE TEMP TABLE dd(x int)").expect("create");
    c.execute_sql("DROP TABLE dd").expect("drop");
    assert!(c.execute_sql("DROP TABLE dd").is_err());
    c.ping().expect("recover after double drop");

    // Type mismatch
    assert!(c.query_sql("SELECT 'not_a_number'::int").is_err());
    assert!(c.query_sql("SELECT 'not_a_bool'::bool").is_err());
    assert!(c.query_sql("SELECT '99999999999999999999'::int").is_err());
    c.ping().expect("recover after type mismatch");

    // Division by zero
    assert!(c.query_sql("SELECT 1/0").is_err());
    assert!(c.query_sql("SELECT 1.0/0.0").is_err());
    c.ping().expect("recover after division by zero");

    // Constraint violations
    c.execute_sql("CREATE TEMP TABLE cv(id int PRIMARY KEY, name text NOT NULL, val int CHECK(val > 0))").expect("create");
    c.execute_sql("INSERT INTO cv VALUES (1, 'a', 1)").expect("ok");
    assert!(c.execute_sql("INSERT INTO cv VALUES (1, 'b', 2)").is_err()); // PK dup
    assert!(c.execute_sql("INSERT INTO cv VALUES (2, NULL, 2)").is_err()); // NOT NULL
    assert!(c.execute_sql("INSERT INTO cv VALUES (3, 'c', -1)").is_err()); // CHECK
    assert!(c.execute_sql("INSERT INTO cv VALUES (4, 'c', 0)").is_err()); // CHECK
    c.ping().expect("recover after constraints");
    assert_eq!(c.query_sql("SELECT count(*) FROM cv").expect("c").get(0).expect("row 0").get_i64(0), Ok(Some(1)));

    // Truncation / overflow
    assert!(c.query_sql("SELECT 2147483648::int").is_err()); // i32 overflow
    assert!(c.query_sql("SELECT 9999999999999999999::bigint").is_err()); // i64 overflow

    // Nested BEGIN (PG issues WARNING, doesn't error)
    c.simple_query("BEGIN").expect("begin");
    let _ = c.simple_query("BEGIN"); // warning, not error
    c.simple_query("ROLLBACK").expect("rollback");

    // CRUD on non-existent after successful ops
    c.execute_sql("CREATE TEMP TABLE ghost(v int)").expect("create");
    c.execute_sql("INSERT INTO ghost VALUES (1)").expect("ins");
    c.execute_sql("DROP TABLE ghost").expect("drop");
    assert!(c.query_sql("SELECT * FROM ghost").is_err());
    assert!(c.execute_sql("INSERT INTO ghost VALUES (2)").is_err());
    c.ping().expect("recover final");

    c.close().expect("close");
}

// A command whose reply interleaves an asynchronous NoticeResponse
// (WARNING/NOTICE) ahead of its `CommandComplete` must not make the pump
// report completion early. Were it to do so, the command's tag would be
// captured empty and the protocol would still be `Busy` when the NEXT
// command is pushed — the follow-up would then fail `NotReady`. This pins
// the terminal-based completion contract: the NOTICE-bearing statement
// reports its correct tag AND the following command succeeds.
#[test]
#[ignore = "requires local PG"]
fn notice_then_command_recovers() {
    let mut c = $config_fn();

    let real = format!("notice_real_{}", std::process::id());
    let ghost = format!("notice_ghost_{}", std::process::id());
    // Pre-clean any residue from a crashed prior run (idempotent).
    let _ = c.simple_query(&format!("DROP TABLE IF EXISTS {real}"));

    // (1) NOTICE path: DROP TABLE IF EXISTS on a non-existent table makes PG
    // emit a NoticeResponse ("table ... does not exist, skipping") ahead of
    // CommandComplete. The tag must still be captured, and the immediately
    // following command must succeed (not NotReady).
    for _ in 0..16 {
        let tag = c.simple_query(&format!("DROP TABLE IF EXISTS {ghost}"))
            .expect("drop-if-exists with NOTICE must not error");
        assert_eq!(tag, "DROP TABLE", "NOTICE-bearing DROP must report its tag, not empty");
        // The very next command would observe a `Busy` protocol if the pump
        // had reported Done prematurely.
        c.ping().expect("ping after NOTICE must not be NotReady");
        let r = c.query_sql("SELECT 1::int").expect("SELECT after NOTICE must succeed");
        assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
    }

    // (2) WARNING path: a nested BEGIN makes PG emit a WARNING NoticeResponse
    // ("there is already a transaction in progress"). The ROLLBACK that
    // follows is the exact statement that flaked as `NotReady` before the fix.
    for _ in 0..16 {
        c.simple_query("BEGIN").expect("begin");
        let warn_tag = c.simple_query("BEGIN").expect("nested BEGIN warns, never errors");
        assert_eq!(warn_tag, "BEGIN", "WARNING-bearing BEGIN must report its tag");
        c.simple_query("ROLLBACK").expect("rollback after WARNING must not be NotReady");
    }

    // (3) Tag-capture correctness on a real DROP: a NOTICE never appears, so
    // this confirms the fix did not regress ordinary tag capture.
    c.execute_sql(&format!("CREATE TABLE {real}(x int)")).expect("create");
    let tag = c.simple_query(&format!("DROP TABLE {real}")).expect("drop real");
    assert_eq!(tag, "DROP TABLE", "real DROP must report DROP TABLE tag");

    // Cleanup.
    let _ = c.simple_query(&format!("DROP TABLE IF EXISTS {real}"));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_extreme_values() {
    let mut c = $config_fn();

    // Integer extremes
    let r = c.query_sql("SELECT (-2147483647-1)::int, 2147483647::int").expect("i32 extremes");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(i32::MIN)));
    assert_eq!(r.get(0).expect("row 0").get_i32(1), Ok(Some(i32::MAX)));

    let r = c.query_sql("SELECT (-9223372036854775807-1)::bigint, 9223372036854775807::bigint").expect("i64 extremes");
    assert_eq!(r.get(0).expect("row 0").get_i64(0), Ok(Some(i64::MIN)));
    assert_eq!(r.get(0).expect("row 0").get_i64(1), Ok(Some(i64::MAX)));

    let r = c.query_sql("SELECT (-32767-1)::smallint, 32767::smallint").expect("i16 extremes");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(i16::MIN as i32)));
    assert_eq!(r.get(0).expect("row 0").get_i32(1), Ok(Some(i16::MAX as i32)));

    // Zero
    let r = c.query_sql("SELECT 0::int, 0::bigint, 0::smallint, 0.0::float4, 0.0::float8").expect("zeros");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(0)));
    assert_eq!(r.get(0).expect("row 0").get_i64(1), Ok(Some(0)));
    assert_eq!(r.get(0).expect("row 0").get_i32(2), Ok(Some(0)));

    // Float specials
    let r = c.query_sql("SELECT 'NaN'::float8, 'Infinity'::float8, '-Infinity'::float8").expect("float specials");
    let nan = r.get(0).expect("row 0").get_f64(0).expect("nan decodes").expect("nan present");
    assert!(nan.is_nan());
    let inf = r.get(0).expect("row 0").get_f64(1).expect("inf decodes").expect("inf present");
    assert!(inf.is_infinite() && inf > 0.0);
    let neg_inf = r.get(0).expect("row 0").get_f64(2).expect("neg_inf decodes").expect("neg_inf present");
    assert!(neg_inf.is_infinite() && neg_inf < 0.0);

    // Empty string vs NULL
    let r = c.query_sql("SELECT ''::text, NULL::text, ' '::text").expect("empty vs null");
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("")));
    assert!(r.get(0).expect("row 0").is_null(1));
    assert_eq!(r.get(0).expect("row 0").get_str(2), Ok(Some(" ")));

    // Long text via params — an 8 KiB value, FAR past the old ~2 KiB bounded-
    // Bind cap. The Bind now streams its parameter block onto the growable send
    // buffer, so there is no fixed cap on parameter size.
    c.execute_sql("CREATE TEMP TABLE longtext(v text)").expect("create");
    let long = "x".repeat(8000);
    c.execute_params("INSERT INTO longtext VALUES ($1)", &(long.as_str(),)).expect("insert 8000");
    let r = c.query_sql("SELECT length(v), v FROM longtext").expect("select");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(8000)));
    assert_eq!(r.get(0).expect("row 0").get_str(1).expect("v decodes").map(|s| s.len()), Some(8000));

    // Very long text via SQL literal (an alternate large-value path)
    let big_literal = "y".repeat(50_000);
    c.execute_sql(&format!("INSERT INTO longtext VALUES ('{big_literal}')")).expect("insert 50K literal");
    let r = c.query_sql("SELECT length(v) FROM longtext WHERE v LIKE 'y%'").expect("select big");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(50_000)));

    // Unicode stress
    let r = c.query_sql("SELECT '🎭🎪🎨'::text, '中文测试'::text, 'العربية'::text, '日本語テスト'::text").expect("unicode");
    assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("🎭🎪🎨")));
    assert_eq!(r.get(0).expect("row 0").get_str(1), Ok(Some("中文测试")));
    assert_eq!(r.get(0).expect("row 0").get_str(2), Ok(Some("العربية")));
    assert_eq!(r.get(0).expect("row 0").get_str(3), Ok(Some("日本語テスト")));

    // All NULLs
    let r = c.query_sql("SELECT NULL::int, NULL::text, NULL::bool, NULL::float8, NULL::bigint").expect("all null");
    for i in 0..5 { assert!(r.get(0).expect("row 0").is_null(i), "col {i} should be null"); }

    // Booleans
    let r = c.query_sql("SELECT true, false, NOT true, true AND false, true OR false").expect("bools");
    assert_eq!(r.get(0).expect("row 0").get_bool(0), Ok(Some(true)));
    assert_eq!(r.get(0).expect("row 0").get_bool(1), Ok(Some(false)));
    assert_eq!(r.get(0).expect("row 0").get_bool(2), Ok(Some(false)));
    assert_eq!(r.get(0).expect("row 0").get_bool(3), Ok(Some(false)));
    assert_eq!(r.get(0).expect("row 0").get_bool(4), Ok(Some(true)));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_empty_and_boundary_results() {
    let mut c = $config_fn();

    // Empty result set
    c.execute_sql("CREATE TEMP TABLE empty_t(v int)").expect("create");
    let r = c.query_sql("SELECT * FROM empty_t").expect("empty");
    assert_eq!(r.len(), 0);
    assert!(r.column_names.len() > 0);

    // WHERE that matches nothing
    c.execute_sql("INSERT INTO empty_t VALUES (1),(2),(3)").expect("ins");
    let r = c.query_sql("SELECT * FROM empty_t WHERE v > 999").expect("no match");
    assert_eq!(r.len(), 0);

    // Single row, single column
    let r = c.query_sql("SELECT 42::int").expect("scalar");
    assert_eq!(r.len(), 1);
    assert_eq!(r.column_names.len(), 1);

    // Single row, many NULLs
    let r = c.query_sql("SELECT NULL::int, NULL::text, NULL::bool, NULL::float8, NULL::int, NULL::text, NULL::bool, NULL::float8, NULL::int, NULL::text").expect("10 nulls");
    assert_eq!(r.len(), 1);
    for i in 0..10 { assert!(r.get(0).expect("row 0").is_null(i)); }

    // Many rows, one column
    let r = c.query_sql("SELECT generate_series(1, 5000)").expect("5k rows");
    assert_eq!(r.len(), 5000);
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
    assert_eq!(r.get(4999).expect("row 4999").get_i32(0), Ok(Some(5000)));

    // LIMIT 0
    let r = c.query_sql("SELECT * FROM empty_t LIMIT 0").expect("limit 0");
    assert_eq!(r.len(), 0);

    // OFFSET past end
    let r = c.query_sql("SELECT * FROM empty_t OFFSET 999").expect("offset past end");
    assert_eq!(r.len(), 0);

    // SELECT with no FROM (synthetic row)
    let r = c.query_sql("SELECT 1 as a, 'b' as b, true as c").expect("no from");
    assert_eq!(r.len(), 1);
    assert_eq!(&*r.column_names, &["a", "b", "c"]);

    // Duplicate column names
    let r = c.query_sql("SELECT 1 as x, 2 as x, 3 as x").expect("dup names");
    assert_eq!(r.column_names.len(), 3);
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
    assert_eq!(r.get(0).expect("row 0").get_i32(2), Ok(Some(3)));

    // Reserved words as identifiers
    c.execute_sql("CREATE TEMP TABLE \"select\"(\"where\" int, \"from\" text)").expect("reserved");
    c.execute_sql("INSERT INTO \"select\" VALUES (1, 'a')").expect("ins");
    let r = c.query_sql("SELECT \"where\", \"from\" FROM \"select\"").expect("select reserved");
    assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(1)));
    assert_eq!(r.get(0).expect("row 0").get_str(1), Ok(Some("a")));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_concurrent_ddl_and_rapid_ops() {
    let mut c = $config_fn();

    // Rapid create/drop cycle
    for i in 0..20u32 {
        let name = format!("rapid_{i}");
        c.execute_sql(&format!("CREATE TEMP TABLE {name}(v int)")).expect("create");
        c.execute_sql(&format!("INSERT INTO {name} VALUES ({i})")).expect("ins");
        assert_eq!(c.query_sql(&format!("SELECT v FROM {name}")).expect("q").get(0).expect("row 0").get_i32(0), Ok(Some(i as i32)));
        c.execute_sql(&format!("DROP TABLE {name}")).expect("drop");
    }
    c.ping().expect("after rapid cycle");

    // Rapid query cycle — 100 queries on one connection
    for i in 0..100u32 {
        assert_eq!(c.query_sql(&format!("SELECT {i}::int")).expect("q").get(0).expect("row 0").get_i32(0), Ok(Some(i as i32)));
    }

    // Interleaved errors and successes
    for _ in 0..10 {
        assert!(c.query_sql("SELECT * FROM nonexistent_table_zzz").is_err());
        assert_eq!(c.query_sql("SELECT 1::int").expect("ok").get(0).expect("row 0").get_i32(0), Ok(Some(1)));
    }

    // Multiple temp tables at once
    for i in 0..10u32 {
        c.execute_sql(&format!("CREATE TEMP TABLE mt_{i}(v int)")).expect("create");
        c.execute_sql(&format!("INSERT INTO mt_{i} VALUES ({i})")).expect("ins");
    }
    for i in 0..10u32 {
        let r = c.query_sql(&format!("SELECT v FROM mt_{i}")).expect("q");
        assert_eq!(r.get(0).expect("row 0").get_i32(0), Ok(Some(i as i32)));
    }
    // Cross-table join across all 10
    let tables: Vec<String> = (0..10).map(|i| format!("mt_{i}")).collect();
    let joins = tables.windows(2).map(|w| format!("{} JOIN {} ON true", w[0], w[1])).collect::<Vec<_>>();
    let sql = format!("SELECT count(*) FROM {}", if joins.is_empty() { "mt_0".to_string() } else {
        format!("mt_0 {}", (1..10).map(|i| format!("JOIN mt_{i} ON true")).collect::<Vec<_>>().join(" "))
    });
    let r = c.query_sql(&sql).expect("mega join");
    assert_eq!(r.get(0).expect("row 0").get_i64(0), Ok(Some(1))); // 1 row each × CROSS JOIN = 1 row

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_transaction_edge_cases() {
    let mut c = $config_fn();

    c.execute_sql("CREATE TEMP TABLE txe(id int PRIMARY KEY, v text)").expect("create");

    // Empty transaction — commit nothing
    c.transaction(|tx| Ok(())).expect("empty tx");

    // Transaction with only reads
    c.execute_sql("INSERT INTO txe VALUES (1, 'a')").expect("seed");
    c.transaction(|tx| {
        let r = tx.query_sql("SELECT v FROM txe WHERE id = 1")?;
        assert_eq!(r.get(0).expect("row 0").get_str(0), Ok(Some("a")));
        Ok(())
    }).expect("read-only tx");

    // Rollback on error
    let result: Result<(), _> = c.transaction(|tx| {
        tx.execute_sql("INSERT INTO txe VALUES (2, 'b')")?;
        tx.execute_sql("INSERT INTO txe VALUES (2, 'dupe')")?; // PK violation
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(c.query_sql("SELECT count(*) FROM txe").expect("c").get(0).expect("row 0").get_i64(0), Ok(Some(1)));

    // Rollback on user error
    let _: Result<(), _> = c.transaction(|tx| {
        tx.execute_sql("INSERT INTO txe VALUES (3, 'c')")?;
        Err(bsql_postgres_core::DriverError::NoRows) // user-triggered rollback
    });
    assert_eq!(c.query_sql("SELECT count(*) FROM txe").expect("c").get(0).expect("row 0").get_i64(0), Ok(Some(1)));

    // Successful multi-statement transaction
    c.transaction(|tx| {
        tx.execute_sql("INSERT INTO txe VALUES (10, 'x')")?;
        tx.execute_sql("INSERT INTO txe VALUES (11, 'y')")?;
        tx.execute_sql("UPDATE txe SET v = 'z' WHERE id = 10")?;
        tx.execute_sql("DELETE FROM txe WHERE id = 1")?;
        Ok(())
    }).expect("multi-stmt tx");
    assert_eq!(c.query_sql("SELECT count(*) FROM txe").expect("c").get(0).expect("row 0").get_i64(0), Ok(Some(2)));
    assert_eq!(c.query_sql("SELECT v FROM txe WHERE id = 10").expect("q").get(0).expect("row 0").get_str(0), Ok(Some("z")));

    c.close().expect("close");
}

    }; // end macro
}

