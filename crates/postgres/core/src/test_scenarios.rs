/// Shared SQL test scenarios for both async and sync drivers.
/// Usage from driver test files:
///
/// ```ignore
/// bsql_postgres_core::define_sync_sql_tests!(ConnectConfig, Connection, SslMode);
/// ```
///
/// Each scenario tests one SQL mechanism. All use one connection.

#[macro_export]
macro_rules! define_sync_sql_tests {
    ($config_fn:expr) => {

#[test]
#[ignore = "requires local PG"]
fn sql_join_types() {
    let mut c = $config_fn();
    c.execute("CREATE TEMP TABLE t1(id int, v text)").expect("t1");
    c.execute("CREATE TEMP TABLE t2(id int, label text)").expect("t2");
    c.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c')").expect("ins t1");
    c.execute("INSERT INTO t2 VALUES (2,'x'),(3,'y'),(4,'z')").expect("ins t2");

    // INNER JOIN
    let r = c.query("SELECT t1.v, t2.label FROM t1 INNER JOIN t2 ON t1.id = t2.id ORDER BY t1.id").expect("inner");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].get_str(0), Some("b"));

    // LEFT JOIN
    let r = c.query("SELECT t1.v, t2.label FROM t1 LEFT JOIN t2 ON t1.id = t2.id ORDER BY t1.id").expect("left");
    assert_eq!(r.rows.len(), 3);
    assert!(r.rows[0].is_null(1)); // t1.id=1 has no match

    // RIGHT JOIN
    let r = c.query("SELECT t1.v, t2.label FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY t2.id").expect("right");
    assert_eq!(r.rows.len(), 3);

    // CROSS JOIN
    let r = c.query("SELECT count(*) FROM t1 CROSS JOIN t2").expect("cross");
    assert_eq!(r.rows[0].get_i64(0), Some(9));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_subqueries_and_cte() {
    let mut c = $config_fn();
    c.execute("CREATE TEMP TABLE emp(id int, name text, dept_id int, salary int)").expect("create");
    c.execute("INSERT INTO emp VALUES (1,'alice',1,100),(2,'bob',1,120),(3,'charlie',2,90),(4,'dave',2,110)").expect("insert");

    // Subquery in WHERE
    let r = c.query("SELECT name FROM emp WHERE salary > (SELECT AVG(salary) FROM emp) ORDER BY name").expect("subq");
    assert_eq!(r.rows.len(), 2); // bob(120), dave(110) > avg(105)

    // CTE (WITH)
    let r = c.query("
        WITH dept_avg AS (
            SELECT dept_id, AVG(salary) as avg_sal FROM emp GROUP BY dept_id
        )
        SELECT e.name, d.avg_sal FROM emp e JOIN dept_avg d ON e.dept_id = d.dept_id
        WHERE e.salary > d.avg_sal ORDER BY e.name
    ").expect("cte");
    assert_eq!(r.rows.len(), 2); // bob > dept1 avg, dave > dept2 avg

    // Correlated subquery
    let r = c.query("
        SELECT name FROM emp e1
        WHERE salary = (SELECT MAX(salary) FROM emp e2 WHERE e2.dept_id = e1.dept_id)
        ORDER BY name
    ").expect("corr subq");
    assert_eq!(r.rows.len(), 2); // bob(max dept1), dave(max dept2)

    // EXISTS
    let r = c.query("SELECT name FROM emp WHERE EXISTS (SELECT 1 FROM emp e2 WHERE e2.dept_id = emp.dept_id AND e2.salary > 110) ORDER BY name").expect("exists");
    assert!(r.rows.len() >= 1);

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_window_functions() {
    let mut c = $config_fn();
    c.execute("CREATE TEMP TABLE sales(id int, region text, amount int)").expect("create");
    c.execute("INSERT INTO sales VALUES (1,'east',100),(2,'east',200),(3,'west',150),(4,'west',300),(5,'east',50)").expect("ins");

    // ROW_NUMBER
    let r = c.query("SELECT id, ROW_NUMBER() OVER (ORDER BY amount DESC) as rn FROM sales").expect("row_number");
    assert_eq!(r.rows.len(), 5);

    // RANK + PARTITION BY
    let r = c.query("SELECT region, amount, RANK() OVER (PARTITION BY region ORDER BY amount DESC) as rnk FROM sales ORDER BY region, rnk").expect("rank");
    assert_eq!(r.rows.len(), 5);
    assert_eq!(r.rows[0].get_i64(2), Some(1)); // top of east partition

    // SUM OVER (running total)
    let r = c.query("SELECT id, SUM(amount) OVER (ORDER BY id) as running FROM sales ORDER BY id").expect("running sum");
    assert_eq!(r.rows[0].get_i64(1), Some(100));
    assert_eq!(r.rows[1].get_i64(1), Some(300));

    // LAG / LEAD
    let r = c.query("SELECT id, amount, LAG(amount) OVER (ORDER BY id) as prev FROM sales ORDER BY id").expect("lag");
    assert!(r.rows[0].is_null(2)); // first row has no prev

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_aggregates_and_grouping() {
    let mut c = $config_fn();
    c.execute("CREATE TEMP TABLE agg_data(cat text, val int)").expect("create");
    c.execute("INSERT INTO agg_data VALUES ('a',10),('a',20),('b',30),('b',40),('a',50)").expect("ins");

    // GROUP BY + HAVING
    let r = c.query("SELECT cat, SUM(val) as s FROM agg_data GROUP BY cat HAVING SUM(val) > 50 ORDER BY cat").expect("having");
    assert_eq!(r.rows.len(), 2); // 'a'(80) and 'b'(70) both > 50
    assert_eq!(r.rows[0].get_str(0), Some("a"));
    assert_eq!(r.rows[0].get_i64(1), Some(80));
    assert_eq!(r.rows[1].get_str(0), Some("b"));
    assert_eq!(r.rows[1].get_i64(1), Some(70));

    // COUNT, MIN, MAX, AVG
    let r = c.query("SELECT COUNT(*), MIN(val), MAX(val), AVG(val)::int FROM agg_data").expect("agg");
    assert_eq!(r.rows[0].get_i64(0), Some(5));
    assert_eq!(r.rows[0].get_i32(1), Some(10));
    assert_eq!(r.rows[0].get_i32(2), Some(50));

    // DISTINCT
    let r = c.query("SELECT DISTINCT cat FROM agg_data ORDER BY cat").expect("distinct");
    assert_eq!(r.rows.len(), 2);

    // GROUP BY ROLLUP (PG extension)
    let r = c.query("SELECT cat, SUM(val) FROM agg_data GROUP BY ROLLUP(cat) ORDER BY cat NULLS LAST").expect("rollup");
    assert!(r.rows.len() >= 2); // a, b (+ optional total row depending on PG version)

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_string_and_type_operations() {
    let mut c = $config_fn();

    // Long identifiers (63 chars = PG NAMEDATALEN-1)
    let long_name = "a".repeat(63);
    c.execute(&format!("CREATE TEMP TABLE \"{long_name}\"(v int)")).expect("long table name");
    c.execute(&format!("INSERT INTO \"{long_name}\" VALUES (1)")).expect("insert long");
    let r = c.query(&format!("SELECT v FROM \"{long_name}\"")).expect("select long");
    assert_eq!(r.rows[0].get_i32(0), Some(1));

    // String functions
    let r = c.query("SELECT LENGTH('hello'), UPPER('hello'), TRIM('  hi  '), CONCAT('a','b','c')").expect("str funcs");
    assert_eq!(r.rows[0].get_i32(0), Some(5));
    assert_eq!(r.rows[0].get_str(1), Some("HELLO"));
    assert_eq!(r.rows[0].get_str(2), Some("hi"));
    assert_eq!(r.rows[0].get_str(3), Some("abc"));

    // Type casts
    let r = c.query("SELECT '42'::int, 3.14::text, true::int, NULL::text").expect("casts");
    assert_eq!(r.rows[0].get_i32(0), Some(42));
    assert_eq!(r.rows[0].get_str(1), Some("3.14"));
    assert_eq!(r.rows[0].get_i32(2), Some(1));
    assert!(r.rows[0].is_null(3));

    // CASE WHEN
    let r = c.query("SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END").expect("case");
    assert_eq!(r.rows[0].get_str(0), Some("yes"));

    // COALESCE + NULLIF
    let r = c.query("SELECT COALESCE(NULL, NULL, 'found'), NULLIF(1, 1), NULLIF(1, 2)").expect("coalesce");
    assert_eq!(r.rows[0].get_str(0), Some("found"));
    assert!(r.rows[0].is_null(1));
    assert_eq!(r.rows[0].get_i32(2), Some(1));

    // Array (PG-specific)
    let r = c.query("SELECT ARRAY[1,2,3]::text").expect("array");
    assert_eq!(r.rows[0].get_str(0), Some("{1,2,3}"));

    // JSON (PG-specific)
    let r = c.query("SELECT '{\"key\": \"value\"}'::json->>'key'").expect("json");
    assert_eq!(r.rows[0].get_str(0), Some("value"));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_where_and_ordering() {
    let mut c = $config_fn();
    c.execute("CREATE TEMP TABLE wh_items(id int, name text, price int, active bool)").expect("create");
    c.execute("INSERT INTO wh_items VALUES (1,'pen',10,true),(2,'book',50,true),(3,'eraser',5,false),(4,'ruler',15,true),(5,'pencil',8,false)").expect("ins");

    // WHERE with AND/OR
    let r = c.query("SELECT name FROM wh_items WHERE active AND price > 10 ORDER BY name").expect("and");
    assert_eq!(r.rows.len(), 2); // book(50), ruler(15)
    let r = c.query("SELECT name FROM wh_items WHERE price < 10 OR price > 40 ORDER BY name").expect("or");
    assert!(r.rows.len() >= 2); // eraser(5), pencil(8), book(50)

    // IN, NOT IN
    let r = c.query("SELECT name FROM wh_items WHERE id IN (1, 3, 5) ORDER BY id").expect("in");
    assert_eq!(r.rows.len(), 3);

    // BETWEEN
    let r = c.query("SELECT name FROM wh_items WHERE price BETWEEN 8 AND 15 ORDER BY price").expect("between");
    assert_eq!(r.rows.len(), 3); // pencil(8), pen(10), ruler(15)

    // LIKE / ILIKE
    let r = c.query("SELECT name FROM wh_items WHERE name LIKE 'p%' ORDER BY name").expect("like");
    assert_eq!(r.rows.len(), 2); // pen, pencil
    let r = c.query("SELECT name FROM wh_items WHERE name ILIKE 'P%' ORDER BY name").expect("ilike");
    assert_eq!(r.rows.len(), 2);

    // ORDER BY + LIMIT + OFFSET
    let r = c.query("SELECT name FROM wh_items ORDER BY price DESC LIMIT 2 OFFSET 1").expect("limit offset");
    assert_eq!(r.rows.len(), 2);

    // IS NULL / IS NOT NULL
    let r = c.query("SELECT name FROM wh_items WHERE active IS NOT NULL").expect("is not null");
    assert_eq!(r.rows.len(), 5);

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_keyset_pagination() {
    let mut c = $config_fn();
    c.execute("CREATE TEMP TABLE pages(id int PRIMARY KEY, val text)").expect("create");
    for i in 1..=20 {
        c.execute(&format!("INSERT INTO pages VALUES ({i}, 'row_{i}')")).expect("ins");
    }

    // Keyset pagination (seek method)
    let page1 = c.query("SELECT id, val FROM pages ORDER BY id LIMIT 5").expect("page1");
    assert_eq!(page1.rows.len(), 5);
    let last_id = page1.rows[4].get_i32(0).expect("last id");

    let page2 = c.query_params(
        "SELECT id, val FROM pages WHERE id > $1 ORDER BY id LIMIT 5",
        &(last_id,),
    ).expect("page2");
    assert_eq!(page2.rows.len(), 5);
    assert_eq!(page2.rows[0].get_i32(0), Some(last_id + 1));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_upsert_and_returning() {
    let mut c = $config_fn();
    c.execute("CREATE TEMP TABLE kv(key text PRIMARY KEY, val int)").expect("create");
    c.execute("INSERT INTO kv VALUES ('a', 1)").expect("ins");

    // ON CONFLICT (upsert)
    c.execute("INSERT INTO kv VALUES ('a', 10) ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val").expect("upsert");
    let r = c.query("SELECT val FROM kv WHERE key = 'a'").expect("check");
    assert_eq!(r.rows[0].get_i32(0), Some(10));

    // INSERT ... RETURNING
    let r = c.query("INSERT INTO kv VALUES ('b', 20) RETURNING key, val").expect("returning");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].get_str(0), Some("b"));
    assert_eq!(r.rows[0].get_i32(1), Some(20));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_generate_series_and_lateral() {
    let mut c = $config_fn();

    // generate_series
    let r = c.query("SELECT * FROM generate_series(1, 5)").expect("series");
    assert_eq!(r.rows.len(), 5);

    // LATERAL join
    c.execute("CREATE TEMP TABLE depts(id int, name text)").expect("create");
    c.execute("CREATE TEMP TABLE emps(id int, dept_id int, name text)").expect("create2");
    c.execute("INSERT INTO depts VALUES (1,'eng'),(2,'sales')").expect("ins d");
    c.execute("INSERT INTO emps VALUES (1,1,'a'),(2,1,'b'),(3,2,'c')").expect("ins e");

    let r = c.query("
        SELECT d.name, e.name FROM depts d,
        LATERAL (SELECT name FROM emps WHERE dept_id = d.id ORDER BY name LIMIT 1) e
        ORDER BY d.name
    ").expect("lateral");
    assert_eq!(r.rows.len(), 2);

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_system_and_meta_queries() {
    let mut c = $config_fn();

    // SELECT version()
    let r = c.query("SELECT version()").expect("version");
    let v = r.rows[0].get_str(0).expect("version str");
    assert!(v.contains("PostgreSQL"));

    // current_database, current_user, current_timestamp
    let r = c.query("SELECT current_database(), current_user, now()::text").expect("meta");
    assert!(r.rows[0].get_str(0).is_some());
    assert!(r.rows[0].get_str(1).is_some());
    assert!(r.rows[0].get_str(2).is_some());

    // pg_catalog queries
    let r = c.query("SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = 'pg_catalog'").expect("pg_tables");
    assert!(r.rows[0].get_i64(0).is_some_and(|n| n > 0));

    // information_schema
    let r = c.query("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'pg_catalog'").expect("info_schema");
    assert!(r.rows[0].get_i64(0).is_some_and(|n| n > 0));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_edge_cases() {
    let mut c = $config_fn();

    // Empty string vs NULL
    c.execute("CREATE TEMP TABLE edge(v text)").expect("create");
    c.execute("INSERT INTO edge VALUES (''), (NULL), (' '), ('hello')").expect("ins");
    let r = c.query("SELECT v FROM edge ORDER BY v NULLS FIRST").expect("q");
    assert!(r.rows[0].is_null(0)); // NULL first
    assert_eq!(r.rows[1].get_str(0), Some("")); // empty string
    assert_eq!(r.rows[2].get_str(0), Some(" ")); // space

    // Long text value (within WriteBuf capacity)
    let long = "x".repeat(1_000);
    c.execute("CREATE TEMP TABLE longval(v text)").expect("create");
    c.execute_params("INSERT INTO longval VALUES ($1)", &(long.as_str(),)).expect("ins long");
    let r = c.query("SELECT LENGTH(v) FROM longval").expect("len");
    assert_eq!(r.rows[0].get_i32(0), Some(1_000));

    // Unicode edge cases
    let r = c.query("SELECT '🦀🐘'::text, E'tab\\there'::text, E'new\\nline'::text").expect("unicode");
    assert_eq!(r.rows[0].get_str(0), Some("🦀🐘"));
    assert!(r.rows[0].get_str(1).is_some_and(|s| s.contains('\t')));
    assert!(r.rows[0].get_str(2).is_some_and(|s| s.contains('\n')));

    // Numeric edge cases
    let r = c.query("SELECT 2147483647::int, (-2147483647-1)::int, 9223372036854775807::bigint").expect("nums");
    assert_eq!(r.rows[0].get_i32(0), Some(i32::MAX));
    assert_eq!(r.rows[0].get_i32(1), Some(i32::MIN));
    assert_eq!(r.rows[0].get_i64(2), Some(i64::MAX));

    // Boolean
    let r = c.query("SELECT true, false, NULL::bool").expect("bools");
    assert_eq!(r.rows[0].get_bool(0), Some(true));
    assert_eq!(r.rows[0].get_bool(1), Some(false));
    assert!(r.rows[0].is_null(2));

    // Empty result set
    let r = c.query("SELECT 1 WHERE false").expect("empty");
    assert_eq!(r.rows.len(), 0);

    // Multiple columns same name (PG allows this)
    let r = c.query("SELECT 1 AS x, 2 AS x").expect("dup cols");
    assert_eq!(r.rows[0].get_i32(0), Some(1));
    assert_eq!(r.rows[0].get_i32(1), Some(2));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn sql_date_and_interval() {
    let mut c = $config_fn();

    let r = c.query("SELECT '2024-01-15'::date::text, '13:45:00'::time::text").expect("date/time");
    assert_eq!(r.rows[0].get_str(0), Some("2024-01-15"));
    assert_eq!(r.rows[0].get_str(1), Some("13:45:00"));

    let r = c.query("SELECT '2024-01-15'::date + interval '1 month' + interval '2 days'").expect("interval");
    assert!(r.rows[0].get_str(0).is_some());

    // EXTRACT
    let r = c.query("SELECT EXTRACT(YEAR FROM '2024-06-15'::date)::int").expect("extract");
    assert_eq!(r.rows[0].get_i32(0), Some(2024));

    c.close().expect("close");
}

    }; // end macro
}
