#![forbid(unsafe_code)]
use bsql_postgres_sync::{ConnectConfig, Connection};

fn sync_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable)
}

// ═══════════════════════════════════════════════════════════
// Driver-specific tests (I/O, protocol, infra — not SQL)
// ═══════════════════════════════════════════════════════════

#[test]
#[ignore = "requires local PG"]
fn connect_and_ping() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.ping().expect("ping");
    assert!(c.is_healthy());
    assert!(c.server_version().is_some());
    assert!(c.backend_pid() > 0);
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn streaming_1k_rows() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let r = c.query("SELECT generate_series(1, 1000)").expect("query");
    assert_eq!(r.rows.len(), 1000);
    assert_eq!(r.rows[0].get_i32(0), Some(1));
    assert_eq!(r.rows[999].get_i32(0), Some(1000));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn streaming_10k_rows() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let r = c.query("SELECT generate_series(1, 10000)").expect("query");
    assert_eq!(r.rows.len(), 10000);
    assert_eq!(r.rows[9999].get_i32(0), Some(10000));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn error_recovery_and_resilience() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    // 4 different error types, all recover
    assert!(c.simple_query("SELCT").is_err());
    assert!(c.query("SELECT * FROM nonexistent_xyz").is_err());
    assert!(c.query("SELECT 'abc'::int").is_err());
    assert!(c.query("SELECT 1/0").is_err());
    c.ping().expect("ping after 4 errors");
    // Full CRUD still works
    c.execute("CREATE TEMP TABLE resilience(v int)").expect("create");
    c.execute("INSERT INTO resilience VALUES (42)").expect("insert");
    assert_eq!(c.query("SELECT v FROM resilience").expect("select").rows[0].get_i32(0), Some(42));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn prepared_reuse_after_constraint_violation() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute("CREATE TEMP TABLE pr_err(id int PRIMARY KEY)").expect("create");
    let stmt = c.prepare("INSERT INTO pr_err VALUES ($1)").expect("prepare");
    c.execute_prepared(&stmt, &(1i32,)).expect("insert 1");
    assert!(c.execute_prepared(&stmt, &(1i32,)).is_err());
    c.execute_prepared(&stmt, &(2i32,)).expect("insert 2 after error");
    assert_eq!(c.query("SELECT count(*) FROM pr_err").expect("count").rows[0].get_i64(0), Some(2));
    c.close_statement(stmt).expect("close stmt");
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn copy_in() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute("CREATE TEMP TABLE cp(id int, name text)").expect("create");
    assert_eq!(c.copy_in("cp", vec!["1\talice", "2\tbob"]).expect("copy"), 2);
    assert_eq!(c.copy_in("cp", Vec::<&str>::new()).expect("copy empty"), 0);
    c.execute("CREATE TEMP TABLE cp_lg(i int)").expect("create lg");
    let rows: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
    assert_eq!(c.copy_in("cp_lg", &rows).expect("copy 1000"), 1000);
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn listen_notify() {
    let mut listener = Connection::connect(&sync_config()).expect("listener");
    let mut notifier = Connection::connect(&sync_config()).expect("notifier");
    listener.listen("bsql_sync_ch").expect("listen");
    notifier.simple_query("NOTIFY bsql_sync_ch, 'hello'").expect("notify");
    let n = listener.recv_notification(std::time::Duration::from_secs(5))
        .expect("recv").expect("should have notification");
    assert_eq!(n.channel, "bsql_sync_ch");
    assert_eq!(n.payload, "hello");
    listener.close().expect("close"); notifier.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn pool() {
    use bsql_postgres_sync::Pool;
    let pool = Pool::new(sync_config(), 3);
    let mut c = pool.get().expect("get"); c.ping().expect("ping"); drop(c);
    assert_eq!(pool.idle_count(), 1);
    let handles: Vec<_> = (0..10u32).map(|i| {
        let p = pool.clone();
        std::thread::spawn(move || {
            let mut conn = p.get().expect("get");
            assert_eq!(conn.query(&format!("SELECT {i}::int")).expect("q").rows[0].get_i32(0), Some(i as i32));
        })
    }).collect();
    for h in handles { h.join().expect("thread"); }
}

#[test]
#[ignore = "requires local PG"]
fn transaction_closure() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute("CREATE TEMP TABLE tx(v int)").expect("create");
    c.transaction(|tx| { tx.execute("INSERT INTO tx VALUES (1)")?; Ok(()) }).expect("commit");
    assert_eq!(c.query("SELECT count(*) FROM tx").expect("c").rows[0].get_i64(0), Some(1));
    let _: Result<(), _> = c.transaction(|tx| {
        tx.execute("INSERT INTO tx VALUES (2)")?;
        Err(bsql_postgres_sync::DriverError::NoRows)
    });
    assert_eq!(c.query("SELECT count(*) FROM tx").expect("c").rows[0].get_i64(0), Some(1));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn row_clone_across_threads() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let row = c.query("SELECT 42::int, 'hello'::text").expect("q").rows[0].clone();
    let handle = std::thread::spawn(move || row.get_i32(0));
    assert_eq!(handle.join().expect("thread"), Some(42));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn full_lifecycle() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute("CREATE TEMP TABLE lc(id serial PRIMARY KEY, name text, val int)").expect("create");
    c.transaction(|tx| {
        tx.execute("INSERT INTO lc(name, val) VALUES ('alice', 95)")?;
        tx.execute("INSERT INTO lc(name, val) VALUES ('bob', 88)")?;
        Ok(())
    }).expect("tx");
    assert_eq!(c.query_params_one("SELECT name FROM lc WHERE val > $1", &(90i32,)).expect("p").get_str(0), Some("alice"));
    let stmt = c.prepare("UPDATE lc SET val = val + $1 WHERE name = $2").expect("prep");
    c.execute_prepared(&stmt, &(5i32, "bob")).expect("update");
    c.close_statement(stmt).expect("close stmt");
    assert!(c.execute("INSERT INTO lc(id) VALUES (1)").is_err()); // dup PK
    c.ping().expect("recover");
    assert_eq!(c.copy_in("lc", Vec::<&str>::new()).expect("copy empty"), 0);
    c.close().expect("close");
}

// ═══════════════════════════════════════════════════════════
// Shared SQL scenario tests (macro — covers ALL SQL mechanics)
// ═══════════════════════════════════════════════════════════

fn make_sync_conn() -> Connection {
    Connection::connect(&sync_config()).expect("connect")
}

bsql_postgres_core::define_sync_sql_tests!(make_sync_conn);

#[test]
#[ignore = "requires local PG"]
fn pool_stress_100_tasks() {
    use bsql_postgres_sync::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable);
    let pool = Pool::new(config, 3);

    let handles: Vec<_> = (0..100u32).map(|i| {
        let p = pool.clone();
        std::thread::spawn(move || {
            let mut c = p.get().expect("get");
            let r = c.query(&format!("SELECT {i}::int, pg_backend_pid()")).expect("q");
            assert_eq!(r.rows[0].get_i32(0), Some(i as i32));
        })
    }).collect();
    let mut ok = 0u32;
    for h in handles {
        if h.join().is_ok() { ok += 1; }
    }
    assert_eq!(ok, 100, "all 100 tasks should succeed");
}

#[test]
#[ignore = "requires local PG"]
fn one_connection_everything() {
    // Single connection exercises every feature sequentially
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // DDL
    c.execute("CREATE TEMP TABLE omni(id serial PRIMARY KEY, name text, val int, active bool)").expect("create");
    c.execute("CREATE INDEX ON omni(val)").expect("index");

    // DML via execute
    c.execute("INSERT INTO omni(name, val, active) VALUES ('a', 10, true)").expect("ins");
    c.execute("INSERT INTO omni(name, val, active) VALUES ('b', 20, false)").expect("ins");
    c.execute("INSERT INTO omni(name, val, active) VALUES ('c', 30, true)").expect("ins");

    // DML via execute_params (uses typed binary encoding)
    c.execute_params("INSERT INTO omni(name, val, active) VALUES ($1, $2, $3)", &("d", 40i32, true)).expect("params");

    // Query
    let r = c.query("SELECT count(*) FROM omni").expect("count");
    assert_eq!(r.rows[0].get_i64(0), Some(4));

    // Query with params
    let r = c.query_params("SELECT name FROM omni WHERE val > $1 ORDER BY val", &(15i32,)).expect("qp");
    assert_eq!(r.rows.len(), 3); // b, c, d

    // Prepared
    let stmt = c.prepare("SELECT name, val FROM omni WHERE active = $1 ORDER BY val").expect("prep");
    let r = c.query_prepared(&stmt, &(true,)).expect("qprep");
    assert_eq!(r.rows.len(), 3); // a, c, d
    c.close_statement(stmt).expect("close stmt");

    // Transaction
    c.transaction(|tx| {
        tx.execute("UPDATE omni SET val = val * 2 WHERE active")?;
        Ok(())
    }).expect("tx");
    let r = c.query("SELECT SUM(val) FROM omni").expect("sum");
    // a:20 + b:20(unchanged) + c:60 + d:80 = 180
    assert_eq!(r.rows[0].get_i64(0), Some(180));

    // Error + recovery
    assert!(c.query("SELECT * FROM nonexistent").is_err());
    c.ping().expect("recover");

    // COPY IN
    c.execute("CREATE TEMP TABLE cp_omni(v int)").expect("create cp");
    c.copy_in("cp_omni", vec!["1", "2", "3"]).expect("copy");

    // Column names
    let r = c.query("SELECT id, name, val FROM omni LIMIT 1").expect("cols");
    assert_eq!(&*r.column_names, &["id", "name", "val"]);

    // Row clone across thread
    let row = c.query("SELECT 'final'::text").expect("q").rows[0].clone();
    let v = std::thread::spawn(move || row.get_str(0).map(String::from)).join().expect("thread");
    assert_eq!(v, Some("final".to_string()));

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn wide_250_columns() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let cols: Vec<String> = (0..250u32).map(|i| format!("{i}::int AS col_{i}")).collect();
    let sql = format!("SELECT {}", cols.join(", "));
    let r = c.query(&sql).expect("250 cols");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.column_names.len(), 250);
    assert_eq!(r.rows[0].get_i32(0), Some(0));
    assert_eq!(r.rows[0].get_i32(249), Some(249));
    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn wide_columns() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    for n in [250u32, 500, 600, 800, 1000, 1600] {
        let cols: Vec<String> = (0..n).map(|i| format!("{i}::int AS col_{i}")).collect();
        let sql = format!("SELECT {}", cols.join(", "));
        let r = c.query(&sql).unwrap_or_else(|e| panic!("{n} cols failed: {e}"));
        assert_eq!(r.rows.len(), 1, "rows at {n} cols");
        assert_eq!(r.column_names.len(), usize::try_from(n).unwrap(), "col names at {n}");
        assert_eq!(r.rows[0].get_i32(0), Some(0), "first col at {n}");
        let last = usize::try_from(n.saturating_sub(1)).unwrap();
        assert_eq!(r.rows[0].get_i32(last), Some(n.saturating_sub(1) as i32), "last col at {n}");
    }
    c.close().expect("close");
}
