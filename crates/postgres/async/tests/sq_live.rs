#![forbid(unsafe_code)]
use bsql_postgres_async::{ConnectConfig, Connection};

// ═══════════════════════════════════════════════════════════
// Driver-specific tests (async I/O, TLS, pool, protocol)
// SQL coverage is in the shared macro at the bottom.
// ═══════════════════════════════════════════════════════════

#[tokio::test]
#[ignore = "requires local PG"]
async fn connect_and_ping() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.ping().await.expect("ping");
    assert!(c.is_healthy());
    assert!(c.server_version().is_some());
    assert!(c.backend_pid() > 0);
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn streaming_1k_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    let r = c.query("SELECT generate_series(1, 1000)").await.expect("q");
    assert_eq!(r.rows.len(), 1000);
    assert_eq!(r.rows[999].get_i32(0), Some(1000));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn streaming_10k_rows() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    let r = c.query("SELECT generate_series(1, 10000)").await.expect("q");
    assert_eq!(r.rows.len(), 10000);
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn error_recovery_and_resilience() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    assert!(c.simple_query("SELCT").await.is_err());
    assert!(c.query("SELECT * FROM nonexistent_xyz").await.is_err());
    assert!(c.query("SELECT 'abc'::int").await.is_err());
    assert!(c.query("SELECT 1/0").await.is_err());
    c.ping().await.expect("recover");
    c.execute("CREATE TEMP TABLE res(v int)").await.expect("create");
    c.execute("INSERT INTO res VALUES (42)").await.expect("insert");
    assert_eq!(c.query("SELECT v FROM res").await.expect("q").rows[0].get_i32(0), Some(42));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn prepared_reuse_after_constraint_violation() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute("CREATE TEMP TABLE pr(id int PRIMARY KEY)").await.expect("create");
    let stmt = c.prepare("INSERT INTO pr VALUES ($1)").await.expect("prepare");
    c.execute_prepared(&stmt, &(1i32,)).await.expect("ok");
    assert!(c.execute_prepared(&stmt, &(1i32,)).await.is_err());
    c.execute_prepared(&stmt, &(2i32,)).await.expect("after error");
    c.close_statement(stmt).await.expect("close stmt");
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn copy_in() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute("CREATE TEMP TABLE cp(v text)").await.expect("create");
    assert_eq!(c.copy_in("cp", vec!["a", "b"]).await.expect("copy"), 2);
    assert_eq!(c.copy_in("cp", Vec::<&str>::new()).await.expect("empty"), 0);
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn listen_notify() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut listener = Connection::connect(&config).await.expect("l");
    let mut notifier = Connection::connect(&config).await.expect("n");
    listener.listen("bsql_ch").await.expect("listen");
    notifier.simple_query("NOTIFY bsql_ch, 'hi'").await.expect("notify");
    let n = listener.recv_notification(std::time::Duration::from_secs(5))
        .await.expect("recv").expect("notif");
    assert_eq!(n.payload, "hi");
    listener.close().await.expect("close"); notifier.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn pool_basic() {
    use bsql_postgres_async::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let pool = Pool::new(config, 3).await.expect("pool");
    let mut c = pool.get().await.expect("get"); c.ping().await.expect("ping"); drop(c);
    assert_eq!(pool.idle_count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires local PG"]
async fn pool_concurrent() {
    use bsql_postgres_async::Pool;
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let pool = Pool::new(config, 3).await.expect("pool");
    let handles: Vec<_> = (0..10u32).map(|i| {
        let p = pool.clone();
        tokio::spawn(async move {
            let mut c = p.get().await.expect("get");
            assert_eq!(c.query(&format!("SELECT {i}::int")).await.expect("q").rows[0].get_i32(0), Some(i as i32));
        })
    }).collect();
    for h in handles { h.await.expect("task"); }
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn row_send_across_await() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    let row = c.query("SELECT 42::int").await.expect("q").rows[0].clone();
    assert_eq!(tokio::task::spawn(async move { row.get_i32(0) }).await.expect("spawn"), Some(42));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG with scram-sha-256 auth"]
async fn scram_auth() {
    let config = ConnectConfig::new("127.0.0.1", "bsql_test_scram")
        .database("postgres".to_string()).password("test_password_123".to_string());
    let mut c = Connection::connect(&config).await.expect("SCRAM");
    assert_eq!(c.query("SELECT current_user").await.expect("q").rows[0].get_raw(0), Some(b"bsql_test_scram".as_slice()));
    c.close().await.expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn ssl_modes() {
    for mode in [bsql_postgres_async::SslMode::Prefer, bsql_postgres_async::SslMode::Disable] {
        let config = ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string()).ssl_mode(mode);
        let mut c = Connection::connect(&config).await.expect("connect");
        c.ping().await.expect("ping");
        c.close().await.expect("close");
    }
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn full_lifecycle() {
    let config = ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string());
    let mut c = Connection::connect(&config).await.expect("connect");
    c.execute("CREATE TEMP TABLE lc(id serial PRIMARY KEY, name text, val int)").await.expect("create");
    c.begin().await.expect("begin");
    c.execute("INSERT INTO lc(name, val) VALUES ('alice', 95)").await.expect("ins");
    c.execute("INSERT INTO lc(name, val) VALUES ('bob', 88)").await.expect("ins");
    c.commit().await.expect("commit");
    assert_eq!(c.query_params_one("SELECT name FROM lc WHERE val > $1", &(90i32,))
        .await.expect("p").get_str(0), Some("alice"));
    let stmt = c.prepare("UPDATE lc SET val = val + $1 WHERE name = $2").await.expect("prep");
    c.execute_prepared(&stmt, &(5i32, "bob")).await.expect("update");
    c.close_statement(stmt).await.expect("close stmt");
    assert!(c.execute("INSERT INTO lc(id) VALUES (1)").await.is_err());
    c.ping().await.expect("recover");
    c.close().await.expect("close");
}
