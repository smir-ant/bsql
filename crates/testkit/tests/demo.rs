//! The moat proof: a consumer scripts a fake, gets a REAL connection, runs a
//! query, and asserts the decoded rows — with no `TcpStream` and no network.
//!
//! Nothing here touches a socket: `FakePostgres::connect` returns the same
//! `Connection` a real `connect` returns, driven by the sans-IO engine over an
//! in-memory transport. The rows asserted below were decoded by the real
//! engine + driver from wire bytes the fake produced.

use bsql_testkit::{rows, FakePostgres};

#[tokio::test]
async fn consumer_runs_query_against_the_fake_with_no_network(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut fake = FakePostgres::new();
    fake.on("SELECT id FROM users").returns(rows![[1_i64], [2_i64]]);

    let mut conn = fake.connect().await?;
    let result = conn.query_raw("SELECT id FROM users").await?;

    assert_eq!(result.len(), 2);
    assert_eq!(result.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
    assert_eq!(result.get(1).expect("row 1").get_i64(0), Ok(Some(2)));
    Ok(())
}

#[tokio::test]
async fn multiple_columns_and_types_decode() -> Result<(), Box<dyn std::error::Error>> {
    let mut fake = FakePostgres::new();
    fake.on("SELECT id, name, active FROM users")
        .returns(rows![[1_i64, "alice", true], [2_i64, "bob", false]]);

    let mut conn = fake.connect().await?;
    let result = conn.query_raw("SELECT id, name, active FROM users").await?;

    assert_eq!(result.len(), 2);
    assert_eq!(result.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
    assert_eq!(result.get(0).expect("row 0").get_str(1), Ok(Some("alice")));
    assert_eq!(result.get(0).expect("row 0").get_bool(2), Ok(Some(true)));
    assert_eq!(result.get(1).expect("row 1").get_str(1), Ok(Some("bob")));
    assert_eq!(result.get(1).expect("row 1").get_bool(2), Ok(Some(false)));
    Ok(())
}

#[tokio::test]
async fn null_cells_decode_as_none() -> Result<(), Box<dyn std::error::Error>> {
    let mut fake = FakePostgres::new();
    fake.on("SELECT nickname FROM users")
        .returns(rows![[Option::<&str>::None], ["yui"]]);

    let mut conn = fake.connect().await?;
    let result = conn.query_raw("SELECT nickname FROM users").await?;

    assert_eq!(result.len(), 2);
    assert!(result.get(0).expect("row 0").is_null(0));
    assert_eq!(result.get(1).expect("row 1").get_str(0), Ok(Some("yui")));
    Ok(())
}

#[tokio::test]
async fn scripted_error_surfaces_as_a_db_error() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT 1/0")
        .returns_error("22012", "division by zero");

    let mut conn = fake.connect().await.expect("connect");
    let err = conn
        .query_raw("SELECT 1/0")
        .await
        .expect_err("scripted error must surface");
    assert!(format!("{err}").contains("division by zero"), "got: {err}");
}

#[tokio::test]
async fn unscripted_query_is_a_loud_error_not_empty_rows() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT 1").returns(rows![[1_i64]]);

    let mut conn = fake.connect().await.expect("connect");
    let err = conn
        .query_raw("SELECT 2")
        .await
        .expect_err("an unscripted query must be a loud error, never empty rows");
    assert!(
        format!("{err}").contains("no scripted reply"),
        "got: {err}"
    );
}
