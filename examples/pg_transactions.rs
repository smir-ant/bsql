//! PostgreSQL transactions with savepoints in bsql.
//!
//! Demonstrates: begin, commit, rollback, savepoint, rollback_to, isolation levels.
//!
//! Requires a PostgreSQL instance with:
//!   CREATE TABLE accounts (id SERIAL PRIMARY KEY, name TEXT NOT NULL, balance INT NOT NULL);
//!   CREATE TABLE audit_log (id SERIAL PRIMARY KEY, account_id INT NOT NULL, delta INT NOT NULL, note TEXT);
//!
//! Run:
//!   BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --bin pg_transactions

use bsql::{BsqlError, IsolationLevel, Pool};

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // --- Basic transaction ---
    // begin() acquires a connection and starts a transaction.
    // commit() makes it permanent. If the Transaction is dropped
    // without calling commit(), it automatically rolls back.
    let tx = pool.begin().await?;

    let from_id = 1i32;
    let to_id = 2i32;
    let amount = 100i32;

    bsql::query!(
        "UPDATE accounts SET balance = balance - $amount: i32 WHERE id = $from_id: i32"
    )
    .execute(&tx)
    .await?;

    bsql::query!(
        "UPDATE accounts SET balance = balance + $amount: i32 WHERE id = $to_id: i32"
    )
    .execute(&tx)
    .await?;

    tx.commit().await?;
    println!("Transfer of {amount} from account {from_id} to {to_id} committed.");

    // --- Transaction with savepoints ---
    // Savepoints let you partially roll back within a transaction.
    let tx = pool.begin().await?;

    // Debit the source account.
    let account_id = 1i32;
    let debit = -50i32;
    bsql::query!(
        "UPDATE accounts SET balance = balance + $debit: i32 WHERE id = $account_id: i32"
    )
    .execute(&tx)
    .await?;

    // Create a savepoint before the audit log insert.
    tx.savepoint("before_audit").await?;

    // Attempt to insert an audit record. If this fails (e.g., constraint
    // violation), we can roll back to the savepoint without losing the debit.
    let note = "monthly fee";
    let audit_result = bsql::query!(
        "INSERT INTO audit_log (account_id, delta, note)
         VALUES ($account_id: i32, $debit: i32, $note: &str)"
    )
    .execute(&tx)
    .await;

    match audit_result {
        Ok(_) => println!("Audit log inserted."),
        Err(e) => {
            println!("Audit insert failed ({e}), rolling back to savepoint.");
            tx.rollback_to("before_audit").await?;
        }
    }

    tx.commit().await?;
    println!("Transaction with savepoint committed.");

    // --- Isolation levels ---
    // Set the isolation level before executing queries in the transaction.
    let tx = pool.begin().await?;
    tx.set_isolation(IsolationLevel::Serializable).await?;

    let account_id = 1i32;
    let account = bsql::query!(
        "SELECT id, name, balance FROM accounts WHERE id = $account_id: i32"
    )
    .fetch_one(&tx)
    .await?;
    println!(
        "Serializable read: account {} has balance {}",
        account.name, account.balance
    );

    tx.commit().await?;

    Ok(())
}
