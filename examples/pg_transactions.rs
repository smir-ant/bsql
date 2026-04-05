//! PostgreSQL transactions with `.defer()` batching.
//!
//! Demonstrates:
//!   - `pool.begin()` to start a transaction
//!   - `.defer(&tx)` to buffer writes (no network I/O until commit)
//!   - `.run(&tx)` for immediate writes within a transaction
//!   - `tx.commit()` to flush deferred operations and commit
//!   - Savepoints for partial rollback within a transaction
//!   - Isolation levels for serializable reads
//!
//! `.defer()` is the recommended way to batch writes. All deferred operations
//! are sent as a single TCP pipeline on commit -- one round-trip for N writes.
//!
//! If a transaction is dropped without `commit()`, it automatically rolls back.
//!
//! ## Setup
//!
//! ```sql
//! CREATE TABLE accounts (
//!     id      SERIAL PRIMARY KEY,
//!     name    TEXT NOT NULL,
//!     balance INT NOT NULL
//! );
//! CREATE TABLE audit_log (
//!     id         SERIAL PRIMARY KEY,
//!     account_id INT NOT NULL,
//!     delta      INT NOT NULL,
//!     note       TEXT
//! );
//! INSERT INTO accounts (name, balance) VALUES ('Alice', 1000), ('Bob', 500);
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! cargo run --bin pg_transactions
//! ```

use bsql::{BsqlError, IsolationLevel, Pool};

fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;

    // ---------------------------------------------------------------
    // Transfer with .defer() — all writes batched into one pipeline
    // ---------------------------------------------------------------
    let tx = pool.begin()?;

    let from_id = 1i32;
    let to_id = 2i32;
    let amount = 100i32;

    // .defer() buffers the write — nothing is sent to PostgreSQL yet.
    bsql::query!(
        "UPDATE accounts SET balance = balance - $amount: i32 WHERE id = $from_id: i32"
    )
    .defer(&tx)?;

    bsql::query!(
        "UPDATE accounts SET balance = balance + $amount: i32 WHERE id = $to_id: i32"
    )
    .defer(&tx)?;

    // Log the transfer in the audit table — also deferred.
    let note = "transfer between accounts";
    let neg_amount = -amount;
    bsql::query!(
        "INSERT INTO audit_log (account_id, delta, note)
         VALUES ($from_id: i32, $neg_amount: i32, $note: &str)"
    )
    .defer(&tx)?;

    bsql::query!(
        "INSERT INTO audit_log (account_id, delta, note)
         VALUES ($to_id: i32, $amount: i32, $note: &str)"
    )
    .defer(&tx)?;

    // commit() flushes all 4 deferred operations in a single TCP write, then commits.
    tx.commit()?;
    println!("Transfer of {amount} from account {from_id} to {to_id} committed.");

    // ---------------------------------------------------------------
    // Savepoints — partial rollback within a transaction
    // ---------------------------------------------------------------
    let tx = pool.begin()?;

    // Debit the source account (immediate write, not deferred).
    let account_id = 1i32;
    let debit = -50i32;
    bsql::query!(
        "UPDATE accounts SET balance = balance + $debit: i32 WHERE id = $account_id: i32"
    )
    .run(&tx)?;

    // Create a savepoint before a risky operation.
    tx.savepoint("before_audit")?;

    // Try to insert an audit record. If this fails, roll back to the
    // savepoint without losing the debit.
    let note = "monthly fee";
    let audit_result = bsql::query!(
        "INSERT INTO audit_log (account_id, delta, note)
         VALUES ($account_id: i32, $debit: i32, $note: &str)"
    )
    .run(&tx);

    match audit_result {
        Ok(_) => println!("Audit log inserted."),
        Err(e) => {
            println!("Audit insert failed ({e}), rolling back to savepoint.");
            tx.rollback_to("before_audit")?;
        }
    }

    tx.commit()?;
    println!("Transaction with savepoint committed.");

    // ---------------------------------------------------------------
    // Isolation levels — serializable reads
    // ---------------------------------------------------------------
    let tx = pool.begin()?;

    // Set isolation before the first query in the transaction.
    tx.set_isolation(IsolationLevel::Serializable)?;

    let account_id = 1i32;
    let accounts = bsql::query!(
        "SELECT id, name, balance FROM accounts WHERE id = $account_id: i32"
    )
    .fetch(&tx)?;
    let account = &accounts[0];
    println!(
        "Serializable read: account {} has balance {}",
        account.name, account.balance
    );

    tx.commit()?;

    Ok(())
}
