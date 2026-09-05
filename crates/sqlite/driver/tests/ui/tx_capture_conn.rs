//! Proves that capturing the outer `conn` inside `conn.transaction` is
//! statically rejected by the borrow checker because `conn` is mutably borrowed
//! for the duration of `transaction(&mut self, ...)`.

use bsql_sqlite::Connection;

fn main() {
    let mut conn = Connection::open_in_memory().expect("open");
    let _ = conn.transaction(|_tx| {
        // Attempting to use conn directly inside closure while conn is mutably borrowed:
        let _ = conn.execute_raw("SELECT 1");
        Ok(())
    });
}
