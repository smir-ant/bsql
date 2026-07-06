// EXPECT: the borrowing `Transaction` guard the async `transaction` hands its
// closure exposes ONLY the data / bulk / session verbs, NOT the six transaction /
// connection LIFECYCLE verbs. Each call below is a method-not-found error (E0599)
// — hand-driving the transaction boundary from inside the body is impossible by
// construction (the compile-time atomicity guarantee), never a silent runtime
// break. `execute_sql` (a data verb) is deliberately NOT here: it compiles, which
// is what makes the exclusion of exactly the lifecycle verbs meaningful.
use bsql_postgres_async::{Connection, DriverError};

async fn misuse(c: &mut Connection) -> Result<(), DriverError> {
    c.transaction(async |tx| {
        let _ = tx.begin();
        let _ = tx.commit();
        let _ = tx.rollback();
        // Nesting: the guard has no `transaction`, so a helper cannot open its own
        // inner transaction and silently flatten the outer's atomic scope. The
        // inner closure is fully annotated so the ONLY error is the missing method.
        let _ = tx.transaction(async |_inner: ()| Ok::<(), DriverError>(()));
        let _ = tx.close();
        let _ = tx.reset_session();
        Ok(())
    })
    .await
}

fn main() {}
