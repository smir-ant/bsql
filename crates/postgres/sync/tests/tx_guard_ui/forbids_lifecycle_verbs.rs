// EXPECT: the borrowing `Transaction` guard the blocking `transaction` hands its
// closure exposes ONLY the data / bulk / session verbs, NOT the six transaction /
// connection LIFECYCLE verbs. Each call below is a method-not-found error (E0599)
// — hand-driving the transaction boundary from inside the body is impossible by
// construction (the compile-time atomicity guarantee), never a silent runtime
// break. `execute_sql` (a data verb) is deliberately NOT here: it compiles, which
// is what makes the exclusion of exactly the lifecycle verbs meaningful.
use bsql_postgres_sync::{Connection, DriverError};

fn misuse(c: &mut Connection) -> Result<(), DriverError> {
    c.transaction(|tx| {
        tx.begin()?;
        tx.commit()?;
        tx.rollback()?;
        // Nesting: the guard has no `transaction`, so a helper cannot open its own
        // inner transaction and silently flatten the outer's atomic scope. The
        // inner closure is fully annotated so the ONLY error is the missing method.
        tx.transaction(|_inner: ()| Ok::<(), DriverError>(()))?;
        tx.close()?;
        tx.reset_session()?;
        Ok(())
    })
}

fn main() {}
