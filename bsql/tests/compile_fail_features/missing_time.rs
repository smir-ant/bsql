// This should fail to compile when feature "time" is not enabled.
// Expected error: "column type is TIMESTAMPTZ — enable feature \"time\" or \"chrono\" in bsql"
fn main() {
    let id = 1i32;
    let _ = bsql::query!("SELECT id, deadline FROM tickets WHERE id = $id: i32");
}
