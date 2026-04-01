// This should fail to compile when feature "decimal" is not enabled.
// Expected error: "column type is NUMERIC — enable feature \"decimal\" in bsql"
fn main() {
    let id = 1i32;
    let _ = bsql::query!("SELECT id, budget FROM tickets WHERE id = $id: i32");
}
