// This should fail to compile when feature "uuid" is not enabled.
// Expected error: "column type is UUID — enable feature \"uuid\" in sasql"
fn main() {
    let id = 1i32;
    let _ = sasql::query!("SELECT id, ticket_uuid FROM tickets WHERE id = $id: i32");
}
