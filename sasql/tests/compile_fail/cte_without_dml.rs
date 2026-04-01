// CTE must be followed by SELECT, INSERT, UPDATE, or DELETE
fn main() {
    let _ = sasql::query!("WITH cte AS (SELECT 1)");
}
