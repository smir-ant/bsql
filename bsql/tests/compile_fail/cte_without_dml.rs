// CTE must be followed by SELECT, INSERT, UPDATE, or DELETE
fn main() {
    let _ = bsql::query!("WITH cte AS (SELECT 1)");
}
