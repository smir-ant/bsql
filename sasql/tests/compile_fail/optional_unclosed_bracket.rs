fn main() {
    let a: Option<i32> = Some(1);
    let _ = sasql::query!(
        "SELECT id FROM tickets WHERE 1 = 1 [AND department_id = $a: Option<i32>"
    );
}
