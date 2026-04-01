fn main() {
    let a = 1i32;
    let _ = sasql::query!(
        "SELECT id FROM tickets WHERE 1 = 1 [AND department_id = $a: i32]"
    );
}
