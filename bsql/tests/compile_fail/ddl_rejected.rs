fn main() {
    let _ = bsql::query!(
        "CREATE TABLE should_not_work (id int)"
    );
}
