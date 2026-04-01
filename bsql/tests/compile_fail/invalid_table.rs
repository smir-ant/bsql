fn main() {
    let _ = bsql::query!(
        "SELECT id FROM nonexistent_table_xyz WHERE id = 1"
    );
}
