// bool cannot be used for text column
fn main() {
    let login = true;
    let _ = sasql::query!(
        "SELECT id FROM users WHERE login = $login: bool"
    );
}
