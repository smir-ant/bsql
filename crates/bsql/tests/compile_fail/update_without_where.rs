fn main() {
    let _ = bsql::query!("UPDATE users SET login = $login: &str");
}
