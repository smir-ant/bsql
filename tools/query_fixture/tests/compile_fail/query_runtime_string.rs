//! `query!` requires a string LITERAL for its SQL; a runtime string (a
//! variable or expression) is rejected at expansion. Routing untrusted
//! runtime data into `query!(Name, user_input)` would defeat the
//! compile-time schema validation and re-open the injection class. The macro
//! parses its SQL argument as a `syn::LitStr`, so a non-literal token is a
//! `compile_error!` at the offending span.

fn main() {
    let user_input = "SELECT id FROM users";
    bsql::query!(RuntimeSql, user_input);
    let _ = user_input;
}
