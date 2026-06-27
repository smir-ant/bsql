// Nine `OPTIONAL(...)` toggled filters exceed the compile-time budget
// (`MAX_OPTIONAL_FILTERS = 8`). The generated `const` assertion fails
// const-evaluation (`error[E0080]`) — a loud build error, never a silent
// truncation of filters.
fn main() {
    bsql_query_macros::query!(
        Row,
        "SELECT id FROM orders WHERE \
         OPTIONAL(total = $1) OR OPTIONAL(total = $2) OR OPTIONAL(total = $3) OR \
         OPTIONAL(total = $4) OR OPTIONAL(total = $5) OR OPTIONAL(total = $6) OR \
         OPTIONAL(total = $7) OR OPTIONAL(total = $8) OR OPTIONAL(total = $9)"
    );
}
