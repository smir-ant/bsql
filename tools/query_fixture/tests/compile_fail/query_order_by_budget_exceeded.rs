// Seventeen distinct `ORDER BY` orderings exceed the compile-time budget
// (`MAX_ORDER_BY_VARIANTS = 16`). Each ordering becomes one baked
// prepared-query wire variant and one selector enum variant; the generated
// `const` assertion fails const-evaluation (`error[E0080]`) — a loud build
// error, never a silent truncation of orderings.
fn main() {
    bsql_query_macros::query!(
        Row,
        "SELECT id FROM wide ORDER BY { \
         c1 ASC | c2 ASC | c3 ASC | c4 ASC | c5 ASC | c6 ASC | \
         c7 ASC | c8 ASC | c9 ASC | c10 ASC | c11 ASC | c12 ASC | \
         c13 ASC | c14 ASC | c15 ASC | c16 ASC | c17 ASC }"
    );
}
