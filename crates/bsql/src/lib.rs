#![forbid(unsafe_code)]

#[cfg(feature = "postgres")]
pub mod pg {
    pub use bsql_postgres::*;
}

#[cfg(feature = "sqlite")]
pub mod sqlite {
    pub use bsql_sqlite::*;
}
