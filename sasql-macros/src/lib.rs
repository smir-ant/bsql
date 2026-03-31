#![forbid(unsafe_code)]

//! Proc macros for sasql.
//!
//! This crate is an implementation detail. Use [`sasql`] instead.

extern crate proc_macro;

mod parse;
mod sql_norm;
mod stmt_name;
