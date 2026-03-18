//! SQL construction utilities.
//!
//! - [`builder`]    — fluent [`builder::QueryBuilder`] for parameterized `SELECT` statements
//! - [`filter`]     — translates GraphQL filter input objects into `WHERE` clause fragments
//! - [`pagination`] — cursor and offset pagination helpers

pub mod builder;
pub mod filter;
pub mod pagination;
