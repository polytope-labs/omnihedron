//! Query protection middleware.
//!
//! Applied before schema execution when `--unsafe-mode` is **not** set:
//! - [`batch`]      — rejects request arrays larger than `--query-batch-limit`
//! - [`depth`]      — rejects queries whose AST nesting exceeds `--query-depth-limit`
//! - [`complexity`] — rejects queries whose field count exceeds `--query-complexity`
//! - [`aliases`]    — rejects queries with more field aliases than `--query-alias-limit`

pub mod aliases;
pub mod batch;
pub mod complexity;
pub mod depth;
