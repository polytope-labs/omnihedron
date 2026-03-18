//! GraphQL field resolvers.
//!
//! Each sub-module handles one category of resolver:
//! - [`connection`]  — list queries with filtering, ordering, and pagination
//! - [`single`]      — single-record lookup by primary key or nodeId
//! - [`relations`]   — forward (FK → parent) and backward (reverse FK → children) relations
//! - [`aggregates`]  — aggregate functions (count, sum, min, max, avg, stddev, variance)
//! - [`metadata`]    — `_metadata` and `_metadatas` queries

pub mod aggregates;
pub mod connection;
pub mod metadata;
pub mod relations;
pub mod single;
