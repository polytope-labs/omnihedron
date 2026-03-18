// Copyright (C) 2026 Polytope Labs.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! PostgreSQL schema introspection.
//!
//! This module queries `information_schema` and `pg_catalog` at startup (and on
//! every hot-reload cycle) to discover the full structure of the target schema:
//! tables, columns, types, primary keys, foreign keys, unique constraints, and
//! enum types.
//!
//! Entry points:
//! - [`queries::introspect_schema`] — returns a [`Vec<model::TableInfo>`]
//! - [`queries::introspect_enums`]  — returns a [`Vec<model::EnumInfo>`]

pub mod model;
pub mod queries;
pub mod types;

pub use model::TableInfo;
#[allow(unused_imports)]
pub use model::{ColumnInfo, EnumInfo, ForeignKey};
pub use queries::{introspect_enums, introspect_schema};
