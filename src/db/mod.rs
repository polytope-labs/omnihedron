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

//! PostgreSQL connection pool and schema discovery.
//!
//! - [`pool::create_pool`]              — builds a `deadpool-postgres` connection pool, optionally
//!   with TLS using the certificates from [`Config`].
//! - [`schema_discovery::discover_schema`] — resolves the target PostgreSQL schema name from the
//!   project name supplied via `--name`.

pub mod pool;
pub mod schema_discovery;

pub use pool::create_pool;
pub use schema_discovery::discover_schema;
