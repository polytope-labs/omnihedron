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

//! Hot schema reload via PostgreSQL `LISTEN`/`NOTIFY`.
//!
//! [`start_schema_listener`] spawns a background task that holds a dedicated
//! PostgreSQL connection and listens on the SubQuery schema-change channel.
//! When a `schema_updated` notification arrives the schema is re-introspected
//! and atomically swapped behind the [`server::SharedSchema`] `RwLock`.
//!
//! A keep-alive `SELECT 1` is sent every `--sl-keep-alive-interval` ms
//! (default 180 s) to prevent the idle connection from being terminated.

pub mod listener;
pub use listener::start_schema_listener;
