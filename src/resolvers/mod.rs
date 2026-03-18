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
