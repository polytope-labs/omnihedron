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

//! SQL construction utilities.
//!
//! - [`builder`]    — fluent [`builder::QueryBuilder`] for parameterized `SELECT` statements
//! - [`filter`]     — translates GraphQL filter input objects into `WHERE` clause fragments
//! - [`pagination`] — cursor and offset pagination helpers

pub mod builder;
pub mod filter;
pub mod pagination;
pub mod search;
