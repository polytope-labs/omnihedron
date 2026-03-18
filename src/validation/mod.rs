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
