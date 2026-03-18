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

//! Cursor-based and offset pagination helpers.
//!
//! [`resolve_pagination`] translates the standard GraphQL pagination arguments
//! (`first`, `last`, `after`, `before`, `offset`) into a [`PaginationResult`]
//! that resolvers can apply directly to their SQL queries.

use crate::schema::cursor::decode_cursor;
use serde_json::Value;

/// The parsed, validated pagination arguments from a GraphQL connection field.
#[derive(Debug, Default)]
pub struct PaginationArgs {
	/// Return the first N results (forward pagination).
	pub first: Option<usize>,
	/// Return the last N results (backward pagination).
	pub last: Option<usize>,
	/// Return results after this opaque cursor (exclusive).
	pub after: Option<String>,
	/// Return results before this opaque cursor (exclusive).
	pub before: Option<String>,
	/// Skip this many rows (simple offset; takes precedence over cursors).
	pub offset: Option<usize>,
}

/// The resolved pagination state to be applied to a SQL query.
pub struct PaginationResult {
	/// The effective `LIMIT` value (derived from `first`/`last` or the default).
	#[allow(dead_code)]
	pub limit: usize,
	/// The effective `OFFSET` value.
	pub offset: usize,
	/// An optional `(WHERE condition, bound values)` pair generated from the
	/// `after` or `before` cursor argument.
	pub cursor_condition: Option<(String, Vec<Value>)>,
	/// `true` when the query is a backward-pagination request (`last` was set).
	/// The resolver may reverse the result set before returning it.
	#[allow(dead_code)]
	pub is_backwards: bool,
}

/// Translate [`PaginationArgs`] into a [`PaginationResult`].
///
/// `order_cols` must be the same columns used in the `ORDER BY` clause so that
/// the cursor condition correctly compares against the right fields.
/// `param_offset` is incremented for each new `$N` placeholder emitted.
/// `default_limit` is used when neither `first` nor `last` is provided.
pub fn resolve_pagination(
	args: &PaginationArgs,
	order_cols: &[String],
	param_offset: &mut usize,
	default_limit: usize,
) -> anyhow::Result<PaginationResult> {
	// Match PostGraphile: reject invalid combinations.
	if args.first.is_some() && args.last.is_some() {
		anyhow::bail!("We don't support setting both first and last");
	}
	if args.offset.is_some() && args.last.is_some() {
		anyhow::bail!("We don't support setting both offset and last");
	}

	let limit = args.first.or(args.last).unwrap_or(default_limit);
	let offset = args.offset.unwrap_or(0);

	let mut cursor_condition: Option<(String, Vec<Value>)> = None;

	if let Some(after) = &args.after {
		let cursor_map = decode_cursor(after)?;
		// Build (col1, col2, ...) > (val1, val2, ...) for ASC ordering
		if !order_cols.is_empty() {
			let cols = order_cols.iter().map(|c| format!("t.{c}")).collect::<Vec<_>>().join(", ");
			let mut vals = vec![];
			let mut placeholders = vec![];
			for col in order_cols {
				*param_offset += 1;
				placeholders.push(format!("${}", param_offset));
				let col_val = cursor_map.get(col).cloned().unwrap_or(Value::Null);
				vals.push(col_val);
			}
			cursor_condition = Some((format!("({cols}) > ({})", placeholders.join(", ")), vals));
		}
	} else if let Some(before) = &args.before {
		let cursor_map = decode_cursor(before)?;
		if !order_cols.is_empty() {
			let cols = order_cols.iter().map(|c| format!("t.{c}")).collect::<Vec<_>>().join(", ");
			let mut vals = vec![];
			let mut placeholders = vec![];
			for col in order_cols {
				*param_offset += 1;
				placeholders.push(format!("${}", param_offset));
				let col_val = cursor_map.get(col).cloned().unwrap_or(Value::Null);
				vals.push(col_val);
			}
			cursor_condition = Some((format!("({cols}) < ({})", placeholders.join(", ")), vals));
		}
	}

	Ok(PaginationResult { limit, offset, cursor_condition, is_backwards: args.last.is_some() })
}
