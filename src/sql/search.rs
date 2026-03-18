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

//! Full-text search query sanitization.
//!
//! [`sanitize_tsquery`] converts a user-provided search string into a safe
//! PostgreSQL `tsquery` input, similar to the `pg-tsquery` npm package used
//! by PostGraphile's `PgSearchPlugin`.
//!
//! Rules:
//! - Splits on whitespace into terms
//! - Strips characters that are invalid in tsquery (parentheses, colons, etc.)
//! - Joins terms with `&` (AND) by default
//! - Empty input returns `None` (caller should skip the search condition)

/// Sanitize a user search string for use in PostgreSQL `to_tsquery()`.
///
/// Returns `None` if the input produces no valid terms.
///
/// # Example
/// ```
/// use omnihedron::sql::search::sanitize_tsquery;
/// assert_eq!(sanitize_tsquery("hello world"), Some("hello & world".to_string()));
/// assert_eq!(sanitize_tsquery("  "), None);
/// assert_eq!(sanitize_tsquery("foo's bar"), Some("foos & bar".to_string()));
/// ```
#[allow(dead_code)]
pub fn sanitize_tsquery(input: &str) -> Option<String> {
	let terms: Vec<String> = input
		.split_whitespace()
		.map(|word| {
			word.chars()
				.filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
				.collect::<String>()
		})
		.filter(|s| !s.is_empty())
		.collect();

	if terms.is_empty() { None } else { Some(terms.join(" & ")) }
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn basic_terms() {
		assert_eq!(sanitize_tsquery("hello world"), Some("hello & world".to_string()));
	}

	#[test]
	fn strips_special_chars() {
		assert_eq!(sanitize_tsquery("foo:bar (baz)"), Some("foobar & baz".to_string()));
	}

	#[test]
	fn empty_input() {
		assert_eq!(sanitize_tsquery(""), None);
		assert_eq!(sanitize_tsquery("   "), None);
	}

	#[test]
	fn single_term() {
		assert_eq!(sanitize_tsquery("hello"), Some("hello".to_string()));
	}

	#[test]
	fn preserves_hyphens_underscores() {
		assert_eq!(
			sanitize_tsquery("my-term another_one"),
			Some("my-term & another_one".to_string())
		);
	}
}
