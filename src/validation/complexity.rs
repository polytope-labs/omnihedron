use std::collections::HashMap;

use async_graphql::{
	Name, Positioned, ServerError,
	parser::types::{ExecutableDocument, FragmentDefinition, Selection},
};

/// Count total field complexity (1 per field). Returns an error if over limit.
/// Skips `IntrospectionQuery`.
pub fn validate_complexity(
	doc: &ExecutableDocument,
	max_complexity: usize,
) -> Result<usize, ServerError> {
	let mut total = 0usize;

	for (name, op) in doc.operations.iter() {
		if name.map(|n| n.as_str()) == Some("IntrospectionQuery") {
			continue;
		}
		total += count_fields(&op.node.selection_set.node.items, &doc.fragments);
	}

	if total > max_complexity {
		return Err(ServerError::new(
			format!(
				"Query complexity {total} exceeds maximum allowed complexity {max_complexity}."
			),
			None,
		));
	}

	Ok(total)
}

fn count_fields(
	items: &[Positioned<Selection>],
	fragments: &HashMap<Name, Positioned<FragmentDefinition>>,
) -> usize {
	let mut count = 0;
	for sel in items {
		match &sel.node {
			Selection::Field(f) => {
				count += 1;
				count += count_fields(&f.node.selection_set.node.items, fragments);
			},
			Selection::InlineFragment(f) => {
				count += count_fields(&f.node.selection_set.node.items, fragments);
			},
			Selection::FragmentSpread(spread) => {
				if let Some(frag) = fragments.get(&spread.node.fragment_name.node) {
					count += count_fields(&frag.node.selection_set.node.items, fragments);
				}
			},
		}
	}
	count
}

#[cfg(test)]
mod tests {
	use async_graphql::parser::parse_query;

	use super::*;

	#[test]
	fn simple_query_under_limit() {
		// 3 fields: a, b, c
		let doc = parse_query("{ a { b { c } } }").unwrap();
		let result = validate_complexity(&doc, 5);
		assert!(result.is_ok(), "complexity 3 should pass limit 5");
		assert_eq!(result.unwrap(), 3);
	}

	#[test]
	fn query_at_exact_limit_passes() {
		// 5 fields: a, b, c, d, e
		let doc = parse_query("{ a { b { c { d { e } } } } }").unwrap();
		let result = validate_complexity(&doc, 5);
		assert!(result.is_ok(), "complexity 5 should pass limit 5");
		assert_eq!(result.unwrap(), 5);
	}

	#[test]
	fn query_over_limit_fails() {
		// 6 fields: a, b, c, d, e, f
		let doc = parse_query("{ a { b { c { d { e { f } } } } } }").unwrap();
		let result = validate_complexity(&doc, 5);
		assert!(result.is_err(), "complexity 6 should fail limit 5");
		let msg = result.unwrap_err().message;
		assert!(
			msg.contains("complexity") || msg.contains("6"),
			"error should mention complexity: {msg}"
		);
	}

	#[test]
	fn sibling_fields_all_counted() {
		// a + b + c = 3 fields at the same level
		let doc = parse_query("{ a b c }").unwrap();
		let result = validate_complexity(&doc, 5);
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), 3);
	}

	#[test]
	fn introspection_query_is_not_counted() {
		let doc =
			parse_query("query IntrospectionQuery { __schema { types { name fields { name } } } }")
				.unwrap();
		let result = validate_complexity(&doc, 1);
		assert!(result.is_ok(), "IntrospectionQuery should not count toward complexity");
		assert_eq!(result.unwrap(), 0);
	}

	#[test]
	fn inline_fragment_fields_are_counted() {
		// Inline fragments are transparent but their fields still count.
		// { a { ... { b { c } } } } → a, b, c = 3 fields
		let doc = parse_query("{ a { ... { b { c } } } }").unwrap();
		let result = validate_complexity(&doc, 5);
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), 3);
	}

	#[test]
	fn fragment_spread_fields_are_counted() {
		// fragment F on T { x y } { a { ...F } } → a + x + y = 3
		let doc = parse_query("fragment F on T { x y } { a { ...F } }").unwrap();
		let result = validate_complexity(&doc, 5);
		assert!(result.is_ok());
		assert_eq!(result.unwrap(), 3);
	}
}
