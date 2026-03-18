use std::collections::HashMap;

use async_graphql::{
	Name, Positioned, ServerError,
	parser::types::{ExecutableDocument, FragmentDefinition, Selection},
};

/// Validate that no operation in the document exceeds `max_depth`.
/// Skips `IntrospectionQuery` operations.
pub fn validate_depth(doc: &ExecutableDocument, max_depth: usize) -> Result<(), ServerError> {
	for (name, op) in doc.operations.iter() {
		if name.map(|n| n.as_str()) == Some("IntrospectionQuery") {
			continue;
		}
		let depth = selection_depth(&op.node.selection_set.node.items, &doc.fragments, 0);
		if depth > max_depth {
			return Err(ServerError::new(
				format!("Query is too deep. Maximum depth allowed is {max_depth}."),
				None,
			));
		}
	}
	Ok(())
}

fn selection_depth(
	items: &[Positioned<Selection>],
	fragments: &HashMap<Name, Positioned<FragmentDefinition>>,
	current: usize,
) -> usize {
	let mut max = current;
	for sel in items {
		let depth = match &sel.node {
			Selection::Field(f) =>
				if f.node.selection_set.node.items.is_empty() {
					current + 1
				} else {
					selection_depth(&f.node.selection_set.node.items, fragments, current + 1)
				},
			Selection::InlineFragment(f) =>
				selection_depth(&f.node.selection_set.node.items, fragments, current),
			Selection::FragmentSpread(spread) => {
				if let Some(frag) = fragments.get(&spread.node.fragment_name.node) {
					selection_depth(&frag.node.selection_set.node.items, fragments, current)
				} else {
					current
				}
			},
		};
		if depth > max {
			max = depth;
		}
	}
	max
}

#[cfg(test)]
mod tests {
	use async_graphql::parser::parse_query;

	use super::*;

	#[test]
	fn shallow_query_passes_depth_limit() {
		// depth 3: a → b → c
		let doc = parse_query("{ a { b { c } } }").unwrap();
		assert!(validate_depth(&doc, 5).is_ok(), "depth 3 should pass limit 5");
	}

	#[test]
	fn query_at_exact_limit_passes() {
		// depth 5: a → b → c → d → e
		let doc = parse_query("{ a { b { c { d { e } } } } }").unwrap();
		assert!(validate_depth(&doc, 5).is_ok(), "depth 5 should pass limit 5");
	}

	#[test]
	fn query_one_over_limit_fails() {
		// depth 6: a → b → c → d → e → f
		let doc = parse_query("{ a { b { c { d { e { f } } } } } }").unwrap();
		let result = validate_depth(&doc, 5);
		assert!(result.is_err(), "depth 6 should fail limit 5");
		let msg = result.unwrap_err().message;
		assert!(
			msg.contains("too deep") || msg.contains("5"),
			"error should mention depth limit: {msg}"
		);
	}

	#[test]
	fn deep_query_fails_depth_limit() {
		// depth 8
		let doc = parse_query("{ a { b { c { d { e { f { g { h } } } } } } } }").unwrap();
		assert!(validate_depth(&doc, 5).is_err(), "depth 8 should fail limit 5");
	}

	#[test]
	fn introspection_query_is_always_allowed() {
		// IntrospectionQuery with depth > max_depth must not be rejected
		let doc = parse_query(
			"query IntrospectionQuery { __schema { types { fields { type { ofType { name } } } } } }",
		)
		.unwrap();
		assert!(validate_depth(&doc, 1).is_ok(), "IntrospectionQuery should bypass depth check");
	}

	#[test]
	fn inline_fragment_does_not_add_depth() {
		// Inline fragments are transparent — they don't add a depth level.
		// This query has real depth 3 (a → b → c) wrapped in an inline fragment.
		let doc = parse_query("{ a { ... { b { c } } } }").unwrap();
		assert!(validate_depth(&doc, 3).is_ok());
		// depth is 3, so limit 2 should fail
		assert!(validate_depth(&doc, 2).is_err());
	}

	#[test]
	fn multiple_siblings_use_max_depth() {
		// The two branches have depths 2 and 4 respectively; max is 4.
		let doc = parse_query("{ x { shallow } y { deep { nested { field } } } }").unwrap();
		assert!(validate_depth(&doc, 4).is_ok(), "max branch depth 4 at limit 4 should pass");
		assert!(validate_depth(&doc, 3).is_err(), "max branch depth 4 exceeds limit 3");
	}
}
