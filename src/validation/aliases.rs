use std::collections::HashMap;

use async_graphql::{
	Name, Positioned, ServerError,
	parser::types::{ExecutableDocument, FragmentDefinition, Selection},
};

/// Count total field aliases in the document.
///
/// Returns an error if the count exceeds `max_aliases`.
pub fn validate_aliases(doc: &ExecutableDocument, max_aliases: usize) -> Result<(), ServerError> {
	let mut total = 0usize;
	for (_name, op) in doc.operations.iter() {
		total += count_aliases(&op.node.selection_set.node.items, &doc.fragments);
	}

	if total > max_aliases {
		return Err(ServerError::new(
			format!("Alias limit exceeded. Maximum allowed aliases is {max_aliases}."),
			None,
		));
	}
	Ok(())
}

fn count_aliases(
	items: &[Positioned<Selection>],
	fragments: &HashMap<Name, Positioned<FragmentDefinition>>,
) -> usize {
	let mut count = 0;
	for sel in items {
		match &sel.node {
			Selection::Field(f) => {
				if f.node.alias.is_some() {
					count += 1;
				}
				count += count_aliases(&f.node.selection_set.node.items, fragments);
			},
			Selection::InlineFragment(f) => {
				count += count_aliases(&f.node.selection_set.node.items, fragments);
			},
			Selection::FragmentSpread(spread) => {
				if let Some(frag) = fragments.get(&spread.node.fragment_name.node) {
					count += count_aliases(&frag.node.selection_set.node.items, fragments);
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
	fn no_aliases_passes() {
		let doc = parse_query("{ a { b } }").unwrap();
		assert!(validate_aliases(&doc, 5).is_ok());
	}

	#[test]
	fn single_alias_at_limit_passes() {
		// x: a is 1 alias
		let doc = parse_query("{ x: a }").unwrap();
		assert!(validate_aliases(&doc, 1).is_ok(), "1 alias should pass limit 1");
	}

	#[test]
	fn single_alias_over_limit_fails() {
		let doc = parse_query("{ x: a }").unwrap();
		let result = validate_aliases(&doc, 0);
		assert!(result.is_err(), "1 alias should fail limit 0");
		let msg = result.unwrap_err().message;
		assert!(msg.to_lowercase().contains("alias"), "error should mention aliases: {msg}");
	}

	#[test]
	fn multiple_aliases_at_limit_passes() {
		// 3 aliases
		let doc = parse_query("{ x: a y: b z: c }").unwrap();
		assert!(validate_aliases(&doc, 3).is_ok(), "3 aliases should pass limit 3");
	}

	#[test]
	fn multiple_aliases_over_limit_fails() {
		// 4 aliases, limit 3
		let doc = parse_query("{ x: a y: b z: c w: d }").unwrap();
		assert!(validate_aliases(&doc, 3).is_err(), "4 aliases should fail limit 3");
	}

	#[test]
	fn non_aliased_fields_not_counted() {
		// a is not aliased, x: b is aliased → 1 alias
		let doc = parse_query("{ a { x: b } }").unwrap();
		assert!(validate_aliases(&doc, 1).is_ok(), "1 alias should pass limit 1");
		assert!(validate_aliases(&doc, 0).is_err(), "1 alias should fail limit 0");
	}

	#[test]
	fn nested_aliases_all_counted() {
		// x: a { y: b { z: c } } → 3 aliases
		let doc = parse_query("{ x: a { y: b { z: c } } }").unwrap();
		assert!(validate_aliases(&doc, 3).is_ok(), "3 nested aliases at limit 3 should pass");
		assert!(validate_aliases(&doc, 2).is_err(), "3 nested aliases should fail limit 2");
	}
}
