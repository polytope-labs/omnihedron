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
