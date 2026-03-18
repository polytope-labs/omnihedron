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
