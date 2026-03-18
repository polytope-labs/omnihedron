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
