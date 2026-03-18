/// Convert a `snake_case` database name to `camelCase` for GraphQL fields.
///
/// Mirrors `graphile-build`'s `formatInsideUnderscores(camelCase)` behaviour:
///   - Leading/trailing underscores are preserved verbatim.
///   - The content between underscores is converted with standard camelCase (each character after
///     `_` is uppercased).
///
/// Examples:
///   block_number  → blockNumber
///   created_at    → createdAt
///   id            → id
///   _global       → _global
pub fn to_camel_case(s: &str) -> String {
	// Preserve leading underscores (graphile formatInsideUnderscores behaviour)
	let leading_len = s.chars().take_while(|&c| c == '_').count();
	let leading = &s[..leading_len];
	let inner = &s[leading_len..];

	let mut result = String::with_capacity(s.len());
	let mut capitalize_next = false;

	for ch in inner.chars() {
		if ch == '_' {
			capitalize_next = true;
		} else if capitalize_next {
			result.extend(ch.to_uppercase());
			capitalize_next = false;
		} else {
			result.push(ch);
		}
	}
	format!("{leading}{result}")
}

/// Convert a `snake_case` database name to `PascalCase` for GraphQL type names.
///
/// Preserves leading underscores (e.g. `_global` → `_Global`) to match
/// PostGraphile's `upperCamelCase` / `formatInsideUnderscores` behaviour.
///
/// Examples:
///   transfer       → Transfer
///   block_extrinsic → BlockExtrinsic
///   _global        → _Global
pub fn to_pascal_case(s: &str) -> String {
	let camel = to_camel_case(s);
	// Uppercase the first non-underscore character
	let leading_len = camel.chars().take_while(|&c| c == '_').count();
	if leading_len == 0 {
		let mut chars = camel.chars();
		match chars.next() {
			None => String::new(),
			Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
		}
	} else {
		let leading = &camel[..leading_len];
		let rest = &camel[leading_len..];
		let mut chars = rest.chars();
		match chars.next() {
			None => camel,
			Some(first) => {
				format!("{}{}{}", leading, first.to_uppercase().collect::<String>(), chars.as_str())
			},
		}
	}
}

/// Convert a `snake_case` column name to `SCREAMING_SNAKE_CASE` for OrderBy
/// and Distinct enum values.
///
/// Examples:
///   block_number → BLOCK_NUMBER
///   id           → ID
pub fn to_screaming_snake(s: &str) -> String {
	s.to_uppercase()
}

/// Pluralize a table name for the GraphQL connection query field.
///
/// This mirrors PostGraphile/PgSimplifyInflectorPlugin behaviour:
/// - Simple English plural rules
/// - Used for the root connection query field name (e.g. `users`, `accounts`)
pub fn pluralize(s: &str) -> String {
	// Irregular Latin neuter: metadatum → metadata
	if s == "metadatum" {
		return "metadata".to_string();
	}
	if s.ends_with("_metadatum") {
		return format!("{}metadata", &s[..s.len() - "_metadatum".len() + 1]);
	}

	if s.ends_with("y") && !ends_with_vowel_y(s) {
		// category → categories
		format!("{}ies", &s[..s.len() - 1])
	} else if s.ends_with("s") ||
		s.ends_with("x") ||
		s.ends_with("z") ||
		s.ends_with("ch") ||
		s.ends_with("sh")
	{
		format!("{s}es")
	} else {
		format!("{s}s")
	}
}

/// Singularize a table name for the GraphQL single-record query field.
///
/// Attempts to reverse common plural suffixes. Falls back to the original
/// string if no rule matches (PostGraphile does the same).
pub fn singularize(s: &str) -> String {
	// Special case: "metadata" is the irregular plural of "metadatum"
	if s == "metadata" {
		return "metadatum".to_string();
	}
	if s.ends_with("_metadata") {
		return format!("{}_metadatum", &s[..s.len() - "_metadata".len()]);
	}

	if s.ends_with("ies") && s.len() > 3 {
		// categories → category
		format!("{}y", &s[..s.len() - 3])
	} else if s.ends_with("ches") || s.ends_with("shes") {
		// churches → church, wishes → wish
		s[..s.len() - 2].to_string()
	} else if s.ends_with("xes") || s.ends_with("zes") {
		// boxes → box, buzzes → buzz
		s[..s.len() - 2].to_string()
	} else if s.ends_with("ses") {
		// Distinguish: "responses" (response + s) vs "buses" (bus + es).
		// Strip "es" to get the base; if that base ends in a vowel or 'e' it
		// is likely not a real stem (e.g. "respons" is not a word).
		// In that case strip just `s` instead (responses → response).
		// If the base without "es" ends in a consonant it IS the real stem
		// (buses → bus).
		let without_es = &s[..s.len() - 2]; // e.g. "bus" or "respons"
		let last_char = without_es.chars().last().unwrap_or('\0');
		if matches!(last_char, 'a' | 'e' | 'i' | 'o' | 'u') || last_char == '\0' {
			// e.g. "respons" ends in 's' — this branch won't fire for that,
			// but "respon" would — fall through to strip just 's'
			s[..s.len() - 1].to_string()
		} else if !matches!(last_char, 's') {
			// "bus" ends in 's' which is a special case we handle next
			without_es.to_string() // buses → bus... wait, 'bus' ends in 's'
		} else {
			// Ambiguous: ends in 's' after stripping "es" (e.g. "buses" → "bus")
			// vs "responses" → strip "es" → "respons" (ends in 's' too).
			// Use heuristic: if the pre-"es" stem ends in a vowel+'s', it's
			// more likely a real word (bus, status). Check letter before 's':
			let bytes = without_es.as_bytes();
			if bytes.len() >= 2 &&
				matches!(bytes[bytes.len() - 2] as char, 'a' | 'e' | 'i' | 'o' | 'u')
			{
				without_es.to_string() // buses → bus (vowel before 's': u)
			} else {
				s[..s.len() - 1].to_string() // responses → response
			}
		}
	} else if s.ends_with('s') && !s.ends_with("ss") {
		s[..s.len() - 1].to_string()
	} else {
		s.to_string()
	}
}

fn ends_with_vowel_y(s: &str) -> bool {
	if s.len() < 2 {
		return false;
	}
	let bytes = s.as_bytes();
	let before_y = bytes[bytes.len() - 2] as char;
	matches!(before_y, 'a' | 'e' | 'i' | 'o' | 'u')
}

/// Build the GraphQL type name for an entity from a table name.
///
/// Table `block_extrinsics` → `BlockExtrinsic`  (singularize then PascalCase)
/// Table `_global`          → `_Global`          (singularize, preserve leading `_`)
///
/// Note: PostGraphile singularizes the table name for the type name.
pub fn table_to_type_name(table: &str) -> String {
	to_pascal_case(&singularize(table))
}

/// Build the plural GraphQL type name from a table name.
///
/// Mirrors PostGraphile's two-pass approach:
///   1. Compute entity type via `table_to_type_name` (singularize + PascalCase)
///   2. Pluralize the last camelCase word of the entity type
///   3. Normalize consecutive-uppercase runs (lodash-style)
///
/// This reproduces the PostGraphile `connection()` type naming, e.g.:
///   Table `accumulated_fees`          → `AccumulatedFees`
///   Table `cumulative_volume_u_s_ds`  → `CumulativeVolumeUsds`
///   Table `_global`                   → `_Globals`
pub fn table_to_plural_type_name(table: &str) -> String {
	let entity_type = table_to_type_name(table);
	let pluralized = pluralize_pascal_type_name(&entity_type);
	normalize_consecutive_upper(&pluralized)
}

/// Build the root connection query field name from a table name.
///
/// Normalises via singularize→pluralize so that tables whose names are already
/// singular (e.g. `request_status_metadata`) get a distinct plural connection
/// field (`requestStatusMetadatas`) separate from their single-record field
/// (`requestStatusMetadata`), preventing field-name collisions on the Query root.
///
/// Table `block_extrinsics` → `blockExtrinsics`
/// Table `request_status_metadata` → `requestStatusMetadatas`
pub fn table_to_connection_field(table: &str) -> String {
	to_camel_case(&pluralize(&singularize(table)))
}

/// Build the root single-record query field name from a table name.
///
/// Table `block_extrinsics` → `blockExtrinsic`
pub fn table_to_single_field(table: &str) -> String {
	to_camel_case(&singularize(table))
}

/// Build the backward-relation field name on the parent type.
///
/// child_table=`transfers`, fk_column=`account_id`
///   → `transfersByAccountId`
pub fn backward_relation_field(child_table: &str, fk_column: &str) -> String {
	let table_part = to_camel_case(child_table);
	let col_part = to_pascal_case(&to_camel_case(fk_column));
	format!("{table_part}By{col_part}")
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Pluralize the last camelCase word of a PascalCase type name.
///
/// Mirrors PostGraphile's `fixChangePlural(pluralize)` — it finds the last
/// PascalCase word boundary (last uppercase letter) and applies English
/// pluralisation to only that suffix.
///
/// Examples:
///   Transfer            → Transfers
///   BlockExtrinsic      → BlockExtrinsics
///   AccumulatedFee      → AccumulatedFees
///   CumulativeVolumeUSD → CumulativeVolumeUSDS   (then normalize_consecutive_upper → Usds)
///   _Global             → _Globals
fn pluralize_pascal_type_name(type_name: &str) -> String {
	// Preserve leading underscores
	let leading_len = type_name.chars().take_while(|&c| c == '_').count();
	let leading = &type_name[..leading_len];
	let inner = &type_name[leading_len..];

	if inner.is_empty() {
		return type_name.to_string();
	}

	// Find the last uppercase letter position (PostGraphile fixChangePlural
	// extracts only the last single uppercase char as the "word" to pluralize).
	let last_upper_byte_pos = inner
		.char_indices()
		.filter(|(_, c)| c.is_ascii_uppercase())
		.last()
		.map(|(i, _)| i)
		.unwrap_or(0);

	let prefix = &inner[..last_upper_byte_pos];
	let last_word = &inner[last_upper_byte_pos..];

	let plural = simple_plural_suffix(last_word);
	format!("{leading}{prefix}{plural}")
}

/// Apply simple English plural rules to a word that appears at the end of a
/// PascalCase type name.
///
/// Mirrors the `pluralize` npm package behaviour for PascalCase strings:
///   all-uppercase words ending in a letter: add "S"
///   digit-ending words (e.g. "V2"): add "s" (lowercase)
///   mixed-case words: add "s" (standard English plural rules)
fn simple_plural_suffix(word: &str) -> String {
	if word.is_empty() {
		return word.to_string();
	}

	// Irregular Latin neuter: -atum → -ata (Metadatum → Metadata)
	if word.ends_with("atum") {
		return format!("{}a", &word[..word.len() - 2]);
	}

	// Use uppercase 'S' only when the last character is itself an uppercase letter.
	// A trailing digit (e.g. "V2") should produce lowercase "s".
	let last_char = word.chars().last().unwrap();
	let suffix = if last_char.is_ascii_uppercase() { "S" } else { "s" };

	// ies / es / s rules
	let lo = word.to_lowercase();
	if lo.ends_with("ies") {
		word.to_string() // already plural
	} else if lo.ends_with('y') && !lo.ends_with("ey") && !lo.ends_with("ay") && !lo.ends_with("oy")
	{
		format!("{}ies", &word[..word.len() - 1])
	} else if lo.ends_with("ches") ||
		lo.ends_with("shes") ||
		lo.ends_with("xes") ||
		lo.ends_with("zes") ||
		lo.ends_with("ses")
	{
		word.to_string() // already plural
	} else if lo.ends_with("ch") || lo.ends_with("sh") || lo.ends_with('x') || lo.ends_with('z') {
		format!("{word}es")
	} else {
		format!("{word}{suffix}")
	}
}

/// Replace runs of 2+ consecutive uppercase ASCII letters with title-case.
///
/// This mirrors what lodash's `camelCase` does when applied to a string that
/// already contains consecutive-uppercase acronyms: it treats the run as one
/// "word" and lowercases all but the first character.
///
/// Leading underscores are preserved.
///
/// Examples:
///   CumulativeVolumeUSDS → CumulativeVolumeUsds
///   _Globals             → _Globals  (no consecutive run)
///   BlockExtrinsics      → BlockExtrinsics
fn normalize_consecutive_upper(s: &str) -> String {
	// Preserve leading underscores
	let leading_len = s.chars().take_while(|&c| c == '_').count();
	let leading = &s[..leading_len];
	let inner = &s[leading_len..];

	let mut result = String::with_capacity(s.len());
	let mut run: Vec<char> = Vec::new();

	for ch in inner.chars() {
		if ch.is_ascii_uppercase() {
			run.push(ch);
		} else {
			flush_run(&run, &mut result);
			run.clear();
			result.push(ch);
		}
	}
	flush_run(&run, &mut result);

	format!("{leading}{result}")
}

fn flush_run(run: &[char], result: &mut String) {
	if run.len() >= 2 {
		result.push(run[0]);
		for &c in &run[1..] {
			result.push(c.to_ascii_lowercase());
		}
	} else {
		for &c in run {
			result.push(c);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn camel_case() {
		assert_eq!(to_camel_case("block_number"), "blockNumber");
		assert_eq!(to_camel_case("created_at"), "createdAt");
		assert_eq!(to_camel_case("id"), "id");
		assert_eq!(to_camel_case("some_long_field_name"), "someLongFieldName");
		// Leading underscore preserved
		assert_eq!(to_camel_case("_global"), "_global");
		assert_eq!(to_camel_case("_block_range"), "_blockRange");
	}

	#[test]
	fn pascal_case() {
		assert_eq!(to_pascal_case("transfer"), "Transfer");
		assert_eq!(to_pascal_case("block_extrinsic"), "BlockExtrinsic");
		// Leading underscore preserved
		assert_eq!(to_pascal_case("_global"), "_Global");
		// Consecutive single-char segments stay uppercase (PostGraphile standard)
		assert_eq!(to_pascal_case("cumulative_volume_u_s_d"), "CumulativeVolumeUSD");
	}

	#[test]
	fn plural() {
		assert_eq!(pluralize("transfer"), "transfers");
		assert_eq!(pluralize("category"), "categories");
		assert_eq!(pluralize("address"), "addresses");
		assert_eq!(pluralize("account"), "accounts");
		// Irregular Latin neuter
		assert_eq!(pluralize("metadatum"), "metadata");
		assert_eq!(pluralize("request_status_metadatum"), "request_status_metadata");
	}

	#[test]
	fn singular() {
		assert_eq!(singularize("transfers"), "transfer");
		assert_eq!(singularize("categories"), "category");
		assert_eq!(singularize("accounts"), "account");
		assert_eq!(singularize("responses"), "response");
		assert_eq!(singularize("buses"), "bus");
		assert_eq!(singularize("churches"), "church");
		assert_eq!(singularize("metadata"), "metadatum");
		assert_eq!(singularize("request_status_metadata"), "request_status_metadatum");
	}

	#[test]
	fn type_name() {
		assert_eq!(table_to_type_name("transfers"), "Transfer");
		assert_eq!(table_to_type_name("block_extrinsics"), "BlockExtrinsic");
		assert_eq!(table_to_type_name("accounts"), "Account");
		assert_eq!(table_to_type_name("metadata"), "Metadatum");
		assert_eq!(table_to_type_name("_global"), "_Global");
		assert_eq!(table_to_type_name("cumulative_volume_u_s_ds"), "CumulativeVolumeUSD");
	}

	#[test]
	fn plural_type_name() {
		assert_eq!(table_to_plural_type_name("accumulated_fees"), "AccumulatedFees");
		assert_eq!(table_to_plural_type_name("transfers"), "Transfers");
		assert_eq!(table_to_plural_type_name("block_extrinsics"), "BlockExtrinsics");
		assert_eq!(table_to_plural_type_name("_global"), "_Globals");
		assert_eq!(table_to_plural_type_name("cumulative_volume_u_s_ds"), "CumulativeVolumeUsds");
		assert_eq!(table_to_plural_type_name("daily_volume_u_s_ds"), "DailyVolumeUsds");
		// Irregular Latin neuter -atum → -ata
		assert_eq!(table_to_plural_type_name("request_status_metadata"), "RequestStatusMetadata");
		// Digit-ending: V2 → V2s (lowercase s, not V2S)
		assert_eq!(table_to_plural_type_name("order_v2s"), "OrderV2s");
	}

	#[test]
	fn connection_field() {
		assert_eq!(table_to_connection_field("transfers"), "transfers");
		assert_eq!(table_to_connection_field("accounts"), "accounts");
		assert_eq!(table_to_connection_field("metadata"), "metadata");
		assert_eq!(table_to_connection_field("request_status_metadata"), "requestStatusMetadata");
		assert_eq!(table_to_connection_field("order_v2s"), "orderV2s");
	}

	#[test]
	fn backward_rel() {
		assert_eq!(backward_relation_field("transfers", "account_id"), "transfersByAccountId");
	}

	#[test]
	fn normalize_upper() {
		// 4-char consecutive uppercase run → title case
		assert_eq!(normalize_consecutive_upper("CumulativeVolumeUSDS"), "CumulativeVolumeUsds");
		// No consecutive uppercase runs → unchanged
		assert_eq!(normalize_consecutive_upper("_Globals"), "_Globals");
		assert_eq!(normalize_consecutive_upper("BlockExtrinsics"), "BlockExtrinsics");
		// 3-char run → also title case (normalize is run-length agnostic ≥2)
		assert_eq!(normalize_consecutive_upper("CumulativeVolumeUSD"), "CumulativeVolumeUsd");
	}
}
