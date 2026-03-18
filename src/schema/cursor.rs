use anyhow::{Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Map, Value};

/// Encode a PostGraphile-compatible `nodeId` from a type name and primary key value.
///
/// Format: base64(JSON.stringify([typeName, pkValue]))
/// e.g. AccumulatedFee + "abc123" → base64('["AccumulatedFee","abc123"]')
pub fn encode_node_id(type_name: &str, pk_value: &Value) -> String {
	let arr = Value::Array(vec![Value::String(type_name.to_string()), pk_value.clone()]);
	BASE64.encode(arr.to_string().as_bytes())
}

/// Decode a `nodeId` back to (typeName, pkValue).
pub fn decode_node_id(node_id: &str) -> Result<(String, Value)> {
	let bytes = BASE64
		.decode(node_id)
		.map_err(|e| anyhow::anyhow!("Invalid nodeId (base64): {e}"))?;
	let json: Value = serde_json::from_slice(&bytes)
		.map_err(|e| anyhow::anyhow!("Invalid nodeId (JSON): {e}"))?;
	match json {
		Value::Array(arr) if arr.len() == 2 => {
			let type_name = arr[0]
				.as_str()
				.ok_or_else(|| anyhow::anyhow!("nodeId type must be a string"))?
				.to_string();
			Ok((type_name, arr[1].clone()))
		},
		_ => bail!("nodeId must be a JSON array [typeName, pkValue]"),
	}
}

/// Encode a cursor from an ordered list of (field_name, value) pairs.
///
/// The cursor is base64(JSON object) — compatible with PostGraphile's format.
pub fn encode_cursor(fields: &[(&str, Value)]) -> String {
	let mut map = Map::new();
	for (k, v) in fields {
		map.insert(k.to_string(), v.clone());
	}
	let json = Value::Object(map).to_string();
	BASE64.encode(json.as_bytes())
}

/// Decode a cursor back to a JSON object.
pub fn decode_cursor(cursor: &str) -> Result<Map<String, Value>> {
	let bytes = BASE64
		.decode(cursor)
		.map_err(|e| anyhow::anyhow!("Invalid cursor (base64): {e}"))?;
	let json: Value = serde_json::from_slice(&bytes)
		.map_err(|e| anyhow::anyhow!("Invalid cursor (JSON): {e}"))?;
	match json {
		Value::Object(map) => Ok(map),
		_ => bail!("Cursor must be a JSON object"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde_json::json;

	#[test]
	fn roundtrip() {
		let fields = [("id", json!("abc123")), ("block_number", json!(42))];
		let encoded = encode_cursor(&fields);
		let decoded = decode_cursor(&encoded).unwrap();
		assert_eq!(decoded["id"], json!("abc123"));
		assert_eq!(decoded["block_number"], json!(42));
	}
}
