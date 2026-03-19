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

//! Shared test infrastructure for integration tests.

use std::collections::HashSet;

use pretty_assertions::assert_eq;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Service URLs
// ---------------------------------------------------------------------------

pub fn rust_url() -> String {
	std::env::var("RUST_SERVICE_URL").unwrap_or_else(|_| "http://localhost:3000".to_string())
}

pub fn ts_url() -> String {
	std::env::var("TS_SERVICE_URL").unwrap_or_else(|_| "http://localhost:3001".to_string())
}

#[allow(dead_code)]
pub fn schema_name() -> String {
	std::env::var("SCHEMA_NAME").expect(
		"SCHEMA_NAME env var is required. \
         Run scripts/setup_db.sh and source .env.test before running tests.",
	)
}

/// Returns true if both services appear to be reachable.
pub fn services_available() -> bool {
	let rust = std::process::Command::new("curl")
		.args(["-sf", "--max-time", "3", &format!("{}/health", rust_url())])
		.output()
		.map(|o| o.status.success())
		.unwrap_or(false);

	let ts = std::process::Command::new("curl")
		.args([
			"-sf",
			"--max-time",
			"5",
			"-X",
			"POST",
			"-H",
			"Content-Type: application/json",
			"-d",
			r#"{"query":"{ __typename }"}"#,
			&ts_url(),
		])
		.output()
		.map(|o| o.status.success())
		.unwrap_or(false);

	rust && ts
}

// ---------------------------------------------------------------------------
// Test client
// ---------------------------------------------------------------------------

pub struct TestClient {
	pub url: String,
	client: reqwest::Client,
}

impl TestClient {
	pub fn new(url: &str) -> Self {
		Self {
			url: url.to_string(),
			client: reqwest::Client::builder()
				.timeout(std::time::Duration::from_secs(30))
				.build()
				.expect("Failed to build HTTP client"),
		}
	}

	pub async fn query(&self, gql: &str) -> Value {
		self.query_with_body(json!({ "query": gql })).await
	}

	pub async fn query_vars(&self, gql: &str, vars: Value) -> Value {
		self.query_with_body(json!({ "query": gql, "variables": vars })).await
	}

	pub async fn query_with_body(&self, body: Value) -> Value {
		let resp = self
			.client
			.post(self.url.clone())
			.header("Content-Type", "application/json")
			.json(&body)
			.send()
			.await
			.unwrap_or_else(|e| panic!("Request to {} failed: {e}", self.url));

		resp.json::<Value>().await.expect("Failed to parse response as JSON")
	}

	pub async fn batch_query(&self, bodies: &[Value]) -> Value {
		let resp = self
			.client
			.post(self.url.clone())
			.header("Content-Type", "application/json")
			.json(&bodies)
			.send()
			.await
			.unwrap_or_else(|e| panic!("Batch request to {} failed: {e}", self.url));

		resp.json::<Value>().await.expect("Failed to parse batch response as JSON")
	}

	pub async fn health(&self) -> reqwest::StatusCode {
		self.client
			.get(format!("{}/health", self.url))
			.send()
			.await
			.unwrap_or_else(|e| panic!("Health request to {} failed: {e}", self.url))
			.status()
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Recursively sort arrays of objects by their "id" field.
pub fn sort_nodes(val: &mut Value) {
	match val {
		Value::Array(arr) => {
			if arr.iter().all(|v| v.get("id").is_some()) {
				arr.sort_by(|a, b| {
					let a_id = a["id"].as_str().unwrap_or("");
					let b_id = b["id"].as_str().unwrap_or("");
					a_id.cmp(b_id)
				});
			}
			for item in arr.iter_mut() {
				sort_nodes(item);
			}
		},
		Value::Object(map) =>
			for v in map.values_mut() {
				sort_nodes(v);
			},
		_ => {},
	}
}

/// Compare two GraphQL responses, stripping implementation-specific fields.
pub fn compare_responses(query_name: &str, ts: &Value, rust: &Value) {
	let ts_data = ts.get("data").cloned().unwrap_or(Value::Null);
	let rust_data = rust.get("data").cloned().unwrap_or(Value::Null);

	let mut ts_clean = ts_data.clone();
	let mut rust_clean = rust_data.clone();

	strip_field(&mut ts_clean, "queryNodeVersion");
	strip_field(&mut rust_clean, "queryNodeVersion");
	strip_field(&mut ts_clean, "indexerNodeVersion");
	strip_field(&mut rust_clean, "indexerNodeVersion");
	strip_field(&mut ts_clean, "cursor");
	strip_field(&mut rust_clean, "cursor");
	strip_field(&mut ts_clean, "startCursor");
	strip_field(&mut rust_clean, "startCursor");
	strip_field(&mut ts_clean, "endCursor");
	strip_field(&mut rust_clean, "endCursor");

	sort_nodes(&mut ts_clean);
	sort_nodes(&mut rust_clean);

	if ts_clean != rust_clean {
		eprintln!("\n[DIFF] Query '{}' responses differ between TS and Rust:", query_name);
		eprintln!("  TS   data: {}", serde_json::to_string_pretty(&ts_clean).unwrap());
		eprintln!("  Rust data: {}", serde_json::to_string_pretty(&rust_clean).unwrap());
	}

	assert_eq!(
		ts_clean, rust_clean,
		"Query '{}': TS and Rust responses differ (see above)",
		query_name
	);
}

pub fn strip_field(val: &mut Value, field: &str) {
	match val {
		Value::Object(map) => {
			map.remove(field);
			for v in map.values_mut() {
				strip_field(v, field);
			}
		},
		Value::Array(arr) =>
			for item in arr.iter_mut() {
				strip_field(item, field);
			},
		_ => {},
	}
}

#[allow(dead_code)]
pub fn extract_query_fields(introspection: &Value) -> Vec<String> {
	let types = introspection
		.pointer("/data/__schema/types")
		.and_then(|v| v.as_array())
		.cloned()
		.unwrap_or_default();

	let query_type = types
		.iter()
		.find(|t| t["name"].as_str() == Some("Query"))
		.cloned()
		.unwrap_or(Value::Null);

	query_type
		.get("fields")
		.and_then(|f| f.as_array())
		.map(|fields| {
			fields
				.iter()
				.filter_map(|f| f["name"].as_str().map(|s| s.to_string()))
				.collect()
		})
		.unwrap_or_default()
}

pub fn all_connection_fields(introspection: &Value) -> Vec<String> {
	let types = introspection
		.pointer("/data/__schema/types")
		.and_then(|v| v.as_array())
		.cloned()
		.unwrap_or_default();

	let query_type = match types.iter().find(|t| t["name"].as_str() == Some("Query")) {
		Some(t) => t.clone(),
		None => return vec![],
	};

	let fields = match query_type.get("fields").and_then(|f| f.as_array()) {
		Some(f) => f.clone(),
		None => return vec![],
	};

	fields
		.iter()
		.filter_map(|field| {
			let name = field["name"].as_str()?;
			if name.starts_with('_') {
				return None;
			}
			let type_name = field
				.pointer("/type/name")
				.and_then(|v| v.as_str())
				.or_else(|| field.pointer("/type/ofType/name").and_then(|v| v.as_str()))
				.or_else(|| field.pointer("/type/ofType/ofType/name").and_then(|v| v.as_str()))
				.unwrap_or("");
			if type_name.ends_with("Connection") { Some(name.to_string()) } else { None }
		})
		.collect()
}

pub async fn find_first_populated_connection_field(
	client: &TestClient,
	introspection: &Value,
) -> Option<String> {
	for field in all_connection_fields(introspection) {
		let gql = format!("{{ {field}(first: 1) {{ totalCount }} }}");
		let resp = client.query(&gql).await;
		let count = resp
			.pointer(&format!("/data/{field}/totalCount"))
			.and_then(|v| v.as_i64())
			.unwrap_or(0);
		if count > 0 {
			return Some(field);
		}
	}
	None
}
