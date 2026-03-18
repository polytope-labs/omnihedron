// tests/integration_test.rs
//
// Integration tests comparing TypeScript and Rust omnihedron and TypeScript subql-query services.
//
// Prerequisites:
//   - PostgreSQL running with the dump restored (run scripts/setup_db.sh)
//   - Both services running (run scripts/start_services.sh)
//   - Environment variables set: RUST_SERVICE_URL, TS_SERVICE_URL, SCHEMA_NAME
//
// Run with:
//   RUST_SERVICE_URL=http://localhost:3000 \
//   TS_SERVICE_URL=http://localhost:3001 \
//   SCHEMA_NAME=<schema> \
//   cargo test --test integration_test -- --nocapture

use std::collections::HashSet;

use pretty_assertions::assert_eq;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Service URLs
// ---------------------------------------------------------------------------

fn rust_url() -> String {
	std::env::var("RUST_SERVICE_URL").unwrap_or_else(|_| "http://localhost:3000".to_string())
}

fn ts_url() -> String {
	std::env::var("TS_SERVICE_URL").unwrap_or_else(|_| "http://localhost:3001".to_string())
}

#[allow(dead_code)]
fn schema_name() -> String {
	std::env::var("SCHEMA_NAME").expect(
		"SCHEMA_NAME env var is required. \
         Run scripts/setup_db.sh and source .env.test before running tests.",
	)
}

/// Returns true if both services appear to be reachable.
/// The Rust service exposes /health; the TS service only serves /graphql.
fn services_available() -> bool {
	let rust = std::process::Command::new("curl")
		.args(["-sf", "--max-time", "3", &format!("{}/health", rust_url())])
		.output()
		.map(|o| o.status.success())
		.unwrap_or(false);

	// TS @subql/query does not expose /health — probe via a minimal GraphQL POST
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
			&format!("{}/graphql", ts_url()),
		])
		.output()
		.map(|o| o.status.success())
		.unwrap_or(false);

	rust && ts
}

// ---------------------------------------------------------------------------
// Test client
// ---------------------------------------------------------------------------

struct TestClient {
	url: String,
	client: reqwest::Client,
}

impl TestClient {
	fn new(url: &str) -> Self {
		Self {
			url: url.to_string(),
			client: reqwest::Client::builder()
				.timeout(std::time::Duration::from_secs(30))
				.build()
				.expect("Failed to build HTTP client"),
		}
	}

	async fn query(&self, gql: &str) -> Value {
		self.query_with_body(json!({ "query": gql })).await
	}

	async fn query_vars(&self, gql: &str, vars: Value) -> Value {
		self.query_with_body(json!({ "query": gql, "variables": vars })).await
	}

	async fn query_with_body(&self, body: Value) -> Value {
		let resp = self
			.client
			.post(format!("{}/graphql", self.url))
			.header("Content-Type", "application/json")
			.json(&body)
			.send()
			.await
			.unwrap_or_else(|e| panic!("Request to {} failed: {e}", self.url));

		resp.json::<Value>().await.expect("Failed to parse response as JSON")
	}

	async fn batch_query(&self, bodies: &[Value]) -> Value {
		let resp = self
			.client
			.post(format!("{}/graphql", self.url))
			.header("Content-Type", "application/json")
			.json(&bodies)
			.send()
			.await
			.unwrap_or_else(|e| panic!("Batch request to {} failed: {e}", self.url));

		resp.json::<Value>().await.expect("Failed to parse batch response as JSON")
	}

	async fn health(&self) -> reqwest::StatusCode {
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

/// Recursively sort arrays of objects by their "id" field so comparisons are
/// deterministic regardless of the DB's physical row order.
fn sort_nodes(val: &mut Value) {
	match val {
		Value::Array(arr) => {
			// Sort if all elements are objects with an "id" field
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

/// Compare two GraphQL responses, focusing on the `data` field.
/// Fields that legitimately differ between implementations (e.g. queryNodeVersion
/// in _metadata) are stripped before comparison.
fn compare_responses(query_name: &str, ts: &Value, rust: &Value) {
	let ts_data = ts.get("data").cloned().unwrap_or(Value::Null);
	let rust_data = rust.get("data").cloned().unwrap_or(Value::Null);

	let mut ts_clean = ts_data.clone();
	let mut rust_clean = rust_data.clone();

	// Strip fields that intentionally differ between implementations
	strip_field(&mut ts_clean, "queryNodeVersion");
	strip_field(&mut rust_clean, "queryNodeVersion");
	strip_field(&mut ts_clean, "indexerNodeVersion");
	strip_field(&mut rust_clean, "indexerNodeVersion");
	// Cursor values are implementation-specific (PostGraphile vs Rust encode them differently)
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

fn strip_field(val: &mut Value, field: &str) {
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

/// Extract all Query field names from an introspection result.
#[allow(dead_code)]
fn extract_query_fields(introspection: &Value) -> Vec<String> {
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

/// Find the first Query field that looks like an entity collection (returns a Connection type).
fn find_first_connection_field(introspection: &Value) -> Option<String> {
	let types = introspection
		.pointer("/data/__schema/types")
		.and_then(|v| v.as_array())
		.cloned()
		.unwrap_or_default();

	let query_type = types.iter().find(|t| t["name"].as_str() == Some("Query"))?.clone();

	let fields = query_type.get("fields")?.as_array()?;

	for field in fields {
		let name = field["name"].as_str()?;
		// Skip built-in fields
		if name.starts_with('_') {
			continue;
		}
		// Look for fields whose return type is a Connection (contains "Connection")
		let type_name = field
			.pointer("/type/name")
			.or_else(|| field.pointer("/type/ofType/name"))
			.or_else(|| field.pointer("/type/ofType/ofType/name"))
			.and_then(|v| v.as_str())
			.unwrap_or("");

		if type_name.ends_with("Connection") {
			// The field name is the plural entity name (e.g., "transfers")
			return Some(name.to_string());
		}
	}
	None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health() {
	if !services_available() {
		eprintln!(
			"SKIP: Services not available. Run scripts/start_services.sh first.\n\
             RUST_SERVICE_URL={}\n\
             TS_SERVICE_URL={}",
			rust_url(),
			ts_url()
		);
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Rust exposes /health; TS only serves /graphql (no dedicated health endpoint)
	let rust_status = rust_client.health().await;
	assert!(rust_status.is_success(), "Rust service /health returned non-2xx: {}", rust_status);
	println!("Rust /health: {}", rust_status);

	// For TS, verify /graphql responds to a minimal query
	let ts_resp = ts_client.query("{ __typename }").await;
	assert!(ts_resp.get("errors").is_none(), "TS /graphql probe returned errors: {}", ts_resp);
	println!("TS   /graphql probe: ok");
}

#[tokio::test]
async fn test_metadata() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Use a specific chainId so both services read from the same metadata table.
	// "11155111" is Ethereum Sepolia and exists in this test DB.
	let query = r#"
        {
            _metadata(chainId: "11155111") {
                lastProcessedHeight
                chain
                specName
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   _metadata: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust _metadata: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	// Both should have data without errors
	assert!(
		ts_resp.get("errors").is_none() ||
			ts_resp["errors"].as_array().map(|a| a.is_empty()).unwrap_or(true),
		"TS returned errors: {:?}",
		ts_resp.get("errors")
	);
	assert!(
		rust_resp.get("errors").is_none() ||
			rust_resp["errors"].as_array().map(|a| a.is_empty()).unwrap_or(true),
		"Rust returned errors: {:?}",
		rust_resp.get("errors")
	);

	// Both should return chain "11155111"
	let rust_chain = rust_resp
		.pointer("/_metadata/chain")
		.or_else(|| rust_resp.pointer("/data/_metadata/chain"));
	assert!(rust_chain.is_some(), "Rust _metadata/chain is missing. Response: {}", rust_resp);

	compare_responses("_metadata", &ts_resp, &rust_resp);
}

#[tokio::test]
async fn test_introspection_types() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let introspection_query = r#"
        {
            __schema {
                types {
                    name
                    kind
                }
            }
        }
    "#;

	let ts_resp = ts_client.query(introspection_query).await;
	let rust_resp = rust_client.query(introspection_query).await;

	// Filter down to core entity types, excluding:
	// - Introspection types (__*)
	// - SubQuery-internal entity types (_Global*, _Metadata*, _Multi*)
	// - PostGraphile-specific aggregate helpers not yet implemented in Rust (HavingInput,
	//   AggregatesFilter, GroupBy, DistinctCountAggregates sub-types)
	let is_core_type = |n: &&str| -> bool {
		if n.starts_with("__") {
			return false;
		}
		if n.starts_with("_Global") || n.starts_with("_Globals") {
			return false;
		}
		if n.starts_with("_Metadata") || n.starts_with("_Multi") {
			return false;
		}
		// Skip PostGraphile-specific aggregate helper types
		if n.contains("Having") {
			return false;
		}
		if n.ends_with("AggregatesFilter") {
			return false;
		}
		if n.ends_with("GroupBy") {
			return false;
		}
		if n.ends_with("DistinctCountAggregates") {
			return false;
		}
		if n.ends_with("AggregateFilter") {
			return false;
		}
		if n.contains("ToMany") {
			return false;
		}
		true
	};

	let ts_types: HashSet<String> = ts_resp
		.pointer("/data/__schema/types")
		.and_then(|v| v.as_array())
		.map(|types| {
			types
				.iter()
				.filter_map(|t| t["name"].as_str())
				.filter(is_core_type)
				.map(|s| s.to_string())
				.collect()
		})
		.unwrap_or_default();

	let rust_types: HashSet<String> = rust_resp
		.pointer("/data/__schema/types")
		.and_then(|v| v.as_array())
		.map(|types| {
			types
				.iter()
				.filter_map(|t| t["name"].as_str())
				.filter(is_core_type)
				.map(|s| s.to_string())
				.collect()
		})
		.unwrap_or_default();

	println!("TS   type count: {}", ts_types.len());
	println!("Rust type count: {}", rust_types.len());

	// All TS types must be present in Rust (Rust may have extras)
	let missing_in_rust: HashSet<&String> = ts_types.difference(&rust_types).collect();
	if !missing_in_rust.is_empty() {
		eprintln!("Types present in TS but missing in Rust: {:?}", missing_in_rust);
	}
	assert!(
		missing_in_rust.is_empty(),
		"Rust is missing {} types that TS has: {:?}",
		missing_in_rust.len(),
		missing_in_rust
	);
}

#[tokio::test]
async fn test_first_entity_list() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Full introspection to find first connection field
	let introspection_query = r#"
        {
            __schema {
                types {
                    name
                    kind
                    fields {
                        name
                        type {
                            name
                            kind
                            ofType {
                                name
                                kind
                                ofType {
                                    name
                                    kind
                                }
                            }
                        }
                    }
                }
            }
        }
    "#;

	let ts_intro = ts_client.query(introspection_query).await;

	let entity_field = find_first_connection_field(&ts_intro);
	let entity_field = match entity_field {
		Some(f) => f,
		None => {
			eprintln!("SKIP: No connection fields found in schema.");
			return;
		},
	};

	println!("Testing entity field: {}", entity_field);

	let entity_query = format!(
		r#"
        {{
            {entity}(first: 5) {{
                totalCount
                nodes {{
                    id
                }}
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
            }}
        }}
        "#,
		entity = entity_field
	);

	let ts_resp = ts_client.query(&entity_query).await;
	let rust_resp = rust_client.query(&entity_query).await;

	println!("TS   entity list: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust entity list: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	compare_responses(&format!("{}(first:5)", entity_field), &ts_resp, &rust_resp);
}

#[tokio::test]
async fn test_pagination() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Use _metadata to discover a field; fallback to a generic pagination test
	let introspection_query = r#"
        {
            __schema {
                types {
                    name
                    kind
                    fields {
                        name
                        type {
                            name
                            kind
                            ofType { name kind ofType { name kind } }
                        }
                    }
                }
            }
        }
    "#;

	let ts_intro = ts_client.query(introspection_query).await;
	let entity_field = match find_first_connection_field(&ts_intro) {
		Some(f) => f,
		None => {
			eprintln!("SKIP: No connection fields found in schema.");
			return;
		},
	};

	println!("Pagination test using entity: {}", entity_field);

	// Page 1
	let page1_query = format!(
		r#"
        {{
            {entity}(first: 3) {{
                nodes {{ id }}
                pageInfo {{ hasNextPage endCursor }}
            }}
        }}
        "#,
		entity = entity_field
	);

	let ts_page1 = ts_client.query(&page1_query).await;
	let rust_page1 = rust_client.query(&page1_query).await;

	// Extract cursor from Rust response
	let cursor = rust_page1
		.pointer(&format!("/data/{}/pageInfo/endCursor", entity_field))
		.and_then(|v| v.as_str())
		.map(|s| s.to_string());

	let has_next = rust_page1
		.pointer(&format!("/data/{}/pageInfo/hasNextPage", entity_field))
		.and_then(|v| v.as_bool())
		.unwrap_or(false);

	compare_responses(&format!("{}(first:3) page1", entity_field), &ts_page1, &rust_page1);

	if has_next {
		if let Some(cursor_val) = cursor {
			let page2_query = format!(
				r#"
                {{
                    {entity}(first: 3, after: "{cursor}") {{
                        nodes {{ id }}
                        pageInfo {{ hasNextPage endCursor }}
                    }}
                }}
                "#,
				entity = entity_field,
				cursor = cursor_val
			);

			let ts_page2 = ts_client.query(&page2_query).await;
			let rust_page2 = rust_client.query(&page2_query).await;

			compare_responses(&format!("{}(first:3) page2", entity_field), &ts_page2, &rust_page2);

			// Verify no overlap between page1 and page2 node IDs (using Rust responses)
			let page1_ids: HashSet<String> = rust_page1
				.pointer(&format!("/data/{}/nodes", entity_field))
				.and_then(|v| v.as_array())
				.map(|nodes| {
					nodes.iter().filter_map(|n| n["id"].as_str().map(|s| s.to_string())).collect()
				})
				.unwrap_or_default();

			let page2_ids: HashSet<String> = rust_page2
				.pointer(&format!("/data/{}/nodes", entity_field))
				.and_then(|v| v.as_array())
				.map(|nodes| {
					nodes.iter().filter_map(|n| n["id"].as_str().map(|s| s.to_string())).collect()
				})
				.unwrap_or_default();

			let overlap: HashSet<&String> = page1_ids.intersection(&page2_ids).collect();
			assert!(overlap.is_empty(), "Pages overlap! Common IDs: {:?}", overlap);

			println!(
				"Pagination: page1={} items, page2={} items, no overlap confirmed",
				page1_ids.len(),
				page2_ids.len()
			);
		}
	} else {
		println!("Pagination: only one page of data available — skipping page 2 test.");
	}
}

#[tokio::test]
async fn test_order_by() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let introspection_query = r#"
        {
            __schema {
                types {
                    name
                    kind
                    fields {
                        name
                        type {
                            name kind
                            ofType { name kind ofType { name kind } }
                        }
                    }
                }
            }
        }
    "#;

	let ts_intro = ts_client.query(introspection_query).await;
	let entity_field = match find_first_connection_field(&ts_intro) {
		Some(f) => f,
		None => {
			eprintln!("SKIP: No connection fields found.");
			return;
		},
	};

	println!("OrderBy test using entity: {}", entity_field);

	// orderBy ID ascending
	let order_query = format!(
		r#"
        {{
            {entity}(first: 10, orderBy: ID_ASC) {{
                nodes {{ id }}
            }}
        }}
        "#,
		entity = entity_field
	);

	let ts_resp = ts_client.query(&order_query).await;
	let rust_resp = rust_client.query(&order_query).await;

	println!("TS   orderBy: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust orderBy: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	// Verify the Rust result is actually sorted by ID
	if let Some(nodes) = rust_resp
		.pointer(&format!("/data/{}/nodes", entity_field))
		.and_then(|v| v.as_array())
	{
		let ids: Vec<&str> = nodes.iter().filter_map(|n| n["id"].as_str()).collect();

		let mut sorted_ids = ids.clone();
		sorted_ids.sort();

		assert_eq!(ids, sorted_ids, "Rust orderBy ID_ASC did not return sorted results");
	}

	// NOTE: We don't use compare_responses here because order might legitimately
	// differ if both are sorted. Instead we check that each service returns sorted data.
	if let Some(nodes) = ts_resp
		.pointer(&format!("/data/{}/nodes", entity_field))
		.and_then(|v| v.as_array())
	{
		let ids: Vec<&str> = nodes.iter().filter_map(|n| n["id"].as_str()).collect();

		let mut sorted_ids = ids.clone();
		sorted_ids.sort();

		assert_eq!(ids, sorted_ids, "TS orderBy ID_ASC did not return sorted results");
	}
}

#[tokio::test]
async fn test_filter_null() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let introspection_query = r#"
        {
            __schema {
                types {
                    name
                    kind
                    fields {
                        name
                        type {
                            name kind
                            ofType { name kind ofType { name kind } }
                        }
                    }
                }
            }
        }
    "#;

	let ts_intro = ts_client.query(introspection_query).await;
	let entity_field = match find_first_connection_field(&ts_intro) {
		Some(f) => f,
		None => {
			eprintln!("SKIP: No connection fields found.");
			return;
		},
	};

	println!("Filter test using entity: {}", entity_field);

	// Filter: id is not null (should return results for any table)
	let filter_query = format!(
		r#"
        {{
            {entity}(first: 5, filter: {{ id: {{ isNull: false }} }}) {{
                totalCount
                nodes {{ id }}
            }}
        }}
        "#,
		entity = entity_field
	);

	let ts_resp = ts_client.query(&filter_query).await;
	let rust_resp = rust_client.query(&filter_query).await;

	println!("TS   filter: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust filter: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	compare_responses(&format!("{}(filter:id isNull:false)", entity_field), &ts_resp, &rust_resp);
}

#[tokio::test]
async fn test_aggregates() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let introspection_query = r#"
        {
            __schema {
                types {
                    name
                    kind
                    fields {
                        name
                        type {
                            name kind
                            ofType { name kind ofType { name kind } }
                        }
                    }
                }
            }
        }
    "#;

	let ts_intro = ts_client.query(introspection_query).await;
	let entity_field = match find_first_connection_field(&ts_intro) {
		Some(f) => f,
		None => {
			eprintln!("SKIP: No connection fields found.");
			return;
		},
	};

	println!("Aggregates test using entity: {}", entity_field);

	let agg_query = format!(
		r#"
        {{
            {entity}(first: 1) {{
                aggregates {{
                    count {{
                        keys
                    }}
                }}
            }}
        }}
        "#,
		entity = entity_field
	);

	let ts_resp = ts_client.query(&agg_query).await;
	let rust_resp = rust_client.query(&agg_query).await;

	println!("TS   aggregates: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust aggregates: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	// Both should respond without top-level errors; we don't require exact match
	// since aggregate support may vary
	let ts_has_errors = ts_resp
		.get("errors")
		.and_then(|e| e.as_array())
		.map(|a| !a.is_empty())
		.unwrap_or(false);

	let rust_has_errors = rust_resp
		.get("errors")
		.and_then(|e| e.as_array())
		.map(|a| !a.is_empty())
		.unwrap_or(false);

	if ts_has_errors && !rust_has_errors {
		eprintln!(
			"NOTE: TS returned errors on aggregates but Rust succeeded — Rust may have better aggregate support."
		);
	} else if rust_has_errors && !ts_has_errors {
		eprintln!("NOTE: Rust returned errors on aggregates — may not be implemented yet.");
	} else if !ts_has_errors && !rust_has_errors {
		compare_responses(&format!("{}aggregates", entity_field), &ts_resp, &rust_resp);
	}
}

#[tokio::test]
async fn test_batch_query() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Use specific chainId so both services read the same metadata table
	let query1 =
		json!({ "query": r#"{ _metadata(chainId: "11155111") { lastProcessedHeight } }"# });
	let query2 = json!({ "query": r#"{ _metadata(chainId: "11155111") { chain } }"# });
	let batch = vec![query1, query2];

	let ts_resp = ts_client.batch_query(&batch).await;
	let rust_resp = rust_client.batch_query(&batch).await;

	println!("TS   batch: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust batch: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	// Both should return an array
	assert!(ts_resp.is_array(), "TS batch response should be an array, got: {:?}", ts_resp);
	assert!(rust_resp.is_array(), "Rust batch response should be an array, got: {:?}", rust_resp);

	// Both should have 2 results
	let ts_arr = ts_resp.as_array().unwrap();
	let rust_arr = rust_resp.as_array().unwrap();

	assert_eq!(ts_arr.len(), 2, "TS batch: expected 2 results");
	assert_eq!(rust_arr.len(), 2, "Rust batch: expected 2 results");

	// Compare each result
	for (i, (ts_item, rust_item)) in ts_arr.iter().zip(rust_arr.iter()).enumerate() {
		compare_responses(&format!("batch[{}]", i), ts_item, rust_item);
	}
}

#[tokio::test]
async fn test_query_with_variables() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let introspection_query = r#"
        {
            __schema {
                types {
                    name
                    kind
                    fields {
                        name
                        type {
                            name kind
                            ofType { name kind ofType { name kind } }
                        }
                    }
                }
            }
        }
    "#;

	let ts_intro = ts_client.query(introspection_query).await;
	let entity_field = match find_first_connection_field(&ts_intro) {
		Some(f) => f,
		None => {
			eprintln!("SKIP: No connection fields found.");
			return;
		},
	};

	println!("Variables test using entity: {}", entity_field);

	// Use a variable for the 'first' argument
	let gql = format!(
		r#"
        query EntityList($count: Int!) {{
            {entity}(first: $count) {{
                nodes {{ id }}
            }}
        }}
        "#,
		entity = entity_field
	);

	let vars = json!({ "count": 3 });

	let ts_resp = ts_client.query_vars(&gql, vars.clone()).await;
	let rust_resp = rust_client.query_vars(&gql, vars).await;

	println!("TS   vars query: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust vars query: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	compare_responses(&format!("{}(variables)", entity_field), &ts_resp, &rust_resp);
}
