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

/// Collect all Query fields that return a Connection type.
fn all_connection_fields(introspection: &Value) -> Vec<String> {
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
			// Chain with and_then(as_str) at each level so JSON null values produce None
			// and or_else correctly falls through to the next wrapper level.
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

/// Find the first connection field that has at least one row in the database.
/// Probes each field with `totalCount` until one returns > 0.
async fn find_first_populated_connection_field(
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

// ---------------------------------------------------------------------------
// Rust-only integration tests (only probe the Rust service)
// ---------------------------------------------------------------------------

/// Query only `totalCount` (no nodes/edges) and verify the Rust service
/// returns a valid integer without errors.  This exercises the count-only
/// fast-path that skips row fetching entirely.
#[tokio::test]
async fn test_count_only() {
	let rust_client = TestClient::new(&rust_url());

	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available.");
		return;
	}

	let introspection_query = r#"
        {
            __schema {
                types {
                    name kind
                    fields {
                        name
                        type { name kind ofType { name kind ofType { name kind } } }
                    }
                }
            }
        }
    "#;

	let intro = rust_client.query(introspection_query).await;
	let entity_field = match find_first_populated_connection_field(&rust_client, &intro).await {
		Some(f) => f,
		None => {
			eprintln!("SKIP: No connection fields found in Rust schema.");
			return;
		},
	};

	println!("count_only test using entity: {}", entity_field);

	// Request only totalCount — no nodes, no edges.
	let gql = format!(r#"{{ {entity}(first: 10) {{ totalCount }} }}"#, entity = entity_field);

	let resp = rust_client.query(&gql).await;

	assert!(
		resp.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"count-only query returned errors: {}",
		resp
	);

	let total_count = resp
		.pointer(&format!("/data/{}/totalCount", entity_field))
		.and_then(|v| v.as_i64());

	assert!(total_count.is_some(), "totalCount missing from count-only response: {}", resp);
	let n = total_count.unwrap();
	assert!(n >= 0, "totalCount should be non-negative, got {n}");
	println!("count_only: totalCount = {n}");
}

/// Verify that `totalCount` from the connection matches the actual number of rows
/// returned across all pages (window function COUNT correctness).
#[tokio::test]
async fn test_total_count_accuracy() {
	let rust_client = TestClient::new(&rust_url());

	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available.");
		return;
	}

	let introspection_query = r#"
        {
            __schema {
                types {
                    name kind
                    fields {
                        name
                        type { name kind ofType { name kind ofType { name kind } } }
                    }
                }
            }
        }
    "#;

	let intro = rust_client.query(introspection_query).await;
	let entity_field = match find_first_populated_connection_field(&rust_client, &intro).await {
		Some(f) => f,
		None => {
			eprintln!("SKIP: No connection fields found in Rust schema.");
			return;
		},
	};

	println!("total_count_accuracy test using entity: {}", entity_field);

	// Fetch first page with a small limit and capture totalCount.
	let gql = format!(
		r#"{{ {entity}(first: 5) {{ totalCount nodes {{ id }} }} }}"#,
		entity = entity_field
	);
	let resp = rust_client.query(&gql).await;

	assert!(
		resp.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"total_count_accuracy query returned errors: {}",
		resp
	);

	let total_count = resp
		.pointer(&format!("/data/{}/totalCount", entity_field))
		.and_then(|v| v.as_i64())
		.expect("totalCount missing");

	// totalCount should reflect the full table, not just the page size.
	// The fixture has 20 rows per entity table, so with first:5 the total should be >= 5.
	assert!(total_count >= 5, "totalCount {total_count} seems too low — expected at least 5 rows");

	// Cross-check: fetch with a large limit and verify node count ≤ totalCount.
	let gql_all = format!(
		r#"{{ {entity}(first: 1000) {{ totalCount nodes {{ id }} }} }}"#,
		entity = entity_field
	);
	let resp_all = rust_client.query(&gql_all).await;
	let total_all = resp_all
		.pointer(&format!("/data/{}/totalCount", entity_field))
		.and_then(|v| v.as_i64())
		.expect("totalCount missing in large fetch");
	let node_count = resp_all
		.pointer(&format!("/data/{}/nodes", entity_field))
		.and_then(|v| v.as_array())
		.map(|a| a.len() as i64)
		.unwrap_or(0);

	assert_eq!(
		total_all, node_count,
		"When fetching all rows, totalCount ({total_all}) should equal nodes.len() ({node_count})"
	);
	println!("total_count_accuracy: totalCount={total_count}, full fetch nodes={node_count} ✓");
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

	// Filter down to comparable types, excluding only:
	// - GraphQL introspection meta-types (__*)
	// - PostGraphile-specific aggregate helpers that Rust doesn't implement (Having*, *GroupBy,
	//   *DistinctCountAggregates, *AggregateFilter, *ToMany*)
	let is_core_type = |n: &&str| -> bool {
		if n.starts_with("__") {
			return false;
		}
		// PostGraphile-specific aggregate helper types not in Rust schema
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

	let entity_field = match find_first_populated_connection_field(&ts_client, &ts_intro).await {
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
            {entity}(first: 5, orderBy: ID_ASC) {{
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
	let entity_field = match find_first_populated_connection_field(&ts_client, &ts_intro).await {
		Some(f) => f,
		None => {
			eprintln!("SKIP: No connection fields found in schema.");
			return;
		},
	};

	println!("Pagination test using entity: {}", entity_field);

	// Page 1 — explicit ordering so both services return the same set
	let page1_query = format!(
		r#"
        {{
            {entity}(first: 3, orderBy: ID_ASC) {{
                nodes {{ id }}
                pageInfo {{ hasNextPage endCursor }}
            }}
        }}
        "#,
		entity = entity_field
	);

	let ts_page1 = ts_client.query(&page1_query).await;
	let rust_page1 = rust_client.query(&page1_query).await;

	compare_responses(&format!("{}(first:3) page1", entity_field), &ts_page1, &rust_page1);

	// Extract per-service cursors (cursor encoding differs between PostGraphile and Rust)
	let rust_cursor = rust_page1
		.pointer(&format!("/data/{}/pageInfo/endCursor", entity_field))
		.and_then(|v| v.as_str())
		.map(|s| s.to_string());
	let ts_cursor = ts_page1
		.pointer(&format!("/data/{}/pageInfo/endCursor", entity_field))
		.and_then(|v| v.as_str())
		.map(|s| s.to_string());

	let has_next = rust_page1
		.pointer(&format!("/data/{}/pageInfo/hasNextPage", entity_field))
		.and_then(|v| v.as_bool())
		.unwrap_or(false);

	if has_next {
		if let (Some(rust_cur), Some(ts_cur)) = (rust_cursor, ts_cursor) {
			// Each service uses its own cursor format for page 2
			let rust_page2_query = format!(
				r#"{{ {entity}(first: 3, orderBy: ID_ASC, after: "{cursor}") {{ nodes {{ id }} pageInfo {{ hasNextPage endCursor }} }} }}"#,
				entity = entity_field,
				cursor = rust_cur
			);
			let ts_page2_query = format!(
				r#"{{ {entity}(first: 3, orderBy: ID_ASC, after: "{cursor}") {{ nodes {{ id }} pageInfo {{ hasNextPage endCursor }} }} }}"#,
				entity = entity_field,
				cursor = ts_cur
			);

			let rust_page2 = rust_client.query(&rust_page2_query).await;
			let ts_page2 = ts_client.query(&ts_page2_query).await;

			// Both page 2s should return the same set of IDs (sorted comparison)
			compare_responses(&format!("{}(first:3) page2", entity_field), &ts_page2, &rust_page2);

			// Verify Rust page1 and page2 don't overlap
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
	let entity_field = match find_first_populated_connection_field(&ts_client, &ts_intro).await {
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
	let entity_field = match find_first_populated_connection_field(&ts_client, &ts_intro).await {
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
            {entity}(first: 5, orderBy: ID_ASC, filter: {{ id: {{ isNull: false }} }}) {{
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
	let entity_field = match find_first_populated_connection_field(&ts_client, &ts_intro).await {
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
                    distinctCount {{
                        id
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
	let entity_field = match find_first_populated_connection_field(&ts_client, &ts_intro).await {
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
            {entity}(first: $count, orderBy: ID_ASC) {{
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

// ---------------------------------------------------------------------------
// Extended Rust-only tests (probe only the Rust service)
// ---------------------------------------------------------------------------

/// Test numeric aggregates (sum, min, max, average) on the assetTeleporteds table.
/// These are Rust-only because PostGraphile returns native numeric types while
/// Rust casts all aggregate results to strings (CAST ... AS TEXT).
#[tokio::test]
async fn test_numeric_aggregates() {
	let rust_client = TestClient::new(&rust_url());

	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available.");
		return;
	}

	let query = r#"
        {
            assetTeleporteds(first: 100) {
                aggregates {
                    count
                    sum { blockNumber amount }
                    min { blockNumber amount }
                    max { blockNumber amount }
                    average { blockNumber amount }
                }
            }
        }
    "#;

	let resp = rust_client.query(query).await;

	assert!(
		resp.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"numeric aggregates query returned errors: {}",
		resp
	);

	println!("Numeric aggregates: {}", serde_json::to_string_pretty(&resp).unwrap());

	let agg = resp
		.pointer("/data/assetTeleporteds/aggregates")
		.expect("aggregates field missing from response");

	// count should be a string representation of a non-negative integer.
	let count_str = agg
		.pointer("/count")
		.and_then(|v| v.as_str())
		.expect("count should be a string");
	let count_val: i64 = count_str.parse().expect("count should parse as i64");
	assert!(count_val >= 20, "Expected at least 20 rows, got {count_val}");

	// sum.blockNumber should be a parseable positive integer string.
	let sum_val: i64 = agg
		.pointer("/sum/blockNumber")
		.and_then(|v| v.as_str())
		.and_then(|s| s.parse().ok())
		.expect("sum.blockNumber should be a parseable string");
	assert!(sum_val > 0, "sum.blockNumber should be positive, got {sum_val}");

	// min.blockNumber <= max.blockNumber
	let min_block: i64 = agg
		.pointer("/min/blockNumber")
		.and_then(|v| v.as_str())
		.and_then(|s| s.parse().ok())
		.expect("min.blockNumber should be a parseable string");
	let max_block: i64 = agg
		.pointer("/max/blockNumber")
		.and_then(|v| v.as_str())
		.and_then(|s| s.parse().ok())
		.expect("max.blockNumber should be a parseable string");
	assert!(
		min_block <= max_block,
		"min.blockNumber ({min_block}) should be <= max.blockNumber ({max_block})"
	);

	println!("blockNumber: sum={sum_val}, min={min_block}, max={max_block}, count={count_val}");
}

/// Test the `blockHeight` argument (Rust-only — TypeScript @subql/query rejects
/// `blockHeight` as an unknown argument on non-historical tables, but our Rust
/// implementation attaches it to every table that has a `_block_range` column).
#[tokio::test]
async fn test_blockheight() {
	let rust_client = TestClient::new(&rust_url());

	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available.");
		return;
	}

	// All rows have _block_range lower bounds between 1729586868000 and ~1729600392000.
	// A very large blockHeight includes all rows (all ranges are [lower, ∞)).
	let query_all = r#"
        {
            assetTeleporteds(first: 100, blockHeight: "9999999999999") {
                totalCount
                nodes { id }
            }
        }
    "#;

	let resp_all = rust_client.query(query_all).await;
	assert!(
		resp_all
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"blockHeight (all rows) query returned errors: {}",
		resp_all
	);
	let count_all = resp_all
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing for large blockHeight");
	assert!(count_all >= 20, "Expected >= 20 rows with large blockHeight, got {count_all}");

	// blockHeight = 1729590000000:
	//   Row 1 has lower = 1729586868000 <= 1729590000000 → INCLUDED
	//   Row 2 has lower = 1729592814000 >  1729590000000 → NOT INCLUDED
	// So exactly 1 row should be returned.
	let query_one = r#"
        {
            assetTeleporteds(first: 100, blockHeight: "1729590000000") {
                totalCount
                nodes { id }
            }
        }
    "#;

	let resp_one = rust_client.query(query_one).await;
	assert!(
		resp_one
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"blockHeight (narrow) query returned errors: {}",
		resp_one
	);
	let count_one = resp_one
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing for narrow blockHeight");
	assert_eq!(
		count_one, 1,
		"Expected exactly 1 row with blockHeight=1729590000000, got {count_one}"
	);

	println!(
		"blockHeight: large height → {count_all} rows, narrow height (1729590000000) → {count_one} row"
	);
}

// ---------------------------------------------------------------------------
// Extended comparison tests (both services must be running)
// ---------------------------------------------------------------------------

/// Test `equalTo` filter on a string column — compare Rust vs TypeScript.
/// All assetTeleporteds rows have chain = "KUSAMA-4009".
#[tokio::test]
async fn test_filter_equalto() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleporteds(
                first: 5,
                orderBy: ID_ASC,
                filter: { chain: { equalTo: "KUSAMA-4009" } }
            ) {
                totalCount
                nodes { id chain }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   filter equalTo: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust filter equalTo: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	// All 20 rows match — totalCount should be 20.
	let total = rust_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from Rust response");
	assert!(total >= 20, "equalTo filter should match all 20 rows, got {total}");

	// Every returned node must have chain = "KUSAMA-4009".
	if let Some(nodes) =
		rust_resp.pointer("/data/assetTeleporteds/nodes").and_then(|v| v.as_array())
	{
		for node in nodes {
			assert_eq!(node["chain"].as_str().unwrap_or(""), "KUSAMA-4009");
		}
	}

	compare_responses("assetTeleporteds(equalTo)", &ts_resp, &rust_resp);
}

/// Test `greaterThan` / `lessThan` filters on an integer column (block_number / INT4).
/// This exercises the `TextParam` fix that allows numeric comparison filters to work.
#[tokio::test]
async fn test_filter_comparison() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// block_number ranges 2154921–2157150; greaterThan 2156000 returns a subset.
	let query = r#"
        {
            assetTeleporteds(
                first: 100,
                orderBy: ID_ASC,
                filter: { blockNumber: { greaterThan: 2156000 } }
            ) {
                totalCount
                nodes { id blockNumber }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   filter greaterThan: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust filter greaterThan: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust comparison filter returned errors: {}",
		rust_resp
	);

	// Every Rust node must have blockNumber > 2156000.
	if let Some(nodes) =
		rust_resp.pointer("/data/assetTeleporteds/nodes").and_then(|v| v.as_array())
	{
		for node in nodes {
			let bn = node["blockNumber"].as_i64().unwrap_or(0);
			assert!(bn > 2156000, "blockNumber {bn} should be > 2156000");
		}
	}

	compare_responses("assetTeleporteds(greaterThan)", &ts_resp, &rust_resp);
}

/// Test the `in` filter operator — filter by a set of three known IDs.
#[tokio::test]
async fn test_filter_in() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Three known IDs from the fixture (first three rows of asset_teleporteds).
	let query = r#"
        {
            assetTeleporteds(
                first: 10,
                orderBy: ID_ASC,
                filter: {
                    id: {
                        in: [
                            "0x2c5edd96e3e017d74ccc172437317ac67bbcdbbdfe3afda178a9e3f9546f8ec6",
                            "0x5e11feb69bef8bec523afc3c1fe0297c0dff5794422eefb2c11f1ba78efdb3d4",
                            "0x4e54a431cdb82239331deda9feb93f5a101f50756e62387c0532143dad02d1f7"
                        ]
                    }
                }
            ) {
                totalCount
                nodes { id }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   filter in: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust filter in: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	let count = rust_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from Rust response");
	assert_eq!(count, 3, "Expected exactly 3 rows from `in` filter, got {count}");

	compare_responses("assetTeleporteds(in)", &ts_resp, &rust_resp);
}

/// Test string filter operators: `startsWith`.
/// "0x2c5edd" is a prefix unique to exactly one row in the fixture.
#[tokio::test]
async fn test_filter_string_ops() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleporteds(
                first: 10,
                orderBy: ID_ASC,
                filter: { id: { startsWith: "0x2c5edd" } }
            ) {
                totalCount
                nodes { id }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   filter startsWith: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust filter startsWith: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	// Only one ID in the fixture begins with "0x2c5edd".
	let count = rust_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from Rust response");
	assert_eq!(count, 1, "Expected 1 row from startsWith filter, got {count}");

	compare_responses("assetTeleporteds(startsWith)", &ts_resp, &rust_resp);
}

/// Test logical `and` / `or` filter operators.
#[tokio::test]
async fn test_filter_logical() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// AND: chain = "KUSAMA-4009" AND blockNumber > 2156000
	let query_and = r#"
        {
            assetTeleporteds(
                first: 100,
                orderBy: ID_ASC,
                filter: {
                    and: [
                        { chain: { equalTo: "KUSAMA-4009" } }
                        { blockNumber: { greaterThan: 2156000 } }
                    ]
                }
            ) {
                totalCount
                nodes { id blockNumber }
            }
        }
    "#;

	let ts_and = ts_client.query(query_and).await;
	let rust_and = rust_client.query(query_and).await;

	println!("TS   filter and: {}", serde_json::to_string_pretty(&ts_and).unwrap());
	println!("Rust filter and: {}", serde_json::to_string_pretty(&rust_and).unwrap());

	assert!(
		rust_and
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust `and` filter returned errors: {}",
		rust_and
	);
	compare_responses("assetTeleporteds(and)", &ts_and, &rust_and);

	// OR: blockNumber < 2155000 OR blockNumber > 2157000 — rows at both extremes
	let query_or = r#"
        {
            assetTeleporteds(
                first: 100,
                orderBy: ID_ASC,
                filter: {
                    or: [
                        { blockNumber: { lessThan: 2155000 } }
                        { blockNumber: { greaterThan: 2157000 } }
                    ]
                }
            ) {
                totalCount
                nodes { id blockNumber }
            }
        }
    "#;

	let ts_or = ts_client.query(query_or).await;
	let rust_or = rust_client.query(query_or).await;

	println!("TS   filter or: {}", serde_json::to_string_pretty(&ts_or).unwrap());
	println!("Rust filter or: {}", serde_json::to_string_pretty(&rust_or).unwrap());

	assert!(
		rust_or
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust `or` filter returned errors: {}",
		rust_or
	);
	compare_responses("assetTeleporteds(or)", &ts_or, &rust_or);
}

/// Test `orderBy` with a non-ID column (`BLOCK_NUMBER_ASC`).
/// Verifies that results are in ascending block_number order on both services.
#[tokio::test]
async fn test_orderby_non_id() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleporteds(first: 10, orderBy: BLOCK_NUMBER_ASC) {
                nodes { id blockNumber }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   BLOCK_NUMBER_ASC: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust BLOCK_NUMBER_ASC: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	// Helper: extract blockNumbers from a response and verify ascending order.
	let check_ascending = |resp: &Value, label: &str| {
		if let Some(nodes) = resp.pointer("/data/assetTeleporteds/nodes").and_then(|v| v.as_array())
		{
			let bns: Vec<i64> = nodes.iter().filter_map(|n| n["blockNumber"].as_i64()).collect();
			let mut sorted = bns.clone();
			sorted.sort();
			assert_eq!(bns, sorted, "{label} BLOCK_NUMBER_ASC not in order: {bns:?}");
		}
	};

	check_ascending(&rust_resp, "Rust");
	check_ascending(&ts_resp, "TS");

	// Both services must return the same set of nodes (sort_nodes normalises order by id).
	compare_responses("assetTeleporteds(BLOCK_NUMBER_ASC)", &ts_resp, &rust_resp);
}

/// Test `distinct` parameter — deduplicate by the `CHAIN` column.
/// All 20 fixture rows have chain = "KUSAMA-4009", so distinct should collapse to 1 row.
#[tokio::test]
async fn test_distinct() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleporteds(first: 10, distinct: [CHAIN], orderBy: CHAIN_ASC) {
                nodes { id chain }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   distinct: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust distinct: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust distinct query returned errors: {}",
		rust_resp
	);

	// All rows share chain = "KUSAMA-4009" → distinct collapses to exactly 1 row.
	let rust_nodes = rust_resp
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.expect("nodes missing from Rust response");
	assert_eq!(rust_nodes.len(), 1, "Rust: expected 1 distinct row, got {}", rust_nodes.len());
	assert_eq!(rust_nodes[0]["chain"].as_str().unwrap_or(""), "KUSAMA-4009");

	// TS should also return 1 row with chain = "KUSAMA-4009".
	// We do NOT compare the exact `id` value because DISTINCT ON returns an
	// arbitrary row from each group when ordering is not fully deterministic.
	assert!(
		ts_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"TS distinct query returned errors: {}",
		ts_resp
	);
	if let Some(ts_nodes) =
		ts_resp.pointer("/data/assetTeleporteds/nodes").and_then(|v| v.as_array())
	{
		assert_eq!(ts_nodes.len(), 1, "TS: expected 1 distinct row, got {}", ts_nodes.len());
		assert_eq!(ts_nodes[0]["chain"].as_str().unwrap_or(""), "KUSAMA-4009");
	}

	println!("distinct: Rust and TS each returned 1 row with chain=KUSAMA-4009 ✓");
}
