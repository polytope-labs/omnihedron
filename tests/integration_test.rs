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

/// Query only `totalCount` (no nodes/edges) and compare both services.
/// Also exercises Rust's count-only fast-path that skips row fetching entirely.
#[tokio::test]
async fn test_count_only() {
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
                    name kind
                    fields {
                        name
                        type { name kind ofType { name kind ofType { name kind } } }
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

	println!("count_only test using entity: {}", entity_field);

	// Request only totalCount — no nodes, no edges.
	let gql = format!(r#"{{ {entity}(first: 10) {{ totalCount }} }}"#, entity = entity_field);

	let ts_resp = ts_client.query(&gql).await;
	let rust_resp = rust_client.query(&gql).await;

	println!("TS   count_only: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust count_only: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"count-only query returned errors: {}",
		rust_resp
	);

	let n = rust_resp
		.pointer(&format!("/data/{}/totalCount", entity_field))
		.and_then(|v| v.as_i64())
		.unwrap_or_else(|| panic!("totalCount missing from count-only response: {rust_resp}"));
	assert!(n >= 0, "totalCount should be non-negative, got {n}");

	compare_responses(&format!("{}(totalCount only)", entity_field), &ts_resp, &rust_resp);
	println!("count_only: totalCount = {n} ✓");
}

/// Verify that `totalCount` matches the actual number of rows returned when fetching
/// all pages — compares both services.  Also cross-checks that Rust's window function
/// `COUNT(*) OVER()` returns the same value as a separate small-page query.
#[tokio::test]
async fn test_total_count_accuracy() {
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
                    name kind
                    fields {
                        name
                        type { name kind ofType { name kind ofType { name kind } } }
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

	println!("total_count_accuracy test using entity: {}", entity_field);

	// Fetch all rows from both services and compare totalCount + nodes.
	let gql_all = format!(
		r#"{{ {entity}(first: 1000, orderBy: ID_ASC) {{ totalCount nodes {{ id }} }} }}"#,
		entity = entity_field
	);

	let ts_resp = ts_client.query(&gql_all).await;
	let rust_resp = rust_client.query(&gql_all).await;

	println!("TS   total_count: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust total_count: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"total_count_accuracy query returned errors: {}",
		rust_resp
	);

	// Rust: totalCount must equal nodes.length (window function correctness).
	let rust_total = rust_resp
		.pointer(&format!("/data/{}/totalCount", entity_field))
		.and_then(|v| v.as_i64())
		.expect("Rust totalCount missing");
	let rust_nodes = rust_resp
		.pointer(&format!("/data/{}/nodes", entity_field))
		.and_then(|v| v.as_array())
		.map(|a| a.len() as i64)
		.unwrap_or(0);
	assert_eq!(
		rust_total, rust_nodes,
		"Rust: totalCount ({rust_total}) should equal nodes.len() ({rust_nodes})"
	);

	// TS: same invariant.
	let ts_total = ts_resp
		.pointer(&format!("/data/{}/totalCount", entity_field))
		.and_then(|v| v.as_i64())
		.expect("TS totalCount missing");
	let ts_nodes = ts_resp
		.pointer(&format!("/data/{}/nodes", entity_field))
		.and_then(|v| v.as_array())
		.map(|a| a.len() as i64)
		.unwrap_or(0);
	assert_eq!(
		ts_total, ts_nodes,
		"TS: totalCount ({ts_total}) should equal nodes.len() ({ts_nodes})"
	);

	compare_responses(&format!("{}(totalCount accuracy)", entity_field), &ts_resp, &rust_resp);
	println!("total_count_accuracy: Rust={rust_total}, TS={ts_total} ✓");
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
/// Compares Rust vs TypeScript — serialisation now matches PostGraphile:
///   - sum/average → BigFloat strings
///   - min/max on INT4 (blockNumber) → native JSON numbers
///   - min/max on BigInt (amount) → BigFloat strings
#[tokio::test]
async fn test_numeric_aggregates() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// `count` is omitted — PostGraphile's pg-aggregates does not expose it.
	let query = r#"
        {
            assetTeleporteds(first: 100) {
                aggregates {
                    sum { blockNumber amount }
                    min { blockNumber amount }
                    max { blockNumber amount }
                    average { blockNumber amount }
                }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   numeric aggregates: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust numeric aggregates: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"numeric aggregates query returned errors: {}",
		rust_resp
	);

	let agg = rust_resp
		.pointer("/data/assetTeleporteds/aggregates")
		.expect("aggregates field missing from response");

	// sum.blockNumber → BigFloat string (SUM of INT4 upcasts to numeric).
	let sum_val: i64 = agg
		.pointer("/sum/blockNumber")
		.and_then(|v| v.as_str())
		.and_then(|s| s.parse().ok())
		.expect("sum.blockNumber should be a parseable BigFloat string");
	assert!(sum_val > 0, "sum.blockNumber should be positive, got {sum_val}");

	// min/max on INT4 (blockNumber) → native JSON numbers (matching PostGraphile).
	let min_block: i64 = agg
		.pointer("/min/blockNumber")
		.and_then(|v| v.as_i64())
		.expect("min.blockNumber should be a native JSON integer");
	let max_block: i64 = agg
		.pointer("/max/blockNumber")
		.and_then(|v| v.as_i64())
		.expect("max.blockNumber should be a native JSON integer");
	assert!(
		min_block <= max_block,
		"min.blockNumber ({min_block}) should be <= max.blockNumber ({max_block})"
	);

	println!("blockNumber: sum={sum_val}, min={min_block}, max={max_block}");

	compare_responses("assetTeleporteds(numeric aggregates)", &ts_resp, &rust_resp);
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

// ---------------------------------------------------------------------------
// Single-record queries
// ---------------------------------------------------------------------------

/// Test `{entity}(id: "...")` single-record query — both services.
#[tokio::test]
async fn test_single_record() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleported(id: "0x2c5edd96e3e017d74ccc172437317ac67bbcdbbdfe3afda178a9e3f9546f8ec6") {
                id
                chain
                blockNumber
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   single record: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust single record: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust single record returned errors: {}",
		rust_resp
	);

	// Verify the returned record has the correct id and chain.
	let id = rust_resp.pointer("/data/assetTeleported/id").and_then(|v| v.as_str());
	assert_eq!(
		id,
		Some("0x2c5edd96e3e017d74ccc172437317ac67bbcdbbdfe3afda178a9e3f9546f8ec6"),
		"Rust single record returned wrong id: {}",
		rust_resp
	);
	assert_eq!(
		rust_resp.pointer("/data/assetTeleported/chain").and_then(|v| v.as_str()),
		Some("KUSAMA-4009")
	);

	compare_responses("assetTeleported(id:...)", &ts_resp, &rust_resp);
}

/// Test `{entity}ByNodeId(nodeId: "...")` — compares Rust vs TypeScript.
/// nodeId format now matches PostGraphile: base64(["table_name", _id_uuid]).
/// Fetches the nodeId from TS, then uses it to query both services.
#[tokio::test]
async fn test_by_node_id() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Fetch nodeId from TS (PostGraphile) — both services now encode identically.
	let fetch_query = r#"
        {
            assetTeleported(id: "0x2c5edd96e3e017d74ccc172437317ac67bbcdbbdfe3afda178a9e3f9546f8ec6") {
                nodeId
                id
            }
        }
    "#;

	let ts_fetch = ts_client.query(fetch_query).await;
	let rust_fetch = rust_client.query(fetch_query).await;

	// Both services must return the same nodeId.
	let ts_node_id = ts_fetch
		.pointer("/data/assetTeleported/nodeId")
		.and_then(|v| v.as_str())
		.expect("TS nodeId missing");
	let rust_node_id = rust_fetch
		.pointer("/data/assetTeleported/nodeId")
		.and_then(|v| v.as_str())
		.expect("Rust nodeId missing");
	assert_eq!(ts_node_id, rust_node_id, "nodeId must be identical between TS and Rust");
	println!("nodeId match confirmed: {ts_node_id}");

	// Use the shared nodeId to query both services.
	let by_node_query = format!(
		r#"
        {{
            assetTeleportedByNodeId(nodeId: "{ts_node_id}") {{
                id
                chain
            }}
        }}
    "#
	);

	let ts_resp = ts_client.query(&by_node_query).await;
	let rust_resp = rust_client.query(&by_node_query).await;

	println!("TS   byNodeId: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust byNodeId: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust byNodeId query returned errors: {}",
		rust_resp
	);

	let returned_id = rust_resp
		.pointer("/data/assetTeleportedByNodeId/id")
		.and_then(|v| v.as_str())
		.expect("id missing from Rust byNodeId response");
	assert_eq!(
		returned_id, "0x2c5edd96e3e017d74ccc172437317ac67bbcdbbdfe3afda178a9e3f9546f8ec6",
		"Rust byNodeId returned wrong entity"
	);

	compare_responses("assetTeleportedByNodeId", &ts_resp, &rust_resp);
	println!("byNodeId: both services returned matching entity ✓");
}

/// Test `node(nodeId: "...")` root query — compares Rust vs TypeScript.
/// Fetches the nodeId from TS then sends the same nodeId to both services.
#[tokio::test]
async fn test_node_interface() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Fetch the canonical nodeId from TS.
	let fetch_query = r#"
        {
            assetTeleported(id: "0x2c5edd96e3e017d74ccc172437317ac67bbcdbbdfe3afda178a9e3f9546f8ec6") {
                nodeId
            }
        }
    "#;

	let ts_fetch = ts_client.query(fetch_query).await;
	let node_id = ts_fetch
		.pointer("/data/assetTeleported/nodeId")
		.and_then(|v| v.as_str())
		.expect("TS nodeId missing");

	println!("Using nodeId from TS: {node_id}");

	let node_query = format!(
		r#"
        {{
            node(nodeId: "{node_id}") {{
                nodeId
                ... on AssetTeleported {{
                    id
                    chain
                }}
            }}
        }}
    "#
	);

	let ts_resp = ts_client.query(&node_query).await;
	let rust_resp = rust_client.query(&node_query).await;

	println!("TS   node(): {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust node(): {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust node() query returned errors: {}",
		rust_resp
	);

	let returned_id = rust_resp
		.pointer("/data/node/id")
		.and_then(|v| v.as_str())
		.expect("id missing from Rust node() response");
	assert_eq!(returned_id, "0x2c5edd96e3e017d74ccc172437317ac67bbcdbbdfe3afda178a9e3f9546f8ec6");

	compare_responses("node(nodeId)", &ts_resp, &rust_resp);
	println!("node(nodeId): both services returned matching entity ✓");
}

// ---------------------------------------------------------------------------
// Pagination variants
// ---------------------------------------------------------------------------

/// Test `offset` pagination — skip first 5 rows, return next 5.
#[tokio::test]
async fn test_offset_pagination() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// First 5 rows.
	let page1_query = r#"
        {
            assetTeleporteds(first: 5, orderBy: ID_ASC) {
                nodes { id }
            }
        }
    "#;
	// Next 5 rows via offset.
	let offset_query = r#"
        {
            assetTeleporteds(first: 5, offset: 5, orderBy: ID_ASC) {
                nodes { id }
            }
        }
    "#;

	let _ts_page1 = ts_client.query(page1_query).await;
	let rust_page1 = rust_client.query(page1_query).await;
	let ts_offset = ts_client.query(offset_query).await;
	let rust_offset = rust_client.query(offset_query).await;

	println!("TS   offset: {}", serde_json::to_string_pretty(&ts_offset).unwrap());
	println!("Rust offset: {}", serde_json::to_string_pretty(&rust_offset).unwrap());

	assert!(
		rust_offset
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust offset query returned errors: {}",
		rust_offset
	);

	// The two pages must not overlap in Rust.
	let page1_ids: HashSet<String> = rust_page1
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.map(|nodes| nodes.iter().filter_map(|n| n["id"].as_str().map(String::from)).collect())
		.unwrap_or_default();
	let offset_ids: HashSet<String> = rust_offset
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.map(|nodes| nodes.iter().filter_map(|n| n["id"].as_str().map(String::from)).collect())
		.unwrap_or_default();

	let overlap: HashSet<&String> = page1_ids.intersection(&offset_ids).collect();
	assert!(overlap.is_empty(), "offset pages overlap: {overlap:?}");
	assert!(!offset_ids.is_empty(), "offset returned 0 rows");

	// Compare TS and Rust offset results.
	compare_responses("assetTeleporteds(offset:5)", &ts_offset, &rust_offset);
	println!(
		"offset: page1={} ids, offset={} ids, no overlap ✓",
		page1_ids.len(),
		offset_ids.len()
	);
}

/// Test `last` backward pagination — return the last 3 rows.
#[tokio::test]
async fn test_last_pagination() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Fetch last 3 rows — should not overlap with first 3 rows.
	let first_query = r#"
        {
            assetTeleporteds(first: 3, orderBy: ID_ASC) {
                nodes { id }
            }
        }
    "#;
	let last_query = r#"
        {
            assetTeleporteds(last: 3, orderBy: ID_ASC) {
                nodes { id }
                pageInfo { hasPreviousPage }
            }
        }
    "#;

	let rust_first = rust_client.query(first_query).await;
	let ts_last = ts_client.query(last_query).await;
	let rust_last = rust_client.query(last_query).await;

	println!("TS   last:3 = {}", serde_json::to_string_pretty(&ts_last).unwrap());
	println!("Rust last:3 = {}", serde_json::to_string_pretty(&rust_last).unwrap());

	assert!(
		rust_last
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust `last` query returned errors: {}",
		rust_last
	);

	// Should return 3 rows.
	let rust_nodes = rust_last
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.expect("nodes missing from Rust last response");
	assert_eq!(rust_nodes.len(), 3, "Rust last:3 should return 3 nodes, got {}", rust_nodes.len());

	// `last` rows and `first` rows should not overlap (20 total rows).
	let first_ids: HashSet<String> = rust_first
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.map(|nodes| nodes.iter().filter_map(|n| n["id"].as_str().map(String::from)).collect())
		.unwrap_or_default();
	let last_ids: HashSet<String> =
		rust_nodes.iter().filter_map(|n| n["id"].as_str().map(String::from)).collect();

	let overlap: HashSet<&String> = first_ids.intersection(&last_ids).collect();
	assert!(overlap.is_empty(), "first:3 and last:3 overlap: {overlap:?}");

	// hasPreviousPage must be true when there are more rows before.
	let has_prev = rust_last
		.pointer("/data/assetTeleporteds/pageInfo/hasPreviousPage")
		.and_then(|v| v.as_bool())
		.unwrap_or(false);
	assert!(has_prev, "hasPreviousPage should be true for last:3 of 20 rows");

	compare_responses("assetTeleporteds(last:3)", &ts_last, &rust_last);
	println!("last pagination: 3 rows, no overlap with first:3, hasPreviousPage=true ✓");
}

/// Test multi-column `orderBy` — `[BLOCK_NUMBER_ASC, ID_ASC]`.
#[tokio::test]
async fn test_orderby_multi_column() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleporteds(first: 10, orderBy: [BLOCK_NUMBER_ASC, ID_ASC]) {
                nodes { id blockNumber }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   multi orderBy: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust multi orderBy: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust multi-column orderBy returned errors: {}",
		rust_resp
	);

	// Verify primary sort (block_number ascending).
	if let Some(nodes) =
		rust_resp.pointer("/data/assetTeleporteds/nodes").and_then(|v| v.as_array())
	{
		let bns: Vec<i64> = nodes.iter().filter_map(|n| n["blockNumber"].as_i64()).collect();
		let mut sorted = bns.clone();
		sorted.sort();
		assert_eq!(bns, sorted, "Rust multi-column orderBy: blockNumbers not ascending: {bns:?}");
	}

	compare_responses("assetTeleporteds([BLOCK_NUMBER_ASC, ID_ASC])", &ts_resp, &rust_resp);
}

// ---------------------------------------------------------------------------
// Filter operator coverage
// ---------------------------------------------------------------------------

/// Test `notEqualTo` filter — chain != "POLKADOT" matches all 20 rows.
#[tokio::test]
async fn test_filter_not_equal() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleporteds(
                first: 100,
                orderBy: ID_ASC,
                filter: { chain: { notEqualTo: "POLKADOT" } }
            ) {
                totalCount
                nodes { id chain }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	let count = rust_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing");
	assert!(count >= 20, "notEqualTo 'POLKADOT' should match all 20 rows, got {count}");

	compare_responses("assetTeleporteds(notEqualTo)", &ts_resp, &rust_resp);
}

/// Test `notIn` filter — exclude 3 known ids, expect 17 remaining.
#[tokio::test]
async fn test_filter_not_in() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleporteds(
                first: 100,
                orderBy: ID_ASC,
                filter: {
                    id: {
                        notIn: [
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

	let count = rust_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from Rust notIn response");

	// The total count without the filter — fetch separately to verify exclusion worked.
	let total_query = r#"{ assetTeleporteds(first: 1) { totalCount } }"#;
	let total_resp = rust_client.query(total_query).await;
	let total_rows = total_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.unwrap_or(0);
	assert_eq!(
		count,
		total_rows - 3,
		"notIn 3 ids should reduce count by 3, got total={total_rows} filtered={count}"
	);

	// None of the excluded IDs should appear in the results.
	let excluded: HashSet<&str> = [
		"0x2c5edd96e3e017d74ccc172437317ac67bbcdbbdfe3afda178a9e3f9546f8ec6",
		"0x5e11feb69bef8bec523afc3c1fe0297c0dff5794422eefb2c11f1ba78efdb3d4",
		"0x4e54a431cdb82239331deda9feb93f5a101f50756e62387c0532143dad02d1f7",
	]
	.into_iter()
	.collect();
	if let Some(nodes) =
		rust_resp.pointer("/data/assetTeleporteds/nodes").and_then(|v| v.as_array())
	{
		for node in nodes {
			let id = node["id"].as_str().unwrap_or("");
			assert!(!excluded.contains(id), "notIn: excluded id {id} appeared in results");
		}
	}

	compare_responses("assetTeleporteds(notIn)", &ts_resp, &rust_resp);
}

/// Test `includes` string filter — `id: { includes: "2c5edd" }` matches exactly 1 row.
/// Uses the PostGraphile operator name (`includes`).
#[tokio::test]
async fn test_filter_contains() {
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
                filter: { id: { includes: "2c5edd" } }
            ) {
                totalCount
                nodes { id }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	let count = rust_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from Rust includes response");
	assert_eq!(count, 1, "includes '2c5edd' should match 1 row, got {count}");
	compare_responses("assetTeleporteds(includes)", &ts_resp, &rust_resp);
	println!("includes filter: matched {count} row ✓");
}

/// Test `endsWith` string filter — `id: { endsWith: "8ec6" }` matches exactly 1 row.
#[tokio::test]
async fn test_filter_ends_with() {
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
                filter: { id: { endsWith: "8ec6" } }
            ) {
                totalCount
                nodes { id }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	let count = rust_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from Rust endsWith response");
	assert_eq!(count, 1, "endsWith '8ec6' should match 1 row, got {count}");
	assert_eq!(
		rust_resp.pointer("/data/assetTeleporteds/nodes/0/id").and_then(|v| v.as_str()),
		Some("0x2c5edd96e3e017d74ccc172437317ac67bbcdbbdfe3afda178a9e3f9546f8ec6")
	);

	compare_responses("assetTeleporteds(endsWith)", &ts_resp, &rust_resp);
}

/// Test `likeInsensitive` case-insensitive LIKE filter.
/// Uses the PostGraphile operator name (`likeInsensitive`).
#[tokio::test]
async fn test_filter_ilike() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleporteds(
                first: 100,
                orderBy: ID_ASC,
                filter: { chain: { likeInsensitive: "kusama%" } }
            ) {
                totalCount
                nodes { id }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	let count = rust_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from Rust likeInsensitive response");
	assert!(count > 0, "likeInsensitive 'kusama%' should match at least 1 row, got {count}");
	compare_responses("assetTeleporteds(likeInsensitive)", &ts_resp, &rust_resp);
	println!("likeInsensitive filter: matched {count} rows ✓");
}

/// Test `greaterThanOrEqualTo` and `lessThanOrEqualTo` range filters.
/// block_number range in fixture: 2154921–2157150.
/// Filter: 2154921 <= block_number <= 2157150 → all 20 rows.
/// Filter: block_number <= 2154921 → exactly 1 row (the minimum).
#[tokio::test]
async fn test_filter_range() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// All rows within the full range.
	let query_full = r#"
        {
            assetTeleporteds(
                first: 100,
                filter: {
                    blockNumber: {
                        greaterThanOrEqualTo: 2154921,
                        lessThanOrEqualTo: 2157150
                    }
                }
            ) {
                totalCount
            }
        }
    "#;

	let ts_full = ts_client.query(query_full).await;
	let rust_full = rust_client.query(query_full).await;

	let count_full = rust_full
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing");
	assert!(count_full >= 20, "full range filter should match all 20 rows, got {count_full}");
	compare_responses("assetTeleporteds(range:full)", &ts_full, &rust_full);

	// Only the minimum block_number row.
	let query_min = r#"
        {
            assetTeleporteds(
                first: 10,
                orderBy: ID_ASC,
                filter: { blockNumber: { lessThanOrEqualTo: 2154921 } }
            ) {
                totalCount
                nodes { id blockNumber }
            }
        }
    "#;

	let ts_min = ts_client.query(query_min).await;
	let rust_min = rust_client.query(query_min).await;

	let count_min = rust_min
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from min filter");
	assert_eq!(count_min, 1, "lessThanOrEqualTo 2154921 should match 1 row, got {count_min}");

	compare_responses("assetTeleporteds(range:min)", &ts_min, &rust_min);
}

/// Test logical `not` filter — `not: { chain: { equalTo: "POLKADOT" } }` matches all 20 rows.
#[tokio::test]
async fn test_filter_not() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleporteds(
                first: 100,
                orderBy: ID_ASC,
                filter: { not: { chain: { equalTo: "POLKADOT" } } }
            ) {
                totalCount
                nodes { id }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	let count = rust_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from Rust not-filter response");
	assert!(count >= 20, "`not` filter should match all 20 rows (none are POLKADOT), got {count}");

	compare_responses("assetTeleporteds(not)", &ts_resp, &rust_resp);
}

// ---------------------------------------------------------------------------
// Metadata queries
// ---------------------------------------------------------------------------

/// Test `_metadatas` query — returns all chain metadata records.
/// Test `_metadatas` query — compares Rust vs TypeScript.
/// Both services support `nodes`; Rust additionally exposes `edges` but TS does not.
#[tokio::test]
async fn test_metadatas() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Use `nodes` — supported by both services.
	let query = r#"
        {
            _metadatas {
                nodes {
                    chain
                    lastProcessedHeight
                }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   _metadatas: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust _metadatas: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust _metadatas returned errors: {}",
		rust_resp
	);

	// At least one node must be present.
	let nodes = rust_resp
		.pointer("/data/_metadatas/nodes")
		.and_then(|v| v.as_array())
		.expect("_metadatas nodes missing from Rust response");
	assert!(!nodes.is_empty(), "_metadatas returned no nodes");

	// At least one node must have a non-null chain field.
	// (Some metadata tables may be partially populated and lack a chain entry.)
	let with_chain = nodes
		.iter()
		.filter(|n| n.get("chain").and_then(|v| v.as_str()).is_some())
		.count();
	assert!(with_chain > 0, "_metadatas: no node has a chain field; nodes: {nodes:?}");

	println!("_metadatas: {with_chain}/{} chains found ✓", nodes.len());

	compare_responses("_metadatas", &ts_resp, &rust_resp);
}

// ---------------------------------------------------------------------------
// Aggregate coverage
// ---------------------------------------------------------------------------

/// Test stddev and variance aggregates — compares Rust vs TypeScript with float tolerance.
///
/// Both services return BigFloat strings, but the final digit can differ by 1 ULP because
/// PostgreSQL numeric arithmetic and JavaScript IEEE 754 doubles take slightly different
/// rounding paths (e.g. `...0761` vs `...0762`).  We parse both sides as f64 and accept
/// values that agree to within a relative tolerance of 1e-9.
#[tokio::test]
async fn test_stddev_variance_aggregates() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleporteds(first: 100) {
                aggregates {
                    stddevSample { blockNumber }
                    stddevPopulation { blockNumber }
                    varianceSample { blockNumber }
                    variancePopulation { blockNumber }
                }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   stddev/variance: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust stddev/variance: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"stddev/variance query returned errors: {}",
		rust_resp
	);

	let rust_agg = rust_resp
		.pointer("/data/assetTeleporteds/aggregates")
		.expect("Rust aggregates field missing");
	let ts_agg = ts_resp
		.pointer("/data/assetTeleporteds/aggregates")
		.expect("TS aggregates field missing");

	// Relative tolerance: 1e-9 comfortably covers the 1-ULP last-digit divergence between
	// PostgreSQL numeric and JavaScript Number while catching any real discrepancies.
	const REL_TOL: f64 = 1e-9;

	for path in &[
		"/stddevSample/blockNumber",
		"/stddevPopulation/blockNumber",
		"/varianceSample/blockNumber",
		"/variancePopulation/blockNumber",
	] {
		let rust_str = rust_agg
			.pointer(path)
			.and_then(|v| v.as_str())
			.unwrap_or_else(|| panic!("Rust aggregate {path} missing or not a string"));
		let ts_str = ts_agg
			.pointer(path)
			.and_then(|v| v.as_str())
			.unwrap_or_else(|| panic!("TS aggregate {path} missing or not a string"));

		let rust_val: f64 = rust_str
			.parse()
			.unwrap_or_else(|_| panic!("Rust {path} = '{rust_str}' should parse as f64"));
		let ts_val: f64 = ts_str
			.parse()
			.unwrap_or_else(|_| panic!("TS {path} = '{ts_str}' should parse as f64"));

		assert!(rust_val >= 0.0, "Rust {path} = {rust_val} should be non-negative");

		// Relative error check.
		let rel_err = if ts_val == 0.0 {
			(rust_val - ts_val).abs()
		} else {
			((rust_val - ts_val) / ts_val).abs()
		};
		assert!(
			rel_err <= REL_TOL,
			"{path}: Rust={rust_str} TS={ts_str} relative_error={rel_err:.2e} > tolerance {REL_TOL:.2e}"
		);

		println!("{path}: Rust={rust_str} TS={ts_str} rel_err={rel_err:.2e} ✓");
	}

	println!("stddev/variance aggregates: all fields within tolerance ✓");
}

// ---------------------------------------------------------------------------
// Type serialisation
// ---------------------------------------------------------------------------

/// Verify that BigInt (int8) fields are serialised as JSON strings, not numbers.
/// Uses `aggregates { distinctCount { id } }` which is BigInt on both Rust and PostGraphile.
#[tokio::test]
async fn test_bigint_serialization() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// distinctCount returns BigInt strings on both Rust and PostGraphile.
	let query = r#"
        {
            assetTeleporteds(first: 100) {
                aggregates { distinctCount { id } }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   BigInt: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust BigInt: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"BigInt query returned errors: {}",
		rust_resp
	);

	let dc_id = rust_resp
		.pointer("/data/assetTeleporteds/aggregates/distinctCount/id")
		.expect("aggregates.distinctCount.id missing");

	// BigInt must be a JSON string, not a JSON number, to avoid 64-bit precision loss.
	assert!(
		dc_id.is_string(),
		"aggregates.distinctCount.id (BigInt) should be a JSON string, got: {dc_id:?}"
	);

	let parsed: i64 = dc_id
		.as_str()
		.unwrap()
		.parse()
		.unwrap_or_else(|e| panic!("distinctCount.id should parse as i64: {e}"));
	assert!(parsed > 0, "distinctCount.id should be positive, got {parsed}");

	println!("BigInt serialization: distinctCount.id='{dc_id}' correctly serialised as string ✓");

	compare_responses("assetTeleporteds(bigint serialization)", &ts_resp, &rust_resp);
}

/// Verify that BigFloat (numeric) aggregate fields are serialised as JSON strings.
/// Compares Rust vs TypeScript — both should return matching string values.
#[tokio::test]
async fn test_bigfloat_serialization() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// aggregates.sum.blockNumber is a BigFloat string (SUM of INT4 upcasted to numeric).
	let query = r#"
        {
            assetTeleporteds(first: 100) {
                aggregates {
                    sum { blockNumber }
                    average { blockNumber }
                }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   BigFloat: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust BigFloat: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"BigFloat query returned errors: {}",
		rust_resp
	);

	let sum_bn = rust_resp
		.pointer("/data/assetTeleporteds/aggregates/sum/blockNumber")
		.expect("sum.blockNumber missing");

	// BigFloat must be a JSON string.
	assert!(
		sum_bn.is_string(),
		"sum.blockNumber (BigFloat) should be a JSON string, got: {sum_bn:?}"
	);

	let parsed: f64 = sum_bn
		.as_str()
		.unwrap()
		.parse()
		.unwrap_or_else(|e| panic!("sum.blockNumber should parse as f64: {e}"));
	assert!(parsed > 0.0, "sum.blockNumber should be positive (rows exist), got {parsed}");

	println!("BigFloat serialization: sum.blockNumber='{sum_bn}' correctly serialised as string ✓");

	compare_responses("assetTeleporteds(bigfloat serialization)", &ts_resp, &rust_resp);
}

/// Test that enum fields on `orders` return valid enum values.
/// The `status` column is a PostgreSQL enum with values: PLACED, FILLED, REDEEMED, REFUNDED.
#[tokio::test]
async fn test_enum_field() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            orders(first: 20, orderBy: ID_ASC) {
                totalCount
                nodes { id status }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   enum field: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust enum field: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust enum field query returned errors: {}",
		rust_resp
	);

	let valid_statuses: HashSet<&str> = ["PLACED", "FILLED", "REDEEMED", "REFUNDED"].into();
	if let Some(nodes) = rust_resp.pointer("/data/orders/nodes").and_then(|v| v.as_array()) {
		assert!(!nodes.is_empty(), "No order rows returned");
		for node in nodes {
			if let Some(status) = node["status"].as_str() {
				assert!(
					valid_statuses.contains(status),
					"status '{status}' is not a valid enum value; valid: {valid_statuses:?}"
				);
			}
		}
	}

	// Don't use compare_responses — TS and Rust may return enum values in different formats.
	// Just verify Rust returns valid values.
	println!("enum field: all status values are valid ✓");
}

// ---------------------------------------------------------------------------
// orderByNull
// ---------------------------------------------------------------------------

/// Test `orderByNull: NULLS_LAST` and `NULLS_FIRST` — compares Rust vs TypeScript.
///
/// PostGraphile's PgOrderByUnique plugin adds an `orderByNull` argument that appends
/// `NULLS FIRST` / `NULLS LAST` to each ORDER BY column.  With no null values in the
/// fixture the result set is identical regardless of null ordering; the test verifies:
///   1. Both services accept the argument without returning errors.
///   2. Both services return identical responses (TS+Rust comparison).
#[tokio::test]
async fn test_order_by_null() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// NULLS_LAST — nulls appear after all non-null values.
	let query_nulls_last = r#"
        {
            assetTeleporteds(
                first: 10,
                orderBy: BLOCK_NUMBER_ASC,
                orderByNull: NULLS_LAST
            ) {
                nodes { id blockNumber }
            }
        }
    "#;

	let ts_last = ts_client.query(query_nulls_last).await;
	let rust_last = rust_client.query(query_nulls_last).await;

	println!("TS   NULLS_LAST: {}", serde_json::to_string_pretty(&ts_last).unwrap());
	println!("Rust NULLS_LAST: {}", serde_json::to_string_pretty(&rust_last).unwrap());

	assert!(
		rust_last
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust NULLS_LAST returned errors: {}",
		rust_last
	);
	assert!(
		ts_last
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"TS NULLS_LAST returned errors: {}",
		ts_last
	);

	compare_responses("assetTeleporteds(orderByNull:NULLS_LAST)", &ts_last, &rust_last);

	// NULLS_FIRST — nulls appear before all non-null values.
	let query_nulls_first = r#"
        {
            assetTeleporteds(
                first: 10,
                orderBy: BLOCK_NUMBER_ASC,
                orderByNull: NULLS_FIRST
            ) {
                nodes { id blockNumber }
            }
        }
    "#;

	let ts_first = ts_client.query(query_nulls_first).await;
	let rust_first = rust_client.query(query_nulls_first).await;

	println!("TS   NULLS_FIRST: {}", serde_json::to_string_pretty(&ts_first).unwrap());
	println!("Rust NULLS_FIRST: {}", serde_json::to_string_pretty(&rust_first).unwrap());

	assert!(
		rust_first
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust NULLS_FIRST returned errors: {}",
		rust_first
	);
	assert!(
		ts_first
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"TS NULLS_FIRST returned errors: {}",
		ts_first
	);

	compare_responses("assetTeleporteds(orderByNull:NULLS_FIRST)", &ts_first, &rust_first);

	println!("orderByNull: NULLS_LAST and NULLS_FIRST both accepted and matched ✓");
}

// ---------------------------------------------------------------------------
// Query limit clamping
// ---------------------------------------------------------------------------

/// Test query limit behaviour — verifies `first` arg is correctly applied
/// and that requesting more rows than `query_limit` is clamped.
///
/// The CI fixture has 20 rows per entity.  With `query_limit=100`:
///   - `first: 5`   → exactly 5 rows (unchanged, below limit)
///   - `first: 200` → clamped to 100 (the default query-limit)
///   - no `first`   → defaults to 100
///
/// This test is `#[ignore]`d because it requires the Rust service to be running
/// **without** `--unsafe-mode`.  CI runs it in a separate step after restarting
/// omnihedron without that flag.
#[tokio::test]
#[ignore]
async fn test_query_limit() {
	let rust_url =
		std::env::var("RUST_SERVICE_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
	let rust_client = TestClient::new(&rust_url);

	// Verify the service is reachable
	let health = reqwest::get(format!("{rust_url}/health")).await;
	if health.is_err() || !health.unwrap().status().is_success() {
		eprintln!("SKIP: Rust service not available at {rust_url}");
		return;
	}

	// ── first: 5 — must return exactly 5 rows ──────────────────────────────
	let query_5 = r#"
        {
            assetTeleporteds(first: 5, orderBy: ID_ASC) {
                nodes { id }
            }
        }
    "#;

	let rust_5 = rust_client.query(query_5).await;
	let rust_count_5 = rust_5
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.map(|a| a.len())
		.unwrap_or_else(|| panic!("nodes missing from rust first:5 response: {rust_5}"));
	assert_eq!(rust_count_5, 5, "first:5 should return exactly 5 rows");

	// ── first: 200 — must be clamped to query_limit (100) ───────────────────
	let query_200 = r#"
        {
            assetTeleporteds(first: 200, orderBy: ID_ASC) {
                nodes { id }
            }
        }
    "#;

	let rust_200 = rust_client.query(query_200).await;
	assert!(
		rust_200
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust first:200 returned errors: {}",
		rust_200
	);

	let rust_count_200 = rust_200
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.map(|a| a.len())
		.unwrap_or_else(|| panic!("nodes missing from rust first:200 response: {rust_200}"));
	assert!(rust_count_200 <= 100, "first:200 should be clamped to 100, got {rust_count_200}");

	// ── no first arg — defaults to query_limit (100) ────────────────────────
	let query_unbounded = r#"
        {
            assetTeleporteds(orderBy: ID_ASC) {
                nodes { id }
            }
        }
    "#;

	let rust_unbounded = rust_client.query(query_unbounded).await;
	assert!(
		rust_unbounded
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust unbounded query returned errors: {}",
		rust_unbounded
	);

	let rust_count_unbounded = rust_unbounded
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.map(|a| a.len())
		.unwrap_or_else(|| panic!("nodes missing from rust unbounded response: {rust_unbounded}"));
	assert!(
		rust_count_unbounded <= 100,
		"unbounded query should default to 100, got {rust_count_unbounded}"
	);

	println!(
		"query_limit: first:5={rust_count_5}, first:200={rust_count_200}, \
         unbounded={rust_count_unbounded} (all ≤100) ✓"
	);
}

// ---------------------------------------------------------------------------
// groupedAggregates
// ---------------------------------------------------------------------------

/// Test `groupedAggregates` — compares Rust vs TypeScript.
///
/// Two cases:
///   1. `groupBy: []` (empty) — single group aggregating all matching rows.
///   2. `groupBy: [CHAIN]`    — one group per distinct chain value. All 20 fixture rows share
///      chain="KUSAMA-4009" so this produces 1 group.
///
/// `average` is excluded from the TS+Rust comparison because PostgreSQL numeric
/// arithmetic and JavaScript IEEE 754 can diverge in the last decimal digit
/// (same root cause as test_stddev_variance_aggregates).
#[tokio::test]
async fn test_grouped_aggregates() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// ── groupBy: [] — aggregate all rows into a single group ──────────────
	let query_empty = r#"
        {
            assetTeleporteds(first: 100) {
                groupedAggregates(groupBy: []) {
                    sum { blockNumber }
                    min { blockNumber }
                    max { blockNumber }
                    distinctCount { id }
                }
            }
        }
    "#;

	let ts_empty = ts_client.query(query_empty).await;
	let rust_empty = rust_client.query(query_empty).await;

	println!("TS   groupedAggregates([]): {}", serde_json::to_string_pretty(&ts_empty).unwrap());
	println!("Rust groupedAggregates([]): {}", serde_json::to_string_pretty(&rust_empty).unwrap());

	assert!(
		rust_empty
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"groupBy:[] returned errors: {}",
		rust_empty
	);

	// Must return exactly 1 group (no grouping = one aggregate over all rows).
	let groups = rust_empty
		.pointer("/data/assetTeleporteds/groupedAggregates")
		.and_then(|v| v.as_array())
		.expect("groupedAggregates array missing from Rust response");
	assert_eq!(groups.len(), 1, "groupBy:[] must return exactly 1 group, got {}", groups.len());

	// sum.blockNumber → BigFloat string (SUM of INT4 → numeric → text).
	let sum_bn = groups[0].pointer("/sum/blockNumber").expect("sum.blockNumber missing");
	assert!(
		sum_bn.is_string(),
		"sum.blockNumber (BigFloat) should be a JSON string, got: {sum_bn:?}"
	);
	sum_bn
		.as_str()
		.unwrap()
		.parse::<f64>()
		.unwrap_or_else(|_| panic!("sum.blockNumber should parse as f64, got: {sum_bn}"));

	// min.blockNumber → native Int (INT4 min preserves source type).
	let min_bn = groups[0].pointer("/min/blockNumber").expect("min.blockNumber missing");
	assert!(
		min_bn.is_number(),
		"min.blockNumber (INT4) should be a native JSON number, got: {min_bn:?}"
	);

	// distinctCount.id → BigInt string.
	let dc_id = groups[0].pointer("/distinctCount/id").expect("distinctCount.id missing");
	assert!(dc_id.is_string(), "distinctCount.id (BigInt) should be a JSON string, got: {dc_id:?}");

	compare_responses("assetTeleporteds(groupedAggregates groupBy:[])", &ts_empty, &rust_empty);

	// ── groupBy: [CHAIN] — one group per distinct chain value ─────────────
	// All 20 rows share chain="KUSAMA-4009" → exactly 1 group.
	//
	// Note: PostGraphile's pg-aggregates returns `keys: [String!]` (raw values),
	// while omnihedron returns `keys: AssetTeleported` (entity object with named
	// fields). The `keys { chain }` sub-selection is only valid on the Rust service.
	// We use a separate TS query without `keys` for the cross-service comparison.
	let query_chain_rust = r#"
        {
            assetTeleporteds(first: 100) {
                groupedAggregates(groupBy: [CHAIN]) {
                    keys { chain }
                    sum { blockNumber }
                    min { blockNumber }
                    max { blockNumber }
                    distinctCount { id }
                }
            }
        }
    "#;
	let query_chain_ts = r#"
        {
            assetTeleporteds(first: 100) {
                groupedAggregates(groupBy: [CHAIN]) {
                    sum { blockNumber }
                    min { blockNumber }
                    max { blockNumber }
                    distinctCount { id }
                }
            }
        }
    "#;

	let ts_chain = ts_client.query(query_chain_ts).await;
	let rust_chain = rust_client.query(query_chain_rust).await;

	println!(
		"TS   groupedAggregates([CHAIN]): {}",
		serde_json::to_string_pretty(&ts_chain).unwrap()
	);
	println!(
		"Rust groupedAggregates([CHAIN]): {}",
		serde_json::to_string_pretty(&rust_chain).unwrap()
	);

	assert!(
		rust_chain
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"groupBy:[CHAIN] returned errors: {}",
		rust_chain
	);

	let chain_groups = rust_chain
		.pointer("/data/assetTeleporteds/groupedAggregates")
		.and_then(|v| v.as_array())
		.expect("groupedAggregates[CHAIN] array missing from Rust response");
	assert!(!chain_groups.is_empty(), "groupBy:[CHAIN] must return at least 1 group");

	// The 20 fixture rows all have chain="KUSAMA-4009" → exactly 1 group.
	let chain_key = chain_groups[0]
		.pointer("/keys/chain")
		.and_then(|v| v.as_str())
		.expect("groupedAggregates[CHAIN] keys.chain missing");
	assert_eq!(
		chain_key, "KUSAMA-4009",
		"groupBy:[CHAIN] group key should be KUSAMA-4009, got: {chain_key}"
	);

	// Compare aggregate values (excluding keys which differ in schema between TS and Rust).
	// Build TS-comparable Rust response by stripping the keys field.
	let mut rust_chain_stripped = rust_chain.clone();
	if let Some(groups) = rust_chain_stripped
		.pointer_mut("/data/assetTeleporteds/groupedAggregates")
		.and_then(|v| v.as_array_mut())
	{
		for g in groups.iter_mut() {
			if let Some(obj) = g.as_object_mut() {
				obj.remove("keys");
			}
		}
	}
	compare_responses(
		"assetTeleporteds(groupedAggregates groupBy:[CHAIN])",
		&ts_chain,
		&rust_chain_stripped,
	);

	println!(
		"groupedAggregates: groupBy:[] → {} group(s), groupBy:[CHAIN] → {} group(s) ✓",
		groups.len(),
		chain_groups.len()
	);
}

// ---------------------------------------------------------------------------
// _metadata null-field coercion
// ---------------------------------------------------------------------------

/// Test that `_metadata` gracefully returns `null` for fields absent from the DB.
///
/// The `_metadata` table is a key-value store.  Fields like `latestSyncedPoiHeight`
/// and `lastFinalizedVerifiedHeight` are often not written by the indexer; both
/// services should return `null` for them without erroring.  Compares Rust vs TypeScript.
#[tokio::test]
async fn test_metadata_null_fields() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Query a broad set of fields.  Some (latestSyncedPoiHeight, lastFinalizedVerifiedHeight,
	// startHeight) are very likely absent from the fixture → both services must return null.
	let query = r#"
        {
            _metadata(chainId: "11155111") {
                lastProcessedHeight
                chain
                specName
                indexerHealthy
                dynamicDatasources
                deployments
                latestSyncedPoiHeight
                lastFinalizedVerifiedHeight
                startHeight
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   _metadata(null fields): {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust _metadata(null fields): {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust _metadata returned errors: {}",
		rust_resp
	);
	assert!(
		ts_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"TS _metadata returned errors: {}",
		ts_resp
	);

	// Fields absent from the DB must coerce to null on the Rust side.
	// `latestSyncedPoiHeight` is not written by test indexers and is reliably null.
	let meta = rust_resp.pointer("/data/_metadata").expect("_metadata missing from Rust");
	let val = meta.get("latestSyncedPoiHeight");
	let is_null = val.map(|v| v.is_null()).unwrap_or(true);
	assert!(
		is_null,
		"_metadata.latestSyncedPoiHeight should be null when absent from DB; got: {val:?}"
	);

	compare_responses("_metadata(null fields)", &ts_resp, &rust_resp);
	println!("_metadata null fields: absent keys correctly coerce to null ✓");
}

// ---------------------------------------------------------------------------
// Historical table filtering (multiple tables + blockHeight)
// ---------------------------------------------------------------------------

/// Test historical queries on `getRequests` — a second historical table with
/// multiple entity versions at different block heights.
///
/// `get_requests` rows have status SOURCE → HYPERBRIDGE_DELIVERED → DESTINATION
/// across successive `_block_range` intervals.  Querying at specific blockHeights
/// returns the correct version at that point in time.
#[tokio::test]
async fn test_historical_get_requests() {
	// Rust-only: the TS service does not support the `blockHeight` argument.
	let rust_client = TestClient::new(&rust_url());
	let rust_available = std::process::Command::new("curl")
		.args(["-sf", "--max-time", "3", &format!("{}/health", rust_url())])
		.output()
		.map(|o| o.status.success())
		.unwrap_or(false);
	if !rust_available {
		eprintln!("SKIP: Rust service not available.");
		return;
	}

	// ── Latest versions (no upper bound on _block_range) ─────────────────
	// blockHeight so large every open-ended range covers it.
	let query_latest = r#"
        {
            getRequests(
                first: 20,
                orderBy: ID_ASC,
                blockHeight: "99999999999999"
            ) {
                totalCount
                nodes { id status }
            }
        }
    "#;

	let resp_latest = rust_client.query(query_latest).await;

	println!("getRequests(latest): {}", serde_json::to_string_pretty(&resp_latest).unwrap());

	assert!(
		resp_latest
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"getRequests(latest) returned errors: {}",
		resp_latest
	);

	let latest_count = resp_latest
		.pointer("/data/getRequests/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from getRequests(latest)");
	assert!(latest_count > 0, "getRequests(latest) should return at least 1 row");

	// Verify the specific test entity's latest status is DESTINATION.
	// (The full DB may have other entities in different statuses, so we only
	// check the entity we track across blockHeight intervals below.)
	let target_id = "0xf6ee5c001d5275afea10a2ce2927b1fb5defaed2eb13a4bdc2a3c2447d8ab458";
	if let Some(nodes) = resp_latest.pointer("/data/getRequests/nodes").and_then(|v| v.as_array()) {
		for node in nodes {
			if node["id"].as_str() == Some(target_id) {
				let status = node["status"].as_str().unwrap_or("<missing>");
				assert_eq!(
					status, "DESTINATION",
					"Latest version of tracked getRequest should have status DESTINATION, got \
                     {status}"
				);
			}
		}
	}

	// ── Historical version at mid-lifecycle blockHeight ───────────────────
	// 1743772850000 falls in the second interval [1743772818000, 1743772918000)
	// for entity 0xf6ee5c... → status should be HYPERBRIDGE_DELIVERED.
	let query_mid = r#"
        {
            getRequests(
                first: 5,
                filter: { id: { equalTo: "0xf6ee5c001d5275afea10a2ce2927b1fb5defaed2eb13a4bdc2a3c2447d8ab458" } },
                blockHeight: "1743772850000"
            ) {
                totalCount
                nodes { id status }
            }
        }
    "#;

	let resp_mid = rust_client.query(query_mid).await;

	println!("getRequests(mid): {}", serde_json::to_string_pretty(&resp_mid).unwrap());

	assert!(
		resp_mid
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"getRequests(mid blockHeight) returned errors: {}",
		resp_mid
	);

	let mid_count = resp_mid
		.pointer("/data/getRequests/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from getRequests(mid)");
	assert_eq!(
		mid_count, 1,
		"At blockHeight 1743772850000 there should be exactly 1 version of this entity"
	);

	let mid_status = resp_mid
		.pointer("/data/getRequests/nodes/0/status")
		.and_then(|v| v.as_str())
		.expect("status missing from mid-lifecycle row");
	assert_eq!(
		mid_status, "HYPERBRIDGE_DELIVERED",
		"At blockHeight 1743772850000 entity status should be HYPERBRIDGE_DELIVERED, got \
         {mid_status}"
	);

	// ── Early version at first interval ───────────────────────────────────
	// 1743772750000 falls in [1743772738000, 1743772818000) → status SOURCE.
	let query_early = r#"
        {
            getRequests(
                first: 5,
                filter: { id: { equalTo: "0xf6ee5c001d5275afea10a2ce2927b1fb5defaed2eb13a4bdc2a3c2447d8ab458" } },
                blockHeight: "1743772750000"
            ) {
                totalCount
                nodes { id status }
            }
        }
    "#;

	let resp_early = rust_client.query(query_early).await;

	assert!(
		resp_early
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"getRequests(early blockHeight) returned errors: {}",
		resp_early
	);

	let early_status = resp_early
		.pointer("/data/getRequests/nodes/0/status")
		.and_then(|v| v.as_str())
		.expect("status missing from early row");
	assert_eq!(
		early_status, "SOURCE",
		"At blockHeight 1743772750000 entity status should be SOURCE, got {early_status}"
	);

	println!(
		"historical getRequests: latest={latest_count} rows (DESTINATION), \
         mid=HYPERBRIDGE_DELIVERED, early=SOURCE ✓"
	);
}

// ---------------------------------------------------------------------------
// Historical nested relation filtering
// ---------------------------------------------------------------------------

/// Verify that backward relation resolvers respect `_block_range` filtering
/// when a `blockHeight` argument is passed on the root connection query.
///
/// Uses dedicated fixture tables `test_authors` / `test_books`:
///   - book-1 has two historical versions: v1 title = "Book One v1" visible at blocks [100, 500) v2
///     title = "Book One v2" visible at blocks [500, ∞)
///   - book-2 "Book Two" is always visible [0, ∞)
///
/// At blockHeight=200 only book-1 v1 + book-2 should appear.
/// At blockHeight=600 only book-1 v2 + book-2 should appear.
#[tokio::test]
async fn test_historical_nested_relation() {
	let rust_available = std::process::Command::new("curl")
		.args(["-sf", "--max-time", "3", &format!("{}/health", rust_url())])
		.output()
		.map(|o| o.status.success())
		.unwrap_or(false);
	if !rust_available {
		eprintln!("SKIP: test_historical_nested_relation — Rust service not reachable");
		return;
	}

	let rust_client = TestClient::new(&rust_url());

	// ── blockHeight 200 → book-1 v1 is active ────────────────────────────
	let res_200 = rust_client
		.query(
			r#"{
			testAuthors(blockHeight: "200") {
				nodes {
					id
					testBooksByAuthorId {
						nodes { id title }
					}
				}
			}
		}"#,
		)
		.await;

	assert!(
		res_200
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"testAuthors(blockHeight:200) returned errors: {}",
		res_200
	);

	let authors_200 = res_200
		.pointer("/data/testAuthors/nodes")
		.and_then(|v| v.as_array())
		.expect("testAuthors nodes missing");

	assert!(!authors_200.is_empty(), "expected at least one test author at blockHeight 200");

	let books_200 = authors_200[0]
		.pointer("/testBooksByAuthorId/nodes")
		.and_then(|v| v.as_array())
		.expect("testBooksByAuthorId nodes missing");

	let titles_200: Vec<&str> = books_200.iter().filter_map(|b| b["title"].as_str()).collect();

	assert!(
		titles_200.contains(&"Book One v1"),
		"blockHeight 200: expected 'Book One v1', got {:?}",
		titles_200
	);
	assert!(
		titles_200.contains(&"Book Two"),
		"blockHeight 200: expected 'Book Two', got {:?}",
		titles_200
	);
	assert!(
		!titles_200.contains(&"Book One v2"),
		"blockHeight 200: 'Book One v2' should not be visible, got {:?}",
		titles_200
	);

	// ── blockHeight 600 → book-1 v2 is active ────────────────────────────
	let res_600 = rust_client
		.query(
			r#"{
			testAuthors(blockHeight: "600") {
				nodes {
					id
					testBooksByAuthorId {
						nodes { id title }
					}
				}
			}
		}"#,
		)
		.await;

	assert!(
		res_600
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"testAuthors(blockHeight:600) returned errors: {}",
		res_600
	);

	let authors_600 = res_600
		.pointer("/data/testAuthors/nodes")
		.and_then(|v| v.as_array())
		.expect("testAuthors nodes missing at blockHeight 600");

	assert!(!authors_600.is_empty(), "expected at least one test author at blockHeight 600");

	let books_600 = authors_600[0]
		.pointer("/testBooksByAuthorId/nodes")
		.and_then(|v| v.as_array())
		.expect("testBooksByAuthorId nodes missing at blockHeight 600");

	let titles_600: Vec<&str> = books_600.iter().filter_map(|b| b["title"].as_str()).collect();

	assert!(
		titles_600.contains(&"Book One v2"),
		"blockHeight 600: expected 'Book One v2', got {:?}",
		titles_600
	);
	assert!(
		titles_600.contains(&"Book Two"),
		"blockHeight 600: expected 'Book Two', got {:?}",
		titles_600
	);
	assert!(
		!titles_600.contains(&"Book One v1"),
		"blockHeight 600: 'Book One v1' should not be visible, got {:?}",
		titles_600
	);

	println!("historical nested relation: block 200 → [v1, book2], block 600 → [v2, book2] ✓");
}
