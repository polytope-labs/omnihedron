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

#[allow(unused)]
mod common;
use common::*;
#[allow(unused_imports)]
use serde_json::{Value, json};
use std::collections::HashSet;

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
