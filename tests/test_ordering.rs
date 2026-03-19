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

/// Test ordering by a forward-relation scalar field (pg-order-by-related).
///
/// Orders `testBooks` by the related `testAuthor`'s `name` field via the
/// `creator_id` FK.  The enum value follows the double-underscore pattern:
/// `TEST_AUTHOR_BY_CREATOR_ID__NAME_ASC`.
#[tokio::test]
async fn test_orderby_related_scalar() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Order books by their author's name ascending
	let query = r#"
        {
            testBooks(orderBy: TEST_AUTHOR_BY_CREATOR_ID__NAME_ASC) {
                nodes { id title creatorId }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   orderBy related: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust orderBy related: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	// Verify Rust returns no errors
	assert!(
		rust_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust orderBy related returned errors: {}",
		rust_resp
	);

	// Verify TS returns no errors
	assert!(
		ts_resp
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"TS orderBy related returned errors: {}",
		ts_resp
	);

	// Verify the ordering: Alice's books should come before Bob's
	if let Some(nodes) = rust_resp.pointer("/data/testBooks/nodes").and_then(|v| v.as_array()) {
		let creator_ids: Vec<&str> = nodes.iter().filter_map(|n| n["creatorId"].as_str()).collect();
		// All Alice books should appear before any Bob books
		let first_bob = creator_ids.iter().position(|id| *id == "author-bob");
		let last_alice = creator_ids.iter().rposition(|id| *id == "author-alice");
		if let (Some(bob_pos), Some(alice_pos)) = (first_bob, last_alice) {
			assert!(
				alice_pos < bob_pos,
				"Alice books should come before Bob books when ordering by name ASC, got: {:?}",
				creator_ids
			);
		}
	}

	// Descending order — Bob before Alice
	let query_desc = r#"
        {
            testBooks(orderBy: TEST_AUTHOR_BY_CREATOR_ID__NAME_DESC) {
                nodes { id title creatorId }
            }
        }
    "#;

	let rust_desc = rust_client.query(query_desc).await;

	assert!(
		rust_desc
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust DESC orderBy related returned errors: {}",
		rust_desc
	);

	if let Some(nodes) = rust_desc.pointer("/data/testBooks/nodes").and_then(|v| v.as_array()) {
		let creator_ids: Vec<&str> = nodes.iter().filter_map(|n| n["creatorId"].as_str()).collect();
		let first_alice = creator_ids.iter().position(|id| *id == "author-alice");
		let last_bob = creator_ids.iter().rposition(|id| *id == "author-bob");
		if let (Some(alice_pos), Some(bob_pos)) = (first_alice, last_bob) {
			assert!(
				bob_pos < alice_pos,
				"Bob books should come before Alice books when ordering by name DESC, got: {:?}",
				creator_ids
			);
		}
	}

	println!("orderBy related scalar: forward relation ordering works ✓");
}

#[tokio::test]
async fn test_distinct_with_filter() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{
        assetTeleporteds(distinct: [CHAIN], filter: { chain: { equalTo: "KUSAMA-4009" } }) {
            nodes { id chain }
            totalCount
        }
    }"#;

	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	assert!(
		rust.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"distinct+filter returned errors: {rust}"
	);

	let rust_nodes = rust
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.expect("nodes missing");
	assert_eq!(rust_nodes.len(), 1, "distinct:[CHAIN] + filter should return 1 row");
	assert_eq!(rust_nodes[0]["chain"], "KUSAMA-4009");

	// TS and Rust may pick different rows for DISTINCT ON, compare counts only
	let ts_count = ts
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.map(|a| a.len())
		.unwrap_or(0);
	assert_eq!(ts_count, 1, "TS should also return 1 distinct row");
	println!("distinct+filter: Rust=1, TS=1 distinct row ✓");
}
