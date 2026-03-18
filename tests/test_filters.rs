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

#[tokio::test]
async fn test_enum_filter() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// Use first:20 to avoid hitting multi-version rows (historical table can have
	// duplicate IDs with different _block_range; unrestricted queries may pick
	// different versions between TS and Rust).
	let query = r#"{ orders(first: 20, filter: { status: { isNull: false } }, orderBy: ID_ASC) { nodes { id status } totalCount } }"#;
	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	assert!(
		rust.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"enum filter returned errors: {rust}"
	);

	compare_responses("enum filter isNull", &ts, &rust);
	let total = rust.pointer("/data/orders/totalCount").and_then(|v| v.as_i64()).unwrap_or(0);
	println!("enum filter: {total} orders ✓");
}
