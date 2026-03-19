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

//! Tests for NUMERIC (BigFloat) column deserialization.
//!
//! These tests verify the fix from PR #8: NUMERIC columns must return non-null
//! string values (not null) when the underlying database value is populated.
//! The root cause was that tokio-postgres binary protocol cannot decode NUMERIC
//! as String — the fix uses `rust_decimal::Decimal` for proper deserialization.

#[allow(unused)]
mod common;
use common::*;
#[allow(unused_imports)]
use serde_json::{Value, json};

/// Verify that the `amount` NUMERIC column on `assetTeleporteds` returns non-null string values.
///
/// Before the fix, `amount` was always `null` because `try_get::<_, Option<String>>()`
/// silently failed on binary-encoded NUMERIC. With `rust_decimal::Decimal`, values are
/// correctly deserialized and serialized as JSON strings (BigFloat format).
#[tokio::test]
async fn test_numeric_columns_not_null() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            assetTeleporteds(first: 20, orderBy: ID_ASC) {
                nodes { id amount }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   numeric columns: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust numeric columns: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(!has_graphql_errors(&rust_resp), "NUMERIC column query returned errors: {}", rust_resp);

	let nodes = rust_resp
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.expect("assetTeleporteds nodes missing from Rust response");

	assert!(!nodes.is_empty(), "assetTeleporteds returned no nodes");

	for (i, node) in nodes.iter().enumerate() {
		let amount = &node["amount"];

		// NUMERIC columns must NOT be null (this was the bug).
		assert!(
			!amount.is_null(),
			"node[{i}].amount is null — NUMERIC deserialization is broken. id={}",
			node["id"]
		);

		// NUMERIC columns must be serialized as JSON strings (BigFloat format).
		assert!(
			amount.is_string(),
			"node[{i}].amount should be a JSON string (BigFloat), got: {amount:?}"
		);

		// The string must be parseable as a number.
		let amount_str = amount.as_str().unwrap();
		let parsed: f64 = amount_str
			.parse()
			.unwrap_or_else(|e| panic!("node[{i}].amount '{amount_str}' should parse as f64: {e}"));
		assert!(parsed >= 0.0, "node[{i}].amount should be non-negative, got {parsed}");
	}

	println!("NUMERIC columns: all {} nodes have non-null, parseable amount values ✓", nodes.len());

	compare_responses("assetTeleporteds(numeric columns)", &ts_resp, &rust_resp);
}

/// Verify that multiple NUMERIC columns on `orders` all return non-null string values.
///
/// The `orders` table has several NUMERIC columns: deadline, nonce, fees, blockNumber,
/// blockTimestamp. All must be correctly deserialized as JSON strings.
#[tokio::test]
async fn test_numeric_columns_on_orders() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"
        {
            orders(first: 20, orderBy: ID_ASC) {
                nodes { id deadline nonce fees blockNumber blockTimestamp }
            }
        }
    "#;

	let ts_resp = ts_client.query(query).await;
	let rust_resp = rust_client.query(query).await;

	println!("TS   orders numeric: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust orders numeric: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(!has_graphql_errors(&rust_resp), "orders NUMERIC query returned errors: {}", rust_resp);

	let nodes = rust_resp
		.pointer("/data/orders/nodes")
		.and_then(|v| v.as_array())
		.expect("orders nodes missing from Rust response");

	assert!(!nodes.is_empty(), "orders returned no nodes");

	let numeric_fields = ["deadline", "nonce", "fees", "blockNumber", "blockTimestamp"];

	for (i, node) in nodes.iter().enumerate() {
		for field in &numeric_fields {
			let val = &node[*field];

			// Must not be null.
			assert!(
				!val.is_null(),
				"node[{i}].{field} is null — NUMERIC deserialization broken. id={}",
				node["id"]
			);

			// Must be a JSON string (BigFloat serialization).
			assert!(
				val.is_string(),
				"node[{i}].{field} should be a JSON string (BigFloat), got: {val:?}"
			);

			// Must parse as a number.
			let s = val.as_str().unwrap();
			s.parse::<f64>()
				.unwrap_or_else(|e| panic!("node[{i}].{field} '{s}' should parse as f64: {e}"));
		}
	}

	println!(
		"orders NUMERIC columns: all {} nodes × {} fields are non-null strings ✓",
		nodes.len(),
		numeric_fields.len()
	);

	compare_responses("orders(numeric columns)", &ts_resp, &rust_resp);
}

/// Verify that BigFloat (NUMERIC) comparison filters work correctly.
///
/// Filters like `greaterThanOrEqualTo` on NUMERIC columns pass values as GraphQL
/// strings (BigFloat scalar). The Rust service must bind these as text-encoded
/// parameters so PostgreSQL can coerce them to NUMERIC for comparison.
#[tokio::test]
async fn test_numeric_filter_comparison() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// First, get the total count.
	let total_query = r#"{ assetTeleporteds(first: 1) { totalCount } }"#;
	let total_resp = rust_client.query(total_query).await;
	let total = total_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing");

	// Get the minimum amount to use as a filter value.
	let sample_query = r#"
        {
            assetTeleporteds(first: 1, orderBy: AMOUNT_ASC) {
                nodes { id amount }
            }
        }
    "#;
	let sample_resp = rust_client.query(sample_query).await;
	let min_amount = sample_resp
		.pointer("/data/assetTeleporteds/nodes/0/amount")
		.and_then(|v| v.as_str())
		.expect("min amount missing — NUMERIC deserialization may be broken");

	println!("total rows: {total}, min amount: {min_amount}");

	// Filter: amount >= min_amount should return all rows.
	// BigFloat values are passed as quoted strings in GraphQL.
	let filter_query = format!(
		r#"
        {{
            assetTeleporteds(
                first: 100,
                orderBy: ID_ASC,
                filter: {{ amount: {{ greaterThanOrEqualTo: "{min_amount}" }} }}
            ) {{
                totalCount
                nodes {{ id amount }}
            }}
        }}
        "#
	);

	let ts_resp = ts_client.query(&filter_query).await;
	let rust_resp = rust_client.query(&filter_query).await;

	println!("TS   numeric filter: {}", serde_json::to_string_pretty(&ts_resp).unwrap());
	println!("Rust numeric filter: {}", serde_json::to_string_pretty(&rust_resp).unwrap());

	assert!(!has_graphql_errors(&rust_resp), "NUMERIC filter query returned errors: {}", rust_resp);

	let filtered_count = rust_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from filtered response");

	assert_eq!(
		filtered_count, total,
		"greaterThanOrEqualTo min amount should return all {total} rows, got {filtered_count}"
	);

	// Verify all returned amounts are non-null strings.
	if let Some(nodes) =
		rust_resp.pointer("/data/assetTeleporteds/nodes").and_then(|v| v.as_array())
	{
		for node in nodes {
			assert!(
				!node["amount"].is_null(),
				"filtered result has null amount — id={}",
				node["id"]
			);
		}
	}

	compare_responses("assetTeleporteds(numeric filter)", &ts_resp, &rust_resp);
	println!("NUMERIC filter comparison: {filtered_count} rows matched ✓");
}

/// Verify that orderBy on a NUMERIC column produces correctly ordered results.
///
/// Orders the `assetTeleporteds` by `AMOUNT_ASC` and `AMOUNT_DESC`, then verifies
/// the returned amounts are monotonically non-decreasing / non-increasing.
#[tokio::test]
async fn test_numeric_ordering() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// ASC ordering — use [AMOUNT_ASC, ID_ASC] to break ties deterministically.
	let query_asc = r#"
        {
            assetTeleporteds(first: 20, orderBy: [AMOUNT_ASC, ID_ASC]) {
                nodes { id amount }
            }
        }
    "#;

	let ts_asc = ts_client.query(query_asc).await;
	let rust_asc = rust_client.query(query_asc).await;

	println!("Rust AMOUNT_ASC: {}", serde_json::to_string_pretty(&rust_asc).unwrap());

	assert!(!has_graphql_errors(&rust_asc), "AMOUNT_ASC query returned errors: {}", rust_asc);

	let asc_nodes = rust_asc
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.expect("nodes missing from AMOUNT_ASC response");

	// Verify monotonically non-decreasing order.
	let asc_amounts: Vec<f64> = asc_nodes
		.iter()
		.map(|n| {
			let s = n["amount"]
				.as_str()
				.unwrap_or_else(|| panic!("amount is not a string: {:?}", n["amount"]));
			s.parse::<f64>().unwrap_or_else(|e| panic!("'{s}' should parse as f64: {e}"))
		})
		.collect();

	for i in 1..asc_amounts.len() {
		assert!(
			asc_amounts[i] >= asc_amounts[i - 1],
			"AMOUNT_ASC: amounts[{i}]={} < amounts[{}]={} — ordering broken",
			asc_amounts[i],
			i - 1,
			asc_amounts[i - 1]
		);
	}

	compare_responses("assetTeleporteds(AMOUNT_ASC)", &ts_asc, &rust_asc);

	// DESC ordering — use [AMOUNT_DESC, ID_DESC] to break ties deterministically.
	let query_desc = r#"
        {
            assetTeleporteds(first: 20, orderBy: [AMOUNT_DESC, ID_DESC]) {
                nodes { id amount }
            }
        }
    "#;

	let ts_desc = ts_client.query(query_desc).await;
	let rust_desc = rust_client.query(query_desc).await;

	assert!(!has_graphql_errors(&rust_desc), "AMOUNT_DESC query returned errors: {}", rust_desc);

	let desc_nodes = rust_desc
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.expect("nodes missing from AMOUNT_DESC response");

	// Verify monotonically non-increasing order.
	let desc_amounts: Vec<f64> = desc_nodes
		.iter()
		.map(|n| {
			let s = n["amount"]
				.as_str()
				.unwrap_or_else(|| panic!("amount is not a string: {:?}", n["amount"]));
			s.parse::<f64>().unwrap_or_else(|e| panic!("'{s}' should parse as f64: {e}"))
		})
		.collect();

	for i in 1..desc_amounts.len() {
		assert!(
			desc_amounts[i] <= desc_amounts[i - 1],
			"AMOUNT_DESC: amounts[{i}]={} > amounts[{}]={} — ordering broken",
			desc_amounts[i],
			i - 1,
			desc_amounts[i - 1]
		);
	}

	compare_responses("assetTeleporteds(AMOUNT_DESC)", &ts_desc, &rust_desc);

	println!(
		"NUMERIC ordering: ASC {} values non-decreasing, DESC {} values non-increasing ✓",
		asc_amounts.len(),
		desc_amounts.len()
	);
}
