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
	// Both TS and Rust now return `keys: [String!]` — a list of string values.
	// We can use the same query for both services.
	let query_chain = r#"
        {
            assetTeleporteds(first: 100) {
                groupedAggregates(groupBy: [CHAIN]) {
                    keys
                    sum { blockNumber }
                    min { blockNumber }
                    max { blockNumber }
                    distinctCount { id }
                }
            }
        }
    "#;

	let ts_chain = ts_client.query(query_chain).await;
	let rust_chain = rust_client.query(query_chain).await;

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

	// keys is now [String!] — the first element should be the chain value.
	let chain_key = chain_groups[0]
		.pointer("/keys/0")
		.and_then(|v| v.as_str())
		.expect("groupedAggregates[CHAIN] keys[0] missing");
	assert!(
		chain_key.contains("KUSAMA"),
		"groupBy:[CHAIN] group key should contain KUSAMA, got: {chain_key}"
	);

	compare_responses(
		"assetTeleporteds(groupedAggregates groupBy:[CHAIN])",
		&ts_chain,
		&rust_chain,
	);

	println!(
		"groupedAggregates: groupBy:[] → {} group(s), groupBy:[CHAIN] → {} group(s) ✓",
		groups.len(),
		chain_groups.len()
	);
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
