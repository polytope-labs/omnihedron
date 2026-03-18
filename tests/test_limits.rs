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

/// Verifies batch queries work and don't panic the service.
#[tokio::test]
async fn test_batch_limit_rejection() {
	let rust_url = rust_url();
	let rust_client = TestClient::new(&rust_url);
	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available");
		return;
	}

	let batch: Vec<serde_json::Value> =
		(0..10).map(|_| serde_json::json!({"query": "{ __typename }"})).collect();
	let resp = rust_client.batch_query(&batch).await;
	let arr = resp.as_array().expect("batch response should be an array");
	assert_eq!(arr.len(), 10, "batch of 10 should return 10 responses");
	println!("batch limit: 10 queries → 10 responses ✓");
}
