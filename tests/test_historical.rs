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
					books {
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
		.pointer("/books/nodes")
		.and_then(|v| v.as_array())
		.expect("books nodes missing");

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
					books {
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
		.pointer("/books/nodes")
		.and_then(|v| v.as_array())
		.expect("books nodes missing at blockHeight 600");

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
