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

#[tokio::test]
async fn test_last_before_cursor() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	// First page: get first 5 rows and the endCursor
	let q1 = r#"{ assetTeleporteds(first: 5, orderBy: ID_ASC) { edges { cursor node { id } } pageInfo { endCursor } } }"#;
	let r1 = rust_client.query(q1).await;
	let end_cursor = r1
		.pointer("/data/assetTeleporteds/pageInfo/endCursor")
		.and_then(|v| v.as_str())
		.expect("endCursor missing");

	// Now fetch last 2 before that cursor
	let q2 = format!(
		r#"{{ assetTeleporteds(last: 2, before: "{end_cursor}", orderBy: ID_ASC) {{ nodes {{ id }} pageInfo {{ hasPreviousPage hasNextPage }} }} }}"#
	);
	let r2 = rust_client.query(&q2).await;
	assert!(
		r2.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"Rust last+before returned errors: {r2}"
	);
	let nodes = r2
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.expect("nodes");
	assert_eq!(nodes.len(), 2, "last:2 before cursor should return 2 rows");

	// ── Verify TS also supports last+before (cursors differ, so use TS's own) ──
	let ts_r1 = ts_client.query(q1).await;
	let ts_end_cursor = ts_r1
		.pointer("/data/assetTeleporteds/pageInfo/endCursor")
		.and_then(|v| v.as_str())
		.expect("TS endCursor missing");
	let ts_q2 = format!(
		r#"{{ assetTeleporteds(last: 2, before: "{ts_end_cursor}", orderBy: ID_ASC) {{ nodes {{ id }} }} }}"#
	);
	let ts_r2 = ts_client.query(&ts_q2).await;
	assert!(
		ts_r2
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"TS last+before returned errors: {ts_r2}"
	);
	let ts_nodes = ts_r2
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.expect("TS nodes");
	assert_eq!(ts_nodes.len(), 2, "TS last:2 before cursor should also return 2 rows");

	println!("last+before cursor: Rust={}, TS={} rows ✓", nodes.len(), ts_nodes.len());
}

#[tokio::test]
async fn test_first_zero() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{ assetTeleporteds(first: 0) { nodes { id } totalCount } }"#;
	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	// TS treats first:0 as falsy (JS !0 === true) → "no first" → default behavior.
	// Rust matches this: first:0 is treated as "not specified".
	compare_responses("first:0", &ts, &rust);
	println!("first:0: TS and Rust match ✓");
}

#[tokio::test]
async fn test_invalid_cursor() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());

	let query = r#"{ assetTeleporteds(after: "not-a-valid-cursor") { nodes { id } } }"#;
	let resp = rust_client.query(query).await;

	let errors = resp.get("errors").and_then(|e| e.as_array());
	assert!(
		errors.is_some() && !errors.unwrap().is_empty(),
		"invalid cursor should return a GraphQL error, got: {resp}"
	);
	println!("invalid cursor: GraphQL error returned ✓");
}

#[tokio::test]
async fn test_first_and_last_rejected() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());

	let query = r#"{ assetTeleporteds(first: 5, last: 3) { nodes { id } } }"#;
	let resp = rust_client.query(query).await;

	let errors = resp.get("errors").and_then(|e| e.as_array());
	assert!(
		errors.is_some() && !errors.unwrap().is_empty(),
		"first+last should return error, got: {resp}"
	);
	println!("first+last rejected ✓");
}

/// Test that cursor pagination works without an explicit `orderBy`.
/// The default ORDER BY is `t.id ASC`, so `after: <endCursor>` must still
/// advance to the next page (regression: order_cols was empty → cursor ignored).
#[tokio::test]
async fn test_cursor_pagination_default_order() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());

	// Page 1 — no explicit orderBy
	let page1_query = r#"
		{
			assetTeleporteds(first: 3) {
				nodes { id }
				pageInfo { hasNextPage endCursor }
			}
		}
	"#;
	let page1 = rust_client.query(page1_query).await;

	let page1_ids: HashSet<String> = page1
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.map(|nodes| nodes.iter().filter_map(|n| n["id"].as_str().map(String::from)).collect())
		.unwrap_or_default();
	assert!(!page1_ids.is_empty(), "page1 returned 0 rows");

	let has_next = page1
		.pointer("/data/assetTeleporteds/pageInfo/hasNextPage")
		.and_then(|v| v.as_bool())
		.unwrap_or(false);
	assert!(has_next, "hasNextPage should be true for first:3 of 20 rows (no orderBy)");

	let end_cursor = page1
		.pointer("/data/assetTeleporteds/pageInfo/endCursor")
		.and_then(|v| v.as_str())
		.expect("endCursor missing from page1");

	// Page 2 — feed endCursor into after, still no explicit orderBy
	let page2_query = format!(
		r#"{{ assetTeleporteds(first: 3, after: "{end_cursor}") {{ nodes {{ id }} pageInfo {{ hasNextPage endCursor }} }} }}"#
	);
	let page2 = rust_client.query(&page2_query).await;

	assert!(
		page2
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"page2 returned errors: {page2}"
	);

	let page2_ids: HashSet<String> = page2
		.pointer("/data/assetTeleporteds/nodes")
		.and_then(|v| v.as_array())
		.map(|nodes| nodes.iter().filter_map(|n| n["id"].as_str().map(String::from)).collect())
		.unwrap_or_default();
	assert!(!page2_ids.is_empty(), "page2 returned 0 rows — cursor was likely ignored");

	let overlap: HashSet<&String> = page1_ids.intersection(&page2_ids).collect();
	assert!(
		overlap.is_empty(),
		"page1 and page2 overlap without explicit orderBy! Common IDs: {:?}",
		overlap
	);
	println!(
		"cursor pagination (default order): page1={}, page2={}, no overlap ✓",
		page1_ids.len(),
		page2_ids.len()
	);
}

/// Test that `totalCount` remains the true total across all pages and is not
/// reduced by cursor filtering (regression: COUNT(*) OVER() included the cursor
/// condition, so page 2's totalCount was lower than page 1's).
#[tokio::test]
async fn test_total_count_stable_across_pages() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());

	// Page 1
	let page1_query = r#"
		{
			assetTeleporteds(first: 3, orderBy: ID_ASC) {
				totalCount
				nodes { id }
				pageInfo { endCursor hasNextPage }
			}
		}
	"#;
	let page1 = rust_client.query(page1_query).await;

	let total_page1 = page1
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from page1");
	assert!(total_page1 > 3, "totalCount should be > 3 to test pagination, got {total_page1}");

	let end_cursor = page1
		.pointer("/data/assetTeleporteds/pageInfo/endCursor")
		.and_then(|v| v.as_str())
		.expect("endCursor missing from page1");

	// Page 2
	let page2_query = format!(
		r#"{{ assetTeleporteds(first: 3, orderBy: ID_ASC, after: "{end_cursor}") {{ totalCount nodes {{ id }} pageInfo {{ hasNextPage }} }} }}"#
	);
	let page2 = rust_client.query(&page2_query).await;

	let total_page2 = page2
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing from page2");

	assert_eq!(
		total_page1, total_page2,
		"totalCount must be stable across pages: page1={total_page1}, page2={total_page2}"
	);
	println!("totalCount stable across pages: {total_page1} == {total_page2} ✓");
}

/// Test that `hasNextPage` is correct on the last page of cursor-paginated results.
#[tokio::test]
async fn test_has_next_page_last_page() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let rust_client = TestClient::new(&rust_url());

	// Fetch all IDs to know the total count.
	let all_query = r#"{ assetTeleporteds(orderBy: ID_ASC) { totalCount } }"#;
	let all_resp = rust_client.query(all_query).await;
	let total = all_resp
		.pointer("/data/assetTeleporteds/totalCount")
		.and_then(|v| v.as_i64())
		.expect("totalCount missing") as usize;

	// Walk through all pages using cursor pagination.
	let page_size = 5;
	let mut cursor: Option<String> = None;
	let mut all_ids: Vec<String> = vec![];
	let mut pages = 0;

	loop {
		let query = if let Some(ref c) = cursor {
			format!(
				r#"{{ assetTeleporteds(first: {page_size}, orderBy: ID_ASC, after: "{c}") {{ nodes {{ id }} pageInfo {{ hasNextPage endCursor }} }} }}"#
			)
		} else {
			format!(
				r#"{{ assetTeleporteds(first: {page_size}, orderBy: ID_ASC) {{ nodes {{ id }} pageInfo {{ hasNextPage endCursor }} }} }}"#
			)
		};

		let resp = rust_client.query(&query).await;
		assert!(
			resp.get("errors")
				.and_then(|e| e.as_array())
				.map(|a| a.is_empty())
				.unwrap_or(true),
			"page {} returned errors: {resp}",
			pages + 1
		);

		let nodes = resp
			.pointer("/data/assetTeleporteds/nodes")
			.and_then(|v| v.as_array())
			.expect("nodes missing");

		let ids: Vec<String> =
			nodes.iter().filter_map(|n| n["id"].as_str().map(String::from)).collect();
		all_ids.extend(ids);

		let has_next = resp
			.pointer("/data/assetTeleporteds/pageInfo/hasNextPage")
			.and_then(|v| v.as_bool())
			.unwrap_or(false);

		pages += 1;

		if has_next {
			cursor = resp
				.pointer("/data/assetTeleporteds/pageInfo/endCursor")
				.and_then(|v| v.as_str())
				.map(String::from);
			assert!(cursor.is_some(), "hasNextPage=true but endCursor is null");
		} else {
			break;
		}

		// Safety: prevent infinite loops in case of bugs.
		assert!(pages < 100, "pagination did not terminate after 100 pages");
	}

	// Verify we collected all rows with no duplicates.
	let unique: HashSet<&String> = all_ids.iter().collect();
	assert_eq!(
		unique.len(),
		total,
		"cursor walk collected {} unique IDs but totalCount is {}",
		unique.len(),
		total
	);
	assert_eq!(
		all_ids.len(),
		total,
		"cursor walk collected {} IDs (with duplicates) but totalCount is {}",
		all_ids.len(),
		total
	);
	println!(
		"full cursor walk: {pages} pages, {} rows, hasNextPage=false on last page ✓",
		all_ids.len()
	);
}
