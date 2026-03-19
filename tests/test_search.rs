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

//! Integration tests for fulltext search (`@fullText` directive).
//!
//! The test fixture creates a `search_test_books(search text)` function
//! with a `@name search_test_books` comment, replicating SubQuery's
//! `@fullText(fields: ["title"])` pattern.

#[allow(unused)]
mod common;
use common::*;
#[allow(unused_imports)]
use serde_json::{Value, json};

/// Verify that the fulltext search field is exposed and returns matching results.
#[tokio::test]
async fn test_fulltext_search_basic() {
	let rust_url = rust_url();
	let rust_client = TestClient::new(&rust_url);
	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available");
		return;
	}

	let query = r#"{
        search_test_books(search: "Book") {
            nodes { id title }
            totalCount
        }
    }"#;
	let resp = rust_client.query(query).await;

	assert!(
		resp.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"fulltext search returned errors: {resp}"
	);

	let total = resp
		.pointer("/data/search_test_books/totalCount")
		.and_then(|v| v.as_i64())
		.unwrap_or(0);
	assert!(total > 0, "search for 'Book' should return results, got {total}");

	println!("fulltext search basic: {total} results ✓");
}

/// Verify that fulltext search returns empty results for non-matching queries.
#[tokio::test]
async fn test_fulltext_search_no_match() {
	let rust_url = rust_url();
	let rust_client = TestClient::new(&rust_url);
	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available");
		return;
	}

	let query = r#"{
        search_test_books(search: "zzzznonexistent") {
            totalCount
        }
    }"#;
	let resp = rust_client.query(query).await;

	assert!(
		resp.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"fulltext search returned errors: {resp}"
	);

	let total = resp
		.pointer("/data/search_test_books/totalCount")
		.and_then(|v| v.as_i64())
		.unwrap_or(-1);
	assert_eq!(total, 0, "search for non-existent term should return 0");

	println!("fulltext search no match: 0 results ✓");
}

/// Verify that the search field appears in introspection.
#[tokio::test]
async fn test_fulltext_search_introspection() {
	let rust_url = rust_url();
	let rust_client = TestClient::new(&rust_url);
	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available");
		return;
	}

	let query = r#"{
        __schema {
            queryType {
                fields {
                    name
                }
            }
        }
    }"#;
	let resp = rust_client.query(query).await;

	let fields: Vec<String> = resp
		.pointer("/data/__schema/queryType/fields")
		.and_then(|v| v.as_array())
		.map(|arr| arr.iter().filter_map(|f| f["name"].as_str().map(String::from)).collect())
		.unwrap_or_default();

	assert!(
		fields.contains(&"search_test_books".to_string()),
		"search_test_books should be in root query fields: {fields:?}"
	);

	println!("fulltext search introspection: field present ✓");
}
