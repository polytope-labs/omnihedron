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

//! Integration tests for PostGraphile smart tag support.
//!
//! These tests verify that `@foreignFieldName` and `@singleForeignFieldName`
//! constraint comments (used by SubQuery's `@derivedFrom` directive) correctly
//! override the auto-generated backward relation field names.

#[allow(unused)]
mod common;
use common::*;
#[allow(unused_imports)]
use serde_json::{Value, json};

/// Verify that `@foreignFieldName books` on `test_books_creator_id_fkey`
/// exposes the backward relation as `books` instead of the default
/// `testBooksByCreatorId`.
#[tokio::test]
async fn test_smart_tag_foreign_field_name() {
	let rust_url = rust_url();
	let rust_client = TestClient::new(&rust_url);
	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available");
		return;
	}

	// Query using the smart-tag-overridden field name "books"
	let query = r#"{
        testAuthor(id: "author-alice") {
            id
            name
            books {
                nodes { id title }
                totalCount
            }
        }
    }"#;
	let resp = rust_client.query(query).await;

	assert!(
		resp.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"smart tag field 'books' returned errors: {resp}"
	);

	let conn = resp.pointer("/data/testAuthor/books").expect("'books' field missing");
	let total = conn["totalCount"].as_i64().expect("totalCount");
	assert_eq!(total, 2, "Alice should have 2 books via 'books' field");

	let nodes = conn["nodes"].as_array().expect("nodes array");
	assert_eq!(nodes.len(), 2);
	println!("smart tag @foreignFieldName: books → {total} results ✓");
}

/// Verify that the default field name `testBooksByCreatorId` is no longer
/// exposed when a `@foreignFieldName` smart tag overrides it.
#[tokio::test]
async fn test_smart_tag_replaces_default_name() {
	let rust_url = rust_url();
	let rust_client = TestClient::new(&rust_url);
	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available");
		return;
	}

	// The old default name should NOT work anymore
	let query = r#"{
        testAuthor(id: "author-alice") {
            testBooksByCreatorId {
                totalCount
            }
        }
    }"#;
	let resp = rust_client.query(query).await;

	let has_errors = resp
		.get("errors")
		.and_then(|e| e.as_array())
		.map(|a| !a.is_empty())
		.unwrap_or(false);

	assert!(
		has_errors,
		"default field name 'testBooksByCreatorId' should NOT be valid when smart tag overrides it"
	);
	println!("smart tag replaces default name: testBooksByCreatorId → error ✓");
}

/// Verify that `@singleForeignFieldName profile` on the one-to-one
/// `test_author_profiles_author_id_fkey` exposes the backward relation
/// as `profile` instead of the default `testAuthorProfileByAuthorId`.
#[tokio::test]
async fn test_smart_tag_single_foreign_field_name() {
	let rust_url = rust_url();
	let rust_client = TestClient::new(&rust_url);
	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available");
		return;
	}

	let query = r#"{
        testAuthor(id: "author-alice") {
            id
            name
            profile {
                id
                bio
            }
        }
    }"#;
	let resp = rust_client.query(query).await;

	assert!(
		resp.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"smart tag field 'profile' returned errors: {resp}"
	);

	let profile = resp.pointer("/data/testAuthor/profile").expect("'profile' field missing");
	assert_eq!(profile["id"], "profile-alice");
	assert_eq!(profile["bio"], "Alice writes mystery novels.");

	println!("smart tag @singleForeignFieldName: profile ✓");
}

/// Verify that the smart-tag-overridden `books` backward relation supports
/// filtering arguments (same as standard backward relations).
#[tokio::test]
async fn test_smart_tag_backward_relation_with_filter() {
	let rust_url = rust_url();
	let rust_client = TestClient::new(&rust_url);
	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available");
		return;
	}

	let query = r#"{
        testAuthor(id: "author-alice") {
            books(filter: { title: { equalTo: "Book Two" } }) {
                nodes { id title }
                totalCount
            }
        }
    }"#;
	let resp = rust_client.query(query).await;

	assert!(
		resp.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"smart tag field 'books' with filter returned errors: {resp}"
	);

	let conn = resp.pointer("/data/testAuthor/books").expect("books connection missing");
	assert_eq!(conn["totalCount"], 1);

	let nodes = conn["nodes"].as_array().expect("nodes");
	assert_eq!(nodes[0]["title"], "Book Two");

	println!("smart tag backward relation with filter ✓");
}

/// Verify that the smart-tag-overridden `books` field works in relation
/// filters (some/none/every) on the parent entity's filter type.
#[tokio::test]
async fn test_smart_tag_relation_filter() {
	let rust_url = rust_url();
	let rust_client = TestClient::new(&rust_url);
	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available");
		return;
	}

	// Filter authors who have at least one book titled "Book Two" using the
	// smart-tag-overridden field name "books" in the filter.
	let query = r#"{
        testAuthors(filter: { books: { some: { title: { equalTo: "Book Two" } } } }) {
            nodes { id name }
            totalCount
        }
    }"#;
	let resp = rust_client.query(query).await;

	assert!(
		resp.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"smart tag relation filter returned errors: {resp}"
	);

	let total = resp
		.pointer("/data/testAuthors/totalCount")
		.and_then(|v| v.as_i64())
		.unwrap_or(0);
	assert_eq!(total, 1, "only Alice has 'Book Two'");

	let nodes = resp.pointer("/data/testAuthors/nodes").and_then(|v| v.as_array()).unwrap();
	assert_eq!(nodes[0]["id"], "author-alice");

	println!("smart tag relation filter (some) ✓");
}

/// Verify that introspection exposes the smart-tag-overridden field names.
#[tokio::test]
async fn test_smart_tag_introspection() {
	let rust_url = rust_url();
	let rust_client = TestClient::new(&rust_url);
	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available");
		return;
	}

	let query = r#"{
        __type(name: "TestAuthor") {
            fields {
                name
            }
        }
    }"#;
	let resp = rust_client.query(query).await;

	let fields: Vec<String> = resp
		.pointer("/data/__type/fields")
		.and_then(|v| v.as_array())
		.map(|arr| arr.iter().filter_map(|f| f["name"].as_str().map(String::from)).collect())
		.unwrap_or_default();

	assert!(
		fields.contains(&"books".to_string()),
		"TestAuthor should have 'books' field from @foreignFieldName: {fields:?}"
	);
	assert!(
		fields.contains(&"profile".to_string()),
		"TestAuthor should have 'profile' field from @singleForeignFieldName: {fields:?}"
	);
	assert!(
		!fields.contains(&"testBooksByCreatorId".to_string()),
		"TestAuthor should NOT have default 'testBooksByCreatorId': {fields:?}"
	);
	assert!(
		!fields.contains(&"testAuthorProfileByAuthorId".to_string()),
		"TestAuthor should NOT have default 'testAuthorProfileByAuthorId': {fields:?}"
	);

	println!("smart tag introspection: books + profile present, defaults absent ✓");
}
