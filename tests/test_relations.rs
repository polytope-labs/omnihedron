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
async fn test_forward_relation() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{ testBook(id: "book-2") { id title creator { id name } } }"#;
	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	let book = rust.pointer("/data/testBook").expect("testBook missing");
	assert_eq!(book["id"], "book-2");
	assert_eq!(book["creator"]["id"], "author-alice");

	compare_responses("testBook forward relation", &ts, &rust);
	println!("forward relation: testBook.author ✓");
}

#[tokio::test]
async fn test_backward_relation() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{ testAuthor(id: "author-alice") { id name testBooksByCreatorId { nodes { id title } totalCount } } }"#;

	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	let conn = rust
		.pointer("/data/testAuthor/testBooksByCreatorId")
		.expect("connection missing");
	let total = conn["totalCount"].as_i64().expect("totalCount");
	assert_eq!(total, 2, "Alice should have 2 books");

	compare_responses("backward relation", &ts, &rust);
	println!("backward relation: {total} books ✓");
}

#[tokio::test]
async fn test_backward_relation_filter() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{ testAuthor(id: "author-alice") { testBooksByCreatorId(filter: { title: { equalTo: "Book Two" } }) { nodes { id title } totalCount } } }"#;

	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	let conn = rust
		.pointer("/data/testAuthor/testBooksByCreatorId")
		.expect("connection missing");
	assert_eq!(conn["totalCount"], 1);

	compare_responses("backward relation filter", &ts, &rust);
	println!("backward relation filter: title=Book Two → 1 result ✓");
}

#[tokio::test]
async fn test_backward_relation_orderby() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{ testAuthor(id: "author-alice") { testBooksByCreatorId(orderBy: TITLE_DESC) { nodes { id title } } } }"#;

	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	compare_responses("backward relation orderBy", &ts, &rust);
	println!("backward relation orderBy: TITLE_DESC ✓");
}

#[tokio::test]
async fn test_backward_relation_pagination() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{
        testAuthor(id: "author-alice") {
            testBooksByCreatorId(first: 1) {
                nodes { id title }
                totalCount
                pageInfo { hasNextPage hasPreviousPage }
            }
        }
    }"#;
	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	let conn = rust
		.pointer("/data/testAuthor/testBooksByCreatorId")
		.expect("connection missing");
	assert_eq!(conn["totalCount"], 2, "totalCount should be 2");

	compare_responses("backward relation pagination", &ts, &rust);
	println!("backward relation pagination: first:1 of 2 ✓");
}

#[tokio::test]
async fn test_forward_relation_filter() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{
        testBooks(filter: { creator: { name: { equalTo: "Alice" } } }) {
            nodes { id title }
            totalCount
        }
    }"#;
	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	let total = rust.pointer("/data/testBooks/totalCount").and_then(|v| v.as_i64()).unwrap_or(0);
	assert_eq!(total, 2, "Alice has 2 books");

	compare_responses("forward relation filter", &ts, &rust);
	println!("forward relation filter: author.name=Alice → {total} books ✓");
}

#[tokio::test]
async fn test_relation_exists_filter() {
	let rust_url = rust_url();
	let rust_client = TestClient::new(&rust_url);
	if rust_client.health().await.is_server_error() {
		eprintln!("SKIP: Rust service not available");
		return;
	}

	let query_true = r#"{ testBooks(filter: { creatorExists: true }) { totalCount } }"#;
	let resp_true = rust_client.query(query_true).await;
	assert!(
		resp_true
			.get("errors")
			.and_then(|e| e.as_array())
			.map(|a| a.is_empty())
			.unwrap_or(true),
		"exists:true returned errors: {resp_true}"
	);
	let count_true = resp_true
		.pointer("/data/testBooks/totalCount")
		.and_then(|v| v.as_i64())
		.unwrap_or(0);
	assert!(count_true > 0, "all test_books have an author → count should be > 0");

	let query_false = r#"{ testBooks(filter: { creatorExists: false }) { totalCount } }"#;
	let resp_false = rust_client.query(query_false).await;
	let count_false = resp_false
		.pointer("/data/testBooks/totalCount")
		.and_then(|v| v.as_i64())
		.unwrap_or(-1);
	assert_eq!(count_false, 0, "all test_books have an author → false should return 0");

	println!("relationExists: true→{count_true}, false→{count_false} ✓");
}

#[tokio::test]
async fn test_relation_some_filter() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{
        testAuthors(filter: { testBooksByCreatorId: { some: { title: { equalTo: "Book Two" } } } }) {
            nodes { id name }
            totalCount
        }
    }"#;
	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	let total = rust
		.pointer("/data/testAuthors/totalCount")
		.and_then(|v| v.as_i64())
		.unwrap_or(0);
	assert_eq!(total, 1, "only Alice has 'Book Two'");

	compare_responses("some filter", &ts, &rust);
	println!("some filter: 1 author ✓");
}

#[tokio::test]
async fn test_relation_none_filter() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{
        testAuthors(filter: { testBooksByCreatorId: { none: { title: { equalTo: "Book Two" } } } }) {
            nodes { id name }
            totalCount
        }
    }"#;
	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	let total = rust
		.pointer("/data/testAuthors/totalCount")
		.and_then(|v| v.as_i64())
		.unwrap_or(-1);
	assert_eq!(total, 0, "Alice has 'Book Two' → none should return 0");

	compare_responses("none filter", &ts, &rust);
	println!("none filter: 0 authors ✓");
}

#[tokio::test]
async fn test_relation_every_filter() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{
        testAuthors(filter: { testBooksByCreatorId: { every: { title: { startsWith: "Book" } } } }) {
            nodes { id name }
            totalCount
        }
    }"#;
	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	let total = rust
		.pointer("/data/testAuthors/totalCount")
		.and_then(|v| v.as_i64())
		.unwrap_or(0);
	assert_eq!(total, 1, "Alice's books all start with 'Book'");

	compare_responses("every filter", &ts, &rust);
	println!("every filter: 1 author ✓");
}
