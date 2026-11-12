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

/// A forward relation must fetch columns whose GraphQL field name differs from the SQL
/// column name.  `birthYear`/`birth_year` is dropped from the SELECT if the resolver
/// compares selection-set names against raw column names, which resolves the field to
/// null — or errors outright when the column is non-null.
#[tokio::test]
async fn test_forward_relation_multiword_column() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	let query = r#"{ testBook(id: "book-2") { id creator { id name birthYear } } }"#;
	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	assert!(rust.get("errors").is_none(), "unexpected errors: {rust:#?}");
	let creator = rust.pointer("/data/testBook/creator").expect("creator missing");
	assert_eq!(creator["id"], "author-alice");
	assert_eq!(creator["birthYear"], 1902, "multi-word column dropped from SELECT: {creator:#?}");

	compare_responses("testBook forward relation multi-word column", &ts, &rust);
	println!("forward relation multi-word column: creator.birthYear ✓");
}

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

	let query =
		r#"{ testAuthor(id: "author-alice") { id name books { nodes { id title } totalCount } } }"#;

	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	let conn = rust.pointer("/data/testAuthor/books").expect("connection missing");
	let total = conn["totalCount"].as_i64().expect("totalCount");
	assert_eq!(total, 3, "Alice should have 3 books");

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

	let query = r#"{ testAuthor(id: "author-alice") { books(filter: { title: { equalTo: "Book Two" } }) { nodes { id title } totalCount } } }"#;

	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	let conn = rust.pointer("/data/testAuthor/books").expect("connection missing");
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

	let query = r#"{ testAuthor(id: "author-alice") { books(orderBy: TITLE_DESC) { nodes { id title } } } }"#;

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
            books(first: 1) {
                nodes { id title }
                totalCount
                pageInfo { hasNextPage hasPreviousPage }
            }
        }
    }"#;
	let ts = ts_client.query(query).await;
	let rust = rust_client.query(query).await;

	let conn = rust.pointer("/data/testAuthor/books").expect("connection missing");
	assert_eq!(conn["totalCount"], 3, "totalCount should be 3");

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
	assert_eq!(total, 3, "Alice has 3 books");

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
        testAuthors(filter: { books: { some: { title: { equalTo: "Book Two" } } } }) {
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
        testAuthors(filter: { books: { none: { title: { equalTo: "Book Two" } } } }) {
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
	assert_eq!(total, 9, "9 authors have no 'Book Two'");

	compare_responses("none filter", &ts, &rust);
	println!("none filter: 1 author (Bob) ✓");
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
        testAuthors(filter: { books: { every: { title: { startsWith: "Book" } } } }) {
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
	assert_eq!(total, 10, "All authors' books start with 'Book'");

	compare_responses("every filter", &ts, &rust);
	println!("every filter: 2 authors ✓");
}

// ---------------------------------------------------------------------------
// Relation filters nested inside another relation filter, and relation filters
// on nested connection fields. A filter that is dropped or joined to the wrong
// table still returns a well-formed response, so each case pins the expected
// rows as well as comparing against the TypeScript service.
//
// Fixture: test_collection_books → test_collections → test_authors, where both
// TestCollection and TestAuthor have a backward relation named `books`.
// ---------------------------------------------------------------------------

/// Run `query` on both services, require an error-free Rust response that
/// matches TypeScript, and return the Rust `data`.
async fn query_both(name: &str, query: &str) -> Value {
	let ts = TestClient::new(&ts_url()).query(query).await;
	let rust = TestClient::new(&rust_url()).query(query).await;
	assert!(!has_graphql_errors(&rust), "{name}: unexpected errors: {rust:#?}");
	compare_responses(name, &ts, &rust);
	rust["data"].clone()
}

fn ids(nodes: &Value) -> Vec<&str> {
	let mut ids: Vec<&str> = nodes
		.as_array()
		.expect("nodes missing")
		.iter()
		.map(|n| n["id"].as_str().expect("id missing"))
		.collect();
	ids.sort();
	ids
}

#[tokio::test]
async fn test_forward_relation_filter_nested_in_forward() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	// `owner` is a relation of TestCollection, not of the root TestCollectionBook.
	let data = query_both(
		"forward → forward filter",
		r#"{
            testCollectionBooks(
                filter: { collection: { owner: { name: { equalTo: "Alice" } } } }
            ) { nodes { id } totalCount }
        }"#,
	)
	.await;
	assert_eq!(
		ids(&data["testCollectionBooks"]["nodes"]),
		["collection-book-1", "collection-book-2"]
	);

	// A scalar beside the nested relation must not hide the relation being dropped:
	// the label alone matches both of Alice's entries.
	let data = query_both(
		"forward → forward filter, no match",
		r#"{
            testCollectionBooks(filter: {
                collection: {
                    label: { startsWith: "Alice" }
                    owner: { name: { equalTo: "Nobody" } }
                }
            }) { totalCount }
        }"#,
	)
	.await;
	assert_eq!(data["testCollectionBooks"]["totalCount"], 0);
}

#[tokio::test]
async fn test_forward_relation_filter_nested_in_backward() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let data = query_both(
		"backward → forward filter",
		r#"{
            testAuthors(
                filter: { books: { some: { creator: { name: { equalTo: "Bob" } } } } }
            ) { nodes { id } totalCount }
        }"#,
	)
	.await;
	assert_eq!(ids(&data["testAuthors"]["nodes"]), ["author-bob"]);

	let data = query_both(
		"backward → forward filter, no match",
		r#"{
            testAuthors(
                filter: { books: { some: { creator: { name: { equalTo: "Nobody" } } } } }
            ) { totalCount }
        }"#,
	)
	.await;
	assert_eq!(data["testAuthors"]["totalCount"], 0);
}

/// Inside `owner`, `books` is the author's books (test_books), although the root
/// TestCollection has a `books` relation of its own (test_collection_books).
/// "Book Three" was written by Bob but is shelved in Alice's collection, so the
/// two readings select different collections.
#[tokio::test]
async fn test_nested_relation_filter_name_shared_with_root() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let data = query_both(
		"nested relation named like a root relation",
		r#"{
            testCollections(
                filter: { owner: { books: { some: { title: { equalTo: "Book Three" } } } } }
            ) { nodes { id } totalCount }
        }"#,
	)
	.await;
	assert_eq!(ids(&data["testCollections"]["nodes"]), ["collection-2"]);

	let data = query_both(
		"root relation of the same name",
		r#"{
            testCollections(
                filter: { books: { some: { title: { equalTo: "Book Three" } } } }
            ) { nodes { id } totalCount }
        }"#,
	)
	.await;
	assert_eq!(ids(&data["testCollections"]["nodes"]), ["collection-1"]);
}

#[tokio::test]
async fn test_relation_filters_on_nested_connection() {
	if !services_available() {
		eprintln!("SKIP: Services not available.");
		return;
	}

	let counts = |data: &Value| -> Vec<i64> {
		data["testCollections"]["nodes"]
			.as_array()
			.expect("nodes missing")
			.iter()
			.map(|n| n["books"]["totalCount"].as_i64().expect("totalCount missing"))
			.collect()
	};

	// Forward relation filter: collection-1 holds 2 entries, collection-2 holds 1.
	let data = query_both(
		"nested connection, forward relation filter",
		r#"{
            testCollections(orderBy: ID_ASC) {
                nodes {
                    id
                    books(filter: { collection: { label: { equalTo: "Alice Favourites" } } }) {
                        nodes { id }
                        totalCount
                    }
                }
            }
        }"#,
	)
	.await;
	assert_eq!(counts(&data), [2, 0, 0]);

	// `…Exists` filter: every entry reached through a collection has one.
	let data = query_both(
		"nested connection, exists filter",
		r#"{
            testCollections(orderBy: ID_ASC) {
                nodes { id books(filter: { collectionExists: false }) { totalCount } }
            }
        }"#,
	)
	.await;
	assert_eq!(counts(&data), [0, 0, 0]);

	// The same filter on the root connection finds the one unshelved entry.
	let data = query_both(
		"root connection, exists filter",
		r#"{ testCollectionBooks(filter: { collectionExists: false }) { nodes { id } totalCount } }"#,
	)
	.await;
	assert_eq!(ids(&data["testCollectionBooks"]["nodes"]), ["collection-book-4"]);
}
