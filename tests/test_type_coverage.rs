// Copyright (C) 2026 Polytope Labs.
// SPDX-License-Identifier: Apache-2.0

//! Tests that every PostgreSQL column type is correctly serialized by omnihedron.
//!
//! These tests run against the `type_coverage` table created by
//! `tests/fixtures/type_coverage.sql`. They verify that:
//! 1. Non-null values are returned as the correct JSON type (not null).
//! 2. Null columns are returned as JSON null.
//! 3. Values match expected representations (strings, numbers, arrays, etc.).

#[allow(unused)]
mod common;
use common::*;
use serde_json::Value;

/// Helper: query the type_coverage table and return the node with the given id.
async fn get_type_coverage_node(client: &TestClient, id: &str) -> Value {
	let query = format!(
		r#"
        {{
            typeCoverages(filter: {{ id: {{ equalTo: "{id}" }} }}) {{
                nodes {{
                    id
                    colBool
                    colInt2
                    colInt4
                    colInt8
                    colFloat4
                    colFloat8
                    colNumeric
                    colText
                    colVarchar
                    colBpchar
                    colBytea
                    colJson
                    colJsonb
                    colTimestamp
                    colTimestamptz
                    colDate
                    colTime
                    colInterval {{ seconds minutes hours days months years }}
                    colUuid
                    colBit
                    colVarbit
                    colInet
                    colCidr
                    colMacaddr
                    colPoint {{ x y }}
                    colBox
                    colEnum
                    colOid
                    colBoolArr
                    colInt2Arr
                    colInt4Arr
                    colInt8Arr
                    colFloat4Arr
                    colFloat8Arr
                    colTextArr
                    colNumericArr
                    colUuidArr
                    colTimestampArr
                    colTimestamptzArr
                    colDateArr
                    colTimeArr
                    colJsonbArr
                    colByteaArr
                    colInetArr
                    colMacaddrArr
                }}
            }}
        }}
    "#
	);
	let resp = client.query(&query).await;
	assert!(!has_graphql_errors(&resp), "type_coverage query returned errors: {}", resp);
	let nodes = resp
		.pointer("/data/typeCoverages/nodes")
		.and_then(|v| v.as_array())
		.expect("typeCoverages nodes missing");
	assert!(!nodes.is_empty(), "no rows returned for id={id}");
	nodes[0].clone()
}

// ─── Scalar type tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_bool_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colBool"], Value::Bool(true));
}

#[tokio::test]
async fn test_int2_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colInt2"], serde_json::json!(32767));
}

#[tokio::test]
async fn test_int4_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colInt4"], serde_json::json!(2147483647));
}

#[tokio::test]
async fn test_int8_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	// INT8 is serialized as a string (BigInt convention).
	assert_eq!(node["colInt8"], serde_json::json!("9223372036854775807"));
}

#[tokio::test]
async fn test_float4_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colFloat4"].as_f64().expect("colFloat4 should be a number");
	assert!((val - 3.14).abs() < 0.01, "colFloat4 = {val}, expected ~3.14");
}

#[tokio::test]
async fn test_float8_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colFloat8"].as_f64().expect("colFloat8 should be a number");
	assert!(
		(val - 2.718281828459045).abs() < 1e-10,
		"colFloat8 = {val}, expected ~2.718281828459045"
	);
}

#[tokio::test]
async fn test_numeric_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colNumeric"].as_str().expect("colNumeric should be a string (BigFloat)");
	assert!(
		val.contains("99999999999999"),
		"colNumeric = '{val}', expected to contain '99999999999999'"
	);
}

#[tokio::test]
async fn test_text_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colText"], serde_json::json!("hello world"));
}

#[tokio::test]
async fn test_varchar_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colVarchar"], serde_json::json!("varchar val"));
}

#[tokio::test]
async fn test_bpchar_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	// BPCHAR is space-padded to the declared length.
	let val = node["colBpchar"].as_str().expect("colBpchar should be a string");
	assert_eq!(val.trim(), "bpchar", "colBpchar = '{val}'");
}

#[tokio::test]
async fn test_bytea_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colBytea"].as_str().expect("colBytea should be a hex string");
	assert_eq!(val.to_lowercase(), "deadbeef");
}

#[tokio::test]
async fn test_json_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(
		node["colJson"]["key"],
		serde_json::json!("value"),
		"colJson = {:?}",
		node["colJson"]
	);
}

#[tokio::test]
async fn test_jsonb_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(
		node["colJsonb"]["nested"]["num"],
		serde_json::json!(42),
		"colJsonb = {:?}",
		node["colJsonb"]
	);
}

#[tokio::test]
async fn test_timestamp_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colTimestamp"].as_str().expect("colTimestamp should be a string");
	assert!(
		val.starts_with("2024-01-15T10:30:00"),
		"colTimestamp = '{val}', expected '2024-01-15T10:30:00'"
	);
}

#[tokio::test]
async fn test_timestamptz_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colTimestamptz"].as_str().expect("colTimestamptz should be a string");
	assert!(
		val.contains("2024-01-15"),
		"colTimestamptz = '{val}', expected to contain '2024-01-15'"
	);
}

#[tokio::test]
async fn test_date_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colDate"], serde_json::json!("2024-01-15"));
}

#[tokio::test]
async fn test_time_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colTime"].as_str().expect("colTime should be a string");
	assert!(val.starts_with("14:30:00"), "colTime = '{val}', expected '14:30:00'");
}

#[tokio::test]
async fn test_interval_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = &node["colInterval"];
	assert!(!val.is_null(), "colInterval should not be null");
	// Interval is now an object type with {seconds, minutes, hours, days, months, years}
	assert!(val.is_object(), "colInterval should be an object, got: {val:?}");
}

#[tokio::test]
async fn test_uuid_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colUuid"], serde_json::json!("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"));
}

#[tokio::test]
async fn test_bit_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colBit"].as_str().expect("colBit should be a string");
	assert_eq!(val, "10101010", "colBit = '{val}'");
}

#[tokio::test]
async fn test_varbit_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colVarbit"].as_str().expect("colVarbit should be a string");
	assert_eq!(val, "1100110011", "colVarbit = '{val}'");
}

#[tokio::test]
async fn test_inet_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colInet"].as_str().expect("colInet should be a string");
	assert!(val.contains("192.168.1.1"), "colInet = '{val}', expected '192.168.1.1'");
}

#[tokio::test]
async fn test_cidr_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colCidr"].as_str().expect("colCidr should be a string");
	assert!(val.contains("10.0.0.0"), "colCidr = '{val}', expected to contain '10.0.0.0'");
}

#[tokio::test]
async fn test_macaddr_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = node["colMacaddr"].as_str().expect("colMacaddr should be a string");
	assert!(
		val.to_lowercase().contains("08:00:2b"),
		"colMacaddr = '{val}', expected to contain '08:00:2b'"
	);
}

#[tokio::test]
async fn test_point_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	// POINT is serialized as {"x": ..., "y": ...}.
	let val = &node["colPoint"];
	assert!(!val.is_null(), "colPoint should not be null");
	let x = val["x"].as_f64().expect("point.x should be a number");
	let y = val["y"].as_f64().expect("point.y should be a number");
	assert!((x - 1.5).abs() < 0.01, "point.x = {x}, expected 1.5");
	assert!((y - 2.5).abs() < 0.01, "point.y = {y}, expected 2.5");
}

#[tokio::test]
async fn test_box_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let val = &node["colBox"];
	assert!(!val.is_null(), "colBox should not be null");
}

#[tokio::test]
async fn test_enum_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colEnum"], serde_json::json!("active"));
}

#[tokio::test]
async fn test_oid_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colOid"], serde_json::json!(12345));
}

// ─── Array type tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_bool_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colBoolArr"], serde_json::json!([true, false, true]));
}

#[tokio::test]
async fn test_int2_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colInt2Arr"], serde_json::json!([1, 2, 3]));
}

#[tokio::test]
async fn test_int4_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colInt4Arr"], serde_json::json!([10, 20, 30]));
}

#[tokio::test]
async fn test_int8_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	// INT8 arrays are serialized as string arrays (BigInt convention).
	assert_eq!(node["colInt8Arr"], serde_json::json!(["100", "200"]));
}

#[tokio::test]
async fn test_float4_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let arr = node["colFloat4Arr"].as_array().expect("colFloat4Arr should be an array");
	assert_eq!(arr.len(), 2);
	assert!((arr[0].as_f64().unwrap() - 1.1).abs() < 0.01);
	assert!((arr[1].as_f64().unwrap() - 2.2).abs() < 0.01);
}

#[tokio::test]
async fn test_float8_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let arr = node["colFloat8Arr"].as_array().expect("colFloat8Arr should be an array");
	assert_eq!(arr.len(), 2);
	assert!((arr[0].as_f64().unwrap() - 3.3).abs() < 0.01);
	assert!((arr[1].as_f64().unwrap() - 4.4).abs() < 0.01);
}

#[tokio::test]
async fn test_text_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colTextArr"], serde_json::json!(["foo", "bar", "baz"]));
}

#[tokio::test]
async fn test_numeric_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let arr = node["colNumericArr"].as_array().expect("colNumericArr should be an array");
	assert_eq!(arr.len(), 2);
	assert_eq!(arr[0].as_str().unwrap(), "1.23");
	assert_eq!(arr[1].as_str().unwrap(), "4.56");
}

#[tokio::test]
async fn test_uuid_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colUuidArr"], serde_json::json!(["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]));
}

#[tokio::test]
async fn test_timestamp_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let arr = node["colTimestampArr"].as_array().expect("colTimestampArr should be an array");
	assert_eq!(arr.len(), 2);
	assert!(arr[0].as_str().unwrap().starts_with("2024-01-15T10:30:00"));
	assert!(arr[1].as_str().unwrap().starts_with("2024-06-15T12:00:00"));
}

#[tokio::test]
async fn test_timestamptz_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let arr = node["colTimestamptzArr"]
		.as_array()
		.expect("colTimestamptzArr should be an array");
	assert_eq!(arr.len(), 1);
	assert!(arr[0].as_str().unwrap().contains("2024-01-15"));
}

#[tokio::test]
async fn test_date_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(node["colDateArr"], serde_json::json!(["2024-01-15", "2024-06-15"]));
}

#[tokio::test]
async fn test_time_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let arr = node["colTimeArr"].as_array().expect("colTimeArr should be an array");
	assert_eq!(arr.len(), 2);
	assert!(arr[0].as_str().unwrap().starts_with("14:30:00"));
	assert!(arr[1].as_str().unwrap().starts_with("08:00:00"));
}

#[tokio::test]
async fn test_jsonb_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let arr = node["colJsonbArr"].as_array().expect("colJsonbArr should be an array");
	assert_eq!(arr.len(), 2);
	assert_eq!(arr[0]["a"], serde_json::json!(1));
	assert_eq!(arr[1]["b"], serde_json::json!(2));
}

#[tokio::test]
async fn test_bytea_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let arr = node["colByteaArr"].as_array().expect("colByteaArr should be an array");
	assert_eq!(arr.len(), 2);
	assert_eq!(arr[0].as_str().unwrap().to_lowercase(), "cafe");
	assert_eq!(arr[1].as_str().unwrap().to_lowercase(), "babe");
}

#[tokio::test]
async fn test_inet_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let arr = node["colInetArr"].as_array().expect("colInetArr should be an array");
	assert_eq!(arr.len(), 2);
	assert!(arr[0].as_str().unwrap().contains("192.168.1.1"));
	assert!(arr[1].as_str().unwrap().contains("10.0.0.1"));
}

#[tokio::test]
async fn test_macaddr_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let arr = node["colMacaddrArr"].as_array().expect("colMacaddrArr should be an array");
	assert_eq!(arr.len(), 1);
	assert!(arr[0].as_str().unwrap().to_lowercase().contains("08:00:2b"));
}

// ─── Null handling ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_all_nulls() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-null").await;

	let fields = [
		"colBool",
		"colInt2",
		"colInt4",
		"colInt8",
		"colFloat4",
		"colFloat8",
		"colNumeric",
		"colText",
		"colVarchar",
		"colBpchar",
		"colBytea",
		"colJson",
		"colJsonb",
		"colTimestamp",
		"colTimestamptz",
		"colDate",
		"colTime",
		"colInterval",
		"colUuid",
		"colBit",
		"colVarbit",
		"colInet",
		"colCidr",
		"colMacaddr",
		"colPoint",
		"colBox",
		"colEnum",
		"colOid",
		"colBoolArr",
		"colInt2Arr",
		"colInt4Arr",
		"colInt8Arr",
		"colFloat4Arr",
		"colFloat8Arr",
		"colTextArr",
		"colNumericArr",
		"colUuidArr",
		"colTimestampArr",
		"colTimestamptzArr",
		"colDateArr",
		"colTimeArr",
		"colJsonbArr",
		"colByteaArr",
		"colInetArr",
		"colMacaddrArr",
	];

	// Object types (Interval, Point) return {all-null-fields} when the DB column is NULL,
	// because async-graphql resolves subfields even when the parent is null.
	let is_null_or_all_null_object = |v: &serde_json::Value| -> bool {
		if v.is_null() {
			return true;
		}
		if let Some(obj) = v.as_object() {
			return obj.values().all(|v| v.is_null());
		}
		false
	};

	for field in &fields {
		assert!(
			is_null_or_all_null_object(&node[field]),
			"{field} should be null for the null row, got: {:?}",
			node[field]
		);
	}
}

// ─── Array column filters ───────────────────────────────────────────────────
//
// `{Scalar}ListFilter` must match the TypeScript service in shape and in the
// rows each operator selects. Row `type-test-1` holds the arrays listed in
// `ARRAY_COLUMNS`; row `type-test-null` holds NULL in every column.

/// Operators that compare the column with a whole array.
const ARRAY_OPERATORS: [&str; 11] = [
	"equalTo",
	"notEqualTo",
	"distinctFrom",
	"notDistinctFrom",
	"lessThan",
	"lessThanOrEqualTo",
	"greaterThan",
	"greaterThanOrEqualTo",
	"contains",
	"containedBy",
	"overlaps",
];

/// Operators that compare one value with the column's elements.
const ARRAY_ANY_OPERATORS: [&str; 6] = [
	"anyEqualTo",
	"anyNotEqualTo",
	"anyLessThan",
	"anyLessThanOrEqualTo",
	"anyGreaterThan",
	"anyGreaterThanOrEqualTo",
];

/// An array column of `type-test-1`, with operands written as GraphQL literals.
struct ArrayColumn {
	field: &'static str,
	/// The array the row stores.
	stored: &'static str,
	/// An array sharing exactly one element with `stored`.
	partial: &'static str,
	/// That shared element.
	element: &'static str,
	/// A value that is not an element of `stored`.
	absent: &'static str,
}

const fn array_column(
	field: &'static str,
	stored: &'static str,
	partial: &'static str,
	element: &'static str,
	absent: &'static str,
) -> ArrayColumn {
	ArrayColumn { field, stored, partial, element, absent }
}

/// Every array column the TypeScript service offers a filter for.
const ARRAY_COLUMNS: [ArrayColumn; 15] = [
	array_column("colBoolArr", "[true, false, true]", "[true]", "true", "false"),
	array_column("colInt2Arr", "[1, 2, 3]", "[2, 9]", "2", "9"),
	array_column("colInt4Arr", "[10, 20, 30]", "[20, 99]", "20", "99"),
	array_column("colInt8Arr", r#"["100", "200"]"#, r#"["200", "999"]"#, r#""200""#, r#""999""#),
	array_column("colFloat4Arr", "[1.1, 2.2]", "[2.2, 9.9]", "2.2", "9.9"),
	array_column("colFloat8Arr", "[3.3, 4.4]", "[4.4, 9.9]", "4.4", "9.9"),
	array_column(
		"colTextArr",
		r#"["foo", "bar", "baz"]"#,
		r#"["bar", "zzz"]"#,
		r#""bar""#,
		r#""zzz""#,
	),
	array_column(
		"colNumericArr",
		r#"["1.23", "4.56"]"#,
		r#"["4.56", "9.99"]"#,
		r#""4.56""#,
		r#""9.99""#,
	),
	array_column(
		"colUuidArr",
		r#"["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]"#,
		r#"["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]"#,
		r#""a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11""#,
		r#""b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11""#,
	),
	array_column(
		"colTimestampArr",
		r#"["2024-01-15T10:30:00", "2024-06-15T12:00:00"]"#,
		r#"["2024-06-15T12:00:00", "2030-01-01T00:00:00"]"#,
		r#""2024-06-15T12:00:00""#,
		r#""2030-01-01T00:00:00""#,
	),
	array_column(
		"colTimestamptzArr",
		r#"["2024-01-15T10:30:00+00:00"]"#,
		r#"["2024-01-15T10:30:00+00:00", "2030-01-01T00:00:00+00:00"]"#,
		r#""2024-01-15T10:30:00+00:00""#,
		r#""2030-01-01T00:00:00+00:00""#,
	),
	array_column(
		"colDateArr",
		r#"["2024-01-15", "2024-06-15"]"#,
		r#"["2024-06-15", "2030-01-01"]"#,
		r#""2024-06-15""#,
		r#""2030-01-01""#,
	),
	array_column(
		"colTimeArr",
		r#"["14:30:00", "08:00:00"]"#,
		r#"["08:00:00", "23:59:59"]"#,
		r#""08:00:00""#,
		r#""23:59:59""#,
	),
	array_column("colJsonbArr", "[{a: 1}, {b: 2}]", "[{b: 2}, {z: 9}]", "{b: 2}", "{z: 9}"),
	array_column(
		"colInetArr",
		r#"["192.168.1.1", "10.0.0.1"]"#,
		r#"["10.0.0.1", "8.8.8.8"]"#,
		r#""10.0.0.1""#,
		r#""8.8.8.8""#,
	),
];

/// Clients for both services, built once per test: building a client costs far
/// more than a request, and the operator matrix makes hundreds of them.
struct Services {
	ts: TestClient,
	rust: TestClient,
}

impl Services {
	fn new() -> Self {
		Self { ts: TestClient::new(&ts_url()), rust: TestClient::new(&rust_url()) }
	}

	/// Run `query` on both services, require an error-free Rust response that
	/// matches TypeScript, and return the ids of the `typeCoverages` rows selected.
	async fn type_coverage_ids(
		&self,
		label: &str,
		query: &str,
		variables: Option<Value>,
	) -> Vec<String> {
		let body = match variables {
			Some(variables) => serde_json::json!({ "query": query, "variables": variables }),
			None => serde_json::json!({ "query": query }),
		};
		let (ts, rust) = tokio::join!(
			self.ts.query_with_body(body.clone()),
			self.rust.query_with_body(body.clone()),
		);
		assert!(!has_graphql_errors(&rust), "{label}: unexpected errors: {rust:#?}");
		compare_responses(label, &ts, &rust);
		let mut ids: Vec<String> = rust["data"]["typeCoverages"]["nodes"]
			.as_array()
			.expect("typeCoverages nodes missing")
			.iter()
			.map(|n| n["id"].as_str().expect("id missing").to_string())
			.collect();
		ids.sort();
		ids
	}

	/// The ids selected by `filter`, a `TypeCoverageFilter` written as a GraphQL literal.
	async fn array_filter_ids(&self, filter: &str) -> Vec<String> {
		let query =
			format!("{{ typeCoverages(filter: {filter}) {{ totalCount nodes {{ id }} }} }}");
		self.type_coverage_ids(filter, &query, None).await
	}
}

/// Every `{Scalar}ListFilter` has the same fields, of the same types, as on the
/// TypeScript service.
#[tokio::test]
async fn test_list_filter_types_match_typescript() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let rust_client = TestClient::new(&rust_url());
	let ts_client = TestClient::new(&ts_url());

	for scalar in [
		"String",
		"Int",
		"BigInt",
		"Float",
		"BigFloat",
		"Boolean",
		"Date",
		"Datetime",
		"JSON",
		"UUID",
		"Time",
		"InternetAddress",
	] {
		let query = format!(
			r#"{{ __type(name: "{scalar}ListFilter") {{
                inputFields {{ name type {{ kind name ofType {{ kind name }} }} }}
            }} }}"#
		);
		let fields = |resp: Value| -> Vec<(String, String)> {
			let mut fields: Vec<(String, String)> = resp
				.pointer("/data/__type/inputFields")
				.and_then(|v| v.as_array())
				.unwrap_or_else(|| panic!("{scalar}ListFilter missing: {resp}"))
				.iter()
				.map(|f| (f["name"].as_str().unwrap().to_string(), f["type"].to_string()))
				.collect();
			fields.sort();
			fields
		};
		let ts = fields(ts_client.query(&query).await);
		let rust = fields(rust_client.query(&query).await);
		assert_eq!(rust.len(), 18, "{scalar}ListFilter should have 18 operators");
		assert_eq!(ts, rust, "{scalar}ListFilter fields differ from TypeScript");
	}
}

/// Every operator, on every array element type, selects the rows TypeScript does.
#[tokio::test]
async fn test_array_filter_operators_match_typescript() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let services = Services::new();

	let mut cases = 0;
	for col in &ARRAY_COLUMNS {
		let field = col.field;
		for op in ARRAY_OPERATORS {
			for operand in [col.stored, col.partial, "[]"] {
				services
					.array_filter_ids(&format!("{{ {field}: {{ {op}: {operand} }} }}"))
					.await;
				cases += 1;
			}
		}
		for op in ARRAY_ANY_OPERATORS {
			for operand in [col.element, col.absent] {
				services
					.array_filter_ids(&format!("{{ {field}: {{ {op}: {operand} }} }}"))
					.await;
				cases += 1;
			}
		}
		for operand in ["true", "false"] {
			services
				.array_filter_ids(&format!("{{ {field}: {{ isNull: {operand} }} }}"))
				.await;
			cases += 1;
		}
	}
	println!("array filters: {cases} operator/operand cases match TypeScript ✓");
}

/// The rows each family of operators selects, pinned so that a filter both
/// services got wrong would still be caught.
#[tokio::test]
async fn test_array_filter_selects_expected_rows() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let services = Services::new();
	let row = vec!["type-test-1".to_string()];
	let none: Vec<String> = vec![];

	// Equality compares the whole array, in order.
	assert_eq!(
		services
			.array_filter_ids(r#"{ colTextArr: { equalTo: ["foo", "bar", "baz"] } }"#)
			.await,
		row
	);
	assert_eq!(
		services
			.array_filter_ids(r#"{ colTextArr: { equalTo: ["baz", "bar", "foo"] } }"#)
			.await,
		none
	);
	assert_eq!(
		services
			.array_filter_ids(r#"{ colNumericArr: { equalTo: ["1.23", "4.56"] } }"#)
			.await,
		row
	);
	assert_eq!(services.array_filter_ids("{ colInt4Arr: { notEqualTo: [10, 20] } }").await, row);

	// Containment.
	assert_eq!(
		services
			.array_filter_ids(r#"{ colTextArr: { contains: ["baz", "foo"] } }"#)
			.await,
		row
	);
	assert_eq!(
		services
			.array_filter_ids(r#"{ colTextArr: { contains: ["foo", "zzz"] } }"#)
			.await,
		none
	);
	assert_eq!(
		services
			.array_filter_ids("{ colInt4Arr: { containedBy: [10, 20, 30, 40] } }")
			.await,
		row
	);
	assert_eq!(services.array_filter_ids("{ colInt4Arr: { containedBy: [10, 20] } }").await, none);

	// Overlap.
	assert_eq!(services.array_filter_ids("{ colInt4Arr: { overlaps: [30, 99] } }").await, row);
	assert_eq!(services.array_filter_ids("{ colInt4Arr: { overlaps: [98, 99] } }").await, none);
	assert_eq!(
		services.array_filter_ids(r#"{ colNumericArr: { overlaps: ["4.56"] } }"#).await,
		row
	);

	// `any…` reads as "some element <op> the value": the smallest element is 10.
	assert_eq!(services.array_filter_ids("{ colInt4Arr: { anyEqualTo: 20 } }").await, row);
	assert_eq!(services.array_filter_ids("{ colInt4Arr: { anyLessThan: 11 } }").await, row);
	assert_eq!(services.array_filter_ids("{ colInt4Arr: { anyLessThan: 10 } }").await, none);
	assert_eq!(
		services.array_filter_ids("{ colInt4Arr: { anyLessThanOrEqualTo: 10 } }").await,
		row
	);
	assert_eq!(
		services
			.array_filter_ids(r#"{ colNumericArr: { anyGreaterThan: "4.56" } }"#)
			.await,
		none
	);
	assert_eq!(
		services
			.array_filter_ids(r#"{ colNumericArr: { anyGreaterThanOrEqualTo: "4.56" } }"#)
			.await,
		row
	);
	assert_eq!(services.array_filter_ids(r#"{ colTextArr: { anyNotEqualTo: "foo" } }"#).await, row);

	// NULL arrays.
	assert_eq!(
		services.array_filter_ids("{ colTextArr: { isNull: true } }").await,
		vec!["type-test-null".to_string()]
	);
	assert_eq!(services.array_filter_ids("{ colTextArr: { isNull: false } }").await, row);
}

/// Operands that reach the SQL builder in a different form: a bare value in
/// place of a list, characters that are special in an array literal, variables,
/// and array filters combined with scalar and logical ones.
#[tokio::test]
async fn test_array_filter_operand_forms() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let services = Services::new();
	let row = vec!["type-test-1".to_string()];
	let none: Vec<String> = vec![];

	// GraphQL coerces a single value to a one-element list.
	assert_eq!(services.array_filter_ids(r#"{ colTextArr: { contains: "foo" } }"#).await, row);
	assert_eq!(services.array_filter_ids(r#"{ colTextArr: { contains: "zzz" } }"#).await, none);

	// Quotes, braces, commas and backslashes stay inside their element.
	assert_eq!(
		services
			.array_filter_ids(r#"{ colTextArr: { overlaps: ["a\"b,c}\\", "foo"] } }"#)
			.await,
		row
	);
	assert_eq!(
		services
			.array_filter_ids(r#"{ colTextArr: { overlaps: ["foo\",\"bar", "{foo}", "NULL"] } }"#)
			.await,
		none
	);
	assert_eq!(services.array_filter_ids("{ colTextArr: { contains: [null] } }").await, none);

	// JSON elements.
	assert_eq!(services.array_filter_ids("{ colJsonbArr: { contains: [{a: 1}] } }").await, row);
	assert_eq!(services.array_filter_ids(r#"{ colJsonbArr: { anyEqualTo: "a" } }"#).await, none);

	let query = "query($filter: TypeCoverageFilter) {
        typeCoverages(filter: $filter) { totalCount nodes { id } }
    }";
	let variables = serde_json::json!({ "filter": {
		"colInt8Arr": { "overlaps": ["200", "5"] },
		"colNumericArr": { "anyGreaterThanOrEqualTo": "4.56" },
		"colDateArr": { "contains": ["2024-06-15"] },
	}});
	assert_eq!(
		services
			.type_coverage_ids("array filters in variables", query, Some(variables))
			.await,
		row
	);

	assert_eq!(
		services
			.array_filter_ids(
				r#"{ or: [
                { colTextArr: { anyEqualTo: "zzz" } }
                { colInt2Arr: { containedBy: [1, 2, 3, 4] }, id: { startsWith: "type-test" } }
            ] }"#
			)
			.await,
		row
	);
}
