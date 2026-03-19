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
                    colInterval
                    colUuid
                    colBit
                    colVarbit
                    colInet
                    colCidr
                    colMacaddr
                    colPoint
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
	assert!(
		!has_graphql_errors(&resp),
		"type_coverage query returned errors: {}",
		resp
	);
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
	let val = node["colInterval"].as_str().expect("colInterval should be a string");
	assert!(!val.is_empty(), "colInterval should not be empty, got '{val}'");
}

#[tokio::test]
async fn test_uuid_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	assert_eq!(
		node["colUuid"],
		serde_json::json!("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	);
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
	assert!(
		val.contains("192.168.1.1"),
		"colInet = '{val}', expected '192.168.1.1'"
	);
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
	assert!(
		val.contains("10.0.0.0"),
		"colCidr = '{val}', expected to contain '10.0.0.0'"
	);
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
	assert_eq!(
		node["colUuidArr"],
		serde_json::json!(["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"])
	);
}

#[tokio::test]
async fn test_timestamp_array_serialization() {
	if !services_available() {
		eprintln!("SKIP");
		return;
	}
	let client = TestClient::new(&rust_url());
	let node = get_type_coverage_node(&client, "type-test-1").await;
	let arr = node["colTimestampArr"]
		.as_array()
		.expect("colTimestampArr should be an array");
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
	assert_eq!(
		node["colDateArr"],
		serde_json::json!(["2024-01-15", "2024-06-15"])
	);
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
		"colBool", "colInt2", "colInt4", "colInt8",
		"colFloat4", "colFloat8", "colNumeric",
		"colText", "colVarchar", "colBpchar",
		"colBytea", "colJson", "colJsonb",
		"colTimestamp", "colTimestamptz", "colDate", "colTime",
		"colInterval", "colUuid",
		"colBit", "colVarbit",
		"colInet", "colCidr", "colMacaddr",
		"colPoint", "colBox",
		"colEnum", "colOid",
		"colBoolArr", "colInt2Arr", "colInt4Arr", "colInt8Arr",
		"colFloat4Arr", "colFloat8Arr",
		"colTextArr", "colNumericArr", "colUuidArr",
		"colTimestampArr", "colTimestamptzArr",
		"colDateArr", "colTimeArr",
		"colJsonbArr", "colByteaArr",
		"colInetArr", "colMacaddrArr",
	];

	for field in &fields {
		assert!(
			node[field].is_null(),
			"{field} should be null for the null row, got: {:?}",
			node[field]
		);
	}
}
