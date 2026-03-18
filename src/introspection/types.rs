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

//! PostgreSQL → GraphQL type mapping.
//!
//! [`pg_type_to_graphql`] is the single source of truth for how database
//! column types are surfaced in the generated schema.  The `is_numeric` flag
//! returned alongside the type name drives aggregation eligibility (sum, min,
//! max, avg) in the schema builder.

/// Map a PostgreSQL column type to a GraphQL scalar type name.
///
/// Returns `(graphql_type_name, is_numeric)` where `is_numeric` indicates
/// the column is eligible for numeric aggregation functions (sum/avg/stddev).
///
/// `pg_type` is the `data_type` value from `information_schema.columns`;
/// `udt_name` is the `udt_name` value (used as a fallback for types that
/// report as a catch-all category such as `"ARRAY"`).
pub fn pg_type_to_graphql(pg_type: &str, udt_name: &str) -> (&'static str, bool) {
	// Array types in PostgreSQL have udt_name prefixed with `_`
	if udt_name.starts_with('_') {
		// Represent arrays as JSON; element-level handling can be added later.
		return ("JSON", false);
	}

	match pg_type {
		"boolean" => ("Boolean", false),
		"smallint" | "integer" => ("Int", true),
		"bigint" => ("BigInt", true),
		"real" | "double precision" => ("Float", true),
		"numeric" | "decimal" => ("BigFloat", true),
		"character varying" | "character" | "text" | "citext" | "name" => ("String", false),
		"bytea" => ("String", false), // hex-encoded
		"date" => ("Date", false),
		"timestamp without time zone" | "timestamp with time zone" => ("Datetime", false),
		"time without time zone" | "time with time zone" => ("String", false),
		"interval" => ("String", false),
		"uuid" => ("String", false),
		"json" | "jsonb" => ("JSON", false),
		"inet" | "cidr" | "macaddr" => ("String", false),
		"bit" | "bit varying" => ("String", false),
		"money" => ("String", true),
		"xml" => ("String", false),
		"USER-DEFINED" => {
			// Enums and custom types — treated as String by default; the schema
			// builder resolves the actual enum display name separately.
			("String", false)
		},
		"ARRAY" => ("JSON", false),
		_ => {
			// Fallback: use udt_name heuristics for types not captured above.
			match udt_name {
				"int2" | "int4" => ("Int", true),
				"int8" => ("BigInt", true),
				"float4" | "float8" => ("Float", true),
				"numeric" | "decimal" => ("BigFloat", true),
				"bool" => ("Boolean", false),
				"json" | "jsonb" => ("JSON", false),
				"uuid" => ("String", false),
				"timestamp" | "timestamptz" => ("Datetime", false),
				"date" => ("Date", false),
				_ => ("String", false),
			}
		},
	}
}

/// Returns `true` if the PostgreSQL type maps to a numeric GraphQL scalar and
/// is therefore eligible for aggregation functions (sum, min, max, avg).
///
/// Equivalent to `pg_type_to_graphql(pg_type, udt_name).1`.
#[allow(dead_code)]
pub fn is_numeric_pg_type(pg_type: &str, udt_name: &str) -> bool {
	pg_type_to_graphql(pg_type, udt_name).1
}

/// Returns `true` if `graphql_type` supports ordering comparisons (i.e. can
/// appear in `lessThan` / `greaterThan` filter operators).
#[allow(dead_code)]
pub fn is_comparable_type(graphql_type: &str) -> bool {
	matches!(graphql_type, "Int" | "BigInt" | "Float" | "BigFloat" | "String" | "Date" | "Datetime")
}
