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

use axum::{
	body::Body,
	extract::Request,
	http::StatusCode,
	middleware::Next,
	response::{IntoResponse, Response},
};
use serde_json::Value;

/// Middleware that rejects batched GraphQL requests exceeding the configured limit.
///
/// GraphQL batch requests are POST bodies where the JSON is an array:
/// `[{query: "..."}, {query: "..."}]`
pub async fn limit_batched_queries(limit: usize, req: Request, next: Next) -> Response {
	if req.method() != axum::http::Method::POST {
		return next.run(req).await;
	}

	// Buffer the body to inspect it
	let (parts, body) = req.into_parts();
	let bytes = match axum::body::to_bytes(body, 1024 * 1024).await {
		Ok(b) => b,
		Err(_) => {
			return (StatusCode::BAD_REQUEST, "Failed to read request body").into_response();
		},
	};

	// Check if body is a JSON array
	if let Ok(Value::Array(arr)) = serde_json::from_slice::<Value>(&bytes) {
		if arr.len() > limit {
			return (
				StatusCode::BAD_REQUEST,
				axum::Json(serde_json::json!({
					"errors": [{ "message": "Batch query limit exceeded" }]
				})),
			)
				.into_response();
		}
	}

	// Reconstruct request with original body
	let req = Request::from_parts(parts, Body::from(bytes));
	next.run(req).await
}
