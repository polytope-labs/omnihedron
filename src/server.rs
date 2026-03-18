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

//! HTTP server: axum router, request handling, WebSocket subscriptions, and GraphiQL playground.
//!
//! Exposes three endpoints:
//! - `POST /graphql` — single and batch GraphQL queries
//! - `GET  /graphql` — WebSocket upgrade for subscriptions (when enabled)
//! - `GET  /health`  — liveness probe returning `200 OK`
//!
//! All query validation (depth, complexity, alias, batch limits) is applied
//! here before the request reaches the schema executor.

use std::sync::Arc;

use async_graphql::{
	Request as GqlRequest, Response as GqlResponse, ServerError, dynamic::Schema,
	http::GraphiQLSource, parser::parse_query,
};
use async_graphql_axum::{GraphQLProtocol, GraphQLResponse, GraphQLWebSocket};
use axum::{
	Router,
	body::Bytes,
	extract::{State, WebSocketUpgrade},
	http::StatusCode,
	middleware,
	response::{Html, IntoResponse, Json, Response},
	routing::{get, post},
};
use serde_json::Value;
use tokio::sync::RwLock;
use tower_http::{
	compression::CompressionLayer,
	cors::{Any, CorsLayer},
	set_header::SetResponseHeaderLayer,
	trace::TraceLayer,
};
use tracing::{debug, info, warn};

use crate::{
	config::Config,
	validation::{
		aliases::validate_aliases, batch::limit_batched_queries, complexity::validate_complexity,
		depth::validate_depth,
	},
};

/// Thread-safe shared reference to the live GraphQL schema, swapped atomically on hot reload.
pub type SharedSchema = Arc<RwLock<Schema>>;

/// Shared state injected into every axum handler via [`axum::extract::State`].
#[derive(Clone)]
pub struct AppState {
	pub schema: SharedSchema,
	pub cfg: Arc<Config>,
}

/// Construct the axum [`Router`] with all routes, middleware, and CORS headers.
pub fn build_router(state: AppState) -> Router {
	let cfg = state.cfg.clone();

	let mut router = Router::new()
		.route("/graphql", post(graphql_handler).get(graphql_handler))
		.route("/health", get(health_handler));

	if cfg.subscription {
		router = router.route("/graphql/ws", get(graphql_ws_handler));
	}

	if cfg.playground {
		router = router.route("/", get(graphiql_handler));
	}

	// Batch query limit middleware
	if let (Some(limit), false) = (cfg.query_batch_limit, cfg.unsafe_mode) {
		let lim = limit;
		router = router
			.layer(middleware::from_fn(move |req, next| limit_batched_queries(lim, req, next)));
	}

	router
		.layer(
			TraceLayer::new_for_http()
				.make_span_with(|req: &axum::http::Request<_>| {
					let request_id = &uuid::Uuid::new_v4().to_string()[..8];
					tracing::info_span!("http",
						request_id = %request_id,
						method = %req.method(),
						path = %req.uri().path(),
					)
				})
				.on_response(
					|res: &axum::http::Response<_>,
					 latency: std::time::Duration,
					 _span: &tracing::Span| {
						info!(status = %res.status(), duration_ms = latency.as_millis(), "request");
					},
				),
		)
		.layer(SetResponseHeaderLayer::if_not_present(
			axum::http::header::CACHE_CONTROL,
			axum::http::HeaderValue::from_static("public, max-age=5"),
		))
		.layer(CompressionLayer::new())
		.layer(CorsLayer::new().allow_origin(Any).allow_headers(Any).allow_methods(Any))
		.with_state(state)
}

async fn graphql_handler(State(state): State<AppState>, body: Bytes) -> Response {
	// Detect batch (JSON array) vs single request
	let body_val: Value = match serde_json::from_slice(&body) {
		Ok(v) => v,
		Err(e) => {
			return (StatusCode::BAD_REQUEST, format!("Invalid JSON: {e}")).into_response();
		},
	};

	if body_val.is_array() {
		let items = body_val.as_array().unwrap();
		let schema = state.schema.read().await;
		let mut responses = Vec::with_capacity(items.len());
		for item in items {
			let gql_req: GqlRequest = match serde_json::from_value(item.clone()) {
				Ok(r) => r,
				Err(e) => {
					let err_resp = GqlResponse::from_errors(vec![ServerError::new(
						format!("Invalid request: {e}"),
						None,
					)]);
					match serde_json::to_value(err_resp) {
						Ok(v) => responses.push(v),
						Err(e) => warn!(error = %e, "Failed to serialize error response"),
					}
					continue;
				},
			};
			let (resp, _complexity) = execute_single(&schema, &state.cfg, gql_req).await;
			match serde_json::to_value(&resp) {
				Ok(v) => responses.push(v),
				Err(e) => warn!(error = %e, "Failed to serialize GraphQL response"),
			}
		}
		return Json(Value::Array(responses)).into_response();
	}

	let gql_req: GqlRequest = match serde_json::from_value(body_val) {
		Ok(r) => r,
		Err(e) => {
			return (StatusCode::BAD_REQUEST, format!("Invalid GraphQL request: {e}"))
				.into_response();
		},
	};

	let schema = state.schema.read().await;
	let (gql_resp, complexity) = execute_single(&schema, &state.cfg, gql_req).await;
	let mut resp = GraphQLResponse::from(gql_resp).into_response();
	if let Some(c) = complexity {
		if let Ok(v) = axum::http::HeaderValue::from_str(&c.to_string()) {
			resp.headers_mut().insert("query-complexity", v);
		}
	}
	if let Some(max) = state.cfg.query_complexity {
		if let Ok(v) = axum::http::HeaderValue::from_str(&max.to_string()) {
			resp.headers_mut().insert("max-query-complexity", v);
		}
	}
	resp
}

/// Returns `(response, computed_complexity)`.
async fn execute_single(
	schema: &Schema,
	cfg: &Config,
	inner: GqlRequest,
) -> (GqlResponse, Option<usize>) {
	let start = std::time::Instant::now();
	let operation = inner.operation_name.as_deref().unwrap_or("<anonymous>").to_string();
	// Collapse whitespace for a compact single-line query preview
	let query_preview: String = inner.query.split_whitespace().collect::<Vec<_>>().join(" ");

	let mut computed_complexity: Option<usize> = None;

	// Pre-execution validation
	if let Ok(doc) = parse_query(&inner.query) {
		// Always compute complexity for the response header
		if let Ok(c) = validate_complexity(&doc, usize::MAX) {
			computed_complexity = Some(c);
		}

		if !cfg.unsafe_mode {
			let mut errors: Vec<ServerError> = vec![];

			if let Some(max_depth) = cfg.query_depth_limit {
				if let Err(e) = validate_depth(&doc, max_depth) {
					errors.push(e);
				}
			}

			if let Some(max_complexity) = cfg.query_complexity {
				if let Some(c) = computed_complexity {
					if c > max_complexity {
						errors.push(ServerError::new(
							format!(
								"Query complexity {c} exceeds maximum allowed complexity {max_complexity}."
							),
							None,
						));
					}
				}
			}

			if let Some(max_aliases) = cfg.query_alias_limit {
				if let Err(e) = validate_aliases(&doc, max_aliases) {
					errors.push(e);
				}
			}

			if !errors.is_empty() {
				return (GqlResponse::from_errors(errors), computed_complexity);
			}
		}
	}

	// Inject schema name into request context for resolvers
	let request = inner.data(cfg.name.clone());
	let resp = schema.execute(request).await;

	let has_errors = !resp.errors.is_empty();
	debug!(
		operation = %operation,
		duration_ms = start.elapsed().as_millis(),
		has_errors,
		query = %query_preview,
		"GraphQL request completed"
	);

	(resp, computed_complexity)
}

async fn graphql_ws_handler(
	State(state): State<AppState>,
	protocol: GraphQLProtocol,
	ws: WebSocketUpgrade,
) -> Response {
	let schema = state.schema.read().await.clone();
	ws.on_upgrade(move |stream| GraphQLWebSocket::new(stream, schema, protocol).serve())
}

async fn graphiql_handler() -> impl IntoResponse {
	Html(
		GraphiQLSource::build()
			.endpoint("/graphql")
			.subscription_endpoint("/graphql/ws")
			.finish(),
	)
}

async fn health_handler() -> impl IntoResponse {
	(StatusCode::OK, "OK")
}
