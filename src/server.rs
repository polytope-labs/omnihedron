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
//! - `POST /` — single and batch GraphQL queries
//! - `GET  /` — GraphiQL playground (when enabled)
//! - `GET  /health`  — liveness probe returning `200 OK`
//!
//! All query validation (depth, complexity, alias, batch limits) is applied
//! here before the request reaches the schema executor.

use std::sync::{
	Arc,
	atomic::{AtomicBool, AtomicUsize, Ordering},
};

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
use tracing::{debug, info, trace, warn};

use crate::{
	config::Config,
	resolvers::dataloader::RelationLoader,
	validation::{
		aliases::validate_aliases, batch::limit_batched_queries, complexity::validate_complexity,
		depth::validate_depth,
	},
};

/// Thread-safe shared reference to the live GraphQL schema, swapped atomically on hot reload.
pub type SharedSchema = Arc<RwLock<Schema>>;

/// Tracks schema reload state for health checks.
pub struct SchemaState {
	/// ISO 8601 timestamp of the last schema reload.
	pub last_reload_at: RwLock<String>,
	/// Number of tables in the current schema.
	pub table_count: RwLock<usize>,
	/// The schema name (for staleness checks).
	pub schema_name: String,
}

impl SchemaState {
	pub fn new(schema_name: String, table_count: usize) -> Self {
		Self {
			last_reload_at: RwLock::new(chrono::Utc::now().to_rfc3339()),
			table_count: RwLock::new(table_count),
			schema_name,
		}
	}

	pub async fn update(&self, table_count: usize) {
		*self.last_reload_at.write().await = chrono::Utc::now().to_rfc3339();
		*self.table_count.write().await = table_count;
	}
}

/// Shared state injected into every axum handler via [`axum::extract::State`].
#[derive(Clone)]
pub struct AppState {
	pub schema: SharedSchema,
	pub cfg: Arc<Config>,
	pub pool: Arc<deadpool_postgres::Pool>,
	/// Prometheus metrics handle, present only when `--metrics` is enabled.
	pub metrics_handle: Option<metrics_exporter_prometheus::PrometheusHandle>,
	/// Schema state for health check depth.
	pub schema_state: Arc<SchemaState>,
	/// Whether the server is shutting down (health returns 503).
	pub shutting_down: Arc<AtomicBool>,
	/// Number of in-flight requests.
	pub in_flight: Arc<AtomicUsize>,
}

/// Construct the axum [`Router`] with all routes, middleware, and CORS headers.
pub fn build_router(state: AppState) -> Router {
	let cfg = state.cfg.clone();

	// Application routes — these are tracked by in-flight and metrics middleware
	let mut app_router = Router::new().route("/", post(graphql_post_handler));

	if cfg.playground {
		app_router = app_router.route("/", get(graphiql_handler));
	}

	if cfg.subscription {
		app_router = app_router.route("/ws", get(graphql_ws_handler));
	}

	// Batch query limit middleware
	if let (Some(limit), false) = (cfg.query_batch_limit, cfg.unsafe_mode) {
		let lim = limit;
		app_router = app_router
			.layer(middleware::from_fn(move |req, next| limit_batched_queries(lim, req, next)));
	}

	// In-flight request tracking middleware
	app_router =
		app_router.layer(middleware::from_fn_with_state(state.clone(), in_flight_middleware));

	// Metrics middleware — records request count and duration per method/path/status
	if cfg.metrics {
		app_router =
			app_router.layer(middleware::from_fn_with_state(state.clone(), metrics_middleware));
	}

	// App routes get info-level request tracing
	app_router = app_router.layer(
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
	);

	// Infrastructure routes — excluded from in-flight tracking so health checks
	// don't delay graceful shutdown and don't inflate request metrics.
	let mut infra_router = Router::new().route("/health", get(health_handler));

	if cfg.metrics {
		infra_router = infra_router.route("/metrics", get(metrics_handler));
	}

	// Infra routes get trace-level request tracing
	infra_router = infra_router.layer(
		TraceLayer::new_for_http()
			.make_span_with(|req: &axum::http::Request<_>| {
				let request_id = &uuid::Uuid::new_v4().to_string()[..8];
				tracing::trace_span!("http",
					request_id = %request_id,
					method = %req.method(),
					path = %req.uri().path(),
				)
			})
			.on_response(
				|res: &axum::http::Response<_>,
				 latency: std::time::Duration,
				 _span: &tracing::Span| {
					trace!(status = %res.status(), duration_ms = latency.as_millis(), "request");
				},
			),
	);

	let router = Router::new().merge(infra_router).merge(app_router);

	router
		.layer(SetResponseHeaderLayer::if_not_present(
			axum::http::header::CACHE_CONTROL,
			axum::http::HeaderValue::from_static("public, max-age=5"),
		))
		.layer(CompressionLayer::new())
		.layer(CorsLayer::new().allow_origin(Any).allow_headers(Any).allow_methods(Any))
		.with_state(state)
}

async fn graphql_post_handler(State(state): State<AppState>, body: Bytes) -> Response {
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
			let (resp, _complexity) =
				execute_single(&schema, &state.cfg, &state.pool, gql_req).await;
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
	let (gql_resp, complexity) = execute_single(&schema, &state.cfg, &state.pool, gql_req).await;
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
	pool: &deadpool_postgres::Pool,
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

	// Inject schema name, dataloader, and shared per-request DB client into context
	let loader =
		async_graphql::dataloader::DataLoader::new(RelationLoader::new(pool.clone()), tokio::spawn);
	let req_client = match crate::db::RequestClient::new(pool).await {
		Ok(c) => std::sync::Arc::new(c),
		Err(e) => {
			return (
				GqlResponse::from_errors(vec![ServerError::new(
					format!("Database unavailable: {e}"),
					None,
				)]),
				computed_complexity,
			);
		},
	};
	let request = inner.data(cfg.name.clone()).data(loader).data(req_client);
	let resp = schema.execute(request).await;

	let duration = start.elapsed();
	let has_errors = !resp.errors.is_empty();
	debug!(
		operation = %operation,
		duration_ms = duration.as_millis(),
		has_errors,
		query = %query_preview,
		"GraphQL request completed"
	);

	// Determine query type from the operation/query for metrics
	let query_type = classify_query(&query_preview);

	// Record GraphQL metrics (no-op if recorder not installed)
	crate::metrics::record_graphql_query(&operation, query_type);
	crate::metrics::record_graphql_duration(query_type, duration.as_secs_f64());
	if let Some(c) = computed_complexity {
		crate::metrics::record_query_complexity(c);
	}
	if let Ok(serialized) = serde_json::to_vec(&resp) {
		crate::metrics::record_response_size(serialized.len());
	}
	if has_errors {
		crate::metrics::record_graphql_error();
	}

	(resp, computed_complexity)
}

/// Classify a query string into a type for metrics labeling.
fn classify_query(query: &str) -> &'static str {
	let q = query.to_lowercase();
	if q.contains("_metadata") {
		"metadata"
	} else if q.contains("aggregates") {
		"aggregate"
	} else if q.contains("subscription") {
		"subscription"
	} else if q.contains("bynodeid") || q.contains("node(") {
		"single"
	} else if q.contains("first") ||
		q.contains("last") ||
		q.contains("after") ||
		q.contains("before") ||
		q.contains("totalcount")
	{
		"connection"
	} else {
		"single"
	}
}

async fn graphql_ws_handler(
	State(state): State<AppState>,
	protocol: GraphQLProtocol,
	ws: WebSocketUpgrade,
) -> Response {
	let schema = state.schema.read().await.clone();
	ws.on_upgrade(move |stream| async move {
		metrics::gauge!(crate::metrics::WEBSOCKET_CONNECTIONS_ACTIVE).increment(1.0);
		GraphQLWebSocket::new(stream, schema, protocol).serve().await;
		metrics::gauge!(crate::metrics::WEBSOCKET_CONNECTIONS_ACTIVE).decrement(1.0);
	})
}

async fn graphiql_handler() -> impl IntoResponse {
	Html(GraphiQLSource::build().endpoint("/").subscription_endpoint("/ws").finish())
}

async fn health_handler(State(state): State<AppState>) -> Response {
	// During shutdown, immediately return 503 so load balancers remove us.
	if state.shutting_down.load(Ordering::Relaxed) {
		let body = serde_json::json!({
			"status": "shutting_down",
			"version": env!("CARGO_PKG_VERSION"),
		});
		return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
	}

	let pool_status = state.pool.status();

	// Check schema staleness state (no DB needed)
	let schema_state = &state.schema_state;
	let last_reload = schema_state.last_reload_at.read().await.clone();
	let table_count = *schema_state.table_count.read().await;

	// Single DB round-trip: connectivity check + schema staleness detection
	let db_start = std::time::Instant::now();
	let (db_ok, schema_status) =
		match tokio::time::timeout(std::time::Duration::from_secs(2), async {
			let client = state.pool.get().await.map_err(|_| ())?;
			let row = client
				.query_one(
					"SELECT COUNT(*)::int AS cnt FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'",
					&[&schema_state.schema_name],
				)
				.await
				.map_err(|_| ())?;
			let current_count: i32 = row.get("cnt");
			Ok::<_, ()>(current_count)
		})
		.await
		{
			Ok(Ok(current_count)) => {
				let status =
					if current_count as usize == table_count { "current" } else { "stale" };
				(true, status)
			},
			_ => (false, "unknown"),
		};
	let db_latency_ms = db_start.elapsed().as_secs_f64() * 1000.0;

	let overall_status = if !db_ok {
		"unhealthy"
	} else if schema_status == "stale" {
		"degraded"
	} else {
		"healthy"
	};

	let http_status = if db_ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };

	let body = serde_json::json!({
		"status": overall_status,
		"checks": {
			"database": {
				"status": if db_ok { "up" } else { "down" },
				"latencyMs": (db_latency_ms * 100.0).round() / 100.0,
				"pool": {
					"size": pool_status.size,
					"available": pool_status.available,
					"maxSize": pool_status.max_size,
				}
			},
			"schema": {
				"status": schema_status,
				"lastReloadAt": last_reload,
				"tableCount": table_count,
			}
		},
		"version": env!("CARGO_PKG_VERSION"),
	});

	// Update pool metrics while we have the info
	crate::metrics::set_db_pool_metrics(
		pool_status.size as f64,
		pool_status.available as f64,
		pool_status.max_size as f64,
	);

	(http_status, Json(body)).into_response()
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
	match &state.metrics_handle {
		Some(handle) => (StatusCode::OK, handle.render()),
		None => (StatusCode::NOT_FOUND, "Metrics not enabled".to_string()),
	}
}

/// Middleware to track in-flight requests.
async fn in_flight_middleware(
	State(state): State<AppState>,
	req: axum::extract::Request,
	next: middleware::Next,
) -> Response {
	let count = state.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
	crate::metrics::set_in_flight_requests(count);
	let response = next.run(req).await;
	let count = state.in_flight.fetch_sub(1, Ordering::Relaxed) - 1;
	crate::metrics::set_in_flight_requests(count);
	response
}

async fn metrics_middleware(
	State(state): State<AppState>,
	req: axum::extract::Request,
	next: middleware::Next,
) -> Response {
	let method = req.method().to_string();
	let path = req.uri().path().to_string();
	let start = std::time::Instant::now();

	let response = next.run(req).await;

	let status = response.status().as_u16().to_string();
	let duration = start.elapsed().as_secs_f64();

	metrics::counter!(
		crate::metrics::HTTP_REQUESTS_TOTAL,
		"method" => method.clone(),
		"path" => path.clone(),
		"status" => status,
	)
	.increment(1);

	metrics::histogram!(
		crate::metrics::HTTP_REQUEST_DURATION_SECONDS,
		"method" => method,
		"path" => path,
	)
	.record(duration);

	// Refresh pool gauges on every request so Grafana always has data
	let pool_status = state.pool.status();
	crate::metrics::set_db_pool_metrics(
		pool_status.size as f64,
		pool_status.available as f64,
		pool_status.max_size as f64,
	);

	response
}
