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
	http::{HeaderMap, StatusCode},
	middleware,
	response::{Html, IntoResponse, Json, Response},
	routing::{get, post},
};
use serde_json::Value;
use tokio::sync::RwLock;
use tower_http::{
	compression::CompressionLayer,
	cors::{Any, CorsLayer},
};

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
		.layer(CompressionLayer::new())
		.layer(CorsLayer::new().allow_origin(Any).allow_headers(Any).allow_methods(Any))
		.with_state(state)
}

async fn graphql_handler(
	State(state): State<AppState>,
	headers: HeaderMap,
	body: Bytes,
) -> Response {
	// Detect batch (JSON array) vs single request
	let body_val: Value = match serde_json::from_slice(&body) {
		Ok(v) => v,
		Err(e) => {
			return (StatusCode::BAD_REQUEST, format!("Invalid JSON: {e}")).into_response();
		},
	};

	if body_val.is_array() {
		// Batch request
		let items = body_val.as_array().unwrap();
		let schema = state.schema.read().await;
		let mut responses = Vec::with_capacity(items.len());
		for item in items {
			let gql_req: GqlRequest = match serde_json::from_value(item.clone()) {
				Ok(r) => r,
				Err(e) => {
					responses.push(
						serde_json::to_value(GqlResponse::from_errors(vec![ServerError::new(
							format!("Invalid request: {e}"),
							None,
						)]))
						.unwrap_or(Value::Null),
					);
					continue;
				},
			};
			let resp = execute_single(&schema, &state.cfg, gql_req).await;
			responses.push(serde_json::to_value(&resp).unwrap_or(Value::Null));
		}
		return Json(Value::Array(responses)).into_response();
	}

	// Single request — re-use the normal flow via axum extractor
	let gql_req: GqlRequest = match serde_json::from_value(body_val) {
		Ok(r) => r,
		Err(e) => {
			return (StatusCode::BAD_REQUEST, format!("Invalid GraphQL request: {e}"))
				.into_response();
		},
	};

	// Check GET-based forwarding via query params isn't needed here; body is always present
	let _ = headers; // headers available if needed later
	let schema = state.schema.read().await;
	let resp: GraphQLResponse = execute_single(&schema, &state.cfg, gql_req).await.into();
	resp.into_response()
}

async fn execute_single(schema: &Schema, cfg: &Config, inner: GqlRequest) -> GqlResponse {
	// Pre-execution validation (unless unsafe mode)
	if !cfg.unsafe_mode {
		if let Ok(doc) = parse_query(&inner.query) {
			let mut errors: Vec<ServerError> = vec![];

			if let Some(max_depth) = cfg.query_depth_limit {
				if let Err(e) = validate_depth(&doc, max_depth) {
					errors.push(e);
				}
			}

			if let Some(max_complexity) = cfg.query_complexity {
				if let Err(e) = validate_complexity(&doc, max_complexity) {
					errors.push(e);
				}
			}

			if let Some(max_aliases) = cfg.query_alias_limit {
				if let Err(e) = validate_aliases(&doc, max_aliases) {
					errors.push(e);
				}
			}

			if !errors.is_empty() {
				return GqlResponse::from_errors(errors);
			}
		}
	}

	// Inject schema name into request context for resolvers
	let request = inner.data(cfg.name.clone());
	schema.execute(request).await
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
