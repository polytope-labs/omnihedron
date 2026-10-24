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

//! `omnihedron healthcheck` — probe a running server's `/health` endpoint.
//!
//! Used as the container `HEALTHCHECK`, so this path must be cheap: it runs on
//! a current-thread runtime and never touches the database or the tracing
//! subscriber.

use std::time::Duration;

use crate::config::HealthcheckArgs;

/// Maximum number of body characters kept in probe messages (`docker inspect`
/// only keeps a little probe output).
const MAX_BODY_CHARS: usize = 512;

/// GET `url` and return a short `"{status} {body}"` summary on a 2xx response.
///
/// Any other outcome — non-2xx status, redirect, transport error, timeout — is
/// an error. Redirects are deliberately not followed: only a real 2xx counts.
pub async fn probe(url: &str, timeout: Duration) -> Result<String, String> {
	let client = reqwest::Client::builder()
		.timeout(timeout)
		// Never let an HTTP_PROXY in the container intercept a localhost probe.
		.no_proxy()
		.redirect(reqwest::redirect::Policy::none())
		.build()
		.map_err(|e| format!("failed to build HTTP client: {e}"))?;

	let response = match client.get(url).send().await {
		Ok(response) => response,
		Err(e) => return Err(describe_error(e, url, timeout)),
	};
	let status = response.status();
	let body = match response.text().await {
		Ok(body) => body,
		Err(e) => return Err(format!("{status}: {}", describe_error(e, url, timeout))),
	};
	let summary = format!("{status} {}", truncate(body.trim()));

	if status.is_success() { Ok(summary) } else { Err(summary) }
}

/// Render a reqwest transport error with its cause chain: its own `Display`
/// only prints "error sending request for url (...)", which makes refused,
/// timed-out and DNS failures look identical in `docker inspect`.
fn describe_error(e: reqwest::Error, url: &str, timeout: Duration) -> String {
	if e.is_timeout() {
		return format!("timed out after {timeout:?} ({url})");
	}
	format!("{:#}", anyhow::Error::from(e))
}

/// Run the probe described by `args` and return the process exit code.
///
/// 0 means healthy, 1 means anything else. Docker reserves exit code 2 for its
/// own use, so it is never returned.
pub fn run(args: &HealthcheckArgs) -> i32 {
	let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
		Ok(runtime) => runtime,
		Err(e) => {
			eprintln!("unhealthy: failed to start async runtime: {e}");
			return 1;
		},
	};

	let result = runtime.block_on(probe(&args.target_url(), Duration::from_secs(args.timeout)));
	// Dropping the runtime waits for the blocking pool (reqwest resolves names
	// via spawn_blocking), which can outlive the probe timeout. Don't block.
	runtime.shutdown_background();

	match result {
		Ok(summary) => {
			println!("{summary}");
			0
		},
		Err(e) => {
			eprintln!("unhealthy: {e}");
			1
		},
	}
}

fn truncate(body: &str) -> String {
	body.chars().take(MAX_BODY_CHARS).collect()
}

#[cfg(test)]
mod tests {
	use std::net::SocketAddr;

	use axum::{Router, http::StatusCode, response::Redirect, routing::get};

	use super::*;

	/// Spawn `app` on an ephemeral port and return its address.
	async fn serve(app: Router) -> SocketAddr {
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
		addr
	}

	fn url(addr: SocketAddr, path: &str) -> String {
		format!("http://{addr}{path}")
	}

	#[tokio::test]
	async fn ok_on_2xx() {
		let app = Router::new().route("/health", get(|| async { "healthy" }));
		let addr = serve(app).await;

		let summary = probe(&url(addr, "/health"), Duration::from_secs(2)).await.unwrap();
		assert!(summary.contains("200"), "{summary}");
	}

	#[tokio::test]
	async fn err_on_503() {
		let app = Router::new()
			.route("/health", get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "down") }));
		let addr = serve(app).await;

		let err = probe(&url(addr, "/health"), Duration::from_secs(2)).await.unwrap_err();
		assert!(err.contains("503"), "{err}");
	}

	#[tokio::test]
	async fn err_on_404() {
		let app = Router::new().route("/health", get(|| async { "healthy" }));
		let addr = serve(app).await;

		assert!(probe(&url(addr, "/nope"), Duration::from_secs(2)).await.is_err());
	}

	#[tokio::test]
	async fn redirects_are_not_followed() {
		let app = Router::new()
			.route("/health", get(|| async { Redirect::temporary("/moved") }))
			.route("/moved", get(|| async { "healthy" }));
		let addr = serve(app).await;

		let err = probe(&url(addr, "/health"), Duration::from_secs(2)).await.unwrap_err();
		assert!(err.contains("307"), "{err}");
	}

	#[tokio::test]
	async fn err_on_timeout() {
		let app = Router::new().route(
			"/health",
			get(|| async {
				tokio::time::sleep(Duration::from_secs(2)).await;
				"healthy"
			}),
		);
		let addr = serve(app).await;

		let err = probe(&url(addr, "/health"), Duration::from_millis(200)).await.unwrap_err();
		assert!(err.contains("timed out"), "{err}");
	}

	#[tokio::test]
	async fn err_on_connection_refused() {
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		drop(listener);

		let err = probe(&url(addr, "/health"), Duration::from_secs(2)).await.unwrap_err();
		assert!(err.to_lowercase().contains("refused"), "{err}");
	}
}
