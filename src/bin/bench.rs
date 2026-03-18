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

// src/bin/bench.rs
//
// Concurrent HTTP benchmarking tool for omnihedron services.
//
// Usage:
//   cargo run --bin bench -- \
//     --url http://localhost:3000 \
//     --concurrency 50 \
//     --requests 1000 \
//     --label "Rust service"
//
// To compare both services:
//   cargo run --bin bench -- --url http://localhost:3000 --label Rust
//   cargo run --bin bench -- --url http://localhost:3001 --label TypeScript

use std::{
	sync::Arc,
	time::{Duration, Instant},
};

use clap::Parser;
use tokio::sync::Semaphore;

#[derive(Parser, Debug)]
#[command(name = "bench", about = "Concurrent GraphQL benchmarking tool for omnihedron services")]
struct BenchArgs {
	/// Target URL (without /graphql — it's appended automatically)
	#[arg(long, default_value = "http://localhost:3000")]
	url: String,

	/// Number of concurrent workers
	#[arg(long, default_value_t = 50)]
	concurrency: usize,

	/// Total number of requests to send
	#[arg(long, default_value_t = 1000)]
	requests: usize,

	/// JSON body to POST (defaults to a simple metadata query)
	#[arg(long, default_value = r#"{"query":"{ _metadata { lastProcessedHeight } }"}"#)]
	body: String,

	/// Human-readable label for this benchmark run
	#[arg(long, default_value = "Benchmark")]
	label: String,

	/// Print each request's latency (verbose — slows output)
	#[arg(long, default_value_t = false)]
	verbose: bool,
}

#[derive(Debug)]
struct RequestResult {
	latency: Duration,
	success: bool,
	#[allow(dead_code)]
	status: u16,
}

#[tokio::main]
async fn main() {
	let args = BenchArgs::parse();

	let graphql_url = format!("{}/graphql", args.url.trim_end_matches('/'));

	println!("========================================");
	println!(" {}", args.label);
	println!("========================================");
	println!("  URL         : {}", graphql_url);
	println!("  Concurrency : {}", args.concurrency);
	println!("  Requests    : {}", args.requests);
	println!("  Body        : {}", args.body);
	println!("----------------------------------------");

	let client = reqwest::Client::builder()
		.timeout(Duration::from_secs(30))
		.pool_max_idle_per_host(args.concurrency + 10)
		.build()
		.expect("Failed to build HTTP client");

	let client = Arc::new(client);
	let semaphore = Arc::new(Semaphore::new(args.concurrency));
	let body = Arc::new(args.body.clone());
	let graphql_url = Arc::new(graphql_url);

	let mut handles = Vec::with_capacity(args.requests);
	let overall_start = Instant::now();

	for req_num in 0..args.requests {
		let permit = semaphore.clone().acquire_owned().await.expect("semaphore closed");
		let client = client.clone();
		let body = body.clone();
		let url = graphql_url.clone();
		let verbose = args.verbose;

		let handle = tokio::spawn(async move {
			let start = Instant::now();
			let result: Result<reqwest::Response, reqwest::Error> = client
				.post(url.as_str())
				.header("Content-Type", "application/json")
				.body(body.as_str().to_string())
				.send()
				.await;

			let latency = start.elapsed();

			let (success, status) = match result {
				Ok(resp) => {
					let s = resp.status().as_u16();
					// Drain body to ensure connection is returned to pool
					let _ = resp.bytes().await;
					(s < 400, s)
				},
				Err(e) => {
					if verbose {
						eprintln!("Request #{} error: {}", req_num, e);
					}
					(false, 0u16)
				},
			};

			drop(permit); // Release semaphore slot

			if verbose {
				println!("  #{:5}  {}ms  status={}", req_num, latency.as_millis(), status);
			}

			RequestResult { latency, success, status }
		});

		handles.push(handle);
	}

	// Collect results
	let mut results: Vec<RequestResult> = Vec::with_capacity(args.requests);
	for handle in handles {
		match handle.await {
			Ok(r) => results.push(r),
			Err(e) => eprintln!("Task panicked: {}", e),
		}
	}

	let total_elapsed = overall_start.elapsed();

	// Compute statistics
	let total = results.len();
	let successes = results.iter().filter(|r| r.success).count();
	let failures = total - successes;

	let mut latencies_ms: Vec<f64> =
		results.iter().map(|r| r.latency.as_secs_f64() * 1000.0).collect();
	latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());

	let mean_ms = if latencies_ms.is_empty() {
		0.0
	} else {
		latencies_ms.iter().sum::<f64>() / latencies_ms.len() as f64
	};

	let min_ms = latencies_ms.first().copied().unwrap_or(0.0);
	let max_ms = latencies_ms.last().copied().unwrap_or(0.0);

	let p50_ms = percentile(&latencies_ms, 50.0);
	let p90_ms = percentile(&latencies_ms, 90.0);
	let p99_ms = percentile(&latencies_ms, 99.0);

	let rps = total as f64 / total_elapsed.as_secs_f64();

	println!("----------------------------------------");
	println!(" Results for: {}", args.label);
	println!("----------------------------------------");
	println!("  Total requests : {}", total);
	println!("  Successes      : {}", successes);
	println!("  Failures       : {}", failures);
	println!("  Total time     : {:.2}s", total_elapsed.as_secs_f64());
	println!("  Req/sec        : {:.2}", rps);
	println!("----------------------------------------");
	println!("  Latency (ms):");
	println!("    Min          : {:.2}", min_ms);
	println!("    Mean         : {:.2}", mean_ms);
	println!("    p50          : {:.2}", p50_ms);
	println!("    p90          : {:.2}", p90_ms);
	println!("    p99          : {:.2}", p99_ms);
	println!("    Max          : {:.2}", max_ms);
	println!("========================================");

	// Machine-readable output for bench_compare.sh
	// Format: | {label} | {concurrency} | {requests} | {rps:.2} | p50={p50:.2}ms p99={p99:.2}ms |
	println!(
		"| {label:<20} | {concurrency:>11} | {requests:>8} | {rps:>12.2} | p50={p50:.2}ms p90={p90:.2}ms p99={p99:.2}ms |",
		label = args.label,
		concurrency = args.concurrency,
		requests = total,
		rps = rps,
		p50 = p50_ms,
		p90 = p90_ms,
		p99 = p99_ms,
	);

	if failures > 0 {
		std::process::exit(1);
	}
}

/// Compute the p-th percentile from a sorted slice of values.
fn percentile(sorted: &[f64], p: f64) -> f64 {
	if sorted.is_empty() {
		return 0.0;
	}
	let idx = (p / 100.0 * (sorted.len() as f64 - 1.0)).round() as usize;
	sorted[idx.min(sorted.len() - 1)]
}
