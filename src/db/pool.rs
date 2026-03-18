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

//! `deadpool-postgres` connection pool factory.
//!
//! [`create_pool`] constructs a [`deadpool_postgres::Pool`] from a
//! [`DbConfig`] and [`Config`].  When TLS certificates are configured
//! (`--pg-ca`, `--pg-key`, `--pg-cert`) a `native-tls` connector is used;
//! otherwise a plain TCP connection is made.

use crate::config::{Config, DbConfig};
use anyhow::{Context, Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::fs;
use tokio_postgres::Config as PgConfig;

pub fn create_pool(db: &DbConfig, cfg: &Config, for_subscription: bool) -> Result<Pool> {
	let host = if for_subscription || db.host_read.is_none() {
		db.host.clone()
	} else {
		db.host_read.clone().unwrap()
	};

	let tls = build_tls_connector(cfg)?;

	let mut pg_config = PgConfig::new();
	pg_config
		.host(&host)
		.port(db.port)
		.user(&db.user)
		.password(db.password.as_str())
		.dbname(&db.database)
		.options(&format!("-c statement_timeout={}", cfg.query_timeout));

	let manager = Manager::from_config(
		pg_config,
		tls,
		ManagerConfig { recycling_method: RecyclingMethod::Fast },
	);

	Pool::builder(manager)
		.max_size(cfg.max_connection)
		.build()
		.context("Failed to build PostgreSQL connection pool")
}

/// Build a TLS connector from the configuration.
/// Exposed as `pub` so the hot-reload listener can create a raw connection.
pub fn build_tls_connector(cfg: &Config) -> Result<MakeTlsConnector> {
	let mut builder = TlsConnector::builder();

	if let Some(ca_path) = &cfg.pg_ca {
		let ca_pem = fs::read(ca_path)
			.with_context(|| format!("Failed to read CA certificate: {ca_path}"))?;
		let cert = Certificate::from_pem(&ca_pem)
			.with_context(|| format!("Failed to parse CA certificate: {ca_path}"))?;
		builder.add_root_certificate(cert);
	}

	if let (Some(key_path), Some(cert_path)) = (&cfg.pg_key, &cfg.pg_cert) {
		let key_pem =
			fs::read(key_path).with_context(|| format!("Failed to read client key: {key_path}"))?;
		let cert_pem = fs::read(cert_path)
			.with_context(|| format!("Failed to read client certificate: {cert_path}"))?;
		let identity = Identity::from_pkcs8(&cert_pem, &key_pem)
			.context("Failed to build TLS identity from key + certificate")?;
		builder.identity(identity);
	}

	if cfg.pg_ca.is_none() {
		builder.danger_accept_invalid_certs(true);
	}

	Ok(MakeTlsConnector::new(builder.build().context("Failed to build TLS connector")?))
}
