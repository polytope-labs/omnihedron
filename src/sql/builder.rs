//! Parameterized SQL query builder.
//!
//! [`QueryBuilder`] provides a fluent API for constructing `SELECT` statements
//! with safe `$N` parameter binding.  Column and table names are always sourced
//! from introspection (never from user input), so they are safe to interpolate
//! directly into the SQL string.
//!
//! Most resolvers currently build SQL inline for clarity; this builder is
//! available for more complex query construction.

/// A safe, parameterized SQL query builder.
///
/// All user-supplied values are bound as `$N` parameters via
/// [`QueryBuilder::bind`].  Column and table names are whitelisted via
/// introspection and never derived from user input.
#[allow(dead_code)]
pub struct QueryBuilder {
	table: String,
	schema: String,
	columns: Vec<String>,
	conditions: Vec<String>,
	params: Vec<serde_json::Value>,
	order_by: Vec<String>,
	limit: Option<usize>,
	offset: Option<usize>,
	distinct_on: Vec<String>,
}

#[allow(dead_code)]
impl QueryBuilder {
	/// Create a new builder targeting `schema.table`.
	pub fn new(schema: &str, table: &str) -> Self {
		Self {
			table: table.to_string(),
			schema: schema.to_string(),
			columns: vec![],
			conditions: vec![],
			params: vec![],
			order_by: vec![],
			limit: None,
			offset: None,
			distinct_on: vec![],
		}
	}

	/// Select specific columns. If not called, defaults to `SELECT t.*`.
	pub fn select(mut self, cols: Vec<String>) -> Self {
		self.columns = cols;
		self
	}

	/// Add a raw `WHERE` condition. Use `$N` placeholders and push the
	/// corresponding values via [`QueryBuilder::bind`].
	pub fn where_raw(mut self, cond: String) -> Self {
		self.conditions.push(cond);
		self
	}

	/// Bind a parameter value, advancing the internal `$N` counter.
	pub fn bind(mut self, value: serde_json::Value) -> Self {
		self.params.push(value);
		self
	}

	/// Add an `ORDER BY` clause fragment, e.g. `"t.block_number ASC"`.
	pub fn order(mut self, clause: String) -> Self {
		self.order_by.push(clause);
		self
	}

	/// Set the `LIMIT` clause.
	pub fn limit(mut self, n: usize) -> Self {
		self.limit = Some(n);
		self
	}

	/// Set the `OFFSET` clause.
	pub fn offset(mut self, n: usize) -> Self {
		self.offset = Some(n);
		self
	}

	/// Add `DISTINCT ON (cols)` to the query.
	pub fn distinct_on(mut self, cols: Vec<String>) -> Self {
		self.distinct_on = cols;
		self
	}

	/// Finalize and return `(sql_string, bound_params)`.
	pub fn build(self) -> (String, Vec<serde_json::Value>) {
		let alias = "t";
		let select_clause = if self.columns.is_empty() {
			format!("{alias}.*")
		} else {
			self.columns
				.iter()
				.map(|c| format!("{alias}.{c}"))
				.collect::<Vec<_>>()
				.join(", ")
		};

		let distinct_clause = if self.distinct_on.is_empty() {
			String::new()
		} else {
			let cols = self
				.distinct_on
				.iter()
				.map(|c| format!("{alias}.{c}"))
				.collect::<Vec<_>>()
				.join(", ");
			format!("DISTINCT ON ({cols}) ")
		};

		let from_clause = format!(r#""{}"."{}""#, self.schema, self.table);

		let where_clause = if self.conditions.is_empty() {
			String::new()
		} else {
			format!("WHERE {}", self.conditions.join(" AND "))
		};

		let order_clause = if self.order_by.is_empty() {
			String::new()
		} else {
			format!("ORDER BY {}", self.order_by.join(", "))
		};

		let limit_clause = self.limit.map(|n| format!("LIMIT {n}")).unwrap_or_default();

		let offset_clause = self.offset.map(|n| format!("OFFSET {n}")).unwrap_or_default();

		let sql = format!(
            "SELECT {distinct_clause}{select_clause} FROM {from_clause} AS {alias} {where_clause} {order_clause} {limit_clause} {offset_clause}"
        )
        .trim()
        .to_string();

		(sql, self.params)
	}

	/// Build a `COUNT(*)` version of the same query (no `ORDER BY`, `LIMIT`, or `OFFSET`).
	pub fn build_count(self) -> (String, Vec<serde_json::Value>) {
		let alias = "t";
		let from_clause = format!(r#""{}"."{}""#, self.schema, self.table);
		let where_clause = if self.conditions.is_empty() {
			String::new()
		} else {
			format!("WHERE {}", self.conditions.join(" AND "))
		};

		let sql = format!("SELECT COUNT(*) AS total FROM {from_clause} AS {alias} {where_clause}")
			.trim()
			.to_string();

		(sql, self.params)
	}

	/// Current number of bound parameters — useful for generating `$N` placeholders.
	pub fn param_count(&self) -> usize {
		self.params.len()
	}
}
