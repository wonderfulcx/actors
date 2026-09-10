use super::*;
use libsqlite3_sys::{
	SQLITE_INTERRUPT, SQLITE_TOOBIG, sqlite3_close, sqlite3_next_stmt, sqlite3_open,
};
use std::panic::{AssertUnwindSafe, catch_unwind};

struct MemoryDb(*mut sqlite3);

impl MemoryDb {
	fn open() -> Self {
		let name = CString::new(":memory:").unwrap();
		let mut db = ptr::null_mut();
		let rc = unsafe { sqlite3_open(name.as_ptr(), &mut db) };
		assert_eq!(rc, SQLITE_OK);
		Self(db)
	}

	fn as_ptr(&self) -> *mut sqlite3 {
		self.0
	}
}

impl Drop for MemoryDb {
	fn drop(&mut self) {
		assert_eq!(unsafe { sqlite3_close(self.0) }, SQLITE_OK);
	}
}

#[test]
fn run_and_query_bind_typed_params() {
	let db = MemoryDb::open();
	exec_statements(
		db.as_ptr(),
		"CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT, score REAL, payload BLOB);",
	)
	.unwrap();

	for (label, payload) in [("alpha", vec![1, 2, 3]), ("empty", Vec::new())] {
		let result = execute_statement(
			db.as_ptr(),
			"INSERT INTO items(label, score, payload) VALUES (?, ?, ?);",
			Some(&[
				BindParam::Text(label.to_owned()),
				BindParam::Float(3.5),
				BindParam::Blob(payload.clone()),
			]),
		)
		.unwrap();
		assert_eq!(result.changes, 1);

		let rows = query_statement(
			db.as_ptr(),
			"SELECT id, label, score, payload FROM items WHERE label = ?;",
			Some(&[BindParam::Text(label.to_owned())]),
		)
		.unwrap();
		assert_eq!(rows.columns, vec!["id", "label", "score", "payload"]);
		assert_eq!(rows.rows.len(), 1);
		assert_eq!(rows.rows[0][1], ColumnValue::Text(label.to_owned()));
		assert_eq!(rows.rows[0][2], ColumnValue::Float(3.5));
		assert_eq!(rows.rows[0][3], ColumnValue::Blob(payload));
	}
}

#[test]
fn exec_returns_last_statement_rows() {
	let db = MemoryDb::open();
	let result = exec_statements(
		db.as_ptr(),
		"CREATE TABLE items(id INTEGER); INSERT INTO items VALUES (1), (2); SELECT COUNT(*) AS count FROM items;",
	)
	.unwrap();

	assert_eq!(result.columns, vec!["count"]);
	assert_eq!(result.rows, vec![vec![ColumnValue::Integer(2)]]);
}

#[test]
fn exec_reports_the_actual_failing_statement_index() {
	let db = MemoryDb::open();
	exec_statements(
		db.as_ptr(),
		"CREATE TABLE unique_items(value INTEGER UNIQUE);",
	)
	.unwrap();

	let error = exec_statements(
		db.as_ptr(),
		"INSERT INTO unique_items VALUES (1); INSERT INTO unique_items VALUES (1);",
	)
	.expect_err("the second statement should violate the unique constraint");
	let typed = error.downcast_ref::<SqliteStatementError>().unwrap();
	assert_eq!(typed.code, libsqlite3_sys::SQLITE_CONSTRAINT_UNIQUE);
	assert_eq!(typed.statement_index, 1);
}

#[test]
fn execute_single_statement_returns_rows_and_metadata() {
	let db = MemoryDb::open();
	let result = execute_single_statement(db.as_ptr(), "SELECT 7 AS value;", None).unwrap();

	assert_eq!(result.columns, vec!["value"]);
	assert_eq!(result.rows, vec![vec![ColumnValue::Integer(7)]]);
	assert_eq!(result.changes, 0);
	assert_eq!(result.last_insert_row_id, None);
}

#[test]
fn execute_single_statement_enforces_incremental_result_limits() {
	let db = MemoryDb::open();
	for profiled in [false, true] {
		for sql in [
			"/*rivet-result-limits:1:1024*/SELECT 1 UNION ALL SELECT 2",
			"/*rivet-result-limits:2:32*/SELECT zeroblob(16) AS x UNION ALL SELECT zeroblob(16)",
		] {
			let error = execute(&db, sql, None, profiled)
				.expect_err("result limit should reject statement");
			assert!(error.to_string().contains("result exceeds configured"));
		}
	}
}

#[test]
fn profiled_execute_counts_bind_and_result_logical_bytes_during_conversion() {
	let db = MemoryDb::open();
	let (result, profile) = execute_single_statement_profiled(
		db.as_ptr(),
		"SELECT ?, ?, ?, ?;",
		Some(&[
			BindParam::Integer(7),
			BindParam::Text("hello".to_owned()),
			BindParam::Blob(vec![1, 2, 3]),
			BindParam::Blob(Vec::new()),
		]),
	)
	.unwrap();

	assert_eq!(result.rows.len(), 1);
	assert_eq!(result.rows[0][3], ColumnValue::Blob(Vec::new()));
	assert_eq!(profile.bind_count, 4);
	assert_eq!(profile.bind_logical_bytes, 16);
	assert_eq!(profile.result_rows, 1);
	assert_eq!(profile.result_columns, 4);
	assert_eq!(profile.result_logical_bytes, 16);
}

#[test]
fn execute_single_statement_returns_write_metadata() {
	let db = MemoryDb::open();
	exec_statements(
		db.as_ptr(),
		"CREATE TABLE execute_items(id INTEGER PRIMARY KEY, label TEXT);",
	)
	.unwrap();

	let result = execute_single_statement(
		db.as_ptr(),
		"INSERT INTO execute_items(label) VALUES (?);",
		Some(&[BindParam::Text("alpha".to_owned())]),
	)
	.unwrap();

	assert_eq!(result.columns, Vec::<String>::new());
	assert_eq!(result.rows, Vec::<Vec<ColumnValue>>::new());
	assert_eq!(result.changes, 1);
	assert_eq!(result.last_insert_row_id, Some(1));
}

#[test]
fn execute_single_statement_collects_insert_returning_rows() {
	let db = MemoryDb::open();
	exec_statements(
		db.as_ptr(),
		"CREATE TABLE execute_returning(id INTEGER PRIMARY KEY, label TEXT);",
	)
	.unwrap();

	let result = execute_single_statement(
		db.as_ptr(),
		"INSERT INTO execute_returning(label) VALUES ('bravo') RETURNING id, label;",
		None,
	)
	.unwrap();

	assert_eq!(result.columns, vec!["id", "label"]);
	assert_eq!(
		result.rows,
		vec![vec![
			ColumnValue::Integer(1),
			ColumnValue::Text("bravo".to_owned())
		]]
	);
	assert_eq!(result.changes, 1);
	assert_eq!(result.last_insert_row_id, Some(1));
}

#[test]
fn execute_single_statement_collects_readonly_pragma_rows() {
	let db = MemoryDb::open();
	let result = execute_single_statement(db.as_ptr(), "PRAGMA user_version;", None).unwrap();

	assert_eq!(result.columns, vec!["user_version"]);
	assert_eq!(result.rows, vec![vec![ColumnValue::Integer(0)]]);
	assert_eq!(result.changes, 0);
}

#[test]
fn execute_single_statement_runs_mutating_pragma() {
	let db = MemoryDb::open();
	let result = execute_single_statement(db.as_ptr(), "PRAGMA user_version = 9;", None).unwrap();

	assert_eq!(result.columns, Vec::<String>::new());
	assert_eq!(result.rows, Vec::<Vec<ColumnValue>>::new());

	let version = execute_single_statement(db.as_ptr(), "PRAGMA user_version;", None).unwrap();
	assert_eq!(version.rows, vec![vec![ColumnValue::Integer(9)]]);
}

#[test]
fn execute_single_statement_rejects_multi_statement_sql() {
	let db = MemoryDb::open();
	let err = execute_single_statement(db.as_ptr(), "SELECT 1; SELECT 2;", None)
		.expect_err("multi statement execute should fail");

	assert!(
		err.to_string().contains("single statement"),
		"unexpected error: {err:#}"
	);
	let typed = err.downcast_ref::<SqliteStatementError>().unwrap();
	assert_eq!(typed.code, -1);
	assert_eq!(typed.statement_index, 0);
}

#[test]
fn execute_single_statement_reports_extended_result_code() {
	let db = MemoryDb::open();
	exec_statements(
		db.as_ptr(),
		"CREATE TABLE unique_items(value TEXT UNIQUE); INSERT INTO unique_items VALUES ('same');",
	)
	.unwrap();
	let err = execute_single_statement(
		db.as_ptr(),
		"INSERT INTO unique_items VALUES ('same')",
		None,
	)
	.expect_err("unique constraint should fail");
	let typed = err.downcast_ref::<SqliteStatementError>().unwrap();
	assert_eq!(typed.code, libsqlite3_sys::SQLITE_CONSTRAINT_UNIQUE);
}

#[test]
fn execute_single_statement_reports_malformed_sql() {
	let db = MemoryDb::open();
	let err = execute_single_statement(db.as_ptr(), "SELECT FROM", None)
		.expect_err("malformed execute should fail");

	assert!(
		err.to_string().contains("failed to prepare"),
		"unexpected error: {err:#}"
	);
}

fn execute(
	db: &MemoryDb,
	sql: &str,
	params: Option<&[BindParam]>,
	profiled: bool,
) -> Result<ExecuteResult> {
	if profiled {
		execute_single_statement_profiled(db.as_ptr(), sql, params).map(|(result, _)| result)
	} else {
		execute_single_statement(db.as_ptr(), sql, params)
	}
}

#[test]
fn guest_native_work_budget_interrupts_aggregate_before_first_row() {
	for profiled in [false, true] {
		let db = MemoryDb::open();
		// The aggregate emits only one small row, after all recursive work is done.
		let error = execute(
			&db,
			"/*rivet-result-limits:1:1024*/WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000000) SELECT sum(x) AS total FROM n",
			None,
			profiled,
		).expect_err("native work must be bounded before the first result row");
		assert_eq!(
			error.downcast_ref::<SqliteStatementError>().unwrap().code,
			SQLITE_INTERRUPT
		);
	}
}

#[test]
fn guest_native_length_limit_rejects_large_intermediate_expression() {
	for profiled in [false, true] {
		let db = MemoryDb::open();
		// The output is only an integer. A result-byte check cannot protect the
		// randomblob allocation evaluated inside length().
		let error = execute(
			&db,
			"/*rivet-result-limits:1:1024*/SELECT length(randomblob(16777216)) AS size",
			None,
			profiled,
		)
		.expect_err("SQLite must reject the oversized intermediate blob");
		assert_eq!(
			error.downcast_ref::<SqliteStatementError>().unwrap().code,
			SQLITE_TOOBIG
		);
	}
}

fn current_limits(db: &MemoryDb) -> Vec<(c_int, c_int)> {
	GUEST_CONNECTION_LIMITS
		.iter()
		.map(|&(category, _)| category)
		.chain([SQLITE_LIMIT_LENGTH])
		.map(|category| {
			(category, unsafe {
				sqlite3_limit(db.as_ptr(), category, -1)
			})
		})
		.collect()
}

fn assert_restored(db: &MemoryDb, previous: &[(c_int, c_int)]) {
	assert_eq!(current_limits(db), previous);
	assert!(unsafe { sqlite3_next_stmt(db.as_ptr(), ptr::null_mut()) }.is_null());
	// This exceeds the guest work budget and its 1024-byte value limit. It also
	// exercises the next connection user after the callback's storage is freed.
	let result = execute_single_statement(
		db.as_ptr(),
		"WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000) SELECT sum(x), length(randomblob(2048)) FROM n",
		None,
	).unwrap();
	assert_eq!(
		result.rows,
		vec![vec![
			ColumnValue::Integer(5000050000),
			ColumnValue::Integer(2048)
		]]
	);
}

#[test]
fn guest_native_budgets_restore_after_every_exit() {
	for profiled in [false, true] {
		let db = MemoryDb::open();
		let previous = current_limits(&db);
		for (sql, params, succeeds) in [
			("/*rivet-result-limits:1:1024*/SELECT 7 AS x", None, true),
			("/*rivet-result-limits:1:1024*/ -- no statement", None, true),
			("/*rivet-result-limits:1:1024*/SELECT FROM", None, false),
			(
				"/*rivet-result-limits:1:1024*/SELECT 1; SELECT 2",
				None,
				false,
			),
			(
				"/*rivet-result-limits:1:1024*/SELECT 1",
				Some(vec![BindParam::Integer(1)]),
				false,
			),
			(
				"/*rivet-result-limits:1:1024*/SELECT ?",
				Some(vec![BindParam::Blob(vec![0; 2048])]),
				false,
			),
			(
				"/*rivet-result-limits:1:1024*/SELECT randomblob(2048) AS x",
				None,
				false,
			),
			(
				"/*rivet-result-limits:1:1024*/SELECT 1 UNION ALL SELECT 2",
				None,
				false,
			),
			(
				"/*rivet-result-limits:2:32*/SELECT zeroblob(16) AS x UNION ALL SELECT zeroblob(16)",
				None,
				false,
			),
			("/*rivet-result-limits:1:1024*/SELECT '\0'", None, false),
			(
				"/*rivet-result-limits:1:1024:1:1000*/CREATE TABLE expired(x)",
				None,
				false,
			),
			(
				"/*rivet-result-limits:1:1024:18446744073709551615:1000*/WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000) SELECT sum(x) FROM n",
				None,
				false,
			),
		] {
			let result = execute(&db, sql, params.as_deref(), profiled);
			assert_eq!(result.is_ok(), succeeds, "{sql}: {result:?}");
			assert_restored(&db, &previous);
		}
		assert!(query_statement(db.as_ptr(), "SELECT * FROM expired", None).is_err());
	}
}

#[test]
fn guest_native_limits_only_tighten_and_restore_custom_connection_limits() {
	let db = MemoryDb::open();
	unsafe {
		sqlite3_limit(db.as_ptr(), SQLITE_LIMIT_LENGTH, 512);
		sqlite3_limit(db.as_ptr(), SQLITE_LIMIT_COLUMN, 20);
		sqlite3_limit(db.as_ptr(), SQLITE_LIMIT_EXPR_DEPTH, 50);
		sqlite3_limit(db.as_ptr(), SQLITE_LIMIT_WORKER_THREADS, 2);
	}
	let previous = current_limits(&db);
	let limits = parse_result_limits("/*rivet-result-limits:1:1024*/")
		.unwrap()
		.unwrap();
	{
		let _budget = NativeStatementBudget::install(db.as_ptr(), limits).unwrap();
		for (category, maximum) in GUEST_CONNECTION_LIMITS
			.into_iter()
			.chain([(SQLITE_LIMIT_LENGTH, 1024)])
		{
			let old = previous.iter().find(|(id, _)| *id == category).unwrap().1;
			assert_eq!(
				unsafe { sqlite3_limit(db.as_ptr(), category, -1) },
				old.min(maximum)
			);
		}
	}
	assert_eq!(current_limits(&db), previous);
}

#[test]
fn guest_native_budgets_cover_prepare() {
	// Duplicate subquery column names exercise SQLite's prepare-time progress
	// checkpoint while it derives unique names for the outer query.
	for profiled in [false, true] {
		let db = MemoryDb::open();
		let previous = current_limits(&db);
		let error = execute(
			&db,
			"/*rivet-result-limits:1:1024:18446744073709551615:1*/SELECT * FROM (SELECT 1 AS x, 2 AS x)",
			None,
			profiled,
		)
		.expect_err("the progress handler must be installed before prepare");
		assert_eq!(
			error.downcast_ref::<SqliteStatementError>().unwrap().code,
			SQLITE_INTERRUPT
		);
		assert!(error.to_string().contains("prepare"), "{error:#}");
		assert_restored(&db, &previous);
	}
}

#[test]
fn guest_native_deadline_interrupts_sqlite_without_exhausting_work() {
	let db = MemoryDb::open();
	let previous = current_limits(&db);
	let limits = parse_result_limits("/*rivet-result-limits:1:1024*/")
		.unwrap()
		.unwrap();
	{
		let mut budget = NativeStatementBudget::install(db.as_ptr(), limits).unwrap();
		let sql = CString::new("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000) SELECT sum(x) FROM n").unwrap();
		let mut stmt = PreparedStatement(ptr::null_mut());
		assert_eq!(
			unsafe {
				sqlite3_prepare_v2(db.as_ptr(), sql.as_ptr(), -1, &mut stmt.0, ptr::null_mut())
			},
			SQLITE_OK
		);
		// Expire the monotonic deadline deterministically after preparation. This
		// calls SQLite directly so only the installed callback can interrupt it.
		budget.progress.deadline = Instant::now();
		assert_eq!(unsafe { sqlite3_step(stmt.0) }, SQLITE_INTERRUPT);
		assert!(budget.progress.remaining_work > 0);
	}
	assert_restored(&db, &previous);
}

#[test]
fn guest_native_deadline_and_work_cannot_disable_default_caps() {
	let db = MemoryDb::open();
	let limits = parse_result_limits(
		"/*rivet-result-limits:1:18446744073709551615:18446744073709551615:18446744073709551615*/",
	)
	.unwrap()
	.unwrap();
	let before = Instant::now();
	let budget = NativeStatementBudget::install(db.as_ptr(), limits).unwrap();
	assert!(budget.progress.deadline <= Instant::now() + GUEST_MAX_WALL_TIME);
	assert!(budget.progress.deadline > before);
	assert_eq!(budget.progress.remaining_work, GUEST_MAX_VM_STEPS);
	assert_eq!(
		unsafe { sqlite3_limit(db.as_ptr(), SQLITE_LIMIT_LENGTH, -1) },
		GUEST_MAX_VALUE_BYTES as c_int
	);
}

#[test]
fn guest_native_guards_restore_on_unwind() {
	let db = MemoryDb::open();
	let previous = current_limits(&db);
	let limits = parse_result_limits("/*rivet-result-limits:1:1024*/")
		.unwrap()
		.unwrap();
	assert!(
		catch_unwind(AssertUnwindSafe(|| {
			let _budget = NativeStatementBudget::install(db.as_ptr(), limits).unwrap();
			let sql = CString::new("SELECT 1").unwrap();
			let mut stmt = PreparedStatement(ptr::null_mut());
			assert_eq!(
				unsafe {
					sqlite3_prepare_v2(db.as_ptr(), sql.as_ptr(), -1, &mut stmt.0, ptr::null_mut())
				},
				SQLITE_OK
			);
			panic!("exercise finalization and restoration during unwind");
		}))
		.is_err()
	);
	assert_restored(&db, &previous);
}

#[test]
fn guest_limits_reject_malformed_metadata_before_execution() {
	for profiled in [false, true] {
		let db = MemoryDb::open();
		let previous = current_limits(&db);
		for prefix in [
			"/*rivet-result-limits:",
			"/*rivet-result-limits:1*/",
			"/*rivet-result-limits::1024*/",
			"/*rivet-result-limits:1:*/",
			"/*rivet-result-limits:-1:1024*/",
			"/*rivet-result-limits:+1:1024*/",
			"/*rivet-result-limits:1: 1024*/",
			"/*rivet-result-limits:1:1.5*/",
			"/*rivet-result-limits:1:18446744073709551616*/",
			"/*rivet-result-limits:1:1024:1000*/",
			"/*rivet-result-limits:1:1024:1000:*/",
			"/*rivet-result-limits:1:1024:0:1000*/",
			"/*rivet-result-limits:1:1024:1000:0*/",
			"/*rivet-result-limits:1:1024:1000:-1*/",
			"/*rivet-result-limits:1:1024:1000:1:1*/",
			"/*rivet-result-limits:1:1024:18446744073709551616:1*/",
		] {
			let sql = format!("{prefix}CREATE TABLE should_not_exist(x)");
			assert!(execute(&db, &sql, None, profiled).is_err(), "{prefix}");
			assert_eq!(current_limits(&db), previous);
			assert!(unsafe { sqlite3_next_stmt(db.as_ptr(), ptr::null_mut()) }.is_null());
		}
		assert!(query_statement(db.as_ptr(), "SELECT * FROM should_not_exist", None).is_err());
	}
	let limits = parse_result_limits("/*rivet-result-limits:0:0*/")
		.unwrap()
		.unwrap();
	assert_eq!((limits.max_rows, limits.max_bytes), (0, 0));
	assert_eq!(limits.max_vm_steps, GUEST_MAX_VM_STEPS);
	assert_eq!(limits.deadline_unix_ms, None);
	assert!(parse_result_limits("SELECT 1").unwrap().is_none());
	let db = MemoryDb::open();
	for profiled in [false, true] {
		let error = execute(
			&db,
			"/*rivet-result-limits:0:1024*/SELECT 1",
			None,
			profiled,
		)
		.unwrap_err();
		assert!(error.to_string().contains("row limit"));
		let result = execute(&db, "/*rivet-result-limits:0:0*/ -- empty", None, profiled).unwrap();
		assert!(result.rows.is_empty());
	}
}

unsafe extern "C" fn count_progress(context: *mut c_void) -> c_int {
	let count = unsafe { &mut *context.cast::<u64>() };
	*count += 1;
	0
}

#[test]
fn ordinary_sql_leaves_existing_progress_handler_untouched() {
	for profiled in [false, true] {
		let db = MemoryDb::open();
		let mut calls = 0_u64;
		unsafe {
			sqlite3_progress_handler(
				db.as_ptr(),
				1,
				Some(count_progress),
				ptr::from_mut(&mut calls).cast(),
			);
		}
		let first = execute(&db, "SELECT 1", None, profiled);
		let first_calls = calls;
		let second = execute(&db, "SELECT 2", None, profiled);
		unsafe {
			sqlite3_progress_handler(db.as_ptr(), 0, None, ptr::null_mut());
		}
		assert!(first.is_ok());
		assert!(second.is_ok());
		assert!(first_calls > 0);
		assert!(calls > first_calls);
	}
}

#[test]
fn guest_native_limits_bound_prepare_structure() {
	for profiled in [false, true] {
		let db = MemoryDb::open();
		let previous = current_limits(&db);
		let wide = format!("SELECT {}", vec!["1"; 257].join(","));
		let deep = format!("SELECT {}1", "1+".repeat(101));
		let compound = vec!["SELECT 1"; 51].join(" UNION ALL ");
		let long = format!("SELECT 1 -- {}", "x".repeat(GUEST_MAX_SQL_BYTES));
		for sql in [wide, deep, compound, long, "SELECT ?257".to_owned()] {
			let limited = format!("/*rivet-result-limits:1:1024*/{sql}");
			assert!(execute(&db, &limited, None, profiled).is_err());
			assert_eq!(current_limits(&db), previous);
			assert!(unsafe { sqlite3_next_stmt(db.as_ptr(), ptr::null_mut()) }.is_null());
			assert!(
				execute(&db, &sql, None, profiled).is_ok(),
				"ordinary SQL changed: {sql}"
			);
		}
	}
}
