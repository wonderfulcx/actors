use std::error::Error;
use std::ffi::{CStr, CString};
use std::fmt;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
pub use depot_client_types::{BindParam, ColumnValue, ExecResult, ExecuteResult, QueryResult};
use libsqlite3_sys::{
	SQLITE_BLOB, SQLITE_DONE, SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_INTERRUPT,
	SQLITE_LIMIT_ATTACHED, SQLITE_LIMIT_COLUMN, SQLITE_LIMIT_COMPOUND_SELECT,
	SQLITE_LIMIT_EXPR_DEPTH, SQLITE_LIMIT_LENGTH, SQLITE_LIMIT_LIKE_PATTERN_LENGTH,
	SQLITE_LIMIT_SQL_LENGTH, SQLITE_LIMIT_TRIGGER_DEPTH, SQLITE_LIMIT_VARIABLE_NUMBER,
	SQLITE_LIMIT_VDBE_OP, SQLITE_LIMIT_WORKER_THREADS, SQLITE_NULL, SQLITE_OK, SQLITE_ROW,
	SQLITE_TEXT, SQLITE_TRANSIENT, sqlite3, sqlite3_bind_blob, sqlite3_bind_double,
	sqlite3_bind_int64, sqlite3_bind_null, sqlite3_bind_text, sqlite3_changes, sqlite3_column_blob,
	sqlite3_column_bytes, sqlite3_column_count, sqlite3_column_double, sqlite3_column_int64,
	sqlite3_column_name, sqlite3_column_text, sqlite3_column_type, sqlite3_errmsg,
	sqlite3_extended_errcode, sqlite3_finalize, sqlite3_last_insert_rowid, sqlite3_limit,
	sqlite3_prepare_v2, sqlite3_progress_handler, sqlite3_step, sqlite3_stmt,
};

// This exact leading comment is trusted host metadata, not a guest SQL option.
// Both forms are supported:
//   /*rivet-result-limits:rows:bytes*/
//   /*rivet-result-limits:rows:bytes:deadline_unix_ms:max_vm_steps*/
// Fields are unsigned decimal integers. Rows/bytes may be zero; deadline/work
// must be positive. Explicit work and remaining time only tighten the defaults.
// The absolute deadline includes time queued before reaching this executor.
// Work counts approximate VM instructions plus SQLite prepare progress units;
// it rounds down to a callback interval, rather than granting extra work.
const RESULT_LIMITS_PREFIX: &str = "/*rivet-result-limits:";
const GUEST_MAX_WALL_TIME: Duration = Duration::from_secs(1);
const GUEST_MAX_VM_STEPS: u64 = 1_000_000;
const GUEST_PROGRESS_INTERVAL: u64 = 1_000;
const GUEST_MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;
const GUEST_MAX_SQL_BYTES: usize = 1024 * 1024;

// These bound statement structure and native helper work that is not necessarily
// interruptible by the VM progress handler. They never raise an existing limit.
const GUEST_CONNECTION_LIMITS: [(c_int, c_int); 10] = [
	(SQLITE_LIMIT_SQL_LENGTH, GUEST_MAX_SQL_BYTES as c_int),
	(SQLITE_LIMIT_COLUMN, 256),
	(SQLITE_LIMIT_EXPR_DEPTH, 100),
	(SQLITE_LIMIT_COMPOUND_SELECT, 50),
	(SQLITE_LIMIT_VDBE_OP, 25_000),
	(SQLITE_LIMIT_ATTACHED, 0),
	(SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 1024),
	(SQLITE_LIMIT_VARIABLE_NUMBER, 256),
	(SQLITE_LIMIT_TRIGGER_DEPTH, 10),
	(SQLITE_LIMIT_WORKER_THREADS, 0),
];

#[derive(Clone, Copy, Debug)]
struct ResultLimits {
	max_rows: usize,
	max_bytes: usize,
	deadline_unix_ms: Option<u64>,
	max_vm_steps: u64,
}

struct ProgressBudget {
	deadline: Instant,
	remaining_work: u64,
	interval: u64,
}

impl ProgressBudget {
	fn check(&self) -> Result<()> {
		if self.remaining_work == 0 || Instant::now() >= self.deadline {
			return Err(SqliteStatementError {
				code: SQLITE_INTERRUPT,
				statement_index: 0,
				message: "sqlite guest statement exceeded native execution budget".to_owned(),
			}
			.into());
		}
		Ok(())
	}
}

unsafe extern "C" fn guest_progress_handler(context: *mut c_void) -> c_int {
	// SQLite invokes this synchronously on its worker. The guard owns the stable
	// allocation until after finalization and unregisters it before freeing it.
	let budget = unsafe { &mut *context.cast::<ProgressBudget>() };
	budget.remaining_work = budget.remaining_work.saturating_sub(budget.interval);
	c_int::from(budget.remaining_work == 0 || Instant::now() >= budget.deadline)
}

/// Scoped native safeguards, not a process memory or hard real-time sandbox.
/// LENGTH bounds each SQLite value/encoded row (including randomblob before its
/// allocation), not aggregate memory across values, sorters, caches, or temp
/// tables. Progress callbacks cover prepare (SQLite >= 3.41) and VM execution,
/// but cannot preempt an individual native function, VFS I/O, or a busy wait.
/// Stronger isolation needs bounded VFS temp/storage quotas and cache/dirty-page
/// budgets, or an isolated executor with an OS memory/deadline limit. Do not use
/// process-global SQLite heap limits or local temp files to simulate isolation.
///
/// The worker owns exclusive access to the connection and this module owns its
/// progress-handler slot. SQLite has no getter for restoring arbitrary external
/// handlers, so nesting/replacing another owner's handler is not supported.
struct NativeStatementBudget {
	db: *mut sqlite3,
	progress: Box<ProgressBudget>,
	previous_limits: Vec<(c_int, c_int)>,
}

impl NativeStatementBudget {
	fn install(db: *mut sqlite3, limits: ResultLimits) -> Result<Self> {
		let started = Instant::now();
		let remaining = if let Some(deadline) = limits.deadline_unix_ms {
			let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
			Duration::from_millis(deadline)
				.saturating_sub(now)
				.min(GUEST_MAX_WALL_TIME)
		} else {
			GUEST_MAX_WALL_TIME
		};
		let work = limits.max_vm_steps.min(GUEST_MAX_VM_STEPS);
		let interval = work.min(GUEST_PROGRESS_INTERVAL);
		let mut guard = Self {
			db,
			progress: Box::new(ProgressBudget {
				deadline: started + remaining,
				remaining_work: work - work % interval,
				interval,
			}),
			previous_limits: Vec::with_capacity(GUEST_CONNECTION_LIMITS.len() + 1),
		};
		guard.progress.check()?;
		for (category, maximum) in GUEST_CONNECTION_LIMITS.into_iter().chain([(
			SQLITE_LIMIT_LENGTH,
			limits.max_bytes.min(GUEST_MAX_VALUE_BYTES).max(1) as c_int,
		)]) {
			let previous = unsafe { sqlite3_limit(db, category, -1) };
			guard.previous_limits.push((category, previous));
			unsafe {
				sqlite3_limit(db, category, previous.min(maximum));
			}
		}
		unsafe {
			sqlite3_progress_handler(
				db,
				guard.progress.interval as c_int,
				Some(guest_progress_handler),
				ptr::from_mut(guard.progress.as_mut()).cast(),
			);
		}
		Ok(guard)
	}

	fn check(budget: &Option<Self>) -> Result<()> {
		if let Some(budget) = budget {
			budget.progress.check()?;
		}
		Ok(())
	}
}

impl Drop for NativeStatementBudget {
	fn drop(&mut self) {
		unsafe {
			sqlite3_progress_handler(self.db, 0, None, ptr::null_mut());
			for &(category, previous) in &self.previous_limits {
				sqlite3_limit(self.db, category, previous);
			}
		}
	}
}

struct PreparedStatement(*mut sqlite3_stmt);

impl Drop for PreparedStatement {
	fn drop(&mut self) {
		// sqlite3_finalize accepts NULL, including failed/empty preparations.
		unsafe {
			sqlite3_finalize(self.0);
		}
	}
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SqliteQueryProfile {
	pub bind_count: u64,
	pub bind_logical_bytes: u64,
	pub result_rows: u64,
	pub result_columns: u64,
	pub result_logical_bytes: u64,
}

#[derive(Debug)]
pub struct SqliteStatementError {
	pub code: i32,
	pub statement_index: u32,
	pub message: String,
}

impl fmt::Display for SqliteStatementError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.message)
	}
}

impl Error for SqliteStatementError {}

pub fn execute_statement(
	db: *mut sqlite3,
	sql: &str,
	params: Option<&[BindParam]>,
) -> Result<ExecResult> {
	let c_sql = CString::new(sql).map_err(|err| anyhow!(err.to_string()))?;
	let mut stmt = ptr::null_mut();
	let rc = unsafe { sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut stmt, ptr::null_mut()) };
	if rc != SQLITE_OK {
		return Err(sqlite_error(db, 0, "failed to prepare sqlite statement"));
	}
	if stmt.is_null() {
		return Ok(ExecResult { changes: 0 });
	}

	let result = (|| {
		if let Some(params) = params {
			bind_params(db, stmt, params)?;
		}

		loop {
			let step_rc = unsafe { sqlite3_step(stmt) };
			if step_rc == SQLITE_DONE {
				break;
			}
			if step_rc != SQLITE_ROW {
				return Err(sqlite_error(db, 0, "failed to execute sqlite statement"));
			}
		}

		Ok(ExecResult {
			changes: unsafe { sqlite3_changes(db) as i64 },
		})
	})();

	unsafe {
		sqlite3_finalize(stmt);
	}

	result
}

pub fn query_statement(
	db: *mut sqlite3,
	sql: &str,
	params: Option<&[BindParam]>,
) -> Result<QueryResult> {
	let c_sql = CString::new(sql).map_err(|err| anyhow!(err.to_string()))?;
	let mut stmt = ptr::null_mut();
	let rc = unsafe { sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut stmt, ptr::null_mut()) };
	if rc != SQLITE_OK {
		return Err(sqlite_error(db, 0, "failed to prepare sqlite query"));
	}
	if stmt.is_null() {
		return Ok(QueryResult {
			columns: Vec::new(),
			rows: Vec::new(),
		});
	}

	let result = (|| {
		if let Some(params) = params {
			bind_params(db, stmt, params)?;
		}

		let columns = collect_columns(stmt);
		let mut rows = Vec::new();

		loop {
			let step_rc = unsafe { sqlite3_step(stmt) };
			if step_rc == SQLITE_DONE {
				break;
			}
			if step_rc != SQLITE_ROW {
				return Err(sqlite_error(db, 0, "failed to step sqlite query"));
			}

			let mut row = Vec::with_capacity(columns.len());
			for index in 0..columns.len() {
				row.push(column_value(stmt, index as i32));
			}
			rows.push(row);
		}

		Ok(QueryResult { columns, rows })
	})();

	unsafe {
		sqlite3_finalize(stmt);
	}

	result
}

pub fn execute_single_statement(
	db: *mut sqlite3,
	sql: &str,
	params: Option<&[BindParam]>,
) -> Result<ExecuteResult> {
	execute_single_statement_inner(db, sql, params, false).map(|(result, _)| result)
}

pub fn execute_single_statement_profiled(
	db: *mut sqlite3,
	sql: &str,
	params: Option<&[BindParam]>,
) -> Result<(ExecuteResult, SqliteQueryProfile)> {
	execute_single_statement_inner(db, sql, params, true)
}

fn execute_single_statement_inner(
	db: *mut sqlite3,
	sql: &str,
	params: Option<&[BindParam]>,
	profiled: bool,
) -> Result<(ExecuteResult, SqliteQueryProfile)> {
	let result_limits = parse_result_limits(sql)?;
	if result_limits.is_some() && sql.len() > GUEST_MAX_SQL_BYTES {
		return Err(anyhow!(
			"sqlite guest statement exceeds native SQL length limit"
		));
	}
	// Declare the budget before the statement so every exit finalizes first,
	// while limits and callback storage are still alive, then restores state.
	let budget = result_limits
		.map(|limits| NativeStatementBudget::install(db, limits))
		.transpose()?;
	let c_sql = CString::new(sql).map_err(|err| anyhow!(err.to_string()))?;
	let mut prepared = PreparedStatement(ptr::null_mut());
	let mut tail = ptr::null();
	NativeStatementBudget::check(&budget)?;
	let rc = unsafe { sqlite3_prepare_v2(db, c_sql.as_ptr(), -1, &mut prepared.0, &mut tail) };
	let stmt = prepared.0;
	if rc != SQLITE_OK {
		return Err(sqlite_error(
			db,
			0,
			"failed to prepare sqlite execute statement",
		));
	}
	NativeStatementBudget::check(&budget)?;
	if has_non_whitespace_tail(tail) {
		return Err(SqliteStatementError {
			code: -1,
			statement_index: 0,
			message: "sqlite execute only supports a single statement".to_owned(),
		}
		.into());
	}
	if stmt.is_null() {
		return Ok((
			ExecuteResult {
				columns: Vec::new(),
				rows: Vec::new(),
				changes: 0,
				last_insert_row_id: None,
			},
			SqliteQueryProfile::default(),
		));
	}

	let mut profile = SqliteQueryProfile::default();
	if let Some(params) = params {
		if profiled {
			bind_params_profiled(db, stmt, params, &mut profile)?;
		} else {
			bind_params(db, stmt, params)?;
		}
	}

	let columns = collect_columns(stmt);
	profile.result_columns = columns.len() as u64;
	let mut result_bytes = columns.iter().map(String::len).sum::<usize>();
	validate_result_bytes(result_limits, result_bytes)?;
	let mut rows = Vec::new();
	loop {
		NativeStatementBudget::check(&budget)?;
		let step_rc = unsafe { sqlite3_step(stmt) };
		if step_rc != SQLITE_DONE && step_rc != SQLITE_ROW {
			return Err(sqlite_error(
				db,
				0,
				"failed to step sqlite execute statement",
			));
		}
		NativeStatementBudget::check(&budget)?;
		if step_rc == SQLITE_DONE {
			break;
		}

		validate_result_rows(result_limits, rows.len().saturating_add(1))?;
		result_bytes = result_bytes.saturating_add(row_logical_bytes(stmt, columns.len()));
		validate_result_bytes(result_limits, result_bytes)?;
		let mut row = Vec::with_capacity(columns.len());
		for index in 0..columns.len() {
			row.push(if profiled {
				column_value_profiled(stmt, index as i32, &mut profile)
			} else {
				column_value(stmt, index as i32)
			});
		}
		rows.push(row);
		profile.result_rows = profile.result_rows.saturating_add(1);
	}

	let changes = unsafe { sqlite3_changes(db) as i64 };
	Ok((
		ExecuteResult {
			columns,
			rows,
			changes,
			last_insert_row_id: if changes > 0 {
				Some(unsafe { sqlite3_last_insert_rowid(db) })
			} else {
				None
			},
		},
		profile,
	))
}

fn parse_result_limits(sql: &str) -> Result<Option<ResultLimits>> {
	let Some(rest) = sql.strip_prefix(RESULT_LIMITS_PREFIX) else {
		return Ok(None);
	};
	let (encoded, _) = rest
		.split_once("*/")
		.ok_or_else(|| anyhow!("malformed sqlite result limits"))?;
	let mut fields = encoded.split(':');
	let max_rows = parse_limit_field(fields.next())?;
	let max_bytes = parse_limit_field(fields.next())?;
	let (deadline_unix_ms, max_vm_steps) = if let Some(deadline) = fields.next() {
		let deadline = parse_limit_field(Some(deadline))?;
		let work = parse_limit_field(fields.next())?;
		if deadline == 0 || work == 0 {
			return Err(anyhow!(
				"sqlite native deadline and work limits must be positive"
			));
		}
		(Some(deadline), work)
	} else {
		(None, GUEST_MAX_VM_STEPS)
	};
	if fields.next().is_some() {
		return Err(anyhow!("malformed sqlite result limits"));
	}
	Ok(Some(ResultLimits {
		max_rows: usize::try_from(max_rows)?,
		max_bytes: usize::try_from(max_bytes)?,
		deadline_unix_ms,
		max_vm_steps,
	}))
}

fn parse_limit_field(field: Option<&str>) -> Result<u64> {
	let field = field.ok_or_else(|| anyhow!("malformed sqlite result limits"))?;
	if field.is_empty() || !field.bytes().all(|byte| byte.is_ascii_digit()) {
		return Err(anyhow!("invalid sqlite limit field"));
	}
	field
		.parse()
		.map_err(|_| anyhow!("sqlite limit field overflows u64"))
}

fn validate_result_rows(limits: Option<ResultLimits>, rows: usize) -> Result<()> {
	if limits.is_some_and(|limits| rows > limits.max_rows) {
		return Err(anyhow!("sqlite result exceeds configured row limit"));
	}
	Ok(())
}

fn validate_result_bytes(limits: Option<ResultLimits>, bytes: usize) -> Result<()> {
	if limits.is_some_and(|limits| bytes > limits.max_bytes) {
		return Err(anyhow!("sqlite result exceeds configured byte limit"));
	}
	Ok(())
}

fn row_logical_bytes(stmt: *mut libsqlite3_sys::sqlite3_stmt, columns: usize) -> usize {
	(0..columns)
		.map(|index| {
			let index = index as i32;
			match unsafe { sqlite3_column_type(stmt, index) } {
				SQLITE_NULL => 0,
				SQLITE_INTEGER | SQLITE_FLOAT => 8,
				SQLITE_TEXT | SQLITE_BLOB => {
					usize::try_from(unsafe { sqlite3_column_bytes(stmt, index) })
						.unwrap_or(usize::MAX)
				}
				_ => 0,
			}
		})
		.fold(0usize, usize::saturating_add)
}

pub fn exec_statements(db: *mut sqlite3, sql: &str) -> Result<QueryResult> {
	let c_sql = CString::new(sql).map_err(|err| anyhow!(err.to_string()))?;
	let mut remaining = c_sql.as_ptr();
	let mut statement_index = 0_u32;
	let mut final_result = QueryResult {
		columns: Vec::new(),
		rows: Vec::new(),
	};

	while unsafe { *remaining } != 0 {
		let mut stmt = ptr::null_mut();
		let mut tail = ptr::null();
		let rc = unsafe { sqlite3_prepare_v2(db, remaining, -1, &mut stmt, &mut tail) };
		if rc != SQLITE_OK {
			return Err(sqlite_error(
				db,
				statement_index,
				"failed to prepare sqlite exec statement",
			));
		}

		if stmt.is_null() {
			if tail == remaining {
				break;
			}
			remaining = tail;
			continue;
		}

		let result = (|| {
			let columns = collect_columns(stmt);
			let mut rows = Vec::new();
			loop {
				let step_rc = unsafe { sqlite3_step(stmt) };
				if step_rc == SQLITE_DONE {
					break;
				}
				if step_rc != SQLITE_ROW {
					return Err(sqlite_error(
						db,
						statement_index,
						"failed to step sqlite exec statement",
					));
				}

				let mut row = Vec::with_capacity(columns.len());
				for index in 0..columns.len() {
					row.push(column_value(stmt, index as i32));
				}
				rows.push(row);
			}

			Ok((columns, rows))
		})();

		unsafe {
			sqlite3_finalize(stmt);
		}

		let (columns, rows) = result?;
		if !columns.is_empty() || !rows.is_empty() {
			final_result = QueryResult { columns, rows };
		}

		if tail == remaining {
			break;
		}
		remaining = tail;
		statement_index = statement_index.saturating_add(1);
	}

	Ok(final_result)
}

pub fn exec_statements_profiled(
	db: *mut sqlite3,
	sql: &str,
) -> Result<(QueryResult, SqliteQueryProfile)> {
	let c_sql = CString::new(sql).map_err(|err| anyhow!(err.to_string()))?;
	let mut remaining = c_sql.as_ptr();
	let mut statement_index = 0_u32;
	let mut final_result = QueryResult {
		columns: Vec::new(),
		rows: Vec::new(),
	};
	let mut final_profile = SqliteQueryProfile::default();

	while unsafe { *remaining } != 0 {
		let mut stmt = ptr::null_mut();
		let mut tail = ptr::null();
		let rc = unsafe { sqlite3_prepare_v2(db, remaining, -1, &mut stmt, &mut tail) };
		if rc != SQLITE_OK {
			return Err(sqlite_error(
				db,
				statement_index,
				"failed to prepare sqlite exec statement",
			));
		}

		if stmt.is_null() {
			if tail == remaining {
				break;
			}
			remaining = tail;
			continue;
		}

		let result = (|| {
			let columns = collect_columns(stmt);
			let mut profile = SqliteQueryProfile {
				result_columns: columns.len() as u64,
				..Default::default()
			};
			let mut rows = Vec::new();
			loop {
				let step_rc = unsafe { sqlite3_step(stmt) };
				if step_rc == SQLITE_DONE {
					break;
				}
				if step_rc != SQLITE_ROW {
					return Err(sqlite_error(
						db,
						statement_index,
						"failed to step sqlite exec statement",
					));
				}

				let mut row = Vec::with_capacity(columns.len());
				for index in 0..columns.len() {
					row.push(column_value_profiled(stmt, index as i32, &mut profile));
				}
				rows.push(row);
				profile.result_rows = profile.result_rows.saturating_add(1);
			}

			Ok((columns, rows, profile))
		})();

		unsafe {
			sqlite3_finalize(stmt);
		}

		let (columns, rows, profile) = result?;
		if !columns.is_empty() || !rows.is_empty() {
			final_result = QueryResult { columns, rows };
			final_profile = profile;
		}

		if tail == remaining {
			break;
		}
		remaining = tail;
		statement_index = statement_index.saturating_add(1);
	}

	Ok((final_result, final_profile))
}

fn bind_params(
	db: *mut sqlite3,
	stmt: *mut libsqlite3_sys::sqlite3_stmt,
	params: &[BindParam],
) -> Result<()> {
	for (index, param) in params.iter().enumerate() {
		bind_param(db, stmt, (index + 1) as i32, param)?;
	}

	Ok(())
}

fn bind_params_profiled(
	db: *mut sqlite3,
	stmt: *mut libsqlite3_sys::sqlite3_stmt,
	params: &[BindParam],
	profile: &mut SqliteQueryProfile,
) -> Result<()> {
	for (index, param) in params.iter().enumerate() {
		profile.bind_count = profile.bind_count.saturating_add(1);
		profile.bind_logical_bytes = profile
			.bind_logical_bytes
			.saturating_add(logical_bind_bytes(param));
		bind_param(db, stmt, (index + 1) as i32, param)?;
	}

	Ok(())
}

fn bind_param(
	db: *mut sqlite3,
	stmt: *mut libsqlite3_sys::sqlite3_stmt,
	bind_index: i32,
	param: &BindParam,
) -> Result<()> {
	let rc = match param {
		BindParam::Null => unsafe { sqlite3_bind_null(stmt, bind_index) },
		BindParam::Integer(value) => unsafe { sqlite3_bind_int64(stmt, bind_index, *value) },
		BindParam::Float(value) => unsafe { sqlite3_bind_double(stmt, bind_index, *value) },
		BindParam::Text(value) => unsafe {
			sqlite3_bind_text(
				stmt,
				bind_index,
				value.as_ptr() as *const c_char,
				value.len() as i32,
				SQLITE_TRANSIENT(),
			)
		},
		BindParam::Blob(value) => unsafe {
			sqlite3_bind_blob(
				stmt,
				bind_index,
				value.as_ptr() as *const _,
				value.len() as i32,
				SQLITE_TRANSIENT(),
			)
		},
	};

	if rc != SQLITE_OK {
		return Err(sqlite_error(db, 0, "failed to bind sqlite parameter"));
	}

	Ok(())
}

fn collect_columns(stmt: *mut libsqlite3_sys::sqlite3_stmt) -> Vec<String> {
	let column_count = unsafe { sqlite3_column_count(stmt) };
	(0..column_count)
		.map(|index| unsafe {
			let name_ptr = sqlite3_column_name(stmt, index);
			if name_ptr.is_null() {
				String::new()
			} else {
				CStr::from_ptr(name_ptr).to_string_lossy().into_owned()
			}
		})
		.collect()
}

fn column_value(stmt: *mut libsqlite3_sys::sqlite3_stmt, index: i32) -> ColumnValue {
	match unsafe { sqlite3_column_type(stmt, index) } {
		SQLITE_NULL => ColumnValue::Null,
		SQLITE_INTEGER => ColumnValue::Integer(unsafe { sqlite3_column_int64(stmt, index) }),
		SQLITE_FLOAT => ColumnValue::Float(unsafe { sqlite3_column_double(stmt, index) }),
		SQLITE_TEXT => {
			let text_ptr = unsafe { sqlite3_column_text(stmt, index) };
			if text_ptr.is_null() {
				ColumnValue::Null
			} else {
				let text_len = unsafe { sqlite3_column_bytes(stmt, index) } as usize;
				let text = String::from_utf8_lossy(unsafe {
					std::slice::from_raw_parts(text_ptr as *const u8, text_len)
				})
				.into_owned();
				ColumnValue::Text(text)
			}
		}
		SQLITE_BLOB => {
			let blob_len = unsafe { sqlite3_column_bytes(stmt, index) } as usize;
			if blob_len == 0 {
				ColumnValue::Blob(Vec::new())
			} else {
				let blob_ptr = unsafe { sqlite3_column_blob(stmt, index) };
				if blob_ptr.is_null() {
					ColumnValue::Null
				} else {
					let blob =
						unsafe { std::slice::from_raw_parts(blob_ptr as *const u8, blob_len) };
					ColumnValue::Blob(blob.to_vec())
				}
			}
		}
		_ => ColumnValue::Null,
	}
}

fn column_value_profiled(
	stmt: *mut libsqlite3_sys::sqlite3_stmt,
	index: i32,
	profile: &mut SqliteQueryProfile,
) -> ColumnValue {
	let mut logical_bytes = 0_u64;
	let value = match unsafe { sqlite3_column_type(stmt, index) } {
		SQLITE_NULL => ColumnValue::Null,
		SQLITE_INTEGER => {
			logical_bytes = 8;
			ColumnValue::Integer(unsafe { sqlite3_column_int64(stmt, index) })
		}
		SQLITE_FLOAT => {
			logical_bytes = 8;
			ColumnValue::Float(unsafe { sqlite3_column_double(stmt, index) })
		}
		SQLITE_TEXT => {
			let text_ptr = unsafe { sqlite3_column_text(stmt, index) };
			if text_ptr.is_null() {
				ColumnValue::Null
			} else {
				let text_len = unsafe { sqlite3_column_bytes(stmt, index) } as usize;
				logical_bytes = text_len as u64;
				let text = String::from_utf8_lossy(unsafe {
					std::slice::from_raw_parts(text_ptr as *const u8, text_len)
				})
				.into_owned();
				ColumnValue::Text(text)
			}
		}
		SQLITE_BLOB => {
			let blob_len = unsafe { sqlite3_column_bytes(stmt, index) } as usize;
			logical_bytes = blob_len as u64;
			if blob_len == 0 {
				ColumnValue::Blob(Vec::new())
			} else {
				let blob_ptr = unsafe { sqlite3_column_blob(stmt, index) };
				if blob_ptr.is_null() {
					ColumnValue::Null
				} else {
					let blob =
						unsafe { std::slice::from_raw_parts(blob_ptr as *const u8, blob_len) };
					ColumnValue::Blob(blob.to_vec())
				}
			}
		}
		_ => ColumnValue::Null,
	};
	profile.result_logical_bytes = profile.result_logical_bytes.saturating_add(logical_bytes);
	value
}

fn logical_bind_bytes(param: &BindParam) -> u64 {
	match param {
		BindParam::Null => 0,
		BindParam::Integer(_) | BindParam::Float(_) => 8,
		BindParam::Text(value) => value.len() as u64,
		BindParam::Blob(value) => value.len() as u64,
	}
}

fn has_non_whitespace_tail(tail: *const c_char) -> bool {
	if tail.is_null() {
		return false;
	}

	let bytes = unsafe { CStr::from_ptr(tail).to_bytes() };
	bytes.iter().any(|byte| !byte.is_ascii_whitespace())
}

fn sqlite_error(db: *mut sqlite3, statement_index: u32, context: &str) -> anyhow::Error {
	let (code, detail) = unsafe {
		if db.is_null() {
			(-1, "unknown sqlite error".to_string())
		} else {
			(
				sqlite3_extended_errcode(db),
				CStr::from_ptr(sqlite3_errmsg(db))
					.to_string_lossy()
					.into_owned(),
			)
		}
	};
	SqliteStatementError {
		code,
		statement_index,
		message: format!("{context}: {detail}"),
	}
	.into()
}

#[cfg(test)]
#[path = "../tests/inline/query.rs"]
mod tests;
