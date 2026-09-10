use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};

use super::*;
use crate::actor::internal_storage::queries;

#[test]
fn schema_version_is_little_endian_i64() {
	let encoded = encode_schema_version(INTERNAL_SCHEMA_VERSION);
	assert_eq!(
		decode_schema_version(&encoded).unwrap(),
		INTERNAL_SCHEMA_VERSION
	);
}

#[test]
fn ladder_version_matches_migration_count() {
	assert_eq!(MIGRATIONS.len() as i64, INTERNAL_SCHEMA_VERSION);
}

#[test]
fn schema_sql_does_not_embed_workload_annotations() {
	for sql in MIGRATIONS
		.iter()
		.flat_map(|migration| migration.iter().copied())
		.chain([CREATE_META_TABLE])
	{
		assert!(
			!sql.contains("-- W["),
			"workload annotation leaked into SQL: {sql}"
		);
	}
}

#[test]
fn unpublished_schema_has_explicit_values_and_minimal_constraints() {
	let sql = MIGRATIONS
		.iter()
		.flat_map(|migration| migration.iter().copied())
		.collect::<Vec<_>>()
		.join("\n")
		.to_ascii_lowercase();
	assert!(
		!sql.contains(" default "),
		"internal columns must not use defaults"
	);
	assert!(
		!sql.replace("check (id = 1)", "").contains("check"),
		"only the singleton id constraint is allowed"
	);
	assert!(sql.contains("kind             integer not null"));
	assert!(sql.contains("result         integer not null"));

	for statement in MIGRATIONS
		.iter()
		.flat_map(|migration| migration.iter().copied())
		.filter(|statement| statement.trim_start().starts_with("CREATE TABLE"))
	{
		assert!(
			statement.contains("STRICT"),
			"table is not STRICT: {statement}"
		);
	}
}

#[test]
fn logical_run_wake_metadata_keeps_the_v1_schema_openable() {
	use rivetkit_actor_persist::versioned::RunWakeAt;
	use vbare::OwnedVersionedData;

	assert!(INTERNAL_SCHEMA_VERSION >= 2);
	let conn = rusqlite::Connection::open_in_memory().expect("open v1 fixture database");
	conn.execute_batch(CREATE_META_TABLE)
		.expect("initialize v1 metadata table");
	for statement in MIGRATIONS[0] {
		conn.execute_batch(statement)
			.expect("initialize v1 actor schema");
	}
	conn.execute(
		UPSERT_META_TEXT_SQL,
		rusqlite::params![SCHEMA_VERSION_KEY, encode_schema_version(1)],
	)
	.expect("write v1 schema version");
	let logical_wake = RunWakeAt::wrap_latest(Some(1_723_456_789_000))
		.serialize_with_embedded_version(1)
		.expect("encode logical run wake");
	conn.execute(
		"INSERT INTO _rivet_meta (key, value) VALUES (?1, ?2)",
		rusqlite::params![
			crate::actor::internal_storage::RUN_WAKE_AT_META_KEY,
			logical_wake.clone()
		],
	)
	.expect("persist reserved metadata row");
	conn.execute(
		"INSERT INTO _rivet_runtime (id, last_pushed_alarm, inspector_token, queue_next_id) VALUES (1, ?1, NULL, 2)",
		rusqlite::params![1_723_456_789_500_i64],
	)
	.expect("persist v1 runtime row");

	let stored_schema: Vec<u8> = conn
		.query_row(
			LOAD_META_TEXT_SQL,
			rusqlite::params![SCHEMA_VERSION_KEY],
			|row| row.get(0),
		)
		.expect("read schema version as an old runtime would");
	assert_eq!(decode_schema_version(&stored_schema).unwrap(), 1);
	let legacy_runtime: (Option<i64>, Option<String>, i64) = conn
		.query_row(
			"SELECT last_pushed_alarm, inspector_token, queue_next_id FROM _rivet_runtime WHERE id = 1",
			[],
			|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
		)
		.expect("open runtime through the v1 projection");
	assert_eq!(legacy_runtime, (Some(1_723_456_789_500), None, 2));
	let stored_wake: Vec<u8> = conn
		.query_row(
			LOAD_META_TEXT_SQL,
			rusqlite::params![crate::actor::internal_storage::RUN_WAKE_AT_META_KEY],
			|row| row.get(0),
		)
		.expect("preserve unknown metadata row");
	assert_eq!(stored_wake, logical_wake);
	assert_eq!(
		RunWakeAt::deserialize_with_embedded_version(&stored_wake).unwrap(),
		Some(1_723_456_789_000),
	);
}

fn apply_test_migration(conn: &mut Connection, from_version: i64, to_version: i64) {
	let tx = conn.transaction().unwrap();
	for statement in migration_statements(from_version, to_version).unwrap() {
		let params = statement
			.params
			.unwrap_or_default()
			.into_iter()
			.map(|param| match param {
				BindParam::Null => Value::Null,
				BindParam::Integer(value) => Value::Integer(value),
				BindParam::Float(value) => Value::Real(value),
				BindParam::Text(value) => Value::Text(value),
				BindParam::Blob(value) => Value::Blob(value),
			});
		tx.execute(&statement.sql, params_from_iter(params))
			.unwrap();
	}
	tx.commit().unwrap();
}

fn seed_operation(conn: &Connection, operation_id: &str, status: i64) {
	conn.execute(
		"INSERT INTO _rivet_operations (operation_id, input_digest, kind, name, input, status, actor_generation, started_at, completed_at, result) VALUES (?1, ?2, 0, 'run', x'1234', ?3, 1, 100, CASE WHEN ?3 = 0 THEN NULL ELSE 200 END, CASE WHEN ?3 = 1 THEN x'abcd' ELSE NULL END)",
		params![operation_id, vec![status as u8; 32], status],
	).unwrap();
}

fn tombstone_digest(conn: &Connection, operation_id: &str) -> Option<Vec<u8>> {
	conn.query_row(
		queries::LOAD_TRANSACTIONAL_OPERATION_TOMBSTONE_SQL,
		[operation_id],
		|row| row.get(0),
	)
	.optional()
	.unwrap()
}

#[test]
fn schema_v3_preserves_v2_operations_and_expired_identities_after_database_reopen() {
	let directory = tempfile::tempdir().unwrap();
	let path = directory.path().join("operations.sqlite");
	let mut conn = Connection::open(&path).unwrap();
	conn.execute_batch(CREATE_META_TABLE).unwrap();
	apply_test_migration(&mut conn, 0, 2);
	for (id, status) in [
		("started", 0),
		("succeeded", 1),
		("failed", 2),
		("interrupted", 3),
	] {
		seed_operation(&conn, id, status);
	}
	apply_test_migration(&mut conn, 2, 3);
	let version: Vec<u8> = conn
		.query_row(LOAD_META_TEXT_SQL, [SCHEMA_VERSION_KEY], |row| row.get(0))
		.unwrap();
	assert_eq!(decode_schema_version(&version).unwrap(), 3);
	for (id, status) in [
		("started", 0),
		("succeeded", 1),
		("failed", 2),
		("interrupted", 3),
	] {
		let record: (i64, Vec<u8>, Option<Vec<u8>>) = conn
			.query_row(
				"SELECT status, input, result FROM _rivet_operations WHERE operation_id = ?",
				[id],
				|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
			)
			.unwrap();
		assert_eq!(record.0, status);
		assert_eq!(record.1, vec![0x12, 0x34]);
		assert_eq!(
			record.2,
			if status == 1 {
				Some(vec![0xab, 0xcd])
			} else {
				None
			}
		);
		assert!(tombstone_digest(&conn, id).is_none());
	}
	assert_eq!(
		conn.execute(queries::COMPACT_TRANSACTIONAL_OPERATIONS_SQL, [0])
			.unwrap(),
		3
	);
	drop(conn);

	let conn = Connection::open(&path).unwrap();
	for (id, status) in [("succeeded", 1), ("failed", 2), ("interrupted", 3)] {
		assert_eq!(tombstone_digest(&conn, id), Some(vec![status; 32]));
		for digest in [vec![status; 32], vec![99; 32]] {
			assert_eq!(
				conn.execute(
					queries::INSERT_TRANSACTIONAL_OPERATION_SQL,
					params![id, digest, 0, "run", vec![0x12_u8, 0x34], 2, 300],
				)
				.unwrap(),
				0
			);
		}
	}
	assert!(tombstone_digest(&conn, "started").is_none());
	assert_eq!(
		conn.query_row(
			"SELECT status FROM _rivet_operations WHERE operation_id = 'started'",
			[],
			|row| row.get::<_, i64>(0)
		)
		.unwrap(),
		0
	);
	let columns = conn
		.prepare("PRAGMA table_info(_rivet_operation_tombstones)")
		.unwrap()
		.query_map([], |row| row.get::<_, String>(1))
		.unwrap()
		.collect::<rusqlite::Result<Vec<_>>>()
		.unwrap();
	assert_eq!(columns, ["operation_id", "input_digest"]);
}

#[test]
fn tombstone_write_failure_and_transaction_rollback_preserve_original_result() {
	let conn = Connection::open_in_memory().unwrap();
	initialize_test_schema(&conn).unwrap();
	seed_operation(&conn, "succeeded", 1);
	conn.execute_batch(
		"CREATE TRIGGER reject_test_tombstone BEFORE INSERT ON _rivet_operation_tombstones BEGIN SELECT RAISE(ABORT, 'identity storage failure'); END;",
	).unwrap();
	assert!(
		conn.execute(queries::COMPACT_TRANSACTIONAL_OPERATIONS_SQL, [0])
			.is_err()
	);
	assert!(tombstone_digest(&conn, "succeeded").is_none());
	assert_eq!(
		conn.query_row(
			"SELECT result FROM _rivet_operations WHERE operation_id = 'succeeded'",
			[],
			|row| row.get::<_, Vec<u8>>(0)
		)
		.unwrap(),
		vec![0xab, 0xcd]
	);
	conn.execute_batch("DROP TRIGGER reject_test_tombstone; BEGIN IMMEDIATE")
		.unwrap();
	assert_eq!(
		conn.execute(queries::PRUNE_TRANSACTIONAL_OPERATIONS_SQL, [300, 1])
			.unwrap(),
		1
	);
	assert_eq!(tombstone_digest(&conn, "succeeded"), Some(vec![1; 32]));
	conn.execute_batch("ROLLBACK").unwrap();
	assert!(tombstone_digest(&conn, "succeeded").is_none());
	assert_eq!(
		conn.query_row(
			"SELECT result FROM _rivet_operations WHERE operation_id = 'succeeded'",
			[],
			|row| row.get::<_, Vec<u8>>(0)
		)
		.unwrap(),
		vec![0xab, 0xcd]
	);
}

#[test]
fn started_operation_deletion_is_rejected_at_the_sql_boundary() {
	let conn = Connection::open_in_memory().unwrap();
	initialize_test_schema(&conn).unwrap();
	seed_operation(&conn, "started", 0);
	for sql in [
		"DELETE FROM _rivet_operations WHERE operation_id = 'started'",
		"DELETE FROM _rivet_operations",
	] {
		assert!(conn.execute(sql, []).is_err());
		assert!(tombstone_digest(&conn, "started").is_none());
		assert_eq!(
			conn.query_row(
				"SELECT status FROM _rivet_operations WHERE operation_id = 'started'",
				[],
				|row| row.get::<_, i64>(0)
			)
			.unwrap(),
			0
		);
	}
}
