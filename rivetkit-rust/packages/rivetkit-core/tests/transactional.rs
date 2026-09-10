mod moved_tests {
	use anyhow::Context;

	use crate::actor::context::tests::new_with_kv;
	use crate::actor::internal_storage;
	use crate::kv::tests::new_in_memory;
	use crate::sqlite::{BindParam, ColumnValue};
	use crate::testing::ActorContextHarness;
	use crate::{
		ActorConfig, ActorContext, StateDelta, TransactionalEffect,
		TransactionalOperationAdmission, TransactionalOperationFailure, TransactionalOperationKind,
		TransactionalOperationRequest, TransactionalOperationStatus, TransactionalScheduleUpsert,
	};

	fn context(actor_id: &str) -> ActorContext {
		new_with_kv(
			actor_id,
			"transactional",
			Vec::new(),
			"local",
			new_in_memory(),
		)
	}

	fn request(operation_id: &str, input: &[u8]) -> TransactionalOperationRequest {
		TransactionalOperationRequest {
			operation_id: operation_id.to_owned(),
			kind: TransactionalOperationKind::Action,
			name: "increment".to_owned(),
			input: input.to_vec(),
			actor_generation: 7,
			started_at: 100,
		}
	}

	async fn executing(
		ctx: &ActorContext,
		request: TransactionalOperationRequest,
	) -> crate::TransactionalOperation {
		match ctx
			.begin_transactional_operation(request, None)
			.await
			.expect("admit transactional operation")
		{
			TransactionalOperationAdmission::Execute(operation) => operation,
			TransactionalOperationAdmission::Existing(_) => {
				panic!("new operation must execute")
			}
		}
	}

	const AUTOMATIC_ROLLBACK_CASES: [(&str, &str, &str); 3] = [
		(
			"statement",
			"CREATE TABLE rollback_values (value INTEGER PRIMARY KEY)",
			"INSERT OR ROLLBACK INTO rollback_values VALUES (1)",
		),
		(
			"schema",
			"CREATE TABLE rollback_values (value INTEGER PRIMARY KEY ON CONFLICT ROLLBACK)",
			"INSERT INTO rollback_values VALUES (1)",
		),
		(
			"trigger",
			"CREATE TABLE rollback_values (value INTEGER PRIMARY KEY)",
			"INSERT INTO rollback_values VALUES (2)",
		),
	];

	async fn prepare_automatic_rollback(ctx: &ActorContext, case: &str, schema: &str) {
		ctx.sql().execute(schema, None).await.unwrap();
		ctx.sql()
			.execute("INSERT INTO rollback_values VALUES (1)", None)
			.await
			.unwrap();
		if case == "trigger" {
			ctx.sql()
				.execute(
					"CREATE TRIGGER rollback_insert BEFORE INSERT ON rollback_values WHEN NEW.value = 2 BEGIN SELECT RAISE(ROLLBACK, 'trigger rollback'); END",
					None,
				)
				.await
				.unwrap();
		}
	}

	async fn assert_only_committed_rollback_value(ctx: &ActorContext) {
		assert_eq!(
			ctx.sql()
				.query("SELECT value FROM rollback_values ORDER BY value", None)
				.await
				.unwrap()
				.rows,
			vec![vec![crate::sqlite::ColumnValue::Integer(1)]],
		);
	}

	#[tokio::test]
	async fn automatic_rollback_prevents_caught_error_from_committing_aggregate() {
		for (case, schema, failing_sql) in AUTOMATIC_ROLLBACK_CASES {
			let ctx = context(case);
			prepare_automatic_rollback(&ctx, case, schema).await;
			ctx.save_state(vec![StateDelta::ActorState(b"before".to_vec())])
				.await
				.unwrap();
			let operation = executing(&ctx, request("op-rollback", b"input")).await;
			operation
				.execute_sql("INSERT INTO rollback_values VALUES (3)", None)
				.await
				.unwrap();
			// A guest can catch this SQLite error and still return a successful result.
			let _caught = operation.execute_sql(failing_sql, None).await.unwrap_err();
			let commit_error = operation
				.commit(
					vec![StateDelta::ActorState(b"must not persist".to_vec())],
					b"must not succeed".to_vec(),
					vec![TransactionalEffect {
						kind: "socket".to_owned(),
						payload: b"must not deliver".to_vec(),
					}],
					200,
				)
				.await
				.expect_err(case);
			assert_eq!(
				rivet_error::RivetError::extract(&commit_error).code(),
				"transaction_terminal",
				"{case}",
			);
			assert_eq!(ctx.state(), b"before");
			let snapshot = ctx
				.sql()
				.query(
					crate::actor::internal_storage::LOAD_ACTOR_SNAPSHOT_SQL,
					None,
				)
				.await
				.unwrap();
			assert_eq!(
				snapshot.rows[0][2],
				crate::sqlite::ColumnValue::Blob(b"before".to_vec())
			);
			assert_eq!(ctx.transactional_snapshot_version().await.unwrap(), 0);
			assert!(
				ctx.pending_transactional_effects(10)
					.await
					.unwrap()
					.is_empty()
			);
			assert_only_committed_rollback_value(&ctx).await;
			let record = ctx
				.transactional_operation("op-rollback")
				.await
				.unwrap()
				.unwrap();
			assert_eq!(record.status, TransactionalOperationStatus::Started);
			assert!(record.result.is_none());
			assert!(record.commit_sequence.is_none());
			assert_eq!(
				ctx.recover_interrupted_transactional_operations(300)
					.await
					.unwrap(),
				1
			);
			assert_eq!(
				ctx.transactional_operation("op-rollback")
					.await
					.unwrap()
					.unwrap()
					.status,
				TransactionalOperationStatus::Interrupted,
			);
		}
	}

	#[tokio::test]
	async fn automatic_rollback_rejects_subsequent_sql_and_can_record_failure() {
		for (case, schema, failing_sql) in AUTOMATIC_ROLLBACK_CASES {
			let ctx = context(case);
			prepare_automatic_rollback(&ctx, case, schema).await;
			let operation = executing(&ctx, request("op-rollback", b"input")).await;
			operation
				.execute_sql("INSERT INTO rollback_values VALUES (3)", None)
				.await
				.unwrap();
			let caught = operation.execute_sql(failing_sql, None).await.unwrap_err();
			let subsequent = operation
				.execute_sql("INSERT INTO rollback_values VALUES (4)", None)
				.await
				.expect_err(case);
			assert_eq!(
				rivet_error::RivetError::extract(&subsequent).code(),
				"transaction_terminal"
			);
			operation
				.fail(&caught, 200)
				.await
				.expect("durably record a rolled-back operation failure");
			assert_only_committed_rollback_value(&ctx).await;
			let record = ctx
				.transactional_operation("op-rollback")
				.await
				.unwrap()
				.unwrap();
			assert_eq!(record.status, TransactionalOperationStatus::Failed);
			assert!(record.result.is_none());
			assert!(record.commit_sequence.is_none());
			assert_eq!(ctx.transactional_snapshot_version().await.unwrap(), 0);
			assert!(
				ctx.pending_transactional_effects(10)
					.await
					.unwrap()
					.is_empty()
			);
		}
	}

	#[tokio::test]
	async fn automatic_rollback_terminalizes_explicit_transaction_handles() {
		for (case, schema, failing_sql) in AUTOMATIC_ROLLBACK_CASES {
			let ctx = context(case);
			prepare_automatic_rollback(&ctx, case, schema).await;
			let tx = ctx.sql().begin_transaction(None).await.unwrap();
			tx.execute("INSERT INTO rollback_values VALUES (3)", None)
				.await
				.unwrap();
			let _caught = tx.execute(failing_sql, None).await.unwrap_err();
			for error in [
				tx.execute("INSERT INTO rollback_values VALUES (4)", None)
					.await
					.expect_err(case),
				tx.commit().await.expect_err(case),
			] {
				assert_eq!(
					rivet_error::RivetError::extract(&error).code(),
					"transaction_terminal"
				);
			}
			tx.rollback().await.expect("acknowledge completed cleanup");
			assert_only_committed_rollback_value(&ctx).await;
			let next = ctx.sql().begin_transaction(None).await.unwrap();
			next.execute("INSERT INTO rollback_values VALUES (5)", None)
				.await
				.unwrap();
			next.commit().await.expect("coordinator remains usable");
		}
	}

	#[tokio::test]
	async fn statement_errors_conservatively_roll_back_explicit_transactions() {
		for failing_sql in [
			"INSERT INTO rollback_values VALUES (1)",
			"SELECT * FROM missing_table",
		] {
			let ctx = context("statement-error");
			prepare_automatic_rollback(&ctx, "statement", AUTOMATIC_ROLLBACK_CASES[0].1).await;
			let tx = ctx.sql().begin_transaction(None).await.unwrap();
			tx.execute("INSERT INTO rollback_values VALUES (3)", None)
				.await
				.unwrap();
			let error = tx.execute(failing_sql, None).await.unwrap_err();
			assert_eq!(
				rivet_error::RivetError::extract(&error).code(),
				"remote_execution_failed"
			);
			assert_eq!(
				rivet_error::RivetError::extract(&tx.commit().await.unwrap_err()).code(),
				"transaction_terminal"
			);
			tx.rollback().await.unwrap();
			assert_only_committed_rollback_value(&ctx).await;
		}
	}

	#[tokio::test]
	async fn transactional_operation_commits_state_sql_result_and_effects_atomically() {
		let ctx = context("actor-transactional-commit");
		ctx.sql()
			.execute(
				"CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
				None,
			)
			.await
			.expect("create user table");
		let operation = executing(&ctx, request("op-1", b"input")).await;
		operation
			.execute_sql("INSERT INTO counter (id, value) VALUES (1, 42)", None)
			.await
			.expect("stage user SQL");

		let sequence = operation
			.commit(
				vec![StateDelta::ActorState(vec![4, 2])],
				b"result".to_vec(),
				vec![
					TransactionalEffect {
						kind: "socket".to_owned(),
						payload: b"first".to_vec(),
					},
					TransactionalEffect {
						kind: "alarm".to_owned(),
						payload: b"second".to_vec(),
					},
				],
				200,
			)
			.await
			.expect("commit aggregate");
		assert_eq!(sequence, 1);
		assert_eq!(ctx.state(), vec![4, 2]);

		let record = ctx
			.transactional_operation("op-1")
			.await
			.expect("load operation")
			.expect("operation exists");
		assert_eq!(record.status, TransactionalOperationStatus::Succeeded);
		assert_eq!(record.commit_sequence, Some(1));
		assert_eq!(record.result.as_deref(), Some(b"result".as_slice()));
		assert_eq!(record.completed_at, Some(200));

		let effects = ctx
			.pending_transactional_effects(10)
			.await
			.expect("load effects");
		assert_eq!(effects.len(), 2);
		assert_eq!(effects[0].effect_index, 0);
		assert_eq!(effects[0].payload, b"first");
		assert_eq!(effects[1].effect_index, 1);
		assert_eq!(effects[1].payload, b"second");

		let rows = ctx
			.sql()
			.query("SELECT value FROM counter WHERE id = 1", None)
			.await
			.expect("load user row");
		assert_eq!(
			rows.rows,
			vec![vec![crate::sqlite::ColumnValue::Integer(42)]],
		);
	}

	#[tokio::test]
	async fn duplicate_operation_returns_terminal_result_and_rejects_different_input() {
		let ctx = context("actor-transactional-dedup");
		executing(&ctx, request("op-1", b"same"))
			.await
			.commit(Vec::new(), b"cached".to_vec(), Vec::new(), 200)
			.await
			.expect("commit operation");

		match ctx
			.begin_transactional_operation(request("op-1", b"same"), None)
			.await
			.expect("load duplicate")
		{
			TransactionalOperationAdmission::Existing(record) => {
				assert_eq!(record.status, TransactionalOperationStatus::Succeeded);
				assert_eq!(record.result.as_deref(), Some(b"cached".as_slice()));
			}
			TransactionalOperationAdmission::Execute(_) => {
				panic!("terminal duplicate must not execute")
			}
		}

		let error = ctx
			.begin_transactional_operation(request("op-1", b"different"), None)
			.await
			.err()
			.expect("different payload must conflict");
		let structured = rivet_error::RivetError::extract(&error);
		assert_eq!(structured.group(), "actor");
		assert_eq!(structured.code(), "operation_id_conflict");
	}

	#[tokio::test]
	async fn failed_operation_rolls_back_user_sql_and_records_sanitized_failure() {
		let ctx = context("actor-transactional-failure");
		ctx.sql()
			.execute("CREATE TABLE values_table (value TEXT NOT NULL)", None)
			.await
			.expect("create user table");
		let operation = executing(&ctx, request("op-failure", b"input")).await;
		operation
			.execute_sql("INSERT INTO values_table VALUES ('discarded')", None)
			.await
			.expect("stage user row");
		let callback_error = anyhow::anyhow!("private callback detail");
		let failure = operation
			.fail(&callback_error, 250)
			.await
			.expect("record failure");
		assert_eq!(failure.group, "core");
		assert_eq!(failure.code, "internal_error");
		assert_ne!(failure.message, "private callback detail");

		let rows = ctx
			.sql()
			.query("SELECT value FROM values_table", None)
			.await
			.expect("load user rows");
		assert!(rows.rows.is_empty());
		let record = ctx
			.transactional_operation("op-failure")
			.await
			.expect("load operation")
			.expect("operation exists");
		assert_eq!(record.status, TransactionalOperationStatus::Failed);
		assert_eq!(record.error, Some(failure));
	}

	#[tokio::test]
	async fn recovery_terminalizes_started_operations_without_replay() {
		let ctx = context("actor-transactional-recovery");
		let operation = executing(&ctx, request("op-interrupted", b"input")).await;
		drop(operation);

		let recovered = ctx
			.recover_interrupted_transactional_operations(500)
			.await
			.expect("recover operations");
		assert_eq!(recovered, 1);
		let record = ctx
			.transactional_operation("op-interrupted")
			.await
			.expect("load operation")
			.expect("operation exists");
		assert_eq!(record.status, TransactionalOperationStatus::Interrupted);
		assert_eq!(record.completed_at, Some(500));
		assert_eq!(
			record.error.as_ref().map(|error| error.code.as_str()),
			Some("operation_interrupted"),
		);

		match ctx
			.begin_transactional_operation(request("op-interrupted", b"input"), None)
			.await
			.expect("load interrupted duplicate")
		{
			TransactionalOperationAdmission::Existing(existing) => {
				assert_eq!(existing.status, TransactionalOperationStatus::Interrupted);
			}
			TransactionalOperationAdmission::Execute(_) => {
				panic!("interrupted operation must never replay")
			}
		}
	}

	#[tokio::test]
	async fn schedule_mutation_commits_with_its_effect_intent() {
		let ctx = context("actor-transactional-schedule");
		let operation = executing(&ctx, request("op-schedule", b"input")).await;
		operation
			.stage_schedule_upsert(TransactionalScheduleUpsert {
				event_id: "wonderful:test",
				trigger_at: 5_000,
				action: "wake",
				args: &[1, 2],
			})
			.await
			.expect("stage schedule mutation");
		operation
			.commit(
				Vec::new(),
				Vec::new(),
				vec![TransactionalEffect {
					kind: "schedule".to_owned(),
					payload: Vec::new(),
				}],
				200,
			)
			.await
			.expect("commit schedule and effect");

		let schedule = ctx
			.get_scheduled_event("wonderful:test")
			.await
			.expect("load committed schedule")
			.expect("schedule is visible at aggregate commit");
		assert_eq!(schedule.action, "wake");
		assert_eq!(schedule.args, vec![1, 2]);
		assert_eq!(
			ctx.pending_transactional_effect_operation_count()
				.await
				.unwrap(),
			1
		);
	}

	#[tokio::test]
	async fn schedule_capacity_failure_rolls_back_before_effect_commit() {
		let ctx = ActorContextHarness::new().context_with_config(
			"actor-transactional-schedule-capacity",
			"transactional",
			Vec::new(),
			"local",
			ActorConfig {
				max_schedules: 1,
				..ActorConfig::default()
			},
		);
		ctx.at(5_000, "existing", &[])
			.await
			.expect("fill schedule capacity");
		let operation = executing(&ctx, request("op-over-capacity", b"input")).await;
		let error = operation
			.stage_schedule_upsert(TransactionalScheduleUpsert {
				event_id: "wonderful:overflow",
				trigger_at: 6_000,
				action: "wake",
				args: &[],
			})
			.await
			.expect_err("schedule capacity must be checked inside aggregate transaction");
		operation
			.fail_with(TransactionalOperationFailure::from_error(&error), 200)
			.await
			.expect("terminalize rejected schedule operation");
		assert!(
			ctx.get_scheduled_event("wonderful:overflow")
				.await
				.expect("load rejected schedule")
				.is_none()
		);
		assert!(
			ctx.pending_transactional_effects(1)
				.await
				.unwrap()
				.is_empty()
		);
	}

	#[tokio::test]
	async fn compaction_retains_newest_terminal_operations() {
		let ctx = context("actor-transactional-compaction");
		for (operation_id, completed_at) in [("op-1", 100), ("op-2", 200), ("op-3", 300)] {
			executing(&ctx, request(operation_id, operation_id.as_bytes()))
				.await
				.commit(Vec::new(), Vec::new(), Vec::new(), completed_at)
				.await
				.expect("commit operation");
		}

		assert_eq!(
			ctx.compact_transactional_operations(2)
				.await
				.expect("compact operations"),
			1,
		);
		assert!(
			ctx.transactional_operation("op-1")
				.await
				.expect("load oldest operation")
				.is_none()
		);
		for operation_id in ["op-2", "op-3"] {
			assert!(
				ctx.transactional_operation(operation_id)
					.await
					.expect("load retained operation")
					.is_some()
			);
		}
	}

	async fn assert_expired_identity_rejected(ctx: &ActorContext, operation_id: &str) {
		let mut same = request(operation_id, b"original");
		// A new generation and arrival time do not change the operation identity.
		same.actor_generation += 1;
		same.started_at += 1_000;
		let mut different_input = same.clone();
		different_input.input = b"different".to_vec();
		let mut different_name = same.clone();
		different_name.name = "different".to_owned();
		let mut different_kind = same.clone();
		different_kind.kind = TransactionalOperationKind::Http;
		for (candidate, expected_code) in [
			(same, "operation_result_expired"),
			(different_input, "operation_id_conflict"),
			(different_name, "operation_id_conflict"),
			(different_kind, "operation_id_conflict"),
		] {
			let error = ctx
				.begin_transactional_operation(candidate, None)
				.await
				.err()
				.expect("expired operation must reject without entering guest code");
			let structured = rivet_error::RivetError::extract(&error);
			assert_eq!(structured.group(), "actor");
			assert_eq!(structured.code(), expected_code, "{operation_id}");
			assert_eq!(
				structured.metadata(),
				Some(serde_json::json!({ "operation_id": operation_id }))
			);
		}
		assert!(
			ctx.transactional_operation(operation_id)
				.await
				.unwrap()
				.is_none()
		);
	}

	async fn assert_result_retention_never_forgets_execution(prune: bool) {
		let harness = ActorContextHarness::new();
		let ctx = harness.context("retained-identities", "transactional", Vec::new(), "local");
		ctx.sql()
			.execute(
				"CREATE TABLE committed_values (value INTEGER NOT NULL)",
				None,
			)
			.await
			.unwrap();
		let succeeded = executing(&ctx, request("succeeded", b"original")).await;
		succeeded
			.execute_sql("INSERT INTO committed_values VALUES (1)", None)
			.await
			.unwrap();
		succeeded
			.commit(Vec::new(), b"result".to_vec(), Vec::new(), 200)
			.await
			.unwrap();
		let failed = executing(&ctx, request("failed", b"original")).await;
		failed
			.execute_sql("INSERT INTO committed_values VALUES (2)", None)
			.await
			.unwrap();
		failed
			.fail(&anyhow::anyhow!("guest failed"), 250)
			.await
			.unwrap();
		drop(executing(&ctx, request("interrupted", b"original")).await);
		assert_eq!(
			ctx.recover_interrupted_transactional_operations(300)
				.await
				.unwrap(),
			1
		);
		for (operation_id, status) in [
			("succeeded", TransactionalOperationStatus::Succeeded),
			("failed", TransactionalOperationStatus::Failed),
			("interrupted", TransactionalOperationStatus::Interrupted),
		] {
			assert_eq!(
				ctx.transactional_operation(operation_id)
					.await
					.unwrap()
					.unwrap()
					.status,
				status
			);
		}
		executing(&ctx, request("retained", b"original"))
			.await
			.commit(Vec::new(), b"newest result".to_vec(), Vec::new(), 500)
			.await
			.unwrap();
		// Leave a durable start without a result. Retention must not erase it.
		drop(executing(&ctx, request("started", b"original")).await);
		let removed = if prune {
			ctx.prune_transactional_operations(400, 10).await.unwrap()
		} else {
			ctx.compact_transactional_operations(1).await.unwrap()
		};
		assert_eq!(removed, 3);
		assert_eq!(
			ctx.transactional_operation("started")
				.await
				.unwrap()
				.unwrap()
				.status,
			TransactionalOperationStatus::Started,
		);
		assert_eq!(
			ctx.transactional_operation("retained")
				.await
				.unwrap()
				.unwrap()
				.result,
			Some(b"newest result".to_vec()),
		);
		for operation_id in ["succeeded", "failed", "interrupted"] {
			assert_expired_identity_rejected(&ctx, operation_id).await;
		}
		assert_eq!(ctx.transactional_snapshot_version().await.unwrap(), 2);
		drop(ctx);

		// Recreate all actor-local runtime state over the same persisted database.
		let restarted =
			harness.context("retained-identities", "transactional", Vec::new(), "local");
		internal_storage::schema::ensure_internal_schema(restarted.sql())
			.await
			.unwrap();
		assert_eq!(
			restarted
				.recover_interrupted_transactional_operations(1_000)
				.await
				.unwrap(),
			1
		);
		for operation_id in ["succeeded", "failed", "interrupted"] {
			assert_expired_identity_rejected(&restarted, operation_id).await;
		}
		assert_eq!(restarted.transactional_snapshot_version().await.unwrap(), 2);
		assert_eq!(
			restarted
				.sql()
				.query("SELECT value FROM committed_values", None)
				.await
				.unwrap()
				.rows,
			vec![vec![ColumnValue::Integer(1)]],
		);
		assert_eq!(
			restarted
				.sql()
				.query("SELECT COUNT(*) FROM _rivet_operation_tombstones", None)
				.await
				.unwrap()
				.rows,
			vec![vec![ColumnValue::Integer(3)]],
		);
		// Rejected duplicates must not leave a transaction handle or poison admission.
		executing(&restarted, request("new-id", b"original"))
			.await
			.commit(Vec::new(), Vec::new(), Vec::new(), 1_100)
			.await
			.unwrap();
	}

	#[tokio::test]
	async fn compaction_preserves_success_failure_and_interruption_identities_across_restart() {
		assert_result_retention_never_forgets_execution(false).await;
	}

	#[tokio::test]
	async fn pruning_preserves_success_failure_and_interruption_identities_across_restart() {
		assert_result_retention_never_forgets_execution(true).await;
	}

	#[tokio::test]
	async fn expired_identity_is_enforced_by_sql_without_admission_precheck() {
		let ctx = context("actor-transactional-sql-identity");
		executing(&ctx, request("expired", b"original"))
			.await
			.commit(Vec::new(), Vec::new(), Vec::new(), 200)
			.await
			.unwrap();
		assert_eq!(ctx.compact_transactional_operations(0).await.unwrap(), 1);
		for sql in [
			internal_storage::INSERT_TRANSACTIONAL_OPERATION_SQL,
			"INSERT INTO _rivet_operations (operation_id, input_digest, kind, name, input, status, actor_generation, started_at) VALUES (?, ?, ?, ?, ?, 0, ?, ?)",
		] {
			let result = ctx
				.sql()
				.execute(
					sql,
					Some(vec![
						BindParam::Text("expired".to_owned()),
						BindParam::Blob(vec![0; 32]),
						BindParam::Integer(0),
						BindParam::Text("increment".to_owned()),
						BindParam::Blob(b"conflicting input".to_vec()),
						BindParam::Integer(8),
						BindParam::Integer(300),
					]),
				)
				.await
				.unwrap();
			assert_eq!(result.changes, 0);
		}
		assert_expired_identity_rejected(&ctx, "expired").await;
	}

	#[tokio::test]
	async fn effects_are_ordered_acknowledged_and_protect_operations_from_pruning() {
		let ctx = context("actor-transactional-effects");
		for (operation_id, completed_at) in [("op-1", 100), ("op-2", 200)] {
			executing(&ctx, request(operation_id, operation_id.as_bytes()))
				.await
				.commit(
					Vec::new(),
					Vec::new(),
					vec![TransactionalEffect {
						kind: "socket".to_owned(),
						payload: operation_id.as_bytes().to_vec(),
					}],
					completed_at,
				)
				.await
				.with_context(|| format!("commit {operation_id}"))
				.expect("commit operation");
		}

		assert_eq!(
			ctx.pending_transactional_effect_operation_count()
				.await
				.expect("count pending effect operations"),
			2,
		);
		let effects = ctx
			.pending_transactional_effects(10)
			.await
			.expect("load effects");
		assert_eq!(
			effects
				.iter()
				.map(|effect| effect.operation_id.as_str())
				.collect::<Vec<_>>(),
			vec!["op-1", "op-2"],
		);
		assert_eq!(
			ctx.prune_transactional_operations(300, 10)
				.await
				.expect("prune protected operations"),
			0,
		);

		assert!(
			ctx.mark_transactional_effect_attempt_failed("op-1", 0, 300)
				.await
				.expect("record failed attempt")
		);
		assert!(
			ctx.mark_transactional_effect_delivered("op-1", 0, 400)
				.await
				.expect("mark first delivered")
		);
		assert_eq!(
			ctx.pending_transactional_effect_operation_count()
				.await
				.expect("count remaining effect operations"),
			1,
		);
		assert!(
			ctx.mark_transactional_effect_delivered("op-2", 0, 500)
				.await
				.expect("mark second delivered")
		);
		assert_eq!(
			ctx.prune_transactional_operations(300, 1)
				.await
				.expect("prune first operation"),
			1,
		);
		assert!(
			ctx.transactional_operation("op-1")
				.await
				.expect("load pruned operation")
				.is_none()
		);
		assert!(
			ctx.transactional_operation("op-2")
				.await
				.expect("load retained operation")
				.is_some()
		);
	}
}
