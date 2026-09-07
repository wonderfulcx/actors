mod moved_tests {
	use anyhow::Context;

	use crate::actor::context::tests::new_with_kv;
	use crate::kv::tests::new_in_memory;
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
