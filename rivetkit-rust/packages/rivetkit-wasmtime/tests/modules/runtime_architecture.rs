use super::*;
use futures_util::poll;
use std::task::Poll;

#[tokio::test]
async fn cpu_only_guest_observes_deadline_before_exhausting_fuel() {
	let limits = WonderfulWasmtimeLimits {
		command_timeout: Duration::from_millis(1),
		fuel_per_command: 100_000_000,
		..WonderfulWasmtimeLimits::default()
	};
	let bundle = CellBundle::verified(
		Vec::new(),
		br#"{"formatVersion":1,"methods":{"spin":"command"}}"#.to_vec(),
	);
	let runtime = WonderfulWasmtimeActorFactory::new(
		include_bytes!("../fixtures/wonderful_cell_test_guest.component.wasm"),
		Arc::new(StaticCellBundleLoader::new(bundle.clone())),
		ActorConfig::default(),
		limits.clone(),
	)
	.unwrap();
	let mut resident = Resident::activate(
		runtime.engine,
		runtime.component,
		bundle,
		Vec::new(),
		None,
		limits,
	)
	.await
	.unwrap();
	let error = resident
		.invoke("spin", &[], SqlCapability::Unavailable)
		.await
		.unwrap_err();
	assert_eq!(
		error
			.downcast_ref::<CellGuestInvocationError>()
			.unwrap()
			.code,
		"CELL_GUEST_TIMEOUT"
	);
	assert!(
		resident.store.get_fuel().unwrap() > 0,
		"deadline must preempt before fuel runs out"
	);
}

#[tokio::test]
async fn cancelled_query_sql_retains_reader_until_executor_finishes() {
	let ctx =
		rivetkit_core::testing::actor_context("query-cancellation", "cell", Vec::new(), "local");
	let transaction = ctx.begin_state_transaction(None).await.unwrap();
	let gate = Arc::new(RwLock::new(()));
	let reader = Arc::new(gate.clone().read_owned().await);
	let mut host = HostState {
		limits: StoreLimitsBuilder::new().build(),
		sql_capability: SqlCapability::ReadOnly(ctx, reader),
		command_deadline_unix_ms: u64::MAX,
		pending_query_sql: None,
		query_mutation_violation: false,
		sql_policy_violation: false,
		sql_limit_violation: false,
		runtime_limits: WonderfulWasmtimeLimits::default(),
		wasi: WasiCtx::builder().build(),
		resources: ResourceTable::new(),
	};
	let mut call = Box::pin(bindings::wonderful::cells::host::Host::execute_sql(
		&mut host,
		"SELECT 1".to_owned(),
		Vec::new(),
	));
	assert!(matches!(poll!(call.as_mut()), Poll::Pending));
	drop(call);
	drop(host);
	assert!(
		gate.try_write().is_err(),
		"aborted guest must not release in-flight SQL fence"
	);
	transaction.rollback().await.unwrap();
	let _writer = tokio::time::timeout(Duration::from_secs(1), gate.write())
		.await
		.expect("SQL completion releases the fence");
}

#[tokio::test]
async fn cancelled_effect_pump_exits_while_writer_remains_held() {
	let ctx =
		rivetkit_core::testing::actor_context("pump-cancellation", "cell", Vec::new(), "local");
	let gate = Arc::new(RwLock::new(()));
	let writer = gate.write().await;
	let notify = Arc::new(Notify::new());
	let cancel = CancellationToken::new();
	notify.notify_one();
	let pump = WonderfulWasmtimeActorFactory::run_effect_pump(
		ctx,
		notify,
		cancel.clone(),
		gate.clone(),
		1,
	);
	tokio::pin!(pump);
	// Poll until the notified pump is waiting for the deliberately held gate.
	assert!(matches!(poll!(pump.as_mut()), Poll::Pending));
	cancel.cancel();
	assert!(matches!(poll!(pump.as_mut()), Poll::Ready(())));
	drop(writer);
}
