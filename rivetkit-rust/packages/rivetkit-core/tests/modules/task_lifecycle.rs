mod moved_tests {
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::{Arc, Mutex};
	use std::time::Duration;

	use tokio::sync::{mpsc, oneshot};
	use tokio::task::yield_now;
	use tokio::time::{advance, pause, resume};

	use crate::actor::keys::PERSIST_DATA_KEY;
	use crate::actor::messages::{ActorEvent, StateDelta};
	use crate::actor::state::{PersistedActor, encode_persisted_actor};
	use crate::actor::task::{ActorTask, DispatchCommand, LifecycleCommand};
	use crate::actor::task_types::ShutdownKind;
	use crate::{ActorConfig, ActorConsistency, ActorContext, ActorFactory, ConnHandle};

	fn new_task_with_factory(ctx: ActorContext, factory: Arc<ActorFactory>) -> ActorTask {
		let (_lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();
		let (_dispatch_tx, dispatch_rx) = mpsc::unbounded_channel();
		let (_events_tx, events_rx) = mpsc::unbounded_channel();
		ActorTask::new(
			ctx.actor_id().to_owned(),
			0,
			lifecycle_rx,
			dispatch_rx,
			events_rx,
			factory,
			ctx,
			None,
		)
	}

	struct NotifyOnDrop(Mutex<Option<oneshot::Sender<()>>>);

	impl NotifyOnDrop {
		fn new(sender: oneshot::Sender<()>) -> Self {
			Self(Mutex::new(Some(sender)))
		}
	}

	impl Drop for NotifyOnDrop {
		fn drop(&mut self) {
			if let Some(sender) = self.0.lock().expect("drop notify lock poisoned").take() {
				let _ = sender.send(());
			}
		}
	}

	#[tokio::test]
	async fn startup_imports_legacy_state_and_uses_start_input_before_run_handler() {
		let kv = crate::kv::tests::new_in_memory();
		kv.put(
			PERSIST_DATA_KEY,
			&encode_persisted_actor(&PersistedActor {
				input: Some(vec![1, 2, 3]),
				has_initialized: false,
				state: vec![9, 8, 7],
				scheduled_events: Vec::new(),
			})
			.expect("persisted actor should encode"),
		)
		.await
		.expect("legacy actor state should seed");
		let ctx = crate::actor::task::tests::moved_tests::new_with_kv(
			"actor-preloaded-startup",
			"task-lifecycle",
			Vec::new(),
			"local",
			kv,
		);
		let (observed_send, observed_rx) = oneshot::channel();
		let observed_tx = Arc::new(Mutex::new(Some(observed_send)));

		let factory = Arc::new(ActorFactory::new(Default::default(), move |start| {
			let observed_tx = observed_tx.clone();
			Box::pin(async move {
				let mut events = start.events;
				observed_tx
					.lock()
					.expect("observed lock poisoned")
					.take()
					.expect("observed sender should exist")
					.send((
						start.input,
						start.snapshot,
						start.ctx.state(),
						start.ctx.persisted_actor().has_initialized,
						events.try_recv().is_none(),
					))
					.expect("startup observation should send");

				while let Some(event) = events.recv().await {
					match event {
						ActorEvent::SerializeState { reply, .. } => {
							reply.send(Ok(vec![StateDelta::ActorState(start.ctx.state())]));
						}
						ActorEvent::RunGracefulCleanup { reply, .. } => {
							reply.send(Ok(()));
						}
						_ => {}
					}
				}
				Ok(())
			})
		}));
		let mut task = ActorTask::new(
			"actor-preloaded-startup".to_owned(),
			0,
			mpsc::unbounded_channel().1,
			mpsc::unbounded_channel().1,
			mpsc::unbounded_channel().1,
			factory,
			ctx.clone(),
			Some(vec![7, 7, 7]),
		);

		let (start_tx, start_rx) = oneshot::channel();
		task.handle_lifecycle(LifecycleCommand::Start { reply: start_tx })
			.await;
		start_rx
			.await
			.expect("start reply should send")
			.expect("start should succeed");

		let (input, snapshot, state, has_initialized, no_initial_event) =
			observed_rx.await.expect("startup observation should send");
		assert_eq!(input, Some(vec![1, 2, 3]));
		assert_eq!(snapshot, None);
		assert_eq!(state, vec![9, 8, 7]);
		assert!(has_initialized);
		assert!(no_initial_event);

		let run_handle = task.run_handle.take().expect("run handle should exist");
		run_handle.abort();
		let _ = run_handle.await;
	}

	#[tokio::test]
	async fn ordinary_action_dispatch_does_not_take_the_transactional_slot() {
		let ctx = crate::actor::context::tests::new_with_kv(
			"actor-ordinary-concurrent",
			"task-lifecycle",
			Vec::new(),
			"local",
			crate::kv::tests::new_in_memory(),
		);
		let factory = Arc::new(ActorFactory::new(ActorConfig::default(), |_start| {
			Box::pin(async { Ok(()) })
		}));
		let mut task = new_task_with_factory(ctx, factory);
		task.lifecycle = crate::actor::task::LifecycleState::Started;
		let (reply, _reply_rx) = oneshot::channel();
		task.handle_dispatch(DispatchCommand::Action {
			name: "ordinary".to_owned(),
			args: Vec::new(),
			conn: ConnHandle::new("conn", Vec::new(), Vec::new(), false),
			reply,
		})
		.await;

		assert!(!task.transactional_dispatch_active);
		assert!(task.accepting_dispatch());
	}

	#[tokio::test]
	async fn classified_concurrent_action_does_not_take_the_transactional_slot() {
		let ctx = crate::actor::context::tests::new_with_kv(
			"actor-transactional-query",
			"task-lifecycle",
			Vec::new(),
			"local",
			crate::kv::tests::new_in_memory(),
		);
		let factory = Arc::new(
			ActorFactory::new(
				ActorConfig {
					consistency: ActorConsistency::Transactional,
					..ActorConfig::default()
				},
				|_start| Box::pin(async { Ok(()) }),
			)
			.with_transactional_operations()
			.with_action_classifier(|name, _args| {
				if name == "query" {
					crate::ActorActionMode::Concurrent
				} else {
					crate::ActorActionMode::Transactional
				}
			}),
		);
		let mut task = new_task_with_factory(ctx, factory);
		task.lifecycle = crate::actor::task::LifecycleState::Started;
		let (reply, _reply_rx) = oneshot::channel();
		task.handle_dispatch(DispatchCommand::Action {
			name: "query".to_owned(),
			args: Vec::new(),
			conn: ConnHandle::new("conn", Vec::new(), Vec::new(), false),
			reply,
		})
		.await;

		assert!(!task.transactional_dispatch_active);
		assert!(task.accepting_dispatch());
	}

	#[tokio::test]
	async fn transactional_factory_is_required_for_transactional_consistency() {
		let ctx = crate::actor::context::tests::new_with_kv(
			"actor-transactional-runtime-required",
			"task-lifecycle",
			Vec::new(),
			"local",
			crate::kv::tests::new_in_memory(),
		);
		let factory = Arc::new(ActorFactory::new(
			ActorConfig {
				consistency: ActorConsistency::Transactional,
				..ActorConfig::default()
			},
			|_start| Box::pin(async { Ok(()) }),
		));
		let mut task = new_task_with_factory(ctx, factory);
		let (start_tx, start_rx) = oneshot::channel();
		task.handle_lifecycle(LifecycleCommand::Start { reply: start_tx })
			.await;
		let error = start_rx
			.await
			.expect("start reply should send")
			.expect_err("unsupported transactional runtime must fail");
		let structured = rivet_error::RivetError::extract(&error);
		assert_eq!(structured.group(), "actor");
		assert_eq!(structured.code(), "transactional_runtime_required");
	}

	#[tokio::test]
	async fn transactional_dispatch_sends_only_one_action_to_the_runtime_at_a_time() {
		let ctx = crate::actor::context::tests::new_with_kv(
			"actor-transactional-serial",
			"task-lifecycle",
			Vec::new(),
			"local",
			crate::kv::tests::new_in_memory(),
		);
		let (seen_tx, mut seen_rx) = mpsc::unbounded_channel::<String>();
		let (release_tx, release_rx) = oneshot::channel::<()>();
		let release_rx = Arc::new(Mutex::new(Some(release_rx)));
		let factory = Arc::new(
			ActorFactory::new(
				ActorConfig {
					consistency: ActorConsistency::Transactional,
					..ActorConfig::default()
				},
				move |start| {
					let seen_tx = seen_tx.clone();
					let release_rx = release_rx.clone();
					Box::pin(async move {
						let mut events = start.events;
						while let Some(event) = events.recv().await {
							if let ActorEvent::Action { name, reply, .. } = event {
								seen_tx
									.send(name.clone())
									.expect("record dispatched action");
								if name == "first" {
									let receiver = release_rx
										.lock()
										.expect("release lock poisoned")
										.take()
										.expect("release receiver available");
									receiver.await.expect("release first action");
								}
								reply.send(Ok(name.into_bytes()));
							}
						}
						Ok(())
					})
				},
			)
			.with_transactional_operations(),
		);
		let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();
		let (dispatch_tx, dispatch_rx) = mpsc::unbounded_channel();
		let (events_tx, events_rx) = mpsc::unbounded_channel();
		ctx.configure_lifecycle_events(Some(events_tx));
		let task = ActorTask::new(
			ctx.actor_id().to_owned(),
			7,
			lifecycle_rx,
			dispatch_rx,
			events_rx,
			factory,
			ctx,
			None,
		);
		let task_handle = tokio::spawn(task.run());
		let (start_tx, start_rx) = oneshot::channel();
		lifecycle_tx
			.send(LifecycleCommand::Start { reply: start_tx })
			.expect("send start");
		start_rx
			.await
			.expect("start reply should send")
			.expect("start should succeed");

		let conn = ConnHandle::new("conn", Vec::new(), Vec::new(), false);
		let (first_reply_tx, first_reply_rx) = oneshot::channel();
		dispatch_tx
			.send(DispatchCommand::Action {
				name: "first".to_owned(),
				args: Vec::new(),
				conn: conn.clone(),
				reply: first_reply_tx,
			})
			.expect("send first action");
		let (second_reply_tx, second_reply_rx) = oneshot::channel();
		dispatch_tx
			.send(DispatchCommand::Action {
				name: "second".to_owned(),
				args: Vec::new(),
				conn,
				reply: second_reply_tx,
			})
			.expect("send second action");

		assert_eq!(seen_rx.recv().await.as_deref(), Some("first"));
		yield_now().await;
		assert!(
			seen_rx.try_recv().is_err(),
			"second action must remain in the dispatch inbox until the first replies",
		);
		release_tx.send(()).expect("release first action");
		assert_eq!(
			first_reply_rx
				.await
				.expect("first reply channel")
				.expect("first action result"),
			b"first",
		);
		assert_eq!(seen_rx.recv().await.as_deref(), Some("second"));
		assert_eq!(
			second_reply_rx
				.await
				.expect("second reply channel")
				.expect("second action result"),
			b"second",
		);
		task_handle.abort();
		let _ = task_handle.await;
	}

	#[tokio::test]
	async fn sleep_shutdown_aborts_stuck_run_handler_at_grace_deadline() {
		let ctx = crate::actor::context::tests::new_with_kv(
			"actor-stuck-run-grace",
			"task-lifecycle",
			Vec::new(),
			"local",
			crate::kv::tests::new_in_memory(),
		);
		let grace_period = Duration::from_millis(100);
		let dropped_count = Arc::new(AtomicUsize::new(0));
		let (drop_tx, drop_rx) = oneshot::channel();
		let drop_tx = Arc::new(Mutex::new(Some(drop_tx)));
		let dropped_count_for_factory = dropped_count.clone();
		let factory = Arc::new(ActorFactory::new(
			ActorConfig {
				sleep_grace_period: grace_period,
				sleep_grace_period_overridden: true,
				..ActorConfig::default()
			},
			move |_start| {
				let dropped_count = dropped_count_for_factory.clone();
				let drop_tx = drop_tx.clone();
				Box::pin(async move {
					let sender = drop_tx
						.lock()
						.expect("drop sender lock poisoned")
						.take()
						.expect("drop sender should exist");
					let _notify = NotifyOnDrop::new(sender);
					dropped_count.fetch_add(1, Ordering::SeqCst);
					std::future::pending::<()>().await;
					Ok(())
				})
			},
		));
		let mut task = new_task_with_factory(ctx, factory);

		let (start_tx, start_rx) = oneshot::channel();
		task.handle_lifecycle(LifecycleCommand::Start { reply: start_tx })
			.await;
		start_rx
			.await
			.expect("start reply should send")
			.expect("start should succeed");
		yield_now().await;
		assert_eq!(dropped_count.load(Ordering::SeqCst), 1);
		pause();

		let stop = tokio::spawn(async move { task.handle_stop(ShutdownKind::Sleep).await });
		yield_now().await;
		assert!(
			!stop.is_finished(),
			"sleep shutdown should wait for the shared grace deadline"
		);

		advance(grace_period - Duration::from_millis(1)).await;
		yield_now().await;
		assert!(
			!stop.is_finished(),
			"stuck run handler should not be aborted before the grace deadline"
		);

		advance(Duration::from_millis(1)).await;
		resume();
		stop.await
			.expect("sleep stop join should succeed")
			.expect("sleep stop should succeed after grace timeout");
		drop_rx
			.await
			.expect("stuck run handler should be aborted at the grace deadline");
	}
}
