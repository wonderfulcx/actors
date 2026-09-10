#![allow(dead_code)]

#[allow(clippy::collapsible_if, clippy::too_many_arguments)]
#[path = "../../rivetkit-core/tests/integration/common/ctx.rs"]
mod integration_ctx;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::StreamExt;
use integration_ctx::IntegrationCtx;
use reqwest::{Client, StatusCode};
use rivetkit_client_protocol as client_protocol;
use rivetkit_core::{
	ActorActionMode, ActorConfig, ActorEvent, ActorEvents, ActorFactory, CanHibernateWebSocket,
	CoreRegistry, Reply,
};
use rivetkit_wasmtime::{
	CellActionEnvelope, CellBundle, CellBundleLoader, CellMethodMode, CellRuntimeManifest,
	StaticCellBundleLoader, WonderfulWasmtimeActorFactory, WonderfulWasmtimeLimits,
};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use vbare::OwnedVersionedData;

const ACTOR_NAME: &str = "wonderful-cell-fault-model";
const LEGACY_ACTOR_NAME: &str = "wonderful-cell-legacy-schedule";
const TEST_COMPONENT: &[u8] = include_bytes!("fixtures/wonderful_cell_test_guest.component.wasm");
type TestSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
// Envoy's GLOBAL_ENVOY slot permits one live registry endpoint per process.
// Separate real-engine scenarios must release it before the next starts.
static ENGINE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Default)]
struct LostReplyFault {
	injected: AtomicBool,
	committed: Notify,
	disconnect: Notify,
}

struct AbortForwarderOnDrop(tokio::task::AbortHandle);

impl Drop for AbortForwarderOnDrop {
	fn drop(&mut self) {
		self.0.abort();
	}
}

// This test-only transport proxy leaves the real Wasmtime runtime and SQLite
// transactions unchanged. It loses one successful reply after the runtime has
// committed, while the public HTTP request is still in flight.
fn with_lost_reply_fault(factory: ActorFactory, fault: Arc<LostReplyFault>) -> ActorFactory {
	let config = factory.config().clone();
	let factory = Arc::new(factory);
	ActorFactory::new_with_manual_startup_ready(config, move |mut start| {
		let factory = factory.clone();
		let fault = fault.clone();
		Box::pin(async move {
			let (forward_tx, forward_rx) = mpsc::unbounded_channel();
			let mut original = std::mem::replace(&mut start.events, ActorEvents::from(forward_rx));
			let forwarder = tokio::spawn(async move {
				let mut pending = tokio::task::JoinSet::new();
				while let Some(event) = original.recv().await {
					match event {
						ActorEvent::Action {
							name,
							args,
							conn,
							scheduled_fire,
							reply,
						} if CellActionEnvelope::decode(&args).is_ok_and(|envelope| {
							envelope.operation_id == "deduplicated-command"
						}) && !fault.injected.swap(true, Ordering::SeqCst) =>
						{
							let (committed_tx, committed_rx) = oneshot::channel();
							if forward_tx
								.send(ActorEvent::Action {
									name,
									args,
									conn,
									scheduled_fire,
									reply: Reply::from(committed_tx),
								})
								.is_err()
							{
								break;
							}
							let fault = fault.clone();
							pending.spawn(async move {
								match committed_rx.await {
									Ok(Ok(_)) => {
										fault.committed.notify_one();
										fault.disconnect.notified().await;
										drop(reply);
									}
									Ok(Err(error)) => reply.send(Err(error)),
									Err(_) => drop(reply),
								}
							});
						}
						other => {
							if forward_tx.send(other).is_err() {
								break;
							}
						}
					}
				}
				pending.abort_all();
				while pending.join_next().await.is_some() {}
			});
			let _abort_forwarder = AbortForwarderOnDrop(forwarder.abort_handle());
			let outcome = factory.start(start).await;
			forwarder.abort();
			let _ = forwarder.await;
			outcome
		})
	})
	.with_transactional_operations()
	.with_action_classifier(|_, args| match CellActionEnvelope::decode(args) {
		Ok(envelope) if envelope.mode == CellMethodMode::Query => ActorActionMode::Concurrent,
		Ok(_) | Err(_) => ActorActionMode::Transactional,
	})
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct SocketDeliveryEnvelope {
	operation_id: String,
	effect_index: u32,
	payload: Vec<u8>,
}

#[derive(Clone)]
struct CountingBundleLoader {
	bundle: Arc<RwLock<CellBundle>>,
	loads: Arc<AtomicUsize>,
	last_digest: Arc<Mutex<Option<[u8; 32]>>>,
}

#[async_trait]
impl CellBundleLoader for CountingBundleLoader {
	async fn load(&self, _actor_id: &str) -> Result<CellBundle> {
		self.loads.fetch_add(1, Ordering::SeqCst);
		let bundle = self.bundle.read().expect("bundle lock poisoned").clone();
		*self.last_digest.lock().expect("digest lock poisoned") = Some(bundle.bundle_sha256);
		Ok(bundle)
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wonderful_cells_survive_production_fault_boundaries() -> Result<()> {
	let _engine_test = ENGINE_TEST_LOCK.lock().await;
	let mut ctx = IntegrationCtx::builder()
		.actor_start_threshold_ms(10_000)
		.start()
		.await?;
	ctx.create_default_namespace().await?;

	let loads = Arc::new(AtomicUsize::new(0));
	let last_digest = Arc::new(Mutex::new(None));
	let bundle = Arc::new(RwLock::new(test_bundle(b"production fault model v1")));
	let loader = CountingBundleLoader {
		bundle: bundle.clone(),
		loads: loads.clone(),
		last_digest: last_digest.clone(),
	};
	let config = ActorConfig {
		remote_sqlite: true,
		can_hibernate_websocket: CanHibernateWebSocket::Bool(true),
		sleep_timeout: Duration::from_millis(250),
		sleep_grace_period: Duration::from_millis(100),
		sleep_grace_period_overridden: true,
		..ActorConfig::default()
	};
	let runtime = WonderfulWasmtimeActorFactory::new(
		TEST_COMPONENT,
		Arc::new(loader),
		config.clone(),
		WonderfulWasmtimeLimits::default(),
	)?;
	let legacy_runtime = WonderfulWasmtimeActorFactory::new(
		TEST_COMPONENT,
		Arc::new(StaticCellBundleLoader::new(test_bundle(
			b"legacy schedule fixture",
		))),
		config,
		WonderfulWasmtimeLimits::default(),
	)?;
	let legacy_factory = Arc::new(legacy_runtime.actor_factory());
	let legacy_activations = Arc::new(AtomicUsize::new(0));
	let mut registry = CoreRegistry::new();
	registry.register(ACTOR_NAME, runtime.actor_factory());
	registry.register(
		LEGACY_ACTOR_NAME,
		ActorFactory::new_with_manual_startup_ready(legacy_factory.config().clone(), {
			let legacy_activations = legacy_activations.clone();
			move |start| {
				let factory = legacy_factory.clone();
				let activations = legacy_activations.clone();
				Box::pin(async move {
					activations.fetch_add(1, Ordering::SeqCst);
					if start.is_new {
						let due = i64::try_from(
							SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
						)? + 3_000;
						// Literal historical v1 wire bytes. Persist through the real core
						// scheduler before sleeping, then decode/fire in a later activation.
						let legacy =
							b"\x01\x00\x0fschedule:legacy\x01\x04\x07\x00\x00\x00\x00\x00\x00\x00";
						start.ctx.at(due, "echo", legacy).await?;
					}
					factory.start(start).await
				})
			}
		})
		.with_transactional_operations(),
	);
	let registry_task = ctx.serve_registry(registry);

	ctx.wait_for_envoy_ready().await?;
	let actor = ctx.create_actor(ACTOR_NAME).await?;
	let endpoint = ctx.endpoint().to_owned();
	let client = Client::builder()
		.timeout(Duration::from_secs(15))
		.build()
		.context("build bounded production fault model client")?;
	let legacy_actor = ctx.create_actor(LEGACY_ACTOR_NAME).await?;
	assert!(
		wait_for_cell_action(
			&client,
			&endpoint,
			&legacy_actor.actor_id,
			"read",
			"legacy-initial",
			CellMethodMode::Query,
			Vec::new(),
		)
		.await?
		.is_empty()
	);

	let first = wait_for_cell_action(
		&client,
		&endpoint,
		&actor.actor_id,
		"echo",
		"deduplicated-command",
		CellMethodMode::Command,
		vec![1],
	)
	.await?;
	assert_eq!(first, vec![1]);
	let duplicate = wait_for_cell_action(
		&client,
		&endpoint,
		&actor.actor_id,
		"echo",
		"deduplicated-command",
		CellMethodMode::Command,
		vec![1],
	)
	.await?;
	assert_eq!(duplicate, vec![1]);

	let mut commands = tokio::task::JoinSet::new();
	for value in 10_u8..18 {
		let client = client.clone();
		let endpoint = endpoint.clone();
		let actor_id = actor.actor_id.clone();
		commands.spawn(async move {
			wait_for_cell_action(
				&client,
				&endpoint,
				&actor_id,
				"echo",
				&format!("parallel-command-{value}"),
				CellMethodMode::Command,
				vec![value],
			)
			.await
		});
	}
	while let Some(result) = commands.join_next().await {
		let output = result.context("join concurrent command")??;
		assert_eq!(output.len(), 1);
	}

	let committed = cell_action(
		&client,
		&endpoint,
		&actor.actor_id,
		"read",
		"committed-query",
		CellMethodMode::Query,
		Vec::new(),
	)
	.await?;
	assert_eq!(
		committed.len(),
		9,
		"deduplication must prevent a second append"
	);
	assert_eq!(committed.iter().filter(|value| **value == 1).count(), 1);
	for value in 10_u8..18 {
		assert!(
			committed.contains(&value),
			"missing serialized command {value}"
		);
	}

	let mut queries = tokio::task::JoinSet::new();
	for index in 0..8 {
		let client = client.clone();
		let endpoint = endpoint.clone();
		let actor_id = actor.actor_id.clone();
		let expected = committed.clone();
		queries.spawn(async move {
			let output = cell_action(
				&client,
				&endpoint,
				&actor_id,
				"read",
				&format!("parallel-query-{index}"),
				CellMethodMode::Query,
				Vec::new(),
			)
			.await?;
			if output != expected {
				bail!("query observed a non-committed snapshot");
			}
			Ok::<(), anyhow::Error>(())
		});
	}
	while let Some(result) = queries.join_next().await {
		result.context("join concurrent query")??;
	}

	assert_eq!(
		cell_action(
			&client,
			&endpoint,
			&actor.actor_id,
			"sql",
			"sqlite-command",
			CellMethodMode::Command,
			vec![99],
		)
		.await?,
		vec![99]
	);
	assert_eq!(
		cell_action(
			&client,
			&endpoint,
			&actor.actor_id,
			"read_sql",
			"sqlite-query",
			CellMethodMode::Query,
			Vec::new(),
		)
		.await?,
		vec![1]
	);

	let (mut socket, connection_id) = open_cell_socket(&endpoint, &actor.actor_id).await?;
	assert!(!connection_id.is_empty());
	cell_action(
		&client,
		&endpoint,
		&actor.actor_id,
		"send",
		"ordered-send-before-sleep",
		CellMethodMode::Command,
		connection_id.as_bytes().to_vec(),
	)
	.await?;
	assert_eq!(
		next_delivery(&mut socket).await?,
		SocketDeliveryEnvelope {
			operation_id: "ordered-send-before-sleep".to_owned(),
			effect_index: 0,
			payload: vec![1],
		}
	);
	assert_eq!(
		next_delivery(&mut socket).await?,
		SocketDeliveryEnvelope {
			operation_id: "ordered-send-before-sleep".to_owned(),
			effect_index: 1,
			payload: vec![2],
		}
	);

	let first_load_count = loads.load(Ordering::SeqCst);
	assert!(first_load_count >= 1);
	let upgraded_bundle = test_bundle(b"production fault model v2");
	let upgraded_digest = upgraded_bundle.bundle_sha256;
	*bundle.write().expect("bundle lock poisoned") = upgraded_bundle;
	tokio::time::sleep(Duration::from_secs(2)).await;
	let after_sleep = wait_for_cell_action(
		&client,
		&endpoint,
		&actor.actor_id,
		"read",
		"wake-query",
		CellMethodMode::Query,
		Vec::new(),
	)
	.await?;
	assert_eq!(after_sleep.len(), 10);
	assert!(
		loads.load(Ordering::SeqCst) > first_load_count,
		"actor must reload its immutable bundle after sleep"
	);
	assert_eq!(
		*last_digest.lock().expect("digest lock poisoned"),
		Some(upgraded_digest),
		"wake must activate the newly deployed immutable bundle"
	);
	cell_action(
		&client,
		&endpoint,
		&actor.actor_id,
		"send",
		"ordered-send-after-wake",
		CellMethodMode::Command,
		connection_id.as_bytes().to_vec(),
	)
	.await?;
	for (effect_index, payload) in [(0, vec![1]), (1, vec![2])] {
		assert_eq!(
			next_delivery(&mut socket).await?,
			SocketDeliveryEnvelope {
				operation_id: "ordered-send-after-wake".to_owned(),
				effect_index,
				payload,
			}
		);
	}
	socket.close(None).await.context("close Cell websocket")?;
	let before_restart_load_count = loads.load(Ordering::SeqCst);
	// Wait beyond idle + grace so the subsequent read must activate a new
	// resident rather than being served by the pre-restart Store.
	tokio::time::sleep(Duration::from_secs(2)).await;

	ctx.restart_engine().await?;
	ctx.wait_for_envoy_ready().await?;
	let after_engine_restart = wait_for_cell_action(
		&client,
		ctx.endpoint(),
		&actor.actor_id,
		"read",
		"engine-restart-query",
		CellMethodMode::Query,
		Vec::new(),
	)
	.await?;
	assert_eq!(after_engine_restart, after_sleep);
	assert!(
		loads.load(Ordering::SeqCst) > before_restart_load_count,
		"engine restart read must rehydrate a new Cell resident"
	);
	assert_eq!(
		cell_action(
			&client,
			ctx.endpoint(),
			&actor.actor_id,
			"read_sql",
			"engine-restart-sql-query",
			CellMethodMode::Query,
			Vec::new(),
		)
		.await?,
		vec![1]
	);

	assert!(
		cell_action(
			&client,
			ctx.endpoint(),
			&actor.actor_id,
			"destroy",
			"cell-destroy",
			CellMethodMode::Command,
			Vec::new(),
		)
		.await?
		.is_empty()
	);
	wait_for_actor_destroyed(&client, ctx.endpoint(), &actor.actor_id).await?;
	wait_for_actor_rejected(&client, ctx.endpoint(), &actor.actor_id).await?;
	ctx.restart_engine().await?;
	ctx.wait_for_envoy_ready().await?;
	wait_for_actor_rejected(&client, ctx.endpoint(), &actor.actor_id).await?;

	assert_eq!(
		wait_for_cell_action(
			&client,
			ctx.endpoint(),
			&legacy_actor.actor_id,
			"read",
			"legacy-after-wake",
			CellMethodMode::Query,
			Vec::new(),
		)
		.await?,
		vec![4],
		"persisted v1 schedule must fire through the real scheduler exactly once"
	);
	assert!(
		legacy_activations.load(Ordering::SeqCst) >= 2,
		"legacy schedule actor must rehydrate after sleep"
	);

	registry_task.shutdown().await?;
	ctx.shutdown().await?;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_operation_survives_engine_loss_before_http_reply() -> Result<()> {
	let _engine_test = ENGINE_TEST_LOCK.lock().await;
	let mut ctx = IntegrationCtx::builder()
		.actor_start_threshold_ms(10_000)
		.start()
		.await?;
	ctx.create_default_namespace().await?;
	let runtime = WonderfulWasmtimeActorFactory::new(
		TEST_COMPONENT,
		Arc::new(StaticCellBundleLoader::new(test_bundle(
			b"lost-reply fixture",
		))),
		ActorConfig {
			remote_sqlite: true,
			sleep_timeout: Duration::from_secs(60),
			..ActorConfig::default()
		},
		WonderfulWasmtimeLimits::default(),
	)?;
	let fault = Arc::new(LostReplyFault::default());
	let mut registry = CoreRegistry::new();
	registry.register(
		ACTOR_NAME,
		with_lost_reply_fault(runtime.actor_factory(), fault.clone()),
	);
	let registry_task = ctx.serve_registry(registry);
	ctx.wait_for_envoy_ready().await?;
	let actor = ctx.create_actor(ACTOR_NAME).await?;
	let endpoint = ctx.endpoint().to_owned();
	let client = Client::builder().timeout(Duration::from_secs(15)).build()?;
	let lost_request = tokio::spawn({
		let client = client.clone();
		let endpoint = endpoint.clone();
		let actor_id = actor.actor_id.clone();
		async move {
			cell_action(
				&client,
				&endpoint,
				&actor_id,
				"echo",
				"deduplicated-command",
				CellMethodMode::Command,
				vec![1],
			)
			.await
		}
	});
	tokio::time::timeout(Duration::from_secs(15), fault.committed.notified())
		.await
		.context("first operation must commit before fault injection")?;
	// The runtime has committed but the gateway cannot yet receive its reply.
	assert!(
		!lost_request.is_finished(),
		"fault must interrupt an in-flight request"
	);
	ctx.restart_engine().await?;
	fault.disconnect.notify_one();
	assert!(
		lost_request
			.await
			.context("join interrupted HTTP request")?
			.is_err()
	);
	ctx.wait_for_envoy_ready().await?;
	assert_eq!(
		wait_for_cell_action(
			&client,
			ctx.endpoint(),
			&actor.actor_id,
			"echo",
			"deduplicated-command",
			CellMethodMode::Command,
			vec![1]
		)
		.await?,
		vec![1]
	);
	assert_eq!(
		wait_for_cell_action(
			&client,
			ctx.endpoint(),
			&actor.actor_id,
			"read",
			"read-after-reply-loss",
			CellMethodMode::Query,
			Vec::new()
		)
		.await?,
		vec![1],
		"retry must not execute the committed command twice"
	);
	registry_task.shutdown().await?;
	ctx.shutdown().await?;
	Ok(())
}

async fn wait_for_actor_destroyed(client: &Client, endpoint: &str, actor_id: &str) -> Result<()> {
	let deadline = Instant::now() + Duration::from_secs(45);
	loop {
		let response = client
			.get(format!("{endpoint}/actors"))
			.query(&[("namespace", "default"), ("actor_ids", actor_id)])
			.bearer_auth("dev")
			.send()
			.await
			.context("query destroyed Cell")?;
		if !response.status().is_success() {
			bail!("destroyed Cell lookup failed with {}", response.status());
		}
		let body: serde_json::Value = response
			.json()
			.await
			.context("decode destroyed Cell lookup")?;
		if body
			.get("actors")
			.and_then(serde_json::Value::as_array)
			.is_some_and(Vec::is_empty)
		{
			return Ok(());
		}
		if Instant::now() >= deadline {
			bail!("Cell destruction was not reflected by the public engine API: {body}");
		}
		tokio::time::sleep(Duration::from_millis(200)).await;
	}
}

async fn wait_for_actor_rejected(client: &Client, endpoint: &str, actor_id: &str) -> Result<()> {
	let deadline = Instant::now() + Duration::from_secs(30);
	loop {
		let response = send_cell_action(
			client,
			endpoint,
			actor_id,
			"read",
			"deleted-query",
			CellMethodMode::Query,
			Vec::new(),
		)
		.await?;
		if !response.status().is_success() {
			return Ok(());
		}
		if Instant::now() >= deadline {
			bail!("destroyed actor continued accepting output-producing work");
		}
		tokio::time::sleep(Duration::from_millis(200)).await;
	}
}

async fn open_cell_socket(endpoint: &str, actor_id: &str) -> Result<(TestSocket, String)> {
	let websocket_url = format!(
		"{}/gateway/{actor_id}/connect",
		endpoint.replacen("http://", "ws://", 1)
	);
	let mut request = websocket_url
		.into_client_request()
		.context("build Cell websocket request")?;
	request.headers_mut().insert(
		"Sec-WebSocket-Protocol",
		format!(
			"rivet, rivet_target.actor, rivet_actor.{actor_id}, rivet_encoding.bare, rivet_token.dev"
		)
		.parse()
		.context("encode Cell websocket protocols")?,
	);
	let (mut socket, response) = connect_async(request)
		.await
		.context("connect Cell websocket")?;
	if response.status() != StatusCode::SWITCHING_PROTOCOLS {
		bail!("Cell websocket upgrade failed with {}", response.status());
	}
	let init = next_socket_message(&mut socket).await?;
	match init.body {
		client_protocol::ToClientBody::Init(init) => Ok((socket, init.connection_id)),
		other => bail!("expected Cell websocket init, received {other:?}"),
	}
}

async fn next_delivery(socket: &mut TestSocket) -> Result<SocketDeliveryEnvelope> {
	loop {
		let message = next_socket_message(socket).await?;
		match message.body {
			client_protocol::ToClientBody::Event(event)
				if event.name == rivetkit_wasmtime::SOCKET_DELIVERY_EVENT =>
			{
				let mut cursor = std::io::Cursor::new(event.args);
				let version: u16 = serde_bare::from_reader(&mut cursor)
					.context("decode socket delivery version")?;
				if version != 1 {
					bail!("unsupported socket delivery version {version}");
				}
				let delivery: SocketDeliveryEnvelope = serde_bare::from_reader(&mut cursor)
					.context("decode socket delivery envelope")?;
				if usize::try_from(cursor.position()).unwrap_or(usize::MAX)
					!= cursor.get_ref().len()
				{
					bail!("socket delivery contains trailing bytes");
				}
				return Ok(delivery);
			}
			client_protocol::ToClientBody::Error(error) => {
				bail!("Cell websocket error {}: {}", error.code, error.message);
			}
			_ => {}
		}
	}
}

async fn next_socket_message(socket: &mut TestSocket) -> Result<client_protocol::ToClient> {
	let message = tokio::time::timeout(Duration::from_secs(10), socket.next())
		.await
		.context("time out waiting for Cell websocket message")?
		.context("Cell websocket closed")?
		.context("read Cell websocket message")?;
	let Message::Binary(payload) = message else {
		bail!("expected binary Cell websocket message");
	};
	<client_protocol::versioned::ToClient as OwnedVersionedData>::deserialize_with_embedded_version(
		&payload,
	)
	.context("decode Cell websocket message")
}

fn test_bundle(source: &[u8]) -> CellBundle {
	let methods = BTreeMap::from([
		("send".to_owned(), CellMethodMode::Command),
		("destroy".to_owned(), CellMethodMode::Command),
		("echo".to_owned(), CellMethodMode::Command),
		("sql".to_owned(), CellMethodMode::Command),
		("read".to_owned(), CellMethodMode::Query),
		("read_sql".to_owned(), CellMethodMode::Query),
	]);
	let manifest = serde_json::to_vec(&CellRuntimeManifest {
		format_version: CellRuntimeManifest::FORMAT_VERSION,
		methods,
	})
	.expect("encode test manifest");
	CellBundle::verified(source.to_vec(), manifest)
}

async fn wait_for_cell_action(
	client: &Client,
	endpoint: &str,
	actor_id: &str,
	method: &str,
	operation_id: &str,
	mode: CellMethodMode,
	input: Vec<u8>,
) -> Result<Vec<u8>> {
	let deadline = Instant::now() + Duration::from_secs(30);
	loop {
		let response = send_cell_action(
			client,
			endpoint,
			actor_id,
			method,
			operation_id,
			mode,
			input.clone(),
		)
		.await?;
		if response.status().is_success() {
			return decode_cell_action_response(response).await;
		}
		if !matches!(
			response.status(),
			StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
		) || Instant::now() >= deadline
		{
			let status = response.status();
			let body = response.bytes().await.unwrap_or_default();
			bail!(
				"Cell action `{method}` failed with {status}: {}",
				String::from_utf8_lossy(&body)
			);
		}
		tokio::time::sleep(Duration::from_millis(200)).await;
	}
}

async fn cell_action(
	client: &Client,
	endpoint: &str,
	actor_id: &str,
	method: &str,
	operation_id: &str,
	mode: CellMethodMode,
	input: Vec<u8>,
) -> Result<Vec<u8>> {
	let response = send_cell_action(
		client,
		endpoint,
		actor_id,
		method,
		operation_id,
		mode,
		input,
	)
	.await?;
	let status = response.status();
	if !status.is_success() {
		let body = response.bytes().await.unwrap_or_default();
		bail!(
			"Cell action `{method}` failed with {status}: {}",
			String::from_utf8_lossy(&body)
		);
	}
	decode_cell_action_response(response).await
}

async fn send_cell_action(
	client: &Client,
	endpoint: &str,
	actor_id: &str,
	method: &str,
	operation_id: &str,
	mode: CellMethodMode,
	input: Vec<u8>,
) -> Result<reqwest::Response> {
	let args = CellActionEnvelope {
		mode,
		operation_id: operation_id.to_owned(),
		input,
		started_at: 1,
	}
	.encode()?;
	let body = client_protocol::versioned::HttpActionRequest::wrap_latest(
		client_protocol::HttpActionRequest { args },
	)
	.serialize_with_embedded_version(client_protocol::PROTOCOL_VERSION)?;
	client
		.post(format!("{endpoint}/gateway/{actor_id}/action/{method}"))
		.header("x-rivet-encoding", "bare")
		.header("content-type", "application/octet-stream")
		.body(body)
		.send()
		.await
		.context("send Cell action")
}

async fn decode_cell_action_response(response: reqwest::Response) -> Result<Vec<u8>> {
	let payload = response
		.bytes()
		.await
		.context("read Cell action response")?;
	let response = <client_protocol::versioned::HttpActionResponse as OwnedVersionedData>::deserialize_with_embedded_version(&payload)
		.context("decode Cell action response")?;
	Ok(response.output)
}
