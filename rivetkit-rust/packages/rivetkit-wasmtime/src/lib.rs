use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use rivet_error::RivetError;
use rivetkit_core::{
	ActorActionMode, ActorConfig, ActorConsistency, ActorEvent, ActorFactory, ActorStart,
	BindParam, ColumnValue, ConnectionProtocol, PendingTransactionalEffect, StateDelta,
	TransactionalEffect, TransactionalOperation, TransactionalOperationAdmission,
	TransactionalOperationError, TransactionalOperationFailure, TransactionalOperationKind,
	TransactionalOperationRequest, TransactionalOperationStatus, TransactionalScheduleUpsert,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Notify, OwnedRwLockReadGuard, RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use vbare::OwnedVersionedData;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

mod effect_protocol {
	pub mod generated {
		include!(concat!(env!("OUT_DIR"), "/combined_imports.rs"));
	}
}

pub use effect_protocol::generated::v1::{
	ScheduleAtEffect, ScheduleCancelEffect, SocketBroadcastEffect, SocketCloseEffect,
	SocketDeliveryEnvelope, SocketSendEffect, SocketStateEffect,
};

mod bindings {
	wasmtime::component::bindgen!({
		path: "wit",
		world: "cell",
		imports: { default: async },
		exports: { default: async },
	});
}

mod action_protocol;
pub use action_protocol::{CellActionEnvelope, CellMethodMode};
const DEFAULT_MAX_BUNDLE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_MANIFEST_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_STATE_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_RESULT_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_EFFECTS: usize = 256;
const DEFAULT_MAX_EFFECT_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_SQL_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_SQL_PARAMS: usize = 1_024;
const DEFAULT_MAX_SQL_PARAM_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_SQL_ROWS: usize = 10_000;
const DEFAULT_MAX_SQL_RESULT_BYTES: usize = 1024 * 1024;
const EFFECT_DELIVERY_BATCH: u32 = 64;
const EFFECT_RETRY_DELAY: Duration = Duration::from_millis(100);
const SOCKET_OPERATION_PREFIX: &str = "socket:";
const SCHEDULE_OPERATION_PREFIX: &str = "schedule:";
pub const EFFECT_FORMAT_VERSION: u16 = 1;
pub const SOCKET_DELIVERY_FORMAT_VERSION: u16 = 1;

#[derive(Clone, Debug)]
pub struct WonderfulWasmtimeLimits {
	pub max_bundle_bytes: usize,
	pub max_manifest_bytes: usize,
	pub max_state_bytes: usize,
	pub max_result_bytes: usize,
	pub max_effects: usize,
	pub max_effect_bytes: usize,
	pub max_total_effect_bytes: usize,
	pub max_effect_kind_bytes: usize,
	pub max_sql_bytes: usize,
	pub max_sql_params: usize,
	pub max_sql_param_bytes: usize,
	pub max_sql_rows: usize,
	pub max_sql_result_bytes: usize,
	pub max_linear_memory_bytes: usize,
	pub max_component_instances: usize,
	pub max_component_memories: usize,
	pub max_component_tables: usize,
	pub max_table_elements: usize,
	pub max_concurrent_queries: usize,
	pub max_transactional_operations: u32,
	pub fuel_per_activation: u64,
	pub fuel_per_command: u64,
	pub activation_timeout: Duration,
	pub command_timeout: Duration,
}

impl Default for WonderfulWasmtimeLimits {
	fn default() -> Self {
		Self {
			max_bundle_bytes: DEFAULT_MAX_BUNDLE_BYTES,
			max_manifest_bytes: DEFAULT_MAX_MANIFEST_BYTES,
			max_state_bytes: DEFAULT_MAX_STATE_BYTES,
			max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
			max_effects: DEFAULT_MAX_EFFECTS,
			max_effect_bytes: DEFAULT_MAX_EFFECT_BYTES,
			max_total_effect_bytes: 1024 * 1024,
			max_effect_kind_bytes: 128,
			max_sql_bytes: DEFAULT_MAX_SQL_BYTES,
			max_sql_params: DEFAULT_MAX_SQL_PARAMS,
			max_sql_param_bytes: DEFAULT_MAX_SQL_PARAM_BYTES,
			max_sql_rows: DEFAULT_MAX_SQL_ROWS,
			max_sql_result_bytes: DEFAULT_MAX_SQL_RESULT_BYTES,
			max_linear_memory_bytes: 128 * 1024 * 1024,
			max_component_instances: 64,
			max_component_memories: 4,
			max_component_tables: 16,
			max_table_elements: 1_000_000,
			max_concurrent_queries: 8,
			max_transactional_operations: 10_000,
			fuel_per_activation: 100_000_000,
			fuel_per_command: 100_000_000,
			activation_timeout: Duration::from_secs(10),
			command_timeout: Duration::from_secs(30),
		}
	}
}

#[derive(Clone, Debug)]
pub struct CellBundle {
	pub source: Arc<[u8]>,
	pub manifest: Arc<[u8]>,
	pub bundle_sha256: [u8; 32],
}

impl CellBundle {
	pub fn verified(source: Vec<u8>, manifest: Vec<u8>) -> Self {
		let bundle_sha256 = bundle_digest(&source, &manifest);
		Self {
			source: source.into(),
			manifest: manifest.into(),
			bundle_sha256,
		}
	}
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CellRuntimeManifest {
	pub format_version: u16,
	pub methods: BTreeMap<String, CellMethodMode>,
}

pub const SOCKET_SEND_EFFECT: &str = "wonderful.cell.socket.send";
pub const SOCKET_BROADCAST_EFFECT: &str = "wonderful.cell.socket.broadcast";
pub const SOCKET_CLOSE_EFFECT: &str = "wonderful.cell.socket.close";
pub const SOCKET_STATE_EFFECT: &str = "wonderful.cell.socket.state";
pub const SCHEDULE_AT_EFFECT: &str = "wonderful.cell.schedule.at";
pub const SCHEDULE_CANCEL_EFFECT: &str = "wonderful.cell.schedule.cancel";
pub const DESTROY_EFFECT: &str = "wonderful.cell.destroy";
pub const SOCKET_DELIVERY_EVENT: &str = "wonderful.cell.message";
pub const SOCKET_CONNECT_METHOD: &str = "__wonderful_socket_connect";
pub const SOCKET_CLOSE_METHOD: &str = "__wonderful_socket_close";

macro_rules! versioned_payload {
	($versioned:ident, $latest:ident, $version:expr) => {
		enum $versioned {
			V1($latest),
		}

		impl OwnedVersionedData for $versioned {
			type Latest = $latest;

			fn wrap_latest(latest: Self::Latest) -> Self {
				Self::V1(latest)
			}

			fn unwrap_latest(self) -> Result<Self::Latest> {
				match self {
					Self::V1(latest) => Ok(latest),
				}
			}

			fn deserialize_version(payload: &[u8], version: u16) -> Result<Self> {
				if version != $version {
					bail!("unsupported Cell payload format version {version}");
				}
				Ok(Self::V1(serde_bare::from_slice(payload)?))
			}

			fn serialize_version(self, version: u16) -> Result<Vec<u8>> {
				if version != $version {
					bail!("unsupported Cell payload format version {version}");
				}
				match self {
					Self::V1(latest) => Ok(serde_bare::to_vec(&latest)?),
				}
			}
		}

		impl $latest {
			pub fn encode(self) -> Result<Vec<u8>> {
				<$versioned as OwnedVersionedData>::wrap_latest(self)
					.serialize_with_embedded_version($version)
			}

			pub fn decode(bytes: &[u8]) -> Result<Self> {
				<$versioned as OwnedVersionedData>::deserialize_with_embedded_version(bytes)
			}
		}
	};
}

versioned_payload!(
	VersionedSocketSendEffect,
	SocketSendEffect,
	EFFECT_FORMAT_VERSION
);
versioned_payload!(
	VersionedSocketBroadcastEffect,
	SocketBroadcastEffect,
	EFFECT_FORMAT_VERSION
);
versioned_payload!(
	VersionedSocketCloseEffect,
	SocketCloseEffect,
	EFFECT_FORMAT_VERSION
);
versioned_payload!(
	VersionedSocketStateEffect,
	SocketStateEffect,
	EFFECT_FORMAT_VERSION
);
versioned_payload!(
	VersionedScheduleAtEffect,
	ScheduleAtEffect,
	EFFECT_FORMAT_VERSION
);
versioned_payload!(
	VersionedScheduleCancelEffect,
	ScheduleCancelEffect,
	EFFECT_FORMAT_VERSION
);
versioned_payload!(
	VersionedSocketDeliveryEnvelope,
	SocketDeliveryEnvelope,
	SOCKET_DELIVERY_FORMAT_VERSION
);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SocketLifecycleInput {
	pub connection_id: String,
	pub params: Vec<u8>,
	pub state: Vec<u8>,
}

impl CellRuntimeManifest {
	pub const FORMAT_VERSION: u16 = 1;

	fn decode(bytes: &[u8]) -> Result<Self> {
		let manifest: Self =
			serde_json::from_slice(bytes).context("decode Cell runtime manifest")?;
		if manifest.format_version != Self::FORMAT_VERSION {
			bail!(
				"unsupported Cell runtime manifest version {}",
				manifest.format_version
			);
		}
		if manifest.methods.is_empty() {
			bail!("Cell runtime manifest must declare at least one method");
		}
		for name in manifest.methods.keys() {
			if name.is_empty() || name.len() > 256 || name.contains('\0') {
				bail!("Cell runtime manifest contains an invalid method name");
			}
		}
		Ok(manifest)
	}
}

fn bundle_digest(source: &[u8], manifest: &[u8]) -> [u8; 32] {
	let mut hasher = Sha256::new();
	hasher.update((source.len() as u64).to_be_bytes());
	hasher.update(source);
	hasher.update((manifest.len() as u64).to_be_bytes());
	hasher.update(manifest);
	hasher.finalize().into()
}

// Call only while holding the runtime reader/writer gate. Direct scheduled and
// lifecycle events bypass ActorTask's ordinary request admission checks.
fn ensure_cell_execution_admitted(ctx: &rivetkit_core::ActorContext) -> Result<()> {
	if ctx.is_destroy_requested() {
		bail!("CELL_DESTROYED: Cell no longer accepts application work");
	}
	Ok(())
}

fn completion_timestamp(fallback: i64) -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.ok()
		.and_then(|duration| i64::try_from(duration.as_millis()).ok())
		.unwrap_or(fallback)
}

#[async_trait]
pub trait CellBundleLoader: Send + Sync {
	async fn load(&self, actor_id: &str) -> Result<CellBundle>;
}

#[derive(Clone)]
pub struct StaticCellBundleLoader {
	bundle: CellBundle,
}

impl StaticCellBundleLoader {
	pub fn new(bundle: CellBundle) -> Self {
		Self { bundle }
	}
}

#[async_trait]
impl CellBundleLoader for StaticCellBundleLoader {
	async fn load(&self, _actor_id: &str) -> Result<CellBundle> {
		Ok(self.bundle.clone())
	}
}

#[derive(Clone)]
pub struct WonderfulWasmtimeActorFactory {
	engine: Arc<Engine>,
	component: Arc<Component>,
	bundle_loader: Arc<dyn CellBundleLoader>,
	actor_config: ActorConfig,
	limits: WonderfulWasmtimeLimits,
}

impl WonderfulWasmtimeActorFactory {
	pub fn new(
		component_bytes: &[u8],
		bundle_loader: Arc<dyn CellBundleLoader>,
		mut actor_config: ActorConfig,
		limits: WonderfulWasmtimeLimits,
	) -> Result<Self> {
		let mut config = Config::new();
		config.consume_fuel(true);
		config.wasm_component_model(true);
		let engine = Arc::new(
			Engine::new(&config)
				.map_err(|error| anyhow!("create Wonderful Wasmtime engine: {error}"))?,
		);
		let component = Arc::new(
			Component::new(&engine, component_bytes)
				.map_err(|error| anyhow!("compile Wonderful Cell component: {error}"))?,
		);
		actor_config.consistency = ActorConsistency::Transactional;
		Ok(Self {
			engine,
			component,
			bundle_loader,
			actor_config,
			limits,
		})
	}

	pub fn actor_factory(&self) -> ActorFactory {
		let runtime = self.clone();
		ActorFactory::new_with_manual_startup_ready(self.actor_config.clone(), move |start| {
			let runtime = runtime.clone();
			Box::pin(async move { runtime.run_actor(start).await })
		})
		.with_transactional_operations()
		.with_action_classifier(|_name, args| match CellActionEnvelope::decode_mode(args) {
			Ok(CellMethodMode::Query) => ActorActionMode::Concurrent,
			Ok(CellMethodMode::Command) | Err(_) => ActorActionMode::Transactional,
		})
	}

	async fn run_actor(&self, start: ActorStart) -> Result<()> {
		let ActorStart {
			ctx,
			input,
			snapshot,
			mut events,
			startup_ready,
			..
		} = start;
		let generation = ctx
			.generation()
			.context("actor generation unavailable to Wonderful Wasmtime runtime")?;
		let bundle = self
			.bundle_loader
			.load(ctx.actor_id())
			.await
			.context("load immutable Cell bundle")?;
		self.validate_bundle(&bundle)?;
		let manifest = Arc::new(CellRuntimeManifest::decode(&bundle.manifest)?);
		let initial_state = snapshot.unwrap_or_default();
		let resident = Resident::activate(
			Arc::clone(&self.engine),
			Arc::clone(&self.component),
			bundle.clone(),
			initial_state,
			input,
			self.limits.clone(),
		)
		.await;
		let mut resident = match resident {
			Ok(resident) => Some(resident),
			Err(error) => {
				if let Some(startup_ready) = startup_ready {
					let _ = startup_ready.send(Err(anyhow!("Cell activation failed")));
				}
				return Err(error);
			}
		};

		let pending_destroy = match ctx
			.has_pending_transactional_effect_kind(DESTROY_EFFECT)
			.await
		{
			Ok(pending) => pending,
			Err(error) => {
				if let Some(startup_ready) = startup_ready {
					let _ = startup_ready.send(Err(anyhow!("Cell destruction recovery failed")));
				}
				return Err(error.context("recover pending Cell destruction"));
			}
		};
		if pending_destroy && !ctx.is_destroy_requested() {
			if let Err(error) = ctx.destroy() {
				if let Some(startup_ready) = startup_ready {
					let _ = startup_ready.send(Err(anyhow!("Cell destruction recovery failed")));
				}
				return Err(error.context("resume pending Cell destruction"));
			}
		}

		let execution_gate = Arc::new(RwLock::new(()));
		let effect_notify = Arc::new(Notify::new());
		let effect_cancel = CancellationToken::new();
		let _effect_cancel_guard = effect_cancel.clone().drop_guard();
		let mut effect_handle = Some(tokio::spawn(Self::run_effect_pump(
			ctx.clone(),
			Arc::clone(&effect_notify),
			effect_cancel.clone(),
			Arc::clone(&execution_gate),
			self.limits.max_transactional_operations,
		)));
		if let Some(startup_ready) = startup_ready {
			let _ = startup_ready.send(Ok(()));
		}
		effect_notify.notify_one();
		let query_slots = Arc::new(Semaphore::new(self.limits.max_concurrent_queries));
		let mut queries = JoinSet::new();
		let mut request_connections = BTreeSet::new();
		while let Some(event) = events.recv().await {
			match event {
				ActorEvent::Action {
					name,
					args,
					scheduled_fire,
					reply,
					..
				} => {
					if let Err(error) = validate_action_identity(&args, scheduled_fire.is_some()) {
						reply.send(Err(error));
						continue;
					}
					let mode = match validate_action_mode(&manifest, &name, &args) {
						Ok(mode) => mode,
						Err(error) => {
							reply.send(Err(error));
							continue;
						}
					};
					match mode {
						CellMethodMode::Command => {
							let _writer = execution_gate.write().await;
							let outcome = self
								.invoke_action(
									&ctx,
									generation,
									&bundle,
									&mut resident,
									name.clone(),
									args,
								)
								.await;
							drop(_writer);
							match outcome {
								ActionOutcome::Reply(result) => {
									if let Err(error) = &result {
										tracing::warn!(
											method = %name,
											?error,
											"Wonderful Cell command failed"
										);
									}
									reply.send(result);
									effect_notify.notify_one();
								}
								ActionOutcome::DestroyReply(result) => {
									reply.send(Ok(result));
									ctx.submit_fenced_destroy()?;
									effect_notify.notify_one();
								}
								ActionOutcome::Fence(error) => {
									drop(reply);
									queries.abort_all();
									effect_cancel.cancel();
									if let Some(handle) = effect_handle.take() {
										let _ = handle.await;
									}
									return Err(error);
								}
							}
						}
						CellMethodMode::Query => {
							let Ok(query_slot) = Arc::clone(&query_slots).try_acquire_owned()
							else {
								reply.send(Err(anyhow!(
									"CELL_QUERY_OVERLOADED: concurrent query limit reached"
								)));
								continue;
							};
							let runtime = self.clone();
							let ctx = ctx.clone();
							let bundle = bundle.clone();
							let reader = Arc::new(Arc::clone(&execution_gate).read_owned().await);
							queries.spawn(async move {
								let _query_slot = query_slot;
								let result = runtime
									.invoke_query(&ctx, &bundle, name, args, reader)
									.await;
								reply.send(result);
							});
						}
					}
				}
				ActorEvent::SerializeState { reply, .. } => reply.send(Ok(Vec::new())),
				ActorEvent::RunGracefulCleanup { reply, .. } => {
					queries.abort_all();
					while queries.join_next().await.is_some() {}
					effect_cancel.cancel();
					if let Some(handle) = effect_handle.take() {
						let _ = handle.await;
					}
					drop(resident.take());
					reply.send(Ok(()));
				}
				ActorEvent::ConnectionClosed { conn } => {
					if request_connections.remove(conn.id()) {
						continue;
					}
					let writer = execution_gate.write().await;
					let outcome = self
						.invoke_socket_lifecycle(
							&ctx,
							generation,
							&bundle,
							&mut resident,
							&conn,
							SOCKET_CLOSE_METHOD,
							None,
						)
						.await;
					drop(writer);
					match outcome {
						ActionOutcome::Reply(Ok(_)) => effect_notify.notify_one(),
						ActionOutcome::Reply(Err(error)) => {
							tracing::warn!(connection_id = %conn.id(), ?error, "Wonderful Cell socket close handler failed");
						}
						ActionOutcome::DestroyReply(_) => {
							ctx.submit_fenced_destroy()?;
							effect_notify.notify_one();
						}
						ActionOutcome::Fence(error) => {
							queries.abort_all();
							effect_cancel.cancel();
							if let Some(handle) = effect_handle.take() {
								let _ = handle.await;
							}
							return Err(error);
						}
					}
				}
				ActorEvent::RunWake { reply, .. } => reply.send(Ok(())),
				ActorEvent::WorkflowHistoryRequested { reply }
				| ActorEvent::WorkflowReplayRequested { reply, .. } => reply.send(Ok(None)),
				ActorEvent::HttpRequest { reply, .. } => {
					reply.send(Err(anyhow!(
						"raw HTTP is not supported by the transactional Cell runtime"
					)));
				}
				ActorEvent::QueueSend { reply, .. } => {
					reply.send(Err(anyhow!(
						"queues are not supported by the transactional Cell runtime"
					)));
				}
				ActorEvent::ConnectionPreflight {
					conn,
					protocol,
					reply,
					..
				} => {
					let _reader = execution_gate.read().await;
					if let Err(error) = ensure_cell_execution_admitted(&ctx) {
						reply.send(Err(error));
						continue;
					}
					match protocol {
						ConnectionProtocol::ActorConnect => reply.send(Ok(())),
						ConnectionProtocol::Request => {
							request_connections.insert(conn.id().to_owned());
							reply.send(Ok(()));
						}
						ConnectionProtocol::RawWebSocket => reply.send(Err(anyhow!(
							"raw WebSockets are not supported by the transactional Cell runtime"
						))),
					}
				}
				ActorEvent::ConnectionOpen {
					conn, mut reply, ..
				} => {
					let _writer = execution_gate.write().await;
					if let Err(error) = ensure_cell_execution_admitted(&ctx) {
						reply.send(Err(error));
						continue;
					}
					if request_connections.contains(conn.id()) {
						reply.send(Ok(()));
						continue;
					}
					let cancellation = CancellationToken::new();
					let mut lifecycle = Box::pin(self.invoke_socket_lifecycle(
						&ctx,
						generation,
						&bundle,
						&mut resident,
						&conn,
						SOCKET_CONNECT_METHOD,
						Some(&cancellation),
					));
					let outcome = tokio::select! {
						biased;
						_ = reply.closed() => {
							cancellation.cancel();
							let outcome = lifecycle.await;
							drop(_writer);
							match outcome {
								ActionOutcome::Fence(error) => {
									queries.abort_all();
									effect_cancel.cancel();
									if let Some(handle) = effect_handle.take() {
										let _ = handle.await;
									}
									return Err(error);
								}
								ActionOutcome::DestroyReply(_) => ctx.submit_fenced_destroy()?,
								ActionOutcome::Reply(Ok(_)) => effect_notify.notify_one(),
								ActionOutcome::Reply(Err(_)) => {}
							}
							continue;
						}
						outcome = &mut lifecycle => outcome,
					};
					drop(lifecycle);
					drop(_writer);
					match outcome {
						ActionOutcome::Reply(result) => {
							reply.send(result.map(|_| ()));
							effect_notify.notify_one();
						}
						ActionOutcome::DestroyReply(_) => {
							reply.send(Ok(()));
							ctx.submit_fenced_destroy()?;
							effect_notify.notify_one();
						}
						ActionOutcome::Fence(error) => {
							drop(reply);
							queries.abort_all();
							effect_cancel.cancel();
							if let Some(handle) = effect_handle.take() {
								let _ = handle.await;
							}
							return Err(error);
						}
					}
				}
				ActorEvent::WebSocketOpen { reply, .. }
				| ActorEvent::SubscribeRequest { reply, .. }
				| ActorEvent::DisconnectConn { reply, .. } => {
					reply.send(Err(anyhow!(
						"raw connections are not supported by the transactional Cell runtime"
					)));
				}
			}
			while let Some(result) = queries.try_join_next() {
				if let Err(error) = result {
					tracing::warn!(?error, "Wonderful Cell query task failed");
				}
			}
		}
		queries.abort_all();
		effect_cancel.cancel();
		if let Some(handle) = effect_handle.take() {
			let _ = handle.await;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn invoke_socket_lifecycle(
		&self,
		ctx: &rivetkit_core::ActorContext,
		generation: u32,
		bundle: &CellBundle,
		resident: &mut Option<Resident>,
		connection: &rivetkit_core::ConnHandle,
		method: &str,
		cancellation: Option<&CancellationToken>,
	) -> ActionOutcome {
		let args = match socket_lifecycle_envelope(connection, generation, method) {
			Ok(args) => args,
			Err(error) => return ActionOutcome::Reply(Err(error)),
		};
		self.invoke_action_inner(
			ctx,
			generation,
			bundle,
			resident,
			method.to_owned(),
			args,
			cancellation,
		)
		.await
	}

	async fn invoke_query(
		&self,
		ctx: &rivetkit_core::ActorContext,
		bundle: &CellBundle,
		name: String,
		args: Vec<u8>,
		reader: Arc<OwnedRwLockReadGuard<()>>,
	) -> Result<Vec<u8>> {
		ensure_cell_execution_admitted(ctx)?;
		let envelope = CellActionEnvelope::decode(&args)?;
		if envelope.mode != CellMethodMode::Query {
			bail!("CELL_METHOD_MODE_MISMATCH: query was not encoded as read-only");
		}
		let snapshot_version = ctx.transactional_snapshot_version().await?;
		let snapshot = ctx.state();
		tracing::debug!(
			actor_id = %ctx.actor_id(),
			method = %name,
			snapshot_version,
			"executing Wonderful Cell query replica"
		);
		let mut replica = Resident::activate(
			Arc::clone(&self.engine),
			Arc::clone(&self.component),
			bundle.clone(),
			snapshot.clone(),
			None,
			self.limits.clone(),
		)
		.await?;
		let output = replica
			.invoke(
				&name,
				&envelope.input,
				SqlCapability::ReadOnly(ctx.clone(), reader),
			)
			.await?;
		validate_output(&output, &self.limits)?;
		if output.state != snapshot || !output.effects.is_empty() || output.destroyed {
			bail!("CELL_QUERY_MUTATION_VIOLATION: query attempted to mutate Cell state or effects");
		}
		Ok(output.result)
	}

	async fn invoke_action(
		&self,
		ctx: &rivetkit_core::ActorContext,
		generation: u32,
		bundle: &CellBundle,
		resident: &mut Option<Resident>,
		name: String,
		args: Vec<u8>,
	) -> ActionOutcome {
		self.invoke_action_inner(ctx, generation, bundle, resident, name, args, None)
			.await
	}

	#[allow(clippy::too_many_arguments)]
	async fn invoke_action_inner(
		&self,
		ctx: &rivetkit_core::ActorContext,
		generation: u32,
		bundle: &CellBundle,
		resident: &mut Option<Resident>,
		name: String,
		args: Vec<u8>,
		cancellation: Option<&CancellationToken>,
	) -> ActionOutcome {
		if let Err(error) = ensure_cell_execution_admitted(ctx) {
			return ActionOutcome::Reply(Err(error));
		}
		let envelope = match CellActionEnvelope::decode(&args) {
			Ok(envelope) => envelope,
			Err(error) => return ActionOutcome::Reply(Err(error)),
		};
		let request = TransactionalOperationRequest {
			operation_id: envelope.operation_id,
			kind: TransactionalOperationKind::Action,
			name: name.clone(),
			input: envelope.input.clone(),
			actor_generation: generation,
			started_at: envelope.started_at,
		};
		let admission = match ctx
			.begin_transactional_operation(request, Some(self.limits.command_timeout))
			.await
		{
			Ok(admission) => admission,
			Err(error) if is_remote_sqlite_indeterminate(&error) => {
				*resident = None;
				return ActionOutcome::Fence(error);
			}
			Err(error) => return ActionOutcome::Reply(Err(error)),
		};
		let operation = match admission {
			TransactionalOperationAdmission::Execute(operation) => operation,
			TransactionalOperationAdmission::Existing(record) => {
				return ActionOutcome::Reply(existing_operation_result(record));
			}
		};
		if cancellation.is_some_and(CancellationToken::is_cancelled) {
			return fail_operation(
				operation,
				anyhow!("CELL_CONNECTION_SETUP_EXPIRED: connection setup reply expired"),
				completion_timestamp(envelope.started_at),
			)
			.await;
		}

		if resident.is_none() {
			match Resident::activate(
				Arc::clone(&self.engine),
				Arc::clone(&self.component),
				bundle.clone(),
				ctx.state(),
				None,
				self.limits.clone(),
			)
			.await
			{
				Ok(replacement) => *resident = Some(replacement),
				Err(error) => {
					return fail_operation(
						operation,
						error,
						completion_timestamp(envelope.started_at),
					)
					.await;
				}
			}
		}
		let active = Arc::new(Mutex::new(Some(operation)));
		let invoke = resident.as_mut().expect("resident was activated").invoke(
			&name,
			&envelope.input,
			SqlCapability::Transactional(Arc::clone(&active)),
		);
		let invocation = match cancellation {
			Some(cancellation) => {
				tokio::select! {
					biased;
					_ = cancellation.cancelled() => Err(anyhow!("CELL_CONNECTION_SETUP_EXPIRED: connection setup reply expired")),
					result = invoke => result,
				}
			}
			None => invoke.await,
		};
		let operation = active.lock().await.take();
		let Some(operation) = operation else {
			*resident = None;
			return ActionOutcome::Fence(anyhow!(
				"Cell component returned without its transactional operation"
			));
		};
		match invocation {
			Ok(output) => {
				if let Err(error) = validate_output(&output, &self.limits) {
					*resident = None;
					return fail_operation(
						operation,
						error,
						completion_timestamp(envelope.started_at),
					)
					.await;
				}
				let operation_id = operation.request().operation_id.clone();
				let (mut connection_deltas, mut effects) = match prepare_effects(
					ctx,
					bundle,
					&operation_id,
					self.actor_config.max_outgoing_message_size,
					output.effects,
				) {
					Ok(prepared) => prepared,
					Err(error) => {
						*resident = None;
						return fail_operation(
							operation,
							error,
							completion_timestamp(envelope.started_at),
						)
						.await;
					}
				};
				if output.destroyed {
					effects.push(TransactionalEffect {
						kind: DESTROY_EFFECT.to_owned(),
						payload: Vec::new(),
					});
				}
				if !effects.is_empty() && !output.destroyed {
					let pending_operations = match operation.pending_effect_operation_count().await
					{
						Ok(count) => count,
						Err(error) => {
							*resident = None;
							return fail_operation(
								operation,
								error,
								completion_timestamp(envelope.started_at),
							)
							.await;
						}
					};
					if let Err(error) = validate_effect_backlog(
						pending_operations,
						self.limits.max_transactional_operations,
					) {
						*resident = None;
						return fail_operation(
							operation,
							error,
							completion_timestamp(envelope.started_at),
						)
						.await;
					}
				}
				if let Err(error) = stage_schedule_effects(&operation, &effects).await {
					*resident = None;
					return fail_operation(
						operation,
						error,
						completion_timestamp(envelope.started_at),
					)
					.await;
				}
				let result = output.result.clone();
				let mut deltas = vec![StateDelta::ActorState(output.state)];
				deltas.append(&mut connection_deltas);
				if cancellation.is_some_and(CancellationToken::is_cancelled) {
					*resident = None;
					return fail_operation(
						operation,
						anyhow!("CELL_CONNECTION_SETUP_EXPIRED: connection setup reply expired"),
						completion_timestamp(envelope.started_at),
					)
					.await;
				}
				let destroy_requested = output.destroyed;
				match operation
					.commit(
						deltas,
						output.result,
						effects,
						completion_timestamp(envelope.started_at),
					)
					.await
				{
					Ok(_) if destroy_requested => {
						if !ctx.is_destroy_requested() {
							if let Err(error) = ctx.fence_destroy() {
								*resident = None;
								return ActionOutcome::Fence(
									error.context("fence committed Cell destruction"),
								);
							}
						}
						ActionOutcome::DestroyReply(result)
					}
					Ok(_) => ActionOutcome::Reply(Ok(result)),
					Err(error) => {
						*resident = None;
						ActionOutcome::Fence(error)
					}
				}
			}
			Err(error) => {
				*resident = None;
				fail_operation(operation, error, completion_timestamp(envelope.started_at)).await
			}
		}
	}

	async fn run_effect_pump(
		ctx: rivetkit_core::ActorContext,
		notify: Arc<Notify>,
		cancel: CancellationToken,
		execution_gate: Arc<RwLock<()>>,
		max_transactional_operations: u32,
	) {
		loop {
			tokio::select! {
				_ = cancel.cancelled() => return,
				_ = notify.notified() => {}
			}
			let should_stop = ctx
				.internal_keep_awake(async {
					loop {
						if cancel.is_cancelled() {
							return true;
						}
						// Once a delivery starts, drive its actor-local transaction to a
						// terminal commit/rollback before observing cancellation. Dropping
						// an in-flight remote transaction could outlive this generation.
						let delivery = {
							// Remote actor-local SQLite is one connection. Keep effect scans,
							// delivery acknowledgements, and compaction out of command/query
							// transactions by sharing the runtime's writer fence.
							let _writer = tokio::select! {
								biased;
								_ = cancel.cancelled() => return true,
								writer = execution_gate.write() => writer,
							};
							let delivery = Self::deliver_pending_effect_batch(&ctx).await;
							if let Err(error) = ctx
								.compact_transactional_operations(max_transactional_operations)
								.await
							{
								tracing::warn!(actor_id = %ctx.actor_id(), ?error, "Wonderful Cell operation compaction failed");
							}
							delivery
						};
						match delivery {
							Ok((_, true)) => return true,
							Ok((0, false)) => return false,
							Ok((_, false)) => {}
							Err(error) => {
								tracing::warn!(actor_id = %ctx.actor_id(), ?error, "Wonderful Cell effect delivery failed");
								tokio::select! {
									_ = cancel.cancelled() => return true,
									_ = tokio::time::sleep(EFFECT_RETRY_DELAY) => {}
								}
							}
						}
					}
				})
				.await;
			if should_stop {
				return;
			}
		}
	}

	async fn deliver_pending_effect_batch(
		ctx: &rivetkit_core::ActorContext,
	) -> Result<(usize, bool)> {
		let effects = ctx
			.pending_transactional_effects(EFFECT_DELIVERY_BATCH)
			.await?;
		let count = effects.len();
		for effect in effects {
			let attempted_at = completion_timestamp(0);
			if let Err(error) = Self::deliver_effect(ctx, &effect, attempted_at).await {
				ctx.mark_transactional_effect_attempt_failed(
					&effect.operation_id,
					effect.effect_index,
					attempted_at,
				)
				.await
				.context("record Wonderful Cell effect delivery failure")?;
				return Err(error);
			}
			if effect.kind == DESTROY_EFFECT {
				return Ok((count, true));
			}
		}
		Ok((count, false))
	}

	async fn deliver_effect(
		ctx: &rivetkit_core::ActorContext,
		effect: &PendingTransactionalEffect,
		attempted_at: i64,
	) -> Result<()> {
		match effect.kind.as_str() {
			SOCKET_SEND_EFFECT => {
				let send = SocketSendEffect::decode(&effect.payload)?;
				let connection = {
					ctx.conns()
						.find(|connection| connection.id() == send.connection_id)
				};
				if let Some(connection) = connection {
					let payload = SocketDeliveryEnvelope {
						operation_id: effect.operation_id.clone(),
						effect_index: effect.effect_index,
						payload: send.payload,
					}
					.encode()?;
					connection
						.send_admitted(SOCKET_DELIVERY_EVENT, &payload)
						.await?;
				}
				ctx.mark_transactional_effect_delivered(
					&effect.operation_id,
					effect.effect_index,
					attempted_at,
				)
				.await?;
			}
			SOCKET_BROADCAST_EFFECT => {
				let broadcast = SocketBroadcastEffect::decode(&effect.payload)?;
				let excluded = broadcast
					.exclude_connection_ids
					.into_iter()
					.collect::<BTreeSet<_>>();
				let connections = ctx
					.conns()
					.filter(|connection| !excluded.contains(connection.id()))
					.collect::<Vec<_>>();
				let payload = SocketDeliveryEnvelope {
					operation_id: effect.operation_id.clone(),
					effect_index: effect.effect_index,
					payload: broadcast.payload,
				}
				.encode()?;
				for connection in connections {
					connection
						.send_admitted(SOCKET_DELIVERY_EVENT, &payload)
						.await?;
				}
				ctx.mark_transactional_effect_delivered(
					&effect.operation_id,
					effect.effect_index,
					attempted_at,
				)
				.await?;
			}
			SOCKET_CLOSE_EFFECT => {
				let close = SocketCloseEffect::decode(&effect.payload)?;
				let connection = ctx
					.conns()
					.find(|connection| connection.id() == close.connection_id);
				if let Some(connection) = connection {
					connection.disconnect(close.reason.as_deref()).await?;
				}
				ctx.mark_transactional_effect_delivered(
					&effect.operation_id,
					effect.effect_index,
					attempted_at,
				)
				.await?;
			}
			SCHEDULE_AT_EFFECT => {
				let schedule = ScheduleAtEffect::decode(&effect.payload)?;
				validate_schedule_id(&schedule.schedule_id)?;
				ctx.acknowledge_transactional_schedule_change(
					&effect.operation_id,
					effect.effect_index,
					attempted_at,
					&schedule_event_id(&schedule.schedule_id),
				)
				.await?;
			}
			SCHEDULE_CANCEL_EFFECT => {
				let schedule = ScheduleCancelEffect::decode(&effect.payload)?;
				validate_schedule_id(&schedule.schedule_id)?;
				ctx.acknowledge_transactional_schedule_change(
					&effect.operation_id,
					effect.effect_index,
					attempted_at,
					&schedule_event_id(&schedule.schedule_id),
				)
				.await?;
			}
			DESTROY_EFFECT => {
				if !ctx.is_destroy_requested() {
					ctx.destroy()?;
				}
				// Deliberately leave the durable effect pending. ActorIntent::Stop is
				// fire-and-forget, so deleting the intent here would create a crash
				// window before the engine observes the destruction request.
			}
			kind => bail!("unsupported Wonderful Cell effect kind {kind}"),
		}
		Ok(())
	}

	fn validate_bundle(&self, bundle: &CellBundle) -> Result<()> {
		if bundle.source.len() > self.limits.max_bundle_bytes {
			bail!("Cell bundle exceeds configured source limit");
		}
		if bundle.manifest.len() > self.limits.max_manifest_bytes {
			bail!("Cell bundle exceeds configured manifest limit");
		}
		let digest = bundle_digest(&bundle.source, &bundle.manifest);
		if digest != bundle.bundle_sha256 {
			bail!("Cell bundle digest mismatch");
		}
		Ok(())
	}
}

fn validate_action_identity(args: &[u8], scheduled: bool) -> Result<()> {
	let envelope = CellActionEnvelope::decode(args)?;
	let reserved = envelope.operation_id.starts_with(SOCKET_OPERATION_PREFIX)
		|| envelope.operation_id.starts_with(SCHEDULE_OPERATION_PREFIX);
	if scheduled {
		if !envelope.operation_id.starts_with(SCHEDULE_OPERATION_PREFIX) {
			bail!("CELL_OPERATION_ID_INVALID: scheduled command lacks server identity");
		}
	} else if reserved {
		bail!("CELL_OPERATION_ID_RESERVED: operation ID uses a server-owned prefix");
	}
	Ok(())
}

fn validate_action_mode(
	manifest: &CellRuntimeManifest,
	name: &str,
	args: &[u8],
) -> Result<CellMethodMode> {
	let declared = manifest
		.methods
		.get(name)
		.copied()
		.with_context(|| format!("Cell method {name} is not declared"))?;
	let encoded = CellActionEnvelope::decode(args)?.mode;
	if declared != encoded {
		bail!(
			"CELL_METHOD_MODE_MISMATCH: method {name} is declared as {declared:?} but encoded as {encoded:?}"
		);
	}
	Ok(declared)
}

fn socket_lifecycle_envelope(
	connection: &rivetkit_core::ConnHandle,
	generation: u32,
	method: &str,
) -> Result<Vec<u8>> {
	validate_connection_id(connection.id())?;
	CellActionEnvelope {
		mode: CellMethodMode::Command,
		operation_id: format!(
			"{SOCKET_OPERATION_PREFIX}{generation}:{method}:{}",
			connection.id()
		),
		input: serde_bare::to_vec(&SocketLifecycleInput {
			connection_id: connection.id().to_owned(),
			params: connection.params(),
			state: connection.state(),
		})?,
		started_at: completion_timestamp(0),
	}
	.encode()
}

fn validate_connection_id(connection_id: &str) -> Result<()> {
	if connection_id.is_empty() || connection_id.len() > 256 || connection_id.contains('\0') {
		bail!("Cell connection ID must contain 1 to 256 bytes");
	}
	Ok(())
}

fn validate_schedule_id(schedule_id: &str) -> Result<()> {
	if schedule_id.is_empty()
		|| schedule_id.len() > 200
		|| !schedule_id
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
	{
		bail!("Cell schedule ID must contain 1 to 200 ASCII identifier bytes");
	}
	Ok(())
}

fn schedule_event_id(schedule_id: &str) -> String {
	format!("wonderful:{schedule_id}")
}

fn schedule_operation_id(operation_id: &str, effect_index: u32) -> String {
	let mut hasher = Sha256::new();
	hasher.update(operation_id.as_bytes());
	hasher.update(effect_index.to_be_bytes());
	let digest = hasher.finalize();
	let mut encoded = String::with_capacity(73);
	encoded.push_str(SCHEDULE_OPERATION_PREFIX);
	for byte in digest {
		use std::fmt::Write as _;
		write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
	}
	encoded
}

fn is_remote_sqlite_indeterminate(error: &anyhow::Error) -> bool {
	let error = RivetError::extract(error);
	error.group() == "sqlite" && error.code() == "remote_indeterminate_result"
}

#[derive(Debug)]
enum ActionOutcome {
	Reply(Result<Vec<u8>>),
	DestroyReply(Vec<u8>),
	Fence(anyhow::Error),
}

fn existing_operation_result(
	record: rivetkit_core::TransactionalOperationRecord,
) -> Result<Vec<u8>> {
	match record.status {
		TransactionalOperationStatus::Succeeded => record
			.result
			.context("successful transactional operation has no result"),
		TransactionalOperationStatus::Failed => {
			let failure = record
				.error
				.context("failed transactional operation has no durable error")?;
			Err(anyhow!(
				"{}/{}: {}",
				failure.group,
				failure.code,
				failure.message
			))
		}
		TransactionalOperationStatus::Interrupted => {
			Err(TransactionalOperationError::OperationInterrupted {
				operation_id: record.request.operation_id,
			}
			.build())
		}
		TransactionalOperationStatus::Started => {
			Err(TransactionalOperationError::OperationInProgress {
				operation_id: record.request.operation_id,
			}
			.build())
		}
	}
}

async fn fail_operation(
	operation: TransactionalOperation,
	error: anyhow::Error,
	completed_at: i64,
) -> ActionOutcome {
	let (code, message) = error
		.downcast_ref::<CellGuestInvocationError>()
		.map(CellGuestInvocationError::durable_failure)
		.unwrap_or_else(|| {
			(
				"CELL_GUEST_FAILED".to_owned(),
				"Cell guest execution failed.".to_owned(),
			)
		});
	let failure = TransactionalOperationFailure {
		group: "actor".to_owned(),
		code,
		message,
		metadata: None,
	};
	match operation.fail_with(failure, completed_at).await {
		Ok(_) => ActionOutcome::Reply(Err(error)),
		Err(persist_error) => ActionOutcome::Fence(persist_error),
	}
}

#[derive(Debug, thiserror::Error)]
#[error("{code}: {message}")]
struct CellGuestInvocationError {
	code: String,
	message: String,
}

impl CellGuestInvocationError {
	fn bounded_guest_error(code: String, message: String) -> Self {
		Self {
			code: code.chars().take(64).collect(),
			message: message.chars().take(4096).collect(),
		}
	}

	fn durable_failure(&self) -> (String, String) {
		let code = if !self.code.is_empty()
			&& self.code.len() <= 64
			&& self
				.code
				.bytes()
				.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
		{
			self.code.clone()
		} else {
			"CELL_GUEST_FAILED".to_owned()
		};
		let message = self.message.chars().take(4096).collect();
		(code, message)
	}
}

#[derive(Debug)]
struct GuestOutput {
	state: Vec<u8>,
	result: Vec<u8>,
	effects: Vec<GuestEffect>,
	destroyed: bool,
}

#[derive(Debug)]
struct GuestEffect {
	kind: String,
	payload: Vec<u8>,
}

enum SqlCapability {
	Unavailable,
	Transactional(Arc<Mutex<Option<TransactionalOperation>>>),
	ReadOnly(rivetkit_core::ActorContext, Arc<OwnedRwLockReadGuard<()>>),
}

struct HostState {
	limits: StoreLimits,
	sql_capability: SqlCapability,
	command_deadline_unix_ms: u64,
	pending_query_sql: Option<tokio::task::JoinHandle<Result<rivetkit_core::ExecuteResult>>>,
	query_mutation_violation: bool,
	sql_policy_violation: bool,
	sql_limit_violation: bool,
	runtime_limits: WonderfulWasmtimeLimits,
	wasi: WasiCtx,
	resources: ResourceTable,
}

impl WasiView for HostState {
	fn ctx(&mut self) -> WasiCtxView<'_> {
		WasiCtxView {
			ctx: &mut self.wasi,
			table: &mut self.resources,
		}
	}
}

impl bindings::wonderful::cells::host::Host for HostState {
	async fn execute_sql(
		&mut self,
		sql: String,
		params: Vec<bindings::wonderful::cells::host::SqlValue>,
	) -> Result<bindings::wonderful::cells::host::SqlExecuteResult, String> {
		let params = params
			.into_iter()
			.map(bind_param_from_guest)
			.collect::<Vec<_>>();
		if let Err(error) = validate_guest_sql(&sql, &params, &self.runtime_limits) {
			self.sql_policy_violation = true;
			return Err(format!("CELL_SQL_POLICY_VIOLATION: {error}"));
		}
		let sql = sql_with_result_limits(
			sql,
			self.runtime_limits.max_sql_rows,
			self.runtime_limits.max_sql_result_bytes,
			self.command_deadline_unix_ms,
		);
		let result = match &self.sql_capability {
			SqlCapability::Transactional(active) => {
				let guard = active.lock().await;
				let Some(operation) = guard.as_ref() else {
					return Err("transactional operation is no longer active".to_owned());
				};
				operation
					.execute_sql(sql, Some(params))
					.await
					.map_err(|error| format!("{error:#}"))?
			}
			SqlCapability::ReadOnly(ctx, reader) => {
				if let Err(error) = validate_read_only_sql(&sql, &params, &self.runtime_limits) {
					self.query_mutation_violation = true;
					return Err(format!("CELL_QUERY_MUTATION_VIOLATION: {error}"));
				}
				let ctx = ctx.clone();
				let reader = Arc::clone(reader);
				// Retain the committed-version fence until the executor confirms
				// termination, even if the guest or query task is cancelled.
				let execution = tokio::spawn(async move {
					let _reader = reader;
					ctx.sql().execute(sql, Some(params)).await
				});
				self.pending_query_sql = Some(execution);
				let result = self
					.pending_query_sql
					.as_mut()
					.expect("query SQL task installed")
					.await;
				self.pending_query_sql.take();
				result
					.map_err(|error| format!("query SQL task terminated: {error}"))?
					.map_err(|error| format!("{error:#}"))?
			}
			SqlCapability::Unavailable => {
				return Err("SQL is unavailable outside an active method".to_owned());
			}
		};
		if let Err(error) = validate_sql_result(&result, &self.runtime_limits) {
			self.sql_limit_violation = true;
			return Err(format!("CELL_SQL_LIMIT_EXCEEDED: {error:#}"));
		}
		Ok(sql_result_to_guest(result))
	}
}

fn validate_guest_sql(
	sql: &str,
	params: &[BindParam],
	limits: &WonderfulWasmtimeLimits,
) -> Result<()> {
	if sql.len() > limits.max_sql_bytes {
		bail!("Cell SQL exceeds configured size limit");
	}
	if params.len() > limits.max_sql_params {
		bail!("Cell SQL exceeds configured parameter count");
	}
	let parameter_bytes = params
		.iter()
		.map(|param| match param {
			BindParam::Null => 0,
			BindParam::Integer(_) | BindParam::Float(_) => 8,
			BindParam::Text(value) => value.len(),
			BindParam::Blob(value) => value.len(),
		})
		.fold(0usize, usize::saturating_add);
	if parameter_bytes > limits.max_sql_param_bytes {
		bail!("Cell SQL exceeds configured parameter bytes");
	}
	let mut offset = 0;
	let first = next_sql_keyword(sql, &mut offset).context("Cell SQL is empty")?;
	if [
		"BEGIN",
		"COMMIT",
		"END",
		"ROLLBACK",
		"SAVEPOINT",
		"RELEASE",
		"ATTACH",
		"DETACH",
		"PRAGMA",
		"VACUUM",
	]
	.iter()
	.any(|denied| first.eq_ignore_ascii_case(denied))
	{
		bail!("transaction and database-control SQL is unavailable to Cells");
	}
	let normalized = sql.to_ascii_lowercase();
	if [
		"_rivet_",
		"sqlite_",
		"pragma_",
		"load_extension",
		"readfile",
		"writefile",
	]
	.iter()
	.any(|denied| normalized.contains(denied))
	{
		bail!("Rivet runtime storage and side-effecting SQL extensions are unavailable to Cells");
	}
	Ok(())
}

fn validate_read_only_sql(
	sql: &str,
	_params: &[BindParam],
	_limits: &WonderfulWasmtimeLimits,
) -> Result<()> {
	let mut offset = 0;
	let first = next_sql_keyword(sql, &mut offset).context("query SQL is empty")?;
	if !first.eq_ignore_ascii_case("SELECT") {
		bail!("only SELECT statements are available to Cell queries");
	}
	if sql.contains(';') {
		bail!("multiple or terminated SQL statements are unavailable to Cell queries");
	}
	Ok(())
}

fn sql_with_result_limits(
	sql: String,
	max_rows: usize,
	max_bytes: usize,
	deadline_unix_ms: u64,
) -> String {
	// The executor owns the conservative native work ceiling. This absolute
	// deadline also counts time spent queued before SQLite begins execution.
	format!("/*rivet-result-limits:{max_rows}:{max_bytes}:{deadline_unix_ms}:1000000*/{sql}")
}

fn validate_sql_result(
	result: &rivetkit_core::ExecuteResult,
	limits: &WonderfulWasmtimeLimits,
) -> Result<()> {
	if result.rows.len() > limits.max_sql_rows {
		bail!("Cell SQL result exceeds configured row limit");
	}
	let bytes = result
		.columns
		.iter()
		.map(String::len)
		.chain(result.rows.iter().flat_map(|row| {
			row.iter().map(|value| match value {
				ColumnValue::Null => 0,
				ColumnValue::Integer(_) | ColumnValue::Float(_) => 8,
				ColumnValue::Text(value) => value.len(),
				ColumnValue::Blob(value) => value.len(),
			})
		}))
		.fold(0usize, usize::saturating_add);
	if bytes > limits.max_sql_result_bytes {
		bail!("Cell SQL result exceeds configured byte limit");
	}
	Ok(())
}

fn next_sql_keyword<'a>(sql: &'a str, offset: &mut usize) -> Option<&'a str> {
	let bytes = sql.as_bytes();
	loop {
		while bytes.get(*offset).is_some_and(u8::is_ascii_whitespace) {
			*offset += 1;
		}
		if bytes.get(*offset..*offset + 2) == Some(b"--") {
			*offset += 2;
			while bytes.get(*offset).is_some_and(|byte| *byte != b'\n') {
				*offset += 1;
			}
			continue;
		}
		if bytes.get(*offset..*offset + 2) == Some(b"/*") {
			*offset += 2;
			while bytes.get(*offset..*offset + 2) != Some(b"*/") {
				bytes.get(*offset)?;
				*offset += 1;
			}
			*offset += 2;
			continue;
		}
		break;
	}
	let start = *offset;
	while bytes.get(*offset).is_some_and(u8::is_ascii_alphabetic) {
		*offset += 1;
	}
	(start != *offset).then(|| &sql[start..*offset])
}

fn bind_param_from_guest(value: bindings::wonderful::cells::host::SqlValue) -> BindParam {
	use bindings::wonderful::cells::host::SqlValue;
	match value {
		SqlValue::NullValue => BindParam::Null,
		SqlValue::Integer(value) => BindParam::Integer(value),
		SqlValue::Float(value) => BindParam::Float(value),
		SqlValue::Text(value) => BindParam::Text(value),
		SqlValue::Blob(value) => BindParam::Blob(value),
	}
}

fn sql_result_to_guest(
	result: rivetkit_core::ExecuteResult,
) -> bindings::wonderful::cells::host::SqlExecuteResult {
	bindings::wonderful::cells::host::SqlExecuteResult {
		columns: result.columns,
		rows: result
			.rows
			.into_iter()
			.map(|row| row.into_iter().map(sql_value_to_guest).collect())
			.collect(),
		changes: result.changes,
		last_insert_row_id: result.last_insert_row_id,
	}
}

fn sql_value_to_guest(value: ColumnValue) -> bindings::wonderful::cells::host::SqlValue {
	use bindings::wonderful::cells::host::SqlValue;
	match value {
		ColumnValue::Null => SqlValue::NullValue,
		ColumnValue::Integer(value) => SqlValue::Integer(value),
		ColumnValue::Float(value) => SqlValue::Float(value),
		ColumnValue::Text(value) => SqlValue::Text(value),
		ColumnValue::Blob(value) => SqlValue::Blob(value),
	}
}

struct Resident {
	store: Store<HostState>,
	bindings: bindings::Cell,
	limits: WonderfulWasmtimeLimits,
}

impl Resident {
	async fn activate(
		engine: Arc<Engine>,
		component: Arc<Component>,
		bundle: CellBundle,
		initial_state: Vec<u8>,
		input: Option<Vec<u8>>,
		limits: WonderfulWasmtimeLimits,
	) -> Result<Self> {
		let activation_deadline = tokio::time::Instant::now() + limits.activation_timeout;
		if initial_state.len() > limits.max_state_bytes {
			bail!("Cell state exceeds configured activation limit");
		}
		let store_limits = StoreLimitsBuilder::new()
			.memory_size(limits.max_linear_memory_bytes)
			.instances(limits.max_component_instances)
			.memories(limits.max_component_memories)
			.tables(limits.max_component_tables)
			.table_elements(limits.max_table_elements)
			.build();
		let mut store = Store::new(
			&engine,
			HostState {
				limits: store_limits,
				sql_capability: SqlCapability::Unavailable,
				command_deadline_unix_ms: 0,
				pending_query_sql: None,
				query_mutation_violation: false,
				sql_policy_violation: false,
				sql_limit_violation: false,
				runtime_limits: limits.clone(),
				wasi: WasiCtx::builder().build(),
				resources: ResourceTable::new(),
			},
		);
		store.limiter(|state| &mut state.limits);
		store.set_fuel(limits.fuel_per_activation)?;
		// Fuel caps total instructions; periodic yields let Tokio enforce deadlines
		// and make progress on other actors even when guest code never calls a host.
		store.fuel_async_yield_interval(Some(10_000))?;
		let mut linker = Linker::new(&engine);
		wasmtime_wasi::p2::add_to_linker_async(&mut linker)
			.map_err(|error| anyhow!("link sandboxed WASI interfaces: {error}"))?;
		bindings::wonderful::cells::host::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
			&mut linker,
			|state| state,
		)?;
		let bindings = tokio::time::timeout_at(
			activation_deadline,
			bindings::Cell::instantiate_async(&mut store, &component, &linker),
		)
		.await
		.context("Cell component instantiation timed out")?
		.map_err(|error| anyhow!("instantiate Wonderful Cell component: {error}"))?;
		let activation = tokio::time::timeout_at(
			activation_deadline,
			bindings.wonderful_cells_guest().call_activate(
				&mut store,
				&bundle.source,
				&bundle.manifest,
				&initial_state,
				input.as_deref(),
			),
		)
		.await
		.context("Cell activation timed out")?
		.map_err(|error| anyhow!("call Cell activation export: {error}"))?;
		if let Err(error) = activation {
			bail!("{}: {}", error.code, error.message);
		}
		Ok(Self {
			store,
			bindings,
			limits,
		})
	}

	async fn invoke(
		&mut self,
		name: &str,
		input: &[u8],
		sql_capability: SqlCapability,
	) -> Result<GuestOutput> {
		self.store.set_fuel(self.limits.fuel_per_command)?;
		self.store.data_mut().command_deadline_unix_ms = u64::try_from(
			SystemTime::now()
				.duration_since(UNIX_EPOCH)?
				.as_millis()
				.saturating_add(self.limits.command_timeout.as_millis()),
		)
		.context("Cell command deadline exceeds timestamp range")?;
		self.store.data_mut().sql_capability = sql_capability;
		self.store.data_mut().query_mutation_violation = false;
		self.store.data_mut().sql_policy_violation = false;
		self.store.data_mut().sql_limit_violation = false;
		let invocation = tokio::time::timeout(
			self.limits.command_timeout,
			self.bindings
				.wonderful_cells_guest()
				.call_invoke_command(&mut self.store, name, input),
		)
		.await;
		// A timed-out guest can leave a host SQL call in flight. Drain it before
		// returning the query outcome; its task also owns the read fence if this
		// entire future is aborted during shutdown.
		if let Some(execution) = self.store.data_mut().pending_query_sql.take() {
			let _ = execution.await;
		}
		let query_mutation_violation = self.store.data().query_mutation_violation;
		let sql_policy_violation = self.store.data().sql_policy_violation;
		let sql_limit_violation = self.store.data().sql_limit_violation;
		self.store.data_mut().sql_capability = SqlCapability::Unavailable;
		if query_mutation_violation {
			return Err(CellGuestInvocationError {
				code: "CELL_QUERY_MUTATION_VIOLATION".to_owned(),
				message: "Cell query attempted to execute mutable SQL".to_owned(),
			}
			.into());
		}
		if sql_policy_violation {
			return Err(CellGuestInvocationError {
				code: "CELL_SQL_POLICY_VIOLATION".to_owned(),
				message: "Cell SQL violated the runtime storage policy".to_owned(),
			}
			.into());
		}
		if sql_limit_violation {
			return Err(CellGuestInvocationError {
				code: "CELL_SQL_LIMIT_EXCEEDED".to_owned(),
				message: "Cell SQL result exceeded a runtime limit".to_owned(),
			}
			.into());
		}
		let invocation = invocation.map_err(|_| CellGuestInvocationError {
			code: "CELL_GUEST_TIMEOUT".to_owned(),
			message: "Cell guest command exceeded its execution deadline".to_owned(),
		})?;
		let invocation = invocation.map_err(|_error| CellGuestInvocationError {
			code: "CELL_GUEST_TRAP".to_owned(),
			message: "Cell guest execution trapped".to_owned(),
		})?;
		let output = invocation.map_err(|error| {
			CellGuestInvocationError::bounded_guest_error(error.code, error.message)
		})?;
		Ok(GuestOutput {
			state: output.state,
			result: output.value,
			effects: output
				.effects
				.into_iter()
				.map(|effect| GuestEffect {
					kind: effect.kind,
					payload: effect.payload,
				})
				.collect(),
			destroyed: output.destroyed,
		})
	}
}

async fn stage_schedule_effects(
	operation: &TransactionalOperation,
	effects: &[TransactionalEffect],
) -> Result<()> {
	for (effect_index, effect) in effects.iter().enumerate() {
		match effect.kind.as_str() {
			SCHEDULE_AT_EFFECT => {
				let schedule = ScheduleAtEffect::decode(&effect.payload)?;
				let args = CellActionEnvelope {
					mode: CellMethodMode::Command,
					operation_id: schedule_operation_id(
						&operation.request().operation_id,
						u32::try_from(effect_index).context("schedule effect index exceeds u32")?,
					),
					input: schedule.input,
					started_at: schedule.timestamp_ms,
				}
				.encode()?;
				operation
					.stage_schedule_upsert(TransactionalScheduleUpsert {
						event_id: &schedule_event_id(&schedule.schedule_id),
						trigger_at: schedule.timestamp_ms,
						action: &schedule.method,
						args: &args,
					})
					.await?;
			}
			SCHEDULE_CANCEL_EFFECT => {
				let schedule = ScheduleCancelEffect::decode(&effect.payload)?;
				operation
					.stage_schedule_cancel(&schedule_event_id(&schedule.schedule_id))
					.await?;
			}
			_ => {}
		}
	}
	Ok(())
}

fn prepare_effects(
	ctx: &rivetkit_core::ActorContext,
	bundle: &CellBundle,
	operation_id: &str,
	max_outgoing_message_size: u32,
	effects: Vec<GuestEffect>,
) -> Result<(Vec<StateDelta>, Vec<TransactionalEffect>)> {
	let mut deltas = Vec::new();
	let mut durable = Vec::new();
	for effect in effects {
		match effect.kind.as_str() {
			SOCKET_STATE_EFFECT => {
				let state = SocketStateEffect::decode(&effect.payload)?;
				ctx.validate_hibernation_state_delta(&state.connection_id, &state.state)
					.context("validate transactional socket state")?;
				deltas.push(StateDelta::ConnHibernation {
					conn: state.connection_id,
					bytes: state.state,
				});
			}
			SOCKET_SEND_EFFECT => {
				let send = SocketSendEffect::decode(&effect.payload)?;
				validate_connection_id(&send.connection_id)?;
				validate_socket_delivery_size(
					operation_id,
					durable.len(),
					&send.payload,
					max_outgoing_message_size,
				)?;
				durable.push(TransactionalEffect {
					kind: effect.kind,
					payload: effect.payload,
				});
			}
			SOCKET_BROADCAST_EFFECT => {
				let broadcast = SocketBroadcastEffect::decode(&effect.payload)
					.context("decode Cell socket broadcast effect")?;
				if broadcast.exclude_connection_ids.len() > 1024 {
					bail!("Cell socket broadcast exceeds 1024 exclusions");
				}
				for connection_id in &broadcast.exclude_connection_ids {
					validate_connection_id(connection_id)?;
				}
				validate_socket_delivery_size(
					operation_id,
					durable.len(),
					&broadcast.payload,
					max_outgoing_message_size,
				)?;
				durable.push(TransactionalEffect {
					kind: effect.kind,
					payload: effect.payload,
				});
			}
			SOCKET_CLOSE_EFFECT => {
				let close = SocketCloseEffect::decode(&effect.payload)?;
				validate_connection_id(&close.connection_id)?;
				if close
					.reason
					.as_ref()
					.is_some_and(|reason| reason.len() > 1024)
				{
					bail!("Cell socket close reason exceeds 1024 bytes");
				}
				durable.push(TransactionalEffect {
					kind: effect.kind,
					payload: effect.payload,
				});
			}
			SCHEDULE_AT_EFFECT => {
				let schedule = ScheduleAtEffect::decode(&effect.payload)?;
				validate_schedule_id(&schedule.schedule_id)?;
				if schedule.method.is_empty() || schedule.method.len() > 256 {
					bail!("Cell scheduled method must contain 1 to 256 bytes");
				}
				let manifest = CellRuntimeManifest::decode(&bundle.manifest)?;
				if manifest.methods.get(&schedule.method).copied() != Some(CellMethodMode::Command)
				{
					bail!(
						"CELL_SCHEDULE_METHOD_INVALID: scheduled method is not a declared command"
					);
				}
				durable.push(TransactionalEffect {
					kind: effect.kind,
					payload: effect.payload,
				});
			}
			SCHEDULE_CANCEL_EFFECT => {
				let schedule = ScheduleCancelEffect::decode(&effect.payload)?;
				validate_schedule_id(&schedule.schedule_id)?;
				durable.push(TransactionalEffect {
					kind: effect.kind,
					payload: effect.payload,
				});
			}
			kind => bail!("unsupported Wonderful Cell effect kind {kind}"),
		}
	}
	Ok((deltas, durable))
}

fn validate_socket_delivery_size(
	operation_id: &str,
	effect_index: usize,
	payload: &[u8],
	max_outgoing_message_size: u32,
) -> Result<()> {
	let effect_index = u32::try_from(effect_index).context("Cell effect index exceeds u32")?;
	let envelope = SocketDeliveryEnvelope {
		operation_id: operation_id.to_owned(),
		effect_index,
		payload: payload.to_vec(),
	}
	.encode()?;
	// Reserve space for the actor-connect event wrapper. Cells use BARE in
	// production, but the margin also fails closed if another encoding adds
	// modest framing overhead.
	let framed_size = envelope
		.len()
		.saturating_add(SOCKET_DELIVERY_EVENT.len())
		.saturating_add(1024);
	if framed_size > max_outgoing_message_size as usize {
		bail!("Cell socket effect exceeds the configured outgoing message limit");
	}
	Ok(())
}

fn validate_effect_backlog(pending_operations: u64, maximum: u32) -> Result<()> {
	if pending_operations >= u64::from(maximum) {
		return Err(CellGuestInvocationError {
			code: "CELL_EFFECT_BACKLOG_LIMIT".to_owned(),
			message: "Cell effect backlog reached its configured limit".to_owned(),
		}
		.into());
	}
	Ok(())
}

fn validate_output(output: &GuestOutput, limits: &WonderfulWasmtimeLimits) -> Result<()> {
	if output.state.len() > limits.max_state_bytes {
		bail!("Cell state exceeds configured output limit");
	}
	if output.result.len() > limits.max_result_bytes {
		bail!("Cell result exceeds configured output limit");
	}
	if output.effects.len() > limits.max_effects {
		bail!("Cell effect count exceeds configured output limit");
	}
	if output
		.effects
		.iter()
		.any(|effect| effect.kind.len() > limits.max_effect_kind_bytes)
	{
		bail!("Cell effect kind exceeds configured output limit");
	}
	if output
		.effects
		.iter()
		.any(|effect| effect.payload.len() > limits.max_effect_bytes)
	{
		bail!("Cell effect exceeds configured payload limit");
	}
	let total_effect_bytes = output.effects.iter().fold(0usize, |total, effect| {
		total
			.saturating_add(effect.kind.len())
			.saturating_add(effect.payload.len())
	});
	if total_effect_bytes > limits.max_total_effect_bytes {
		bail!("Cell aggregate effects exceed configured output limit");
	}
	Ok(())
}

#[cfg(test)]
#[path = "../tests/modules/runtime_architecture.rs"]
mod architecture_tests;

#[cfg(test)]
mod tests {
	use super::*;

	const TEST_COMPONENT: &[u8] =
		include_bytes!("../tests/fixtures/wonderful_cell_test_guest.component.wasm");

	fn test_bundle() -> CellBundle {
		CellBundle::verified(
			b"test bundle".to_vec(),
			br#"{"formatVersion":1,"methods":{"echo":"command","fail":"command","sql":"command","spin":"command","schedule":"command","send":"command","destroy":"command","read":"query","read_sql":"query","mutating_query":"query"}}"#.to_vec(),
		)
	}

	fn test_runtime() -> WonderfulWasmtimeActorFactory {
		test_runtime_with_limits(WonderfulWasmtimeLimits::default())
	}

	fn test_runtime_with_limits(limits: WonderfulWasmtimeLimits) -> WonderfulWasmtimeActorFactory {
		let bundle = test_bundle();
		WonderfulWasmtimeActorFactory::new(
			TEST_COMPONENT,
			Arc::new(StaticCellBundleLoader::new(bundle)),
			ActorConfig::default(),
			limits,
		)
		.expect("create test Wasmtime runtime")
	}

	async fn begin_test_operation(
		ctx: &rivetkit_core::ActorContext,
		operation_id: &str,
	) -> TransactionalOperation {
		let admission = ctx
			.begin_transactional_operation(
				TransactionalOperationRequest {
					operation_id: operation_id.to_owned(),
					kind: TransactionalOperationKind::Action,
					name: "test-effect".to_owned(),
					input: Vec::new(),
					actor_generation: 1,
					started_at: 1,
				},
				None,
			)
			.await
			.expect("admit effect operation");
		let TransactionalOperationAdmission::Execute(operation) = admission else {
			panic!("test effect operation must be new");
		};
		operation
	}

	async fn commit_test_effect(
		ctx: &rivetkit_core::ActorContext,
		operation_id: &str,
		kind: &str,
		payload: Vec<u8>,
	) {
		begin_test_operation(ctx, operation_id)
			.await
			.commit(
				vec![StateDelta::ActorState(ctx.state())],
				Vec::new(),
				vec![TransactionalEffect {
					kind: kind.to_owned(),
					payload,
				}],
				2,
			)
			.await
			.expect("commit effect operation");
	}

	#[test]
	fn action_envelope_round_trips_and_rejects_trailing_bytes() {
		let envelope = CellActionEnvelope {
			operation_id: "operation-1".to_owned(),
			input: vec![1, 2, 3],
			started_at: 42,
			mode: CellMethodMode::Command,
		};
		let encoded = envelope.encode().expect("encode envelope");
		assert_eq!(
			CellActionEnvelope::decode_mode(&encoded).expect("decode envelope mode"),
			CellMethodMode::Command,
		);
		assert_eq!(
			CellActionEnvelope::decode(&encoded).expect("decode envelope"),
			envelope,
		);
		let mut invalid = encoded;
		invalid.push(0);
		assert!(CellActionEnvelope::decode(&invalid).is_err());

		let legacy = b"\x01\x00\x10legacy-operation\x01\x04\x07\x00\x00\x00\x00\x00\x00\x00";
		assert_eq!(
			CellActionEnvelope::decode(legacy).expect("decode legacy envelope"),
			CellActionEnvelope {
				operation_id: "legacy-operation".to_owned(),
				input: vec![4],
				started_at: 7,
				mode: CellMethodMode::Command,
			}
		);
		let reserved = CellActionEnvelope {
			operation_id: "schedule:server-owned".to_owned(),
			input: Vec::new(),
			started_at: 1,
			mode: CellMethodMode::Command,
		}
		.encode()
		.expect("encode reserved operation");
		assert!(validate_action_identity(&reserved, false).is_err());
		assert!(validate_action_identity(&reserved, true).is_ok());
	}

	#[tokio::test]
	async fn active_cells_own_independent_component_stores_and_heaps() {
		let runtime = test_runtime();
		let bundle = test_bundle();
		let mut first = Resident::activate(
			Arc::clone(&runtime.engine),
			Arc::clone(&runtime.component),
			bundle.clone(),
			vec![1],
			None,
			runtime.limits.clone(),
		)
		.await
		.expect("activate first Cell");
		let mut second = Resident::activate(
			Arc::clone(&runtime.engine),
			Arc::clone(&runtime.component),
			bundle,
			vec![9],
			None,
			runtime.limits.clone(),
		)
		.await
		.expect("activate second Cell");

		let first_output = first
			.invoke("echo", &[2], SqlCapability::Unavailable)
			.await
			.expect("invoke first Cell");
		let second_output = second
			.invoke("echo", &[8], SqlCapability::Unavailable)
			.await
			.expect("invoke second Cell");
		assert_eq!(first_output.state, vec![1, 2]);
		assert_eq!(second_output.state, vec![9, 8]);
		let first_again = first
			.invoke("echo", &[3], SqlCapability::Unavailable)
			.await
			.expect("invoke first Cell again");
		assert_eq!(first_again.state, vec![1, 2, 3]);
	}

	#[tokio::test]
	async fn actor_adapter_commits_sql_state_result_and_effect_before_reply() {
		use rivetkit_core::{ActorEvents, Reply, testing};
		use tokio::sync::{mpsc, oneshot};

		let runtime = test_runtime();
		let ctx = testing::actor_context("cell-1", "cell", Vec::new(), "local");
		ctx.sql()
			.execute("CREATE TABLE test_values (value BLOB NOT NULL)", None)
			.await
			.expect("create guest SQL fixture table");
		let (event_tx, event_rx) = mpsc::unbounded_channel();
		let (ready_tx, ready_rx) = oneshot::channel();
		let run = tokio::spawn({
			let runtime = runtime.clone();
			let ctx = ctx.clone();
			async move {
				runtime
					.run_actor(ActorStart {
						ctx,
						input: None,
						is_new: false,
						snapshot: Some(vec![5]),
						hibernated: Vec::new(),
						events: ActorEvents::from(event_rx),
						startup_ready: Some(ready_tx),
					})
					.await
			}
		});
		ready_rx
			.await
			.expect("startup ready reply")
			.expect("Cell startup succeeds");

		let envelope = CellActionEnvelope {
			operation_id: "operation-commit".to_owned(),
			input: vec![7],
			started_at: 100,
			mode: CellMethodMode::Command,
		};
		let (reply_tx, reply_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::Action {
				name: "sql".to_owned(),
				args: envelope.encode().expect("encode action envelope"),
				conn: None,
				scheduled_fire: None,
				reply: Reply::from(reply_tx),
			})
			.expect("send Cell action");
		assert_eq!(
			reply_rx
				.await
				.expect("action reply")
				.expect("action succeeds"),
			vec![7],
		);

		let operation = ctx
			.transactional_operation("operation-commit")
			.await
			.expect("load committed operation")
			.expect("committed operation exists");
		assert_eq!(operation.status, TransactionalOperationStatus::Succeeded);
		assert_eq!(operation.result, Some(vec![7]));
		assert_eq!(ctx.state(), vec![5, 7]);
		let sql_rows = ctx
			.sql()
			.query("SELECT value FROM test_values", None)
			.await
			.expect("read guest SQL write");
		assert_eq!(sql_rows.rows, vec![vec![ColumnValue::Blob(vec![7])]]);
		let effects = ctx
			.pending_transactional_effects(10)
			.await
			.expect("load committed effects");
		assert!(effects.is_empty());

		drop(event_tx);
		run.await
			.expect("join actor adapter")
			.expect("actor adapter exits cleanly");
	}

	#[tokio::test]
	async fn committed_destruction_fences_before_reply_and_remains_durable() {
		use rivetkit_core::{ActorEvents, Reply, testing};
		use tokio::sync::{mpsc, oneshot};

		let runtime = test_runtime();
		let ctx = testing::actor_context("cell-destroy", "cell", Vec::new(), "local");
		let (event_tx, event_rx) = mpsc::unbounded_channel();
		let (ready_tx, ready_rx) = oneshot::channel();
		let run = tokio::spawn({
			let runtime = runtime.clone();
			let ctx = ctx.clone();
			async move {
				runtime
					.run_actor(ActorStart {
						ctx,
						input: None,
						is_new: false,
						snapshot: Some(Vec::new()),
						hibernated: Vec::new(),
						events: ActorEvents::from(event_rx),
						startup_ready: Some(ready_tx),
					})
					.await
			}
		});
		ready_rx
			.await
			.expect("startup ready reply")
			.expect("Cell startup succeeds");

		let envelope = CellActionEnvelope {
			operation_id: "operation-destroy".to_owned(),
			input: vec![7],
			started_at: 100,
			mode: CellMethodMode::Command,
		};
		let (reply_tx, reply_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::Action {
				name: "destroy".to_owned(),
				args: envelope.encode().expect("encode action envelope"),
				conn: None,
				scheduled_fire: None,
				reply: Reply::from(reply_tx),
			})
			.expect("send destroy action");
		assert_eq!(
			reply_rx
				.await
				.expect("destroy reply")
				.expect("destroy succeeds"),
			vec![7],
		);
		assert!(ctx.is_destroy_requested());
		for (name, mode) in [
			("echo", CellMethodMode::Command),
			("read", CellMethodMode::Query),
			("sql", CellMethodMode::Command),
		] {
			let (late_tx, late_rx) = oneshot::channel();
			event_tx
				.send(ActorEvent::Action {
					name: name.to_owned(),
					args: CellActionEnvelope {
						operation_id: if name == "sql" {
							"schedule:late-sql".to_owned()
						} else {
							format!("late-{name}")
						},
						input: vec![9],
						started_at: 101,
						mode,
					}
					.encode()
					.unwrap(),
					conn: None,
					scheduled_fire: (name == "sql").then(|| {
						rivetkit_core::actor::schedule::ScheduledFireInfo {
							kind: rivetkit_core::actor::schedule::ScheduleKind::At,
							id: "late-sql".to_owned(),
							name: None,
							scheduled_at: 100,
							fired_at: 101,
						}
					}),
					reply: Reply::from(late_tx),
				})
				.unwrap();
			assert!(
				late_rx.await.unwrap().is_err(),
				"direct {name} must be fenced"
			);
			assert!(
				ctx.transactional_operation(&if name == "sql" {
					"schedule:late-sql".to_owned()
				} else {
					format!("late-{name}")
				})
				.await
				.unwrap()
				.is_none()
			);
		}
		let (connection, _messages) = testing::add_connection(&ctx, "late-connection", true);
		let (open_tx, open_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::ConnectionOpen {
				conn: connection.clone(),
				request: None,
				reply: Reply::from(open_tx),
			})
			.unwrap();
		assert!(open_rx.await.unwrap().is_err());
		event_tx
			.send(ActorEvent::ConnectionClosed { conn: connection })
			.unwrap();
		// A FIFO state request provides a barrier after the no-reply close event.
		let (barrier_tx, barrier_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::SerializeState {
				reason: rivetkit_core::SerializeStateReason::Save,
				reply: Reply::from(barrier_tx),
			})
			.unwrap();
		barrier_rx.await.unwrap().unwrap();
		for method in [SOCKET_CONNECT_METHOD, SOCKET_CLOSE_METHOD] {
			assert!(
				ctx.transactional_operation(&format!("socket:1:{method}:late-connection"))
					.await
					.unwrap()
					.is_none()
			);
		}
		assert!(
			ctx.has_pending_transactional_effect_kind(DESTROY_EFFECT)
				.await
				.expect("load destruction intent")
		);

		drop(event_tx);
		run.await
			.expect("join actor adapter")
			.expect("actor adapter exits cleanly");
	}

	#[test]
	fn scheduled_effects_require_versioned_declared_commands() {
		let ctx = rivetkit_core::testing::actor_context(
			"cell-schedule-validation",
			"cell",
			Vec::new(),
			"local",
		);
		let effect = |format_version: u16, method: &str| {
			let mut payload = format_version.to_le_bytes().to_vec();
			payload.extend(
				serde_bare::to_vec(&ScheduleAtEffect {
					schedule_id: "validation".to_owned(),
					timestamp_ms: 1,
					method: method.to_owned(),
					input: Vec::new(),
				})
				.expect("encode schedule effect body"),
			);
			GuestEffect {
				kind: SCHEDULE_AT_EFFECT.to_owned(),
				payload,
			}
		};
		assert!(
			prepare_effects(
				&ctx,
				&test_bundle(),
				"test",
				u32::MAX,
				vec![effect(2, "echo")]
			)
			.is_err()
		);
		assert!(
			prepare_effects(
				&ctx,
				&test_bundle(),
				"test",
				u32::MAX,
				vec![effect(1, "read")]
			)
			.is_err()
		);
		assert!(
			prepare_effects(
				&ctx,
				&test_bundle(),
				"test",
				u32::MAX,
				vec![effect(1, "missing")],
			)
			.is_err()
		);
		assert!(
			prepare_effects(
				&ctx,
				&test_bundle(),
				"test",
				u32::MAX,
				vec![effect(1, "echo")]
			)
			.is_ok()
		);
	}

	#[tokio::test]
	async fn actor_effect_pump_resumes_committed_schedule_delivery() {
		use rivetkit_core::{ActorEvents, Reply, testing};
		use tokio::sync::{mpsc, oneshot};

		let runtime = test_runtime();
		let ctx = testing::actor_context("cell-effect-pump", "cell", Vec::new(), "local");
		let (event_tx, event_rx) = mpsc::unbounded_channel();
		let (ready_tx, ready_rx) = oneshot::channel();
		let mut run = tokio::spawn({
			let runtime = runtime.clone();
			let ctx = ctx.clone();
			async move {
				runtime
					.run_actor(ActorStart {
						ctx,
						input: None,
						is_new: false,
						snapshot: Some(Vec::new()),
						hibernated: Vec::new(),
						events: ActorEvents::from(event_rx),
						startup_ready: Some(ready_tx),
					})
					.await
			}
		});
		ready_rx
			.await
			.expect("startup ready reply")
			.expect("Cell startup succeeds");
		let (reply_tx, reply_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::Action {
				name: "schedule".to_owned(),
				args: CellActionEnvelope {
					mode: CellMethodMode::Command,
					operation_id: "operation-pump".to_owned(),
					input: vec![6],
					started_at: 1,
				}
				.encode()
				.expect("encode action"),
				conn: None,
				scheduled_fire: None,
				reply: Reply::from(reply_tx),
			})
			.expect("send schedule command");
		let command_reply = tokio::select! {
			reply = reply_rx => reply.expect("command reply"),
			result = &mut run => panic!("actor adapter exited during schedule command: {result:?}"),
		};
		command_reply.expect("schedule command succeeds");
		tokio::time::timeout(Duration::from_secs(1), async {
			loop {
				if ctx
					.pending_transactional_effects(1)
					.await
					.expect("load pending effects")
					.is_empty()
				{
					break;
				}
				tokio::task::yield_now().await;
			}
		})
		.await
		.expect("effect pump delivery deadline");
		assert!(
			ctx.get_scheduled_event("wonderful:test-alarm")
				.await
				.expect("load pumped schedule")
				.is_some()
		);
		drop(event_tx);
		run.await
			.expect("join actor adapter")
			.expect("actor adapter exits cleanly");
	}

	#[tokio::test]
	async fn managed_connection_open_runs_as_transactional_command() {
		use rivetkit_core::{ActorEvents, Reply, testing};
		use tokio::sync::{mpsc, oneshot};

		let runtime = test_runtime();
		let ctx = testing::actor_context("cell-connect", "cell", Vec::new(), "local");
		let (connection, _events) = testing::add_connection(&ctx, "connection-open", true);
		let (event_tx, event_rx) = mpsc::unbounded_channel();
		let (ready_tx, ready_rx) = oneshot::channel();
		let run = tokio::spawn({
			let runtime = runtime.clone();
			let ctx = ctx.clone();
			async move {
				runtime
					.run_actor(ActorStart {
						ctx,
						input: None,
						is_new: false,
						snapshot: Some(Vec::new()),
						hibernated: Vec::new(),
						events: ActorEvents::from(event_rx),
						startup_ready: Some(ready_tx),
					})
					.await
			}
		});
		ready_rx
			.await
			.expect("startup ready reply")
			.expect("Cell startup succeeds");

		let (raw_reply_tx, raw_reply_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::ConnectionPreflight {
				conn: connection.clone(),
				protocol: ConnectionProtocol::RawWebSocket,
				params: Vec::new(),
				request: None,
				reply: Reply::from(raw_reply_tx),
			})
			.expect("send raw websocket preflight");
		assert!(raw_reply_rx.await.expect("raw preflight reply").is_err());

		let (request_connection, _events) =
			testing::add_connection(&ctx, "request-connection", false);
		let (request_preflight_tx, request_preflight_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::ConnectionPreflight {
				conn: request_connection.clone(),
				protocol: ConnectionProtocol::Request,
				params: Vec::new(),
				request: None,
				reply: Reply::from(request_preflight_tx),
			})
			.expect("send request preflight");
		request_preflight_rx
			.await
			.expect("request preflight reply")
			.expect("request preflight succeeds");
		let (request_open_tx, request_open_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::ConnectionOpen {
				conn: request_connection.clone(),
				request: None,
				reply: Reply::from(request_open_tx),
			})
			.expect("send request connection open");
		request_open_rx
			.await
			.expect("request connection open reply")
			.expect("request connection open succeeds");
		event_tx
			.send(ActorEvent::ConnectionClosed {
				conn: request_connection,
			})
			.expect("send request connection close");

		let (managed_reply_tx, managed_reply_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::ConnectionPreflight {
				conn: connection.clone(),
				protocol: ConnectionProtocol::ActorConnect,
				params: Vec::new(),
				request: None,
				reply: Reply::from(managed_reply_tx),
			})
			.expect("send managed websocket preflight");
		managed_reply_rx
			.await
			.expect("managed preflight reply")
			.expect("managed preflight succeeds");

		let (reply_tx, reply_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::ConnectionOpen {
				conn: connection,
				request: None,
				reply: Reply::from(reply_tx),
			})
			.expect("send connection open");
		reply_rx
			.await
			.expect("connection open reply")
			.expect("connection open succeeds");
		let operation_id = format!("socket:1:{SOCKET_CONNECT_METHOD}:connection-open");
		assert_eq!(
			ctx.transactional_operation(&operation_id)
				.await
				.expect("load connection operation")
				.expect("connection operation exists")
				.status,
			TransactionalOperationStatus::Succeeded
		);
		assert!(
			ctx.transactional_operation(&format!(
				"socket:1:{SOCKET_CONNECT_METHOD}:request-connection"
			))
			.await
			.expect("load request open operation")
			.is_none(),
			"framework request connections must not invoke Cell socket lifecycle methods"
		);
		assert!(
			ctx.transactional_operation(&format!(
				"socket:1:{SOCKET_CLOSE_METHOD}:request-connection"
			))
			.await
			.expect("load request close operation")
			.is_none(),
			"framework request closes must not invoke Cell socket lifecycle methods"
		);

		let (expired_connection, _events) =
			testing::add_connection(&ctx, "connection-expired", true);
		let (expired_reply_tx, expired_reply_rx) = oneshot::channel();
		drop(expired_reply_rx);
		event_tx
			.send(ActorEvent::ConnectionOpen {
				conn: expired_connection,
				request: None,
				reply: Reply::from(expired_reply_tx),
			})
			.expect("send expired connection open");
		let (barrier_tx, barrier_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::Action {
				name: "echo".to_owned(),
				args: CellActionEnvelope {
					mode: CellMethodMode::Command,
					operation_id: "connection-expiry-barrier".to_owned(),
					input: Vec::new(),
					started_at: 1,
				}
				.encode()
				.expect("encode barrier"),
				conn: None,
				scheduled_fire: None,
				reply: Reply::from(barrier_tx),
			})
			.expect("send barrier action");
		barrier_rx
			.await
			.expect("barrier reply")
			.expect("barrier succeeds");
		assert_eq!(
			ctx.transactional_operation(&format!(
				"socket:1:{SOCKET_CONNECT_METHOD}:connection-expired"
			))
			.await
			.expect("load expired connection operation")
			.expect("expired connection operation is terminal")
			.status,
			TransactionalOperationStatus::Failed,
			"expired connection setup must roll back and terminalize"
		);
		drop(event_tx);
		run.await
			.expect("join actor adapter")
			.expect("actor adapter exits cleanly");
	}

	#[tokio::test]
	async fn socket_state_commits_atomically_with_the_operation() {
		let ctx =
			rivetkit_core::testing::actor_context("cell-socket-state", "cell", Vec::new(), "local");
		let (connection, _events) =
			rivetkit_core::testing::add_connection(&ctx, "connection-state", true);
		let admission = ctx
			.begin_transactional_operation(
				TransactionalOperationRequest {
					operation_id: "operation-socket-state".to_owned(),
					kind: TransactionalOperationKind::Action,
					name: "socket-state".to_owned(),
					input: Vec::new(),
					actor_generation: 1,
					started_at: 1,
				},
				None,
			)
			.await
			.expect("admit socket state operation");
		let TransactionalOperationAdmission::Execute(operation) = admission else {
			panic!("socket state operation must be new");
		};
		let (deltas, effects) = prepare_effects(
			&ctx,
			&test_bundle(),
			"socket-state-operation",
			u32::MAX,
			vec![GuestEffect {
				kind: SOCKET_STATE_EFFECT.to_owned(),
				payload: SocketStateEffect {
					connection_id: "connection-state".to_owned(),
					state: vec![3, 4],
				}
				.encode()
				.expect("encode socket state effect"),
			}],
		)
		.expect("prepare socket state delta");
		assert_eq!(connection.state(), Vec::<u8>::new());
		assert!(effects.is_empty());
		operation
			.commit(deltas, Vec::new(), effects, 2)
			.await
			.expect("commit socket state operation");
		assert_eq!(connection.state(), vec![3, 4]);
	}

	#[tokio::test]
	async fn socket_effect_delivers_only_after_commit_with_stable_identity() {
		let ctx = rivetkit_core::testing::actor_context(
			"cell-socket-effect",
			"cell",
			Vec::new(),
			"local",
		);
		let (_connection, mut events) =
			rivetkit_core::testing::add_connection(&ctx, "connection-1", true);
		assert!(events.try_recv().is_err());
		commit_test_effect(
			&ctx,
			"operation-socket",
			SOCKET_SEND_EFFECT,
			SocketSendEffect {
				connection_id: "connection-1".to_owned(),
				payload: vec![4, 5],
			}
			.encode()
			.expect("encode socket effect"),
		)
		.await;
		assert!(events.try_recv().is_err(), "commit must not send inline");

		assert_eq!(
			WonderfulWasmtimeActorFactory::deliver_pending_effect_batch(&ctx)
				.await
				.expect("deliver socket effect"),
			(1, false),
		);
		let (name, payload) = events.recv().await.expect("receive socket delivery");
		assert_eq!(name, SOCKET_DELIVERY_EVENT);
		assert_eq!(
			SocketDeliveryEnvelope::decode(&payload).expect("decode socket delivery"),
			SocketDeliveryEnvelope {
				operation_id: "operation-socket".to_owned(),
				effect_index: 0,
				payload: vec![4, 5],
			}
		);
		assert!(
			ctx.pending_transactional_effects(10)
				.await
				.expect("load pending effects")
				.is_empty()
		);
	}

	#[tokio::test]
	async fn failed_socket_delivery_is_recorded_and_retried() {
		let ctx =
			rivetkit_core::testing::actor_context("cell-socket-retry", "cell", Vec::new(), "local");
		let (_connection, events) =
			rivetkit_core::testing::add_connection(&ctx, "connection-retry", true);
		drop(events);
		commit_test_effect(
			&ctx,
			"operation-retry",
			SOCKET_SEND_EFFECT,
			SocketSendEffect {
				connection_id: "connection-retry".to_owned(),
				payload: vec![8],
			}
			.encode()
			.expect("encode socket effect"),
		)
		.await;
		assert!(
			WonderfulWasmtimeActorFactory::deliver_pending_effect_batch(&ctx)
				.await
				.is_err()
		);
		let pending = ctx
			.pending_transactional_effects(10)
			.await
			.expect("load failed effect");
		assert_eq!(pending[0].attempts, 1);

		let (_replacement, mut replacement_events) =
			rivetkit_core::testing::add_connection(&ctx, "connection-retry", true);
		WonderfulWasmtimeActorFactory::deliver_pending_effect_batch(&ctx)
			.await
			.expect("retry socket delivery");
		assert!(replacement_events.recv().await.is_some());
		assert!(
			ctx.pending_transactional_effects(10)
				.await
				.expect("load delivered effects")
				.is_empty()
		);
	}

	#[tokio::test]
	async fn failing_head_effect_backpressures_new_effects_at_operation_bound() {
		use rivetkit_core::{ActorEvents, Reply, testing};
		use tokio::sync::{mpsc, oneshot};

		let runtime = test_runtime_with_limits(WonderfulWasmtimeLimits {
			max_transactional_operations: 0,
			..WonderfulWasmtimeLimits::default()
		});
		let ctx = testing::actor_context("cell-effect-bound", "cell", Vec::new(), "local");
		let (_connection, events) = testing::add_connection(&ctx, "blocked", true);
		drop(events);
		commit_test_effect(
			&ctx,
			"operation-blocked",
			SOCKET_SEND_EFFECT,
			SocketSendEffect {
				connection_id: "blocked".to_owned(),
				payload: vec![1],
			}
			.encode()
			.expect("encode blocked effect"),
		)
		.await;

		let (event_tx, event_rx) = mpsc::unbounded_channel();
		let (ready_tx, ready_rx) = oneshot::channel();
		let run = tokio::spawn({
			let runtime = runtime.clone();
			let ctx = ctx.clone();
			async move {
				runtime
					.run_actor(ActorStart {
						ctx,
						input: None,
						is_new: false,
						snapshot: Some(Vec::new()),
						hibernated: Vec::new(),
						events: ActorEvents::from(event_rx),
						startup_ready: Some(ready_tx),
					})
					.await
			}
		});
		ready_rx
			.await
			.expect("startup ready reply")
			.expect("Cell startup succeeds");
		let (reply_tx, reply_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::Action {
				name: "send".to_owned(),
				args: CellActionEnvelope {
					mode: CellMethodMode::Command,
					operation_id: "operation-over-bound".to_owned(),
					input: b"missing".to_vec(),
					started_at: 1,
				}
				.encode()
				.expect("encode action"),
				conn: None,
				scheduled_fire: None,
				reply: Reply::from(reply_tx),
			})
			.expect("send effect-producing command");
		let error = reply_rx
			.await
			.expect("command reply")
			.expect_err("effect backlog must reject another effect-producing command");
		assert!(format!("{error:#}").contains("CELL_EFFECT_BACKLOG_LIMIT"));

		assert_eq!(
			ctx.pending_transactional_effect_operation_count()
				.await
				.unwrap(),
			1
		);
		tokio::time::timeout(Duration::from_secs(1), async {
			while ctx
				.transactional_operation("operation-over-bound")
				.await
				.expect("load compactable rejected operation")
				.is_some()
			{
				tokio::task::yield_now().await;
			}
		})
		.await
		.expect("failed head must not prevent terminal-operation compaction");

		drop(event_tx);
		run.await
			.expect("join bounded actor adapter")
			.expect("bounded actor adapter exits cleanly");
	}

	#[tokio::test]
	async fn schedule_reconciliation_never_replays_stale_mutations() {
		let ctx = rivetkit_core::testing::actor_context(
			"cell-schedule-effect",
			"cell",
			Vec::new(),
			"local",
		);
		let event_id = "wonderful:daily-check";
		let old_payload = ScheduleAtEffect {
			schedule_id: "daily-check".to_owned(),
			timestamp_ms: 50_000,
			method: "echo".to_owned(),
			input: vec![7],
		}
		.encode()
		.expect("encode schedule effect");
		let old_args = CellActionEnvelope {
			mode: CellMethodMode::Command,
			operation_id: schedule_operation_id("operation-schedule", 0),
			input: vec![7],
			started_at: 50_000,
		}
		.encode()
		.expect("encode old schedule action");
		let old_operation = begin_test_operation(&ctx, "operation-schedule").await;
		old_operation
			.stage_schedule_upsert(TransactionalScheduleUpsert {
				event_id,
				trigger_at: 50_000,
				action: "echo",
				args: &old_args,
			})
			.await
			.expect("stage old schedule");
		old_operation
			.commit(
				Vec::new(),
				Vec::new(),
				vec![TransactionalEffect {
					kind: SCHEDULE_AT_EFFECT.to_owned(),
					payload: old_payload,
				}],
				2,
			)
			.await
			.expect("commit old schedule aggregate");

		let replacement_args = b"replacement".to_vec();
		let replacement = begin_test_operation(&ctx, "operation-replacement").await;
		replacement
			.stage_schedule_upsert(TransactionalScheduleUpsert {
				event_id,
				trigger_at: 60_000,
				action: "replacement",
				args: &replacement_args,
			})
			.await
			.expect("stage replacement schedule");
		replacement
			.commit(Vec::new(), Vec::new(), Vec::new(), 3)
			.await
			.expect("commit replacement schedule");

		WonderfulWasmtimeActorFactory::deliver_pending_effect_batch(&ctx)
			.await
			.expect("reconcile old schedule effect");
		let scheduled = ctx
			.get_scheduled_event(event_id)
			.await
			.expect("load replacement schedule")
			.expect("replacement schedule exists");
		assert_eq!(scheduled.run_at, 60_000);
		assert_eq!(scheduled.action, "replacement");
		assert_eq!(scheduled.args, replacement_args);
		assert!(
			!ctx.acknowledge_transactional_schedule_change("operation-schedule", 0, 4, event_id,)
				.await
				.expect("redeliver acknowledged schedule reconciliation")
		);

		let cancellation = begin_test_operation(&ctx, "operation-cancel").await;
		cancellation
			.stage_schedule_cancel(event_id)
			.await
			.expect("stage schedule cancellation");
		cancellation
			.commit(
				Vec::new(),
				Vec::new(),
				vec![TransactionalEffect {
					kind: SCHEDULE_CANCEL_EFFECT.to_owned(),
					payload: ScheduleCancelEffect {
						schedule_id: "daily-check".to_owned(),
					}
					.encode()
					.expect("encode cancellation effect"),
				}],
				5,
			)
			.await
			.expect("commit cancellation aggregate");
		let post_cancel_replacement = begin_test_operation(&ctx, "operation-post-cancel").await;
		post_cancel_replacement
			.stage_schedule_upsert(TransactionalScheduleUpsert {
				event_id,
				trigger_at: 70_000,
				action: "post-cancel",
				args: &[],
			})
			.await
			.expect("stage post-cancel replacement");
		post_cancel_replacement
			.commit(Vec::new(), Vec::new(), Vec::new(), 6)
			.await
			.expect("commit post-cancel replacement");

		WonderfulWasmtimeActorFactory::deliver_pending_effect_batch(&ctx)
			.await
			.expect("reconcile old cancellation effect");
		let scheduled = ctx
			.get_scheduled_event(event_id)
			.await
			.expect("load post-cancel replacement")
			.expect("old cancellation must not delete replacement");
		assert_eq!(scheduled.run_at, 70_000);
		assert_eq!(scheduled.action, "post-cancel");
	}

	#[tokio::test]
	async fn queries_use_immutable_replicas_and_reject_mutation() {
		use rivetkit_core::{ActorEvents, Reply, testing};
		use tokio::sync::{mpsc, oneshot};

		let runtime = test_runtime();
		let ctx = testing::actor_context("cell-query", "cell", Vec::new(), "local");
		ctx.set_state_initial(vec![1]);
		ctx.sql()
			.execute("CREATE TABLE test_values (value BLOB NOT NULL)", None)
			.await
			.expect("create query fixture table");
		ctx.sql()
			.execute(
				"INSERT INTO test_values (value) VALUES (?)",
				Some(vec![BindParam::Blob(vec![9])]),
			)
			.await
			.expect("seed query fixture table");
		let (event_tx, event_rx) = mpsc::unbounded_channel();
		let (ready_tx, ready_rx) = oneshot::channel();
		let run = tokio::spawn({
			let runtime = runtime.clone();
			let ctx = ctx.clone();
			async move {
				runtime
					.run_actor(ActorStart {
						ctx,
						input: None,
						is_new: false,
						snapshot: Some(vec![1]),
						hibernated: Vec::new(),
						events: ActorEvents::from(event_rx),
						startup_ready: Some(ready_tx),
					})
					.await
			}
		});
		ready_rx
			.await
			.expect("startup ready reply")
			.expect("Cell startup succeeds");
		let encode = |operation_id: &str, input: Vec<u8>, mode| {
			CellActionEnvelope {
				operation_id: operation_id.to_owned(),
				input,
				started_at: 1,
				mode,
			}
			.encode()
			.expect("encode action")
		};

		let (query_tx, query_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::Action {
				name: "read_sql".to_owned(),
				args: encode("query-before-command", Vec::new(), CellMethodMode::Query),
				conn: None,
				scheduled_fire: None,
				reply: Reply::from(query_tx),
			})
			.expect("send query");
		let (command_tx, command_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::Action {
				name: "echo".to_owned(),
				args: encode("command-after-query", vec![2], CellMethodMode::Command),
				conn: None,
				scheduled_fire: None,
				reply: Reply::from(command_tx),
			})
			.expect("send command");
		assert_eq!(
			query_rx
				.await
				.expect("query reply")
				.expect("query succeeds"),
			vec![1],
		);
		assert_eq!(
			command_rx
				.await
				.expect("command reply")
				.expect("command succeeds"),
			vec![2],
		);
		assert_eq!(ctx.state(), vec![1, 2]);
		assert_eq!(
			ctx.transactional_snapshot_version()
				.await
				.expect("load committed snapshot version"),
			1,
		);
		assert!(
			ctx.transactional_operation("query-before-command")
				.await
				.expect("query operation lookup")
				.is_none(),
			"queries must not enter the canonical writer operation log"
		);

		let (mutation_tx, mutation_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::Action {
				name: "mutating_query".to_owned(),
				args: encode("query-mutation", vec![8], CellMethodMode::Query),
				conn: None,
				scheduled_fire: None,
				reply: Reply::from(mutation_tx),
			})
			.expect("send mutating query");
		let error = mutation_rx
			.await
			.expect("mutation reply")
			.expect_err("mutating query must fail");
		assert!(error.to_string().contains("CELL_QUERY_MUTATION_VIOLATION"));
		assert_eq!(ctx.state(), vec![1, 2]);
		let rows = ctx
			.sql()
			.query("SELECT value FROM test_values", None)
			.await
			.expect("read query fixture table");
		assert_eq!(rows.rows, vec![vec![ColumnValue::Blob(vec![9])]]);

		let (mismatch_tx, mismatch_rx) = oneshot::channel();
		event_tx
			.send(ActorEvent::Action {
				name: "read".to_owned(),
				args: encode("query-mode-mismatch", Vec::new(), CellMethodMode::Command),
				conn: None,
				scheduled_fire: None,
				reply: Reply::from(mismatch_tx),
			})
			.expect("send mismatched query");
		assert!(
			mismatch_rx
				.await
				.expect("mismatch reply")
				.expect_err("mode mismatch must fail")
				.to_string()
				.contains("CELL_METHOD_MODE_MISMATCH")
		);

		drop(event_tx);
		run.await
			.expect("join actor adapter")
			.expect("actor adapter exits cleanly");
	}

	#[tokio::test]
	async fn failed_guest_is_discarded_and_rehydrates_only_committed_state() {
		let runtime = test_runtime();
		let ctx = rivetkit_core::testing::actor_context("cell-reset", "cell", Vec::new(), "local");
		ctx.set_state_initial(vec![1]);
		let bundle = test_bundle();
		let mut resident = Some(
			Resident::activate(
				Arc::clone(&runtime.engine),
				Arc::clone(&runtime.component),
				bundle.clone(),
				vec![1],
				None,
				runtime.limits.clone(),
			)
			.await
			.expect("activate Cell"),
		);
		let encoded = |operation_id: &str, input: Vec<u8>, started_at| {
			CellActionEnvelope {
				operation_id: operation_id.to_owned(),
				input,
				started_at,
				mode: CellMethodMode::Command,
			}
			.encode()
			.expect("encode action")
		};

		let first = runtime
			.invoke_action(
				&ctx,
				1,
				&bundle,
				&mut resident,
				"echo".to_owned(),
				encoded("operation-ok-1", vec![2], 1),
			)
			.await;
		assert!(matches!(first, ActionOutcome::Reply(Ok(ref value)) if value == &[2]));
		assert_eq!(ctx.state(), vec![1, 2]);

		let failed = runtime
			.invoke_action(
				&ctx,
				1,
				&bundle,
				&mut resident,
				"fail".to_owned(),
				encoded("operation-failed", vec![9], 2),
			)
			.await;
		assert!(matches!(failed, ActionOutcome::Reply(Err(_))));
		assert!(resident.is_none(), "failed resident must be discarded");
		assert_eq!(ctx.state(), vec![1, 2]);
		assert_eq!(
			ctx.transactional_operation("operation-failed")
				.await
				.expect("load failure")
				.expect("failure exists")
				.status,
			TransactionalOperationStatus::Failed,
		);

		let second = runtime
			.invoke_action(
				&ctx,
				1,
				&bundle,
				&mut resident,
				"echo".to_owned(),
				encoded("operation-ok-2", vec![3], 3),
			)
			.await;
		assert!(matches!(second, ActionOutcome::Reply(Ok(ref value)) if value == &[3]));
		assert_eq!(ctx.state(), vec![1, 2, 3]);
	}

	#[tokio::test]
	async fn fuel_exhaustion_terminalizes_and_discards_the_resident() {
		let runtime = test_runtime_with_limits(WonderfulWasmtimeLimits {
			fuel_per_command: 50_000,
			..WonderfulWasmtimeLimits::default()
		});
		let ctx = rivetkit_core::testing::actor_context("cell-fuel", "cell", Vec::new(), "local");
		let bundle = test_bundle();
		let mut resident = Some(
			Resident::activate(
				Arc::clone(&runtime.engine),
				Arc::clone(&runtime.component),
				bundle.clone(),
				Vec::new(),
				None,
				runtime.limits.clone(),
			)
			.await
			.expect("activate Cell"),
		);
		let args = CellActionEnvelope {
			operation_id: "operation-fuel".to_owned(),
			input: Vec::new(),
			started_at: 1,
			mode: CellMethodMode::Command,
		}
		.encode()
		.expect("encode action");
		let outcome = runtime
			.invoke_action(&ctx, 1, &bundle, &mut resident, "spin".to_owned(), args)
			.await;
		assert!(matches!(outcome, ActionOutcome::Reply(Err(_))));
		assert!(resident.is_none());
		let operation = ctx
			.transactional_operation("operation-fuel")
			.await
			.expect("load operation")
			.expect("operation exists");
		assert_eq!(operation.status, TransactionalOperationStatus::Failed);
		assert_eq!(
			operation.error.expect("durable failure").code,
			"CELL_GUEST_TRAP"
		);
	}

	#[test]
	fn remote_sqlite_indeterminate_errors_require_resident_fencing() {
		let error = anyhow!(RivetError {
			kind: rivet_error::RivetErrorKind::Dynamic {
				group: "sqlite".to_owned(),
				code: "remote_indeterminate_result".to_owned(),
				default_message: "indeterminate".to_owned(),
			},
			meta: None,
			message: None,
			actor: None,
		});
		assert!(is_remote_sqlite_indeterminate(&error));
		assert!(!is_remote_sqlite_indeterminate(&anyhow!("unavailable")));
	}

	#[test]
	fn runtime_manifest_requires_declared_versioned_methods() {
		assert_eq!(
			CellRuntimeManifest::decode(&test_bundle().manifest)
				.expect("decode test manifest")
				.methods["read"],
			CellMethodMode::Query,
		);
		for invalid in [
			br#"{"formatVersion":2,"methods":{"read":"query"}}"#.as_slice(),
			br#"{"formatVersion":1,"methods":{}}"#.as_slice(),
			br#"{"formatVersion":1,"methods":{"read":"unknown"}}"#.as_slice(),
		] {
			assert!(CellRuntimeManifest::decode(invalid).is_err());
		}
	}

	#[test]
	fn cell_sql_capability_accepts_only_bounded_public_storage() {
		let limits = WonderfulWasmtimeLimits::default();
		let public_select =
			"/* comment */ -- comment\n SELECT value FROM test_values WHERE value = ?";
		let params = [BindParam::Blob(vec![1])];
		assert!(validate_guest_sql(public_select, &params, &limits).is_ok());
		assert!(validate_read_only_sql(public_select, &params, &limits).is_ok());

		for sql in [
			"SELECT * FROM _rivet_operations",
			"SELECT * FROM SQLITE_SCHEMA",
			"CREATE VIEW leaked AS SELECT * FROM `_RiVeT_OpErAtIoNs`",
			"SELECT load_extension('guest-extension')",
			"/* hide */ COMMIT",
			"-- hide\nROLLBACK",
			"BEGIN IMMEDIATE",
			"END TRANSACTION",
			"SAVEPOINT guest",
			"RELEASE guest",
			"ATTACH DATABASE ':memory:' AS escape",
			"DETACH DATABASE escape",
			"PRAGMA writable_schema = 1",
			"VACUUM INTO '/tmp/escape.db'",
		] {
			assert!(validate_guest_sql(sql, &[], &limits).is_err(), "{sql}");
		}
		for sql in [
			"INSERT INTO test_values VALUES (1)",
			"WITH value AS (SELECT 1) SELECT * FROM value",
			"SELECT 1; SELECT 2",
		] {
			assert!(validate_read_only_sql(sql, &[], &limits).is_err(), "{sql}");
		}

		let small = WonderfulWasmtimeLimits {
			max_sql_bytes: 8,
			max_sql_params: 1,
			max_sql_param_bytes: 1,
			..limits
		};
		assert!(validate_guest_sql("SELECT 10", &[], &small).is_err());
		assert!(
			validate_guest_sql(
				"SELECT ?",
				&[BindParam::Integer(1), BindParam::Integer(2)],
				&small
			)
			.is_err()
		);
		assert!(validate_guest_sql("SELECT ?", &[BindParam::Blob(vec![1, 2])], &small).is_err());
	}

	#[test]
	fn bundle_digest_and_output_limits_fail_closed() {
		assert!(validate_effect_backlog(0, 1).is_ok());
		assert!(
			format!("{:#}", validate_effect_backlog(1, 1).unwrap_err())
				.contains("CELL_EFFECT_BACKLOG_LIMIT")
		);
		let runtime = test_runtime();
		let mut bundle = CellBundle::verified(vec![1], Vec::new());
		bundle.bundle_sha256 = [0; 32];
		assert!(runtime.validate_bundle(&bundle).is_err());

		let limits = WonderfulWasmtimeLimits {
			max_result_bytes: 1,
			..WonderfulWasmtimeLimits::default()
		};
		let output = GuestOutput {
			state: Vec::new(),
			result: vec![1, 2],
			effects: Vec::new(),
			destroyed: false,
		};
		assert!(validate_output(&output, &limits).is_err());

		let limits = WonderfulWasmtimeLimits {
			max_effect_kind_bytes: 3,
			max_total_effect_bytes: 5,
			..WonderfulWasmtimeLimits::default()
		};
		let output = GuestOutput {
			state: Vec::new(),
			result: Vec::new(),
			effects: vec![GuestEffect {
				kind: "kind".to_owned(),
				payload: vec![1, 2],
			}],
			destroyed: false,
		};
		assert!(validate_output(&output, &limits).is_err());

		let error =
			CellGuestInvocationError::bounded_guest_error("X".repeat(100), "message".repeat(1_000));
		assert_eq!(error.code.len(), 64);
		assert_eq!(error.message.chars().count(), 4096);
	}
}
