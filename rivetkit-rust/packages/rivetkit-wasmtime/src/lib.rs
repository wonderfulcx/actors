use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use rivetkit_core::{
	ActorActionMode, ActorConfig, ActorConsistency, ActorEvent, ActorFactory, ActorStart,
	BindParam, ColumnValue, StateDelta, TransactionalEffect, TransactionalOperation,
	TransactionalOperationAdmission, TransactionalOperationError, TransactionalOperationFailure,
	TransactionalOperationKind, TransactionalOperationRequest, TransactionalOperationStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

mod bindings {
	wasmtime::component::bindgen!({
		path: "wit",
		world: "cell",
		imports: { default: async },
		exports: { default: async },
	});
}

const ACTION_ENVELOPE_VERSION_V1: u16 = 1;
const ACTION_ENVELOPE_VERSION: u16 = 2;
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

#[derive(Clone, Debug)]
pub struct WonderfulWasmtimeLimits {
	pub max_bundle_bytes: usize,
	pub max_manifest_bytes: usize,
	pub max_state_bytes: usize,
	pub max_result_bytes: usize,
	pub max_effects: usize,
	pub max_effect_bytes: usize,
	pub max_sql_bytes: usize,
	pub max_sql_params: usize,
	pub max_sql_param_bytes: usize,
	pub max_sql_rows: usize,
	pub max_sql_result_bytes: usize,
	pub max_linear_memory_bytes: usize,
	pub max_component_instances: usize,
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
			max_sql_bytes: DEFAULT_MAX_SQL_BYTES,
			max_sql_params: DEFAULT_MAX_SQL_PARAMS,
			max_sql_param_bytes: DEFAULT_MAX_SQL_PARAM_BYTES,
			max_sql_rows: DEFAULT_MAX_SQL_ROWS,
			max_sql_result_bytes: DEFAULT_MAX_SQL_RESULT_BYTES,
			max_linear_memory_bytes: 128 * 1024 * 1024,
			max_component_instances: 64,
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellMethodMode {
	Command,
	Query,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CellRuntimeManifest {
	pub format_version: u16,
	pub methods: BTreeMap<String, CellMethodMode>,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CellActionEnvelope {
	pub mode: CellMethodMode,
	pub operation_id: String,
	pub input: Vec<u8>,
	pub started_at: i64,
}

#[derive(Deserialize, Serialize)]
struct CellActionEnvelopeV1 {
	operation_id: String,
	input: Vec<u8>,
	started_at: i64,
}

impl CellActionEnvelope {
	fn decode_mode(bytes: &[u8]) -> Result<CellMethodMode> {
		let mut cursor = std::io::Cursor::new(bytes);
		let version: u16 =
			serde_bare::from_reader(&mut cursor).context("decode Cell action envelope version")?;
		match version {
			ACTION_ENVELOPE_VERSION_V1 => Ok(CellMethodMode::Command),
			ACTION_ENVELOPE_VERSION => {
				serde_bare::from_reader(&mut cursor).context("decode Cell action envelope mode")
			}
			_ => bail!("unsupported Cell action envelope version {version}"),
		}
	}

	pub fn encode(&self) -> Result<Vec<u8>> {
		let mut bytes = Vec::new();
		serde_bare::to_writer(&mut bytes, &ACTION_ENVELOPE_VERSION)
			.context("encode Cell action envelope version")?;
		serde_bare::to_writer(&mut bytes, self).context("encode Cell action envelope")?;
		Ok(bytes)
	}

	pub fn decode(bytes: &[u8]) -> Result<Self> {
		let mut cursor = std::io::Cursor::new(bytes);
		let version: u16 =
			serde_bare::from_reader(&mut cursor).context("decode Cell action envelope version")?;
		let envelope = match version {
			ACTION_ENVELOPE_VERSION_V1 => {
				let legacy: CellActionEnvelopeV1 = serde_bare::from_reader(&mut cursor)
					.context("decode Cell action envelope v1")?;
				Self {
					operation_id: legacy.operation_id,
					input: legacy.input,
					started_at: legacy.started_at,
					mode: CellMethodMode::Command,
				}
			}
			ACTION_ENVELOPE_VERSION => {
				serde_bare::from_reader(&mut cursor).context("decode Cell action envelope v2")?
			}
			_ => bail!("unsupported Cell action envelope version {version}"),
		};
		if usize::try_from(cursor.position()).unwrap_or(usize::MAX) != bytes.len() {
			bail!("Cell action envelope contains trailing bytes");
		}
		Ok(envelope)
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
			Ok(resident) => {
				if let Some(startup_ready) = startup_ready {
					let _ = startup_ready.send(Ok(()));
				}
				Some(resident)
			}
			Err(error) => {
				if let Some(startup_ready) = startup_ready {
					let _ = startup_ready.send(Err(anyhow!("Cell activation failed")));
				}
				return Err(error);
			}
		};

		let execution_gate = Arc::new(RwLock::new(()));
		let mut queries = JoinSet::new();
		while let Some(event) = events.recv().await {
			match event {
				ActorEvent::Action {
					name, args, reply, ..
				} => {
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
								.invoke_action(&ctx, generation, &bundle, &mut resident, name, args)
								.await;
							match outcome {
								ActionOutcome::Reply(result) => reply.send(result),
								ActionOutcome::Fence(error) => {
									drop(reply);
									queries.abort_all();
									return Err(error);
								}
							}
						}
						CellMethodMode::Query => {
							let runtime = self.clone();
							let ctx = ctx.clone();
							let bundle = bundle.clone();
							let reader = Arc::clone(&execution_gate).read_owned().await;
							queries.spawn(async move {
								let _reader = reader;
								let result = runtime.invoke_query(&ctx, &bundle, name, args).await;
								reply.send(result);
							});
						}
					}
				}
				ActorEvent::SerializeState { reply, .. } => reply.send(Ok(Vec::new())),
				ActorEvent::RunGracefulCleanup { reply, .. } => {
					queries.abort_all();
					while queries.join_next().await.is_some() {}
					drop(resident.take());
					reply.send(Ok(()));
					break;
				}
				ActorEvent::ConnectionClosed { .. } => {}
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
				ActorEvent::WebSocketOpen { reply, .. }
				| ActorEvent::ConnectionPreflight { reply, .. }
				| ActorEvent::ConnectionOpen { reply, .. }
				| ActorEvent::SubscribeRequest { reply, .. }
				| ActorEvent::DisconnectConn { reply, .. } => {
					reply.send(Err(anyhow!(
						"connections are not yet supported by the transactional Cell runtime"
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
		Ok(())
	}

	async fn invoke_query(
		&self,
		ctx: &rivetkit_core::ActorContext,
		bundle: &CellBundle,
		name: String,
		args: Vec<u8>,
	) -> Result<Vec<u8>> {
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
			.invoke(&name, &envelope.input, SqlCapability::ReadOnly(ctx.clone()))
			.await?;
		validate_output(&output, &self.limits)?;
		if output.state != snapshot || !output.effects.is_empty() || output.destroyed {
			bail!("CELL_QUERY_MUTATION_VIOLATION: query attempted to mutate Cell state or effects");
		}
		Ok(output.result)
	}

	#[allow(clippy::too_many_arguments)]
	async fn invoke_action(
		&self,
		ctx: &rivetkit_core::ActorContext,
		generation: u32,
		bundle: &CellBundle,
		resident: &mut Option<Resident>,
		name: String,
		args: Vec<u8>,
	) -> ActionOutcome {
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
			Err(error) => return ActionOutcome::Reply(Err(error)),
		};
		let operation = match admission {
			TransactionalOperationAdmission::Execute(operation) => operation,
			TransactionalOperationAdmission::Existing(record) => {
				return ActionOutcome::Reply(existing_operation_result(record));
			}
		};

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
		let invocation = resident
			.as_mut()
			.expect("resident was activated")
			.invoke(
				&name,
				&envelope.input,
				SqlCapability::Transactional(Arc::clone(&active)),
			)
			.await;
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
				let mut effects = output
					.effects
					.into_iter()
					.map(|effect| TransactionalEffect {
						kind: effect.kind,
						payload: effect.payload,
					})
					.collect::<Vec<_>>();
				if output.destroyed {
					effects.push(TransactionalEffect {
						kind: "wonderful.cell.destroy".to_owned(),
						payload: Vec::new(),
					});
				}
				let result = output.result.clone();
				match operation
					.commit(
						vec![StateDelta::ActorState(output.state)],
						output.result,
						effects,
						completion_timestamp(envelope.started_at),
					)
					.await
				{
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

#[derive(Debug)]
enum ActionOutcome {
	Reply(Result<Vec<u8>>),
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
	ReadOnly(rivetkit_core::ActorContext),
}

struct HostState {
	limits: StoreLimits,
	sql_capability: SqlCapability,
	query_mutation_violation: bool,
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
			SqlCapability::ReadOnly(ctx) => {
				if let Err(error) = validate_read_only_sql(&sql, &params, &self.runtime_limits) {
					self.query_mutation_violation = true;
					return Err(format!("CELL_QUERY_MUTATION_VIOLATION: {error}"));
				}
				tokio::time::timeout(
					self.runtime_limits.command_timeout,
					ctx.sql().execute(sql, Some(params)),
				)
				.await
				.map_err(|_| "CELL_QUERY_TIMEOUT: query SQL exceeded its deadline".to_owned())?
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

fn validate_read_only_sql(
	sql: &str,
	params: &[BindParam],
	limits: &WonderfulWasmtimeLimits,
) -> Result<()> {
	if sql.len() > limits.max_sql_bytes {
		bail!("query SQL exceeds configured size limit");
	}
	if params.len() > limits.max_sql_params {
		bail!("query SQL exceeds configured parameter count");
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
		bail!("query SQL exceeds configured parameter bytes");
	}
	let mut offset = 0;
	let first = next_sql_keyword(sql, &mut offset).context("query SQL is empty")?;
	if !first.eq_ignore_ascii_case("SELECT") {
		bail!("only SELECT statements are available to Cell queries");
	}
	if sql.contains(';') {
		bail!("multiple or terminated SQL statements are unavailable to Cell queries");
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
		bail!("internal tables and side-effecting functions are unavailable to Cell queries");
	}
	Ok(())
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
		if initial_state.len() > limits.max_state_bytes {
			bail!("Cell state exceeds configured activation limit");
		}
		let store_limits = StoreLimitsBuilder::new()
			.memory_size(limits.max_linear_memory_bytes)
			.instances(limits.max_component_instances)
			.build();
		let mut store = Store::new(
			&engine,
			HostState {
				limits: store_limits,
				sql_capability: SqlCapability::Unavailable,
				query_mutation_violation: false,
				sql_limit_violation: false,
				runtime_limits: limits.clone(),
				wasi: WasiCtx::builder().build(),
				resources: ResourceTable::new(),
			},
		);
		store.limiter(|state| &mut state.limits);
		store.set_fuel(limits.fuel_per_activation)?;
		let mut linker = Linker::new(&engine);
		wasmtime_wasi::p2::add_to_linker_async(&mut linker)
			.map_err(|error| anyhow!("link sandboxed WASI interfaces: {error}"))?;
		bindings::wonderful::cells::host::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
			&mut linker,
			|state| state,
		)?;
		let bindings = bindings::Cell::instantiate_async(&mut store, &component, &linker)
			.await
			.map_err(|error| anyhow!("instantiate Wonderful Cell component: {error}"))?;
		let activation = tokio::time::timeout(
			limits.activation_timeout,
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
		self.store.data_mut().sql_capability = sql_capability;
		self.store.data_mut().query_mutation_violation = false;
		self.store.data_mut().sql_limit_violation = false;
		let invocation = tokio::time::timeout(
			self.limits.command_timeout,
			self.bindings
				.wonderful_cells_guest()
				.call_invoke_command(&mut self.store, name, input),
		)
		.await;
		let query_mutation_violation = self.store.data().query_mutation_violation;
		let sql_limit_violation = self.store.data().sql_limit_violation;
		self.store.data_mut().sql_capability = SqlCapability::Unavailable;
		if query_mutation_violation {
			return Err(CellGuestInvocationError {
				code: "CELL_QUERY_MUTATION_VIOLATION".to_owned(),
				message: "Cell query attempted to execute mutable SQL".to_owned(),
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
		let output = invocation.map_err(|error| CellGuestInvocationError {
			code: error.code,
			message: error.message,
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
		.any(|effect| effect.payload.len() > limits.max_effect_bytes)
	{
		bail!("Cell effect exceeds configured payload limit");
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	const TEST_COMPONENT: &[u8] =
		include_bytes!("../tests/fixtures/wonderful_cell_test_guest.component.wasm");

	fn test_bundle() -> CellBundle {
		CellBundle::verified(
			b"test bundle".to_vec(),
			br#"{"formatVersion":1,"methods":{"echo":"command","fail":"command","sql":"command","spin":"command","read":"query","read_sql":"query","mutating_query":"query"}}"#.to_vec(),
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

		let mut legacy =
			serde_bare::to_vec(&ACTION_ENVELOPE_VERSION_V1).expect("encode legacy version");
		legacy.extend(
			serde_bare::to_vec(&CellActionEnvelopeV1 {
				operation_id: "legacy-operation".to_owned(),
				input: vec![4],
				started_at: 7,
			})
			.expect("encode legacy envelope"),
		);
		assert_eq!(
			CellActionEnvelope::decode(&legacy).expect("decode legacy envelope"),
			CellActionEnvelope {
				operation_id: "legacy-operation".to_owned(),
				input: vec![4],
				started_at: 7,
				mode: CellMethodMode::Command,
			}
		);
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
		assert_eq!(effects.len(), 1);
		assert_eq!(effects[0].operation_id, "operation-commit");
		assert_eq!(effects[0].payload, vec![7]);

		drop(event_tx);
		run.await
			.expect("join actor adapter")
			.expect("actor adapter exits cleanly");
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
	fn query_sql_capability_accepts_only_bounded_public_selects() {
		let limits = WonderfulWasmtimeLimits::default();
		assert!(
			validate_read_only_sql(
				"/* comment */ -- comment\n SELECT value FROM test_values WHERE value = ?",
				&[BindParam::Blob(vec![1])],
				&limits,
			)
			.is_ok()
		);
		for sql in [
			"INSERT INTO test_values VALUES (1)",
			"WITH value AS (SELECT 1) SELECT * FROM value",
			"SELECT * FROM _rivet_operations",
			"SELECT 1; SELECT 2",
		] {
			assert!(validate_read_only_sql(sql, &[], &limits).is_err(), "{sql}");
		}
	}

	#[test]
	fn bundle_digest_and_output_limits_fail_closed() {
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
	}
}
