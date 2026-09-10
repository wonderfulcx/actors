use std::time::Duration;

use anyhow::{Context, Result, bail};
use rivet_error::RivetError;
use sha2::{Digest, Sha256};

use crate::actor::context::ActorContext;
use crate::actor::internal_storage;
use crate::actor::messages::StateDelta;
use crate::actor::state::ActorStateTransaction;
use crate::error::{TransactionalOperationError, client_error_message, client_error_metadata};
use crate::sqlite::{BindParam, ColumnValue};

const MAX_OPERATION_ID_BYTES: usize = 128;
const MAX_OPERATION_NAME_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum TransactionalOperationKind {
	Action = 0,
	Http = 1,
	WebSocket = 2,
	Alarm = 3,
	Lifecycle = 4,
}

impl TryFrom<i64> for TransactionalOperationKind {
	type Error = anyhow::Error;

	fn try_from(value: i64) -> Result<Self> {
		match value {
			0 => Ok(Self::Action),
			1 => Ok(Self::Http),
			2 => Ok(Self::WebSocket),
			3 => Ok(Self::Alarm),
			4 => Ok(Self::Lifecycle),
			_ => bail!("invalid transactional operation kind {value}"),
		}
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum TransactionalOperationStatus {
	Started = 0,
	Succeeded = 1,
	Failed = 2,
	Interrupted = 3,
}

impl TryFrom<i64> for TransactionalOperationStatus {
	type Error = anyhow::Error;

	fn try_from(value: i64) -> Result<Self> {
		match value {
			0 => Ok(Self::Started),
			1 => Ok(Self::Succeeded),
			2 => Ok(Self::Failed),
			3 => Ok(Self::Interrupted),
			_ => bail!("invalid transactional operation status {value}"),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionalOperationRequest {
	pub operation_id: String,
	pub kind: TransactionalOperationKind,
	pub name: String,
	pub input: Vec<u8>,
	pub actor_generation: u32,
	pub started_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionalOperationFailure {
	pub group: String,
	pub code: String,
	pub message: String,
	pub metadata: Option<serde_json::Value>,
}

impl TransactionalOperationFailure {
	pub fn from_error(error: &anyhow::Error) -> Self {
		let extracted = RivetError::extract(error);
		let metadata = extracted.metadata();
		Self {
			group: extracted.group().to_owned(),
			code: extracted.code().to_owned(),
			message: client_error_message(extracted.group(), extracted.code(), extracted.message())
				.to_owned(),
			metadata: client_error_metadata(extracted.group(), extracted.code(), metadata.as_ref())
				.cloned(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionalOperationRecord {
	pub request: TransactionalOperationRequest,
	pub input_digest: [u8; 32],
	pub status: TransactionalOperationStatus,
	pub completed_at: Option<i64>,
	pub commit_sequence: Option<u64>,
	pub result: Option<Vec<u8>>,
	pub error: Option<TransactionalOperationFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionalEffect {
	pub kind: String,
	pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionalScheduleUpsert<'a> {
	pub event_id: &'a str,
	pub trigger_at: i64,
	pub action: &'a str,
	pub args: &'a [u8],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingTransactionalEffect {
	pub operation_id: String,
	pub effect_index: u32,
	pub commit_sequence: u64,
	pub kind: String,
	pub payload: Vec<u8>,
	pub attempts: u32,
}

pub enum TransactionalOperationAdmission {
	Execute(TransactionalOperation),
	Existing(TransactionalOperationRecord),
}

pub struct TransactionalOperation {
	ctx: ActorContext,
	request: TransactionalOperationRequest,
	transaction: ActorStateTransaction,
}

impl TransactionalOperation {
	pub fn request(&self) -> &TransactionalOperationRequest {
		&self.request
	}

	pub async fn execute_sql(
		&self,
		sql: impl Into<String>,
		params: Option<Vec<BindParam>>,
	) -> Result<crate::sqlite::ExecuteResult> {
		self.transaction.execute(sql, params).await
	}

	#[doc(hidden)]
	pub async fn pending_effect_operation_count(&self) -> Result<u64> {
		let result = self
			.transaction
			.execute(
				internal_storage::COUNT_PENDING_TRANSACTIONAL_EFFECT_OPERATIONS_SQL,
				None,
			)
			.await
			.context("count pending effect operations in transaction")?;
		decode_count(&result.rows, "pending transactional effect operation count")
	}

	#[doc(hidden)]
	pub async fn stage_schedule_upsert(
		&self,
		upsert: TransactionalScheduleUpsert<'_>,
	) -> Result<()> {
		let existing = !self
			.transaction
			.execute(
				internal_storage::LOAD_SCHEDULE_SQL,
				Some(vec![BindParam::Text(upsert.event_id.to_owned())]),
			)
			.await
			.context("load staged transactional schedule")?
			.rows
			.is_empty();
		if !existing {
			let count = self
				.transaction
				.execute(internal_storage::COUNT_SCHEDULES_SQL, None)
				.await
				.context("count staged transactional schedules")?
				.rows
				.first()
				.and_then(|row| row.first())
				.and_then(|value| match value {
					ColumnValue::Integer(value) => Some(*value),
					_ => None,
				})
				.context("transactional schedule count is not an integer")?;
			if count >= i64::from(self.ctx.0.max_schedules) {
				return Err(crate::error::ScheduleRuntimeError::MaxSchedulesExceeded {
					maximum: self.ctx.0.max_schedules,
				}
				.build());
			}
		}
		self.transaction
			.execute(
				internal_storage::UPSERT_NAMED_SCHEDULE_EVENT_SQL,
				Some(vec![
					BindParam::Text(upsert.event_id.to_owned()),
					BindParam::Integer(upsert.trigger_at),
					BindParam::Text(upsert.action.to_owned()),
					BindParam::Blob(upsert.args.to_vec()),
					BindParam::Integer(super::schedule::ScheduleKind::At.as_i64()),
				]),
			)
			.await
			.context("stage transactional schedule upsert")?;
		Ok(())
	}

	#[doc(hidden)]
	pub async fn stage_schedule_cancel(&self, event_id: &str) -> Result<()> {
		self.transaction
			.execute(
				internal_storage::CANCEL_SCHEDULE_SQL,
				Some(vec![
					BindParam::Text(event_id.to_owned()),
					BindParam::Integer(super::schedule::ScheduleKind::At.as_i64()),
				]),
			)
			.await
			.context("stage transactional schedule cancellation")?;
		Ok(())
	}

	pub async fn commit(
		self,
		deltas: Vec<StateDelta>,
		result: Vec<u8>,
		effects: Vec<TransactionalEffect>,
		completed_at: i64,
	) -> Result<u64> {
		let sequence_result = self
			.transaction
			.execute(
				internal_storage::NEXT_TRANSACTIONAL_COMMIT_SEQUENCE_SQL,
				None,
			)
			.await
			.context("allocate transactional operation commit sequence")?;
		let commit_sequence =
			read_returned_u64(&sequence_result.rows, "transactional commit sequence")?;
		let commit_sequence_i64 = i64::try_from(commit_sequence)
			.context("transactional commit sequence exceeds sqlite integer range")?;
		let completion = self
			.transaction
			.execute(
				internal_storage::COMPLETE_TRANSACTIONAL_OPERATION_SQL,
				Some(vec![
					BindParam::Integer(completed_at),
					BindParam::Integer(commit_sequence_i64),
					BindParam::Blob(result),
					BindParam::Text(self.request.operation_id.clone()),
				]),
			)
			.await
			.context("stage transactional operation completion")?;
		if completion.changes != 1 {
			let _ = self.transaction.rollback().await;
			bail!("transactional operation was not in started state at commit");
		}

		for (index, effect) in effects.into_iter().enumerate() {
			let effect_index = i64::try_from(index)
				.context("transactional effect index exceeds sqlite integer range")?;
			self.transaction
				.execute(
					internal_storage::INSERT_TRANSACTIONAL_EFFECT_SQL,
					Some(vec![
						BindParam::Text(self.request.operation_id.clone()),
						BindParam::Integer(effect_index),
						BindParam::Integer(commit_sequence_i64),
						BindParam::Text(effect.kind),
						BindParam::Blob(effect.payload),
					]),
				)
				.await
				.context("stage transactional operation effect")?;
		}

		self.transaction
			.commit(deltas)
			.await
			.context("commit transactional operation aggregate")?;
		Ok(commit_sequence)
	}

	pub async fn fail(
		self,
		error: &anyhow::Error,
		completed_at: i64,
	) -> Result<TransactionalOperationFailure> {
		let failure = TransactionalOperationFailure::from_error(error);
		self.fail_with(failure, completed_at).await
	}

	/// Rolls back guest changes and durably records an adapter-sanitized failure.
	pub async fn fail_with(
		self,
		failure: TransactionalOperationFailure,
		completed_at: i64,
	) -> Result<TransactionalOperationFailure> {
		self.transaction
			.rollback()
			.await
			.context("roll back failed transactional operation")?;
		persist_terminal_failure(
			&self.ctx,
			&self.request.operation_id,
			TransactionalOperationStatus::Failed,
			&failure,
			completed_at,
		)
		.await?;
		Ok(failure)
	}
}

impl ActorContext {
	/// Returns the latest committed transactional writer sequence.
	pub async fn transactional_snapshot_version(&self) -> Result<u64> {
		let result = self
			.sql()
			.query(
				internal_storage::LOAD_TRANSACTIONAL_COMMIT_SEQUENCE_SQL,
				None,
			)
			.await
			.context("load transactional snapshot version")?;
		let row = result
			.rows
			.first()
			.context("transactional runtime row is missing")?;
		u64::try_from(read_i64(row, 0, "commit_sequence")?)
			.context("transactional snapshot version is negative")
	}

	pub async fn begin_transactional_operation(
		&self,
		request: TransactionalOperationRequest,
		timeout: Option<Duration>,
	) -> Result<TransactionalOperationAdmission> {
		validate_request(&request)?;
		let input_digest = operation_digest(&request);
		let inserted = self
			.sql()
			.execute(
				internal_storage::INSERT_TRANSACTIONAL_OPERATION_SQL,
				Some(vec![
					BindParam::Text(request.operation_id.clone()),
					BindParam::Blob(input_digest.to_vec()),
					BindParam::Integer(request.kind as i64),
					BindParam::Text(request.name.clone()),
					BindParam::Blob(request.input.clone()),
					BindParam::Integer(i64::from(request.actor_generation)),
					BindParam::Integer(request.started_at),
				]),
			)
			.await
			.context("persist transactional operation durable start")?;
		let Some(record) = self.transactional_operation(&request.operation_id).await? else {
			// A terminal result can be compacted between admission and this read.
			// Its identity survives atomically, and must never execute again.
			let tombstone = self
				.sql()
				.query(
					internal_storage::LOAD_TRANSACTIONAL_OPERATION_TOMBSTONE_SQL,
					Some(vec![BindParam::Text(request.operation_id.clone())]),
				)
				.await
				.context("load expired transactional operation identity")?;
			let row = tombstone
				.rows
				.first()
				.context("transactional operation identity missing after admission")?;
			if read_blob(row, 0, "input_digest")? != input_digest {
				return Err(TransactionalOperationError::OperationIdConflict {
					operation_id: request.operation_id,
				}
				.build());
			}
			return Err(TransactionalOperationError::OperationResultExpired {
				operation_id: request.operation_id,
			}
			.build());
		};
		if record.input_digest != input_digest {
			return Err(TransactionalOperationError::OperationIdConflict {
				operation_id: request.operation_id,
			}
			.build());
		}
		if inserted.changes == 0 {
			return Ok(TransactionalOperationAdmission::Existing(record));
		}

		match self.begin_state_transaction(timeout).await {
			Ok(transaction) => Ok(TransactionalOperationAdmission::Execute(
				TransactionalOperation {
					ctx: self.clone(),
					request,
					transaction,
				},
			)),
			Err(error) => {
				let failure = TransactionalOperationFailure::from_error(&error);
				persist_terminal_failure(
					self,
					&record.request.operation_id,
					TransactionalOperationStatus::Interrupted,
					&failure,
					record.request.started_at,
				)
				.await
				.context("terminalize operation after transaction begin failure")?;
				Err(error)
			}
		}
	}

	pub async fn transactional_operation(
		&self,
		operation_id: &str,
	) -> Result<Option<TransactionalOperationRecord>> {
		let result = self
			.sql()
			.query(
				internal_storage::LOAD_TRANSACTIONAL_OPERATION_SQL,
				Some(vec![BindParam::Text(operation_id.to_owned())]),
			)
			.await
			.context("load transactional operation")?;
		result
			.rows
			.first()
			.map(|row| decode_operation_record(row))
			.transpose()
	}

	pub async fn recover_interrupted_transactional_operations(
		&self,
		completed_at: i64,
	) -> Result<u64> {
		let result = self
			.sql()
			.execute(
				internal_storage::RECOVER_TRANSACTIONAL_OPERATIONS_SQL,
				Some(vec![BindParam::Integer(completed_at)]),
			)
			.await
			.context("terminalize interrupted transactional operations")?;
		u64::try_from(result.changes).context("recovered operation count is negative")
	}

	pub async fn has_pending_transactional_effect_kind(&self, kind: &str) -> Result<bool> {
		let result = self
			.sql()
			.execute(
				internal_storage::HAS_PENDING_TRANSACTIONAL_EFFECT_KIND_SQL,
				Some(vec![BindParam::Text(kind.to_owned())]),
			)
			.await
			.context("check pending transactional effect kind")?;
		Ok(!result.rows.is_empty())
	}

	pub async fn pending_transactional_effect_operation_count(&self) -> Result<u64> {
		let result = self
			.sql()
			.query(
				internal_storage::COUNT_PENDING_TRANSACTIONAL_EFFECT_OPERATIONS_SQL,
				None,
			)
			.await
			.context("count operations with pending transactional effects")?;
		decode_count(&result.rows, "pending transactional effect operation count")
	}

	pub async fn pending_transactional_effects(
		&self,
		limit: u32,
	) -> Result<Vec<PendingTransactionalEffect>> {
		if limit == 0 {
			return Ok(Vec::new());
		}
		let result = self
			.sql()
			.query(
				internal_storage::LOAD_PENDING_TRANSACTIONAL_EFFECTS_SQL,
				Some(vec![BindParam::Integer(i64::from(limit))]),
			)
			.await
			.context("load pending transactional effects")?;
		result
			.rows
			.iter()
			.map(|row| decode_pending_effect(row))
			.collect()
	}

	pub async fn mark_transactional_effect_delivered(
		&self,
		operation_id: &str,
		effect_index: u32,
		attempted_at: i64,
	) -> Result<bool> {
		let result = self
			.sql()
			.execute(
				internal_storage::MARK_TRANSACTIONAL_EFFECT_DELIVERED_SQL,
				Some(vec![
					BindParam::Integer(attempted_at),
					BindParam::Text(operation_id.to_owned()),
					BindParam::Integer(i64::from(effect_index)),
				]),
			)
			.await
			.context("mark transactional effect delivered")?;
		Ok(result.changes == 1)
	}

	pub async fn acknowledge_transactional_schedule_change(
		&self,
		operation_id: &str,
		effect_index: u32,
		attempted_at: i64,
		event_id: &str,
	) -> Result<bool> {
		if event_id.is_empty() || event_id.len() > 256 {
			bail!("transactional schedule event ID must contain 1 to 256 bytes");
		}
		let _mutation = self.0.schedule_mutation_lock.lock().await;
		let transaction = self.begin_state_transaction(None).await?;
		let acknowledged = transaction
			.execute(
				internal_storage::MARK_TRANSACTIONAL_EFFECT_DELIVERED_SQL,
				Some(vec![
					BindParam::Integer(attempted_at),
					BindParam::Text(operation_id.to_owned()),
					BindParam::Integer(i64::from(effect_index)),
				]),
			)
			.await
			.context("stage transactional schedule effect acknowledgement")?;
		if acknowledged.changes != 1 {
			transaction.rollback().await?;
			return Ok(false);
		}
		transaction.commit(Vec::new()).await?;
		// The schedule row was already mutated by the originating aggregate.
		// Reconcile transport from current committed state rather than replaying
		// stale upserts/cancels over newer commands.
		self.finalize_transactional_schedule_change().await;
		Ok(true)
	}

	pub async fn mark_transactional_effect_attempt_failed(
		&self,
		operation_id: &str,
		effect_index: u32,
		attempted_at: i64,
	) -> Result<bool> {
		let result = self
			.sql()
			.execute(
				internal_storage::MARK_TRANSACTIONAL_EFFECT_ATTEMPT_FAILED_SQL,
				Some(vec![
					BindParam::Integer(attempted_at),
					BindParam::Text(operation_id.to_owned()),
					BindParam::Integer(i64::from(effect_index)),
				]),
			)
			.await
			.context("record transactional effect delivery failure")?;
		Ok(result.changes == 1)
	}

	/// Bounds retained terminal results while preserving Started rows and pending effects.
	/// Minimal execution identities remain permanently and grow with lifetime operations.
	pub async fn compact_transactional_operations(&self, retain: u32) -> Result<u64> {
		let result = self
			.sql()
			.execute(
				internal_storage::COMPACT_TRANSACTIONAL_OPERATIONS_SQL,
				Some(vec![BindParam::Integer(i64::from(retain))]),
			)
			.await
			.context("compact terminal transactional operations")?;
		u64::try_from(result.changes).context("compacted operation count is negative")
	}

	/// Removes old terminal results, never their permanent execution identities.
	/// Started rows and operations with pending effects are not eligible.
	pub async fn prune_transactional_operations(
		&self,
		completed_before: i64,
		limit: u32,
	) -> Result<u64> {
		if limit == 0 {
			return Ok(0);
		}
		let result = self
			.sql()
			.execute(
				internal_storage::PRUNE_TRANSACTIONAL_OPERATIONS_SQL,
				Some(vec![
					BindParam::Integer(completed_before),
					BindParam::Integer(i64::from(limit)),
				]),
			)
			.await
			.context("prune terminal transactional operations")?;
		u64::try_from(result.changes).context("pruned operation count is negative")
	}
}

fn decode_count(rows: &[Vec<ColumnValue>], label: &str) -> Result<u64> {
	let count = rows
		.first()
		.and_then(|row| row.first())
		.and_then(|value| match value {
			ColumnValue::Integer(value) => Some(*value),
			_ => None,
		})
		.with_context(|| format!("{label} is not an integer"))?;
	u64::try_from(count).with_context(|| format!("{label} is negative"))
}

async fn persist_terminal_failure(
	ctx: &ActorContext,
	operation_id: &str,
	status: TransactionalOperationStatus,
	failure: &TransactionalOperationFailure,
	completed_at: i64,
) -> Result<()> {
	if !matches!(
		status,
		TransactionalOperationStatus::Failed | TransactionalOperationStatus::Interrupted
	) {
		bail!("transactional terminal failure requires failed or interrupted status");
	}
	let metadata = failure
		.metadata
		.as_ref()
		.map(encode_error_metadata)
		.transpose()?;
	let result = ctx
		.sql()
		.execute(
			internal_storage::FAIL_TRANSACTIONAL_OPERATION_SQL,
			Some(vec![
				BindParam::Integer(status as i64),
				BindParam::Integer(completed_at),
				BindParam::Text(failure.group.clone()),
				BindParam::Text(failure.code.clone()),
				BindParam::Text(failure.message.clone()),
				optional_blob(metadata),
				BindParam::Text(operation_id.to_owned()),
			]),
		)
		.await
		.context("persist transactional operation terminal failure")?;
	if result.changes != 1 {
		bail!("transactional operation was not in started state at failure");
	}
	Ok(())
}

fn validate_request(request: &TransactionalOperationRequest) -> Result<()> {
	if request.operation_id.is_empty() || request.operation_id.len() > MAX_OPERATION_ID_BYTES {
		return Err(TransactionalOperationError::InvalidOperation {
			reason: format!("operation ID must contain 1 to {MAX_OPERATION_ID_BYTES} bytes"),
		}
		.build());
	}
	if request.name.is_empty() || request.name.len() > MAX_OPERATION_NAME_BYTES {
		return Err(TransactionalOperationError::InvalidOperation {
			reason: format!("operation name must contain 1 to {MAX_OPERATION_NAME_BYTES} bytes"),
		}
		.build());
	}
	Ok(())
}

fn operation_digest(request: &TransactionalOperationRequest) -> [u8; 32] {
	let mut hasher = Sha256::new();
	hasher.update((request.kind as i64).to_be_bytes());
	hasher.update((request.name.len() as u64).to_be_bytes());
	hasher.update(request.name.as_bytes());
	hasher.update((request.input.len() as u64).to_be_bytes());
	hasher.update(&request.input);
	hasher.finalize().into()
}

fn decode_operation_record(row: &[ColumnValue]) -> Result<TransactionalOperationRecord> {
	let input_digest: [u8; 32] = read_blob(row, 1, "input_digest")?
		.try_into()
		.map_err(|_| anyhow::anyhow!("transactional operation digest must contain 32 bytes"))?;
	let error_group = read_optional_text(row, 11, "error_group")?;
	let error_code = read_optional_text(row, 12, "error_code")?;
	let error_message = read_optional_text(row, 13, "error_message")?;
	let error_metadata = read_optional_blob(row, 14, "error_metadata")?
		.map(|value| decode_error_metadata(&value))
		.transpose()?;
	let error = match (error_group, error_code, error_message) {
		(None, None, None) => None,
		(Some(group), Some(code), Some(message)) => Some(TransactionalOperationFailure {
			group,
			code,
			message,
			metadata: error_metadata,
		}),
		_ => bail!("transactional operation error columns are incomplete"),
	};
	Ok(TransactionalOperationRecord {
		request: TransactionalOperationRequest {
			operation_id: read_text(row, 0, "operation_id")?,
			kind: TransactionalOperationKind::try_from(read_i64(row, 2, "kind")?)?,
			name: read_text(row, 3, "name")?,
			input: read_blob(row, 4, "input")?,
			actor_generation: u32::try_from(read_i64(row, 6, "actor_generation")?)
				.context("transactional actor generation is invalid")?,
			started_at: read_i64(row, 7, "started_at")?,
		},
		input_digest,
		status: TransactionalOperationStatus::try_from(read_i64(row, 5, "status")?)?,
		completed_at: read_optional_i64(row, 8, "completed_at")?,
		commit_sequence: read_optional_i64(row, 9, "commit_sequence")?
			.map(|value| u64::try_from(value).context("transactional commit sequence is invalid"))
			.transpose()?,
		result: read_optional_blob(row, 10, "result")?,
		error,
	})
}

fn decode_pending_effect(row: &[ColumnValue]) -> Result<PendingTransactionalEffect> {
	Ok(PendingTransactionalEffect {
		operation_id: read_text(row, 0, "operation_id")?,
		effect_index: u32::try_from(read_i64(row, 1, "effect_index")?)
			.context("transactional effect index is invalid")?,
		commit_sequence: u64::try_from(read_i64(row, 2, "commit_sequence")?)
			.context("transactional effect commit sequence is invalid")?,
		kind: read_text(row, 3, "kind")?,
		payload: read_blob(row, 4, "payload")?,
		attempts: u32::try_from(read_i64(row, 5, "attempts")?)
			.context("transactional effect attempt count is invalid")?,
	})
}

fn read_returned_u64(rows: &[Vec<ColumnValue>], label: &str) -> Result<u64> {
	let row = rows
		.first()
		.with_context(|| format!("missing {label} row"))?;
	u64::try_from(read_i64(row, 0, label)?).with_context(|| format!("invalid {label}"))
}

fn read_i64(row: &[ColumnValue], index: usize, label: &str) -> Result<i64> {
	match row.get(index) {
		Some(ColumnValue::Integer(value)) => Ok(*value),
		_ => bail!("invalid {label} column"),
	}
}

fn read_text(row: &[ColumnValue], index: usize, label: &str) -> Result<String> {
	match row.get(index) {
		Some(ColumnValue::Text(value)) => Ok(value.clone()),
		_ => bail!("invalid {label} column"),
	}
}

fn read_optional_text(row: &[ColumnValue], index: usize, label: &str) -> Result<Option<String>> {
	match row.get(index) {
		Some(ColumnValue::Text(value)) => Ok(Some(value.clone())),
		Some(ColumnValue::Null) => Ok(None),
		_ => bail!("invalid {label} column"),
	}
}

fn read_blob(row: &[ColumnValue], index: usize, label: &str) -> Result<Vec<u8>> {
	match row.get(index) {
		Some(ColumnValue::Blob(value)) => Ok(value.clone()),
		_ => bail!("invalid {label} column"),
	}
}

fn read_optional_blob(row: &[ColumnValue], index: usize, label: &str) -> Result<Option<Vec<u8>>> {
	match row.get(index) {
		Some(ColumnValue::Blob(value)) => Ok(Some(value.clone())),
		Some(ColumnValue::Null) => Ok(None),
		_ => bail!("invalid {label} column"),
	}
}

fn read_optional_i64(row: &[ColumnValue], index: usize, label: &str) -> Result<Option<i64>> {
	match row.get(index) {
		Some(ColumnValue::Integer(value)) => Ok(Some(*value)),
		Some(ColumnValue::Null) => Ok(None),
		_ => bail!("invalid {label} column"),
	}
}

fn optional_blob(value: Option<Vec<u8>>) -> BindParam {
	value.map_or(BindParam::Null, BindParam::Blob)
}

fn encode_error_metadata(metadata: &serde_json::Value) -> Result<Vec<u8>> {
	let mut output = Vec::new();
	ciborium::into_writer(metadata, &mut output)
		.context("encode transactional operation error metadata")?;
	Ok(output)
}

fn decode_error_metadata(value: &[u8]) -> Result<serde_json::Value> {
	ciborium::from_reader(value).context("decode transactional operation error metadata")
}

#[cfg(test)]
#[path = "../../tests/transactional.rs"]
mod tests;
