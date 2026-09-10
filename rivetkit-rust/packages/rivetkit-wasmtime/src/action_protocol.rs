//! Versioned Cell action envelopes. Reserved operation IDs are authorized by the
//! runtime at admission, not by this codec, because scheduled and socket callers
//! legitimately encode those IDs.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use vbare::OwnedVersionedData;

mod generated {
	pub mod v1 {
		include!(concat!(env!("OUT_DIR"), "/action_v1_generated.rs"));
	}
	pub mod v2 {
		include!(concat!(env!("OUT_DIR"), "/action_v2_generated.rs"));
	}
}

use generated::{v1, v2};

const ACTION_ENVELOPE_VERSION_V1: u16 = 1;
const ACTION_ENVELOPE_VERSION: u16 = 2;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellMethodMode {
	Command,
	Query,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CellActionEnvelope {
	pub mode: CellMethodMode,
	pub operation_id: String,
	pub input: Vec<u8>,
	pub started_at: i64,
}

impl CellActionEnvelope {
	pub(crate) fn decode_mode(bytes: &[u8]) -> Result<CellMethodMode> {
		// Scheduling shares the version dispatch and validation used for execution.
		// In particular, a malformed query must not enter the concurrent lane.
		Ok(Self::decode(bytes)?.mode)
	}

	pub fn encode(&self) -> Result<Vec<u8>> {
		self.encode_latest()
	}

	pub fn encode_latest(&self) -> Result<Vec<u8>> {
		VersionedCellActionEnvelope::wrap_latest(self.clone().into())
			.serialize_with_embedded_version(ACTION_ENVELOPE_VERSION)
			.context("encode Cell action envelope")
	}

	pub fn decode(bytes: &[u8]) -> Result<Self> {
		VersionedCellActionEnvelope::deserialize_with_embedded_version(bytes)
			.map(Into::into)
			.context("decode Cell action envelope")
	}
}

impl From<CellMethodMode> for v2::CellMethodMode {
	fn from(mode: CellMethodMode) -> Self {
		match mode {
			CellMethodMode::Command => Self::Command,
			CellMethodMode::Query => Self::Query,
		}
	}
}

impl From<v2::CellMethodMode> for CellMethodMode {
	fn from(mode: v2::CellMethodMode) -> Self {
		match mode {
			v2::CellMethodMode::Command => Self::Command,
			v2::CellMethodMode::Query => Self::Query,
		}
	}
}

impl From<CellActionEnvelope> for v2::CellActionEnvelope {
	fn from(envelope: CellActionEnvelope) -> Self {
		Self {
			mode: envelope.mode.into(),
			operation_id: envelope.operation_id,
			input: envelope.input,
			started_at: envelope.started_at,
		}
	}
}

impl From<v2::CellActionEnvelope> for CellActionEnvelope {
	fn from(envelope: v2::CellActionEnvelope) -> Self {
		Self {
			mode: envelope.mode.into(),
			operation_id: envelope.operation_id,
			input: envelope.input,
			started_at: envelope.started_at,
		}
	}
}

enum VersionedCellActionEnvelope {
	V1(v1::CellActionEnvelope),
	V2(v2::CellActionEnvelope),
}

impl OwnedVersionedData for VersionedCellActionEnvelope {
	type Latest = v2::CellActionEnvelope;

	fn wrap_latest(latest: Self::Latest) -> Self {
		Self::V2(latest)
	}

	fn unwrap_latest(self) -> Result<Self::Latest> {
		match self {
			Self::V1(_) => bail!("Cell action envelope version is not latest"),
			Self::V2(latest) => Ok(latest),
		}
	}

	fn deserialize_version(payload: &[u8], version: u16) -> Result<Self> {
		// serde_bare::from_slice permits trailing bytes. Reading through a slice
		// retains the unconsumed suffix without hand-parsing any schema fields.
		let mut remaining = payload;
		let envelope = match version {
			ACTION_ENVELOPE_VERSION_V1 => Self::V1(
				serde_bare::from_reader(&mut remaining)
					.context("decode Cell action envelope v1")?,
			),
			ACTION_ENVELOPE_VERSION => Self::V2(
				serde_bare::from_reader(&mut remaining)
					.context("decode Cell action envelope v2")?,
			),
			_ => bail!("unsupported Cell action envelope version {version}"),
		};
		if !remaining.is_empty() {
			bail!("Cell action envelope contains trailing bytes");
		}
		Ok(envelope)
	}

	fn serialize_version(self, version: u16) -> Result<Vec<u8>> {
		match (self, version) {
			(Self::V1(envelope), ACTION_ENVELOPE_VERSION_V1) => Ok(serde_bare::to_vec(&envelope)?),
			(Self::V2(envelope), ACTION_ENVELOPE_VERSION) => Ok(serde_bare::to_vec(&envelope)?),
			(Self::V1(_) | Self::V2(_), _) => {
				bail!("unexpected Cell action envelope version {version}")
			}
		}
	}

	fn deserialize_converters() -> Vec<impl Fn(Self) -> Result<Self>> {
		vec![Self::v1_to_v2]
	}

	fn serialize_converters() -> Vec<impl Fn(Self) -> Result<Self>> {
		vec![Self::v2_to_v1]
	}
}

impl VersionedCellActionEnvelope {
	fn v1_to_v2(self) -> Result<Self> {
		let Self::V1(envelope) = self else {
			bail!("expected Cell action envelope v1");
		};
		Ok(Self::V2(v2::CellActionEnvelope {
			mode: v2::CellMethodMode::Command,
			operation_id: envelope.operation_id,
			input: envelope.input,
			started_at: envelope.started_at,
		}))
	}

	fn v2_to_v1(self) -> Result<Self> {
		let Self::V2(envelope) = self else {
			bail!("expected Cell action envelope v2");
		};
		match envelope.mode {
			v2::CellMethodMode::Command => Ok(Self::V1(v1::CellActionEnvelope {
				operation_id: envelope.operation_id,
				input: envelope.input,
				started_at: envelope.started_at,
			})),
			v2::CellMethodMode::Query => {
				bail!("Cell action envelope v1 does not support queries")
			}
		}
	}
}

// Private scheduling and converter checks share this module's visibility.
#[cfg(test)]
#[path = "../tests/modules/action_protocol.rs"]
mod tests;
