use super::*;

// Literal pre-migration wire vectors. The header and timestamp are fixed-width
// little-endian integers; mode, string lengths, and data lengths are BARE uints.
const COMMAND_V1: &[u8] =
	b"\x01\x00\x0boperation-1\x03\x01\x02\x03\x2a\x00\x00\x00\x00\x00\x00\x00";
const COMMAND_V2: &[u8] =
	b"\x02\x00\x00\x0boperation-1\x03\x01\x02\x03\x2a\x00\x00\x00\x00\x00\x00\x00";
const SOCKET_V1: &[u8] =
	b"\x01\x00\x0dsocket:conn:0\x03\x01\x02\x03\x2a\x00\x00\x00\x00\x00\x00\x00";
const SOCKET_V2: &[u8] =
	b"\x02\x00\x00\x0dsocket:conn:0\x03\x01\x02\x03\x2a\x00\x00\x00\x00\x00\x00\x00";
const SCHEDULE_V1: &[u8] =
	b"\x01\x00\x0eschedule:job:0\x03\x01\x02\x03\x2a\x00\x00\x00\x00\x00\x00\x00";
const SCHEDULE_V2: &[u8] =
	b"\x02\x00\x00\x0eschedule:job:0\x03\x01\x02\x03\x2a\x00\x00\x00\x00\x00\x00\x00";
const QUERY_V2: &[u8] = b"\x02\x00\x01\x07query-1\x02\x00\xff\xfe\xff\xff\xff\xff\xff\xff\xff";

fn command(operation_id: &str) -> CellActionEnvelope {
	CellActionEnvelope {
		mode: CellMethodMode::Command,
		operation_id: operation_id.to_owned(),
		input: vec![1, 2, 3],
		started_at: 42,
	}
}

#[test]
fn command_golden_vectors_preserve_both_versions_and_internal_caller_ids() {
	for (operation_id, v1, v2) in [
		("operation-1", COMMAND_V1, COMMAND_V2),
		("socket:conn:0", SOCKET_V1, SOCKET_V2),
		("schedule:job:0", SCHEDULE_V1, SCHEDULE_V2),
	] {
		let expected = command(operation_id);
		for wire in [v1, v2] {
			assert_eq!(CellActionEnvelope::decode(wire).unwrap(), expected);
			assert_eq!(
				CellActionEnvelope::decode_mode(wire).unwrap(),
				CellMethodMode::Command,
			);
		}
		assert_eq!(expected.encode().unwrap(), v2);
		assert_eq!(expected.encode_latest().unwrap(), v2);
		assert_eq!(
			VersionedCellActionEnvelope::wrap_latest(expected.into())
				.serialize_with_embedded_version(1)
				.unwrap(),
			v1,
		);
		assert_eq!(
			CellActionEnvelope::decode(v1)
				.unwrap()
				.encode_latest()
				.unwrap(),
			v2,
		);
	}
}

#[test]
fn query_golden_vector_preserves_mode_binary_input_and_signed_timestamp() {
	let expected = CellActionEnvelope {
		mode: CellMethodMode::Query,
		operation_id: "query-1".to_owned(),
		input: vec![0, 255],
		started_at: -2,
	};
	assert_eq!(CellActionEnvelope::decode(QUERY_V2).unwrap(), expected);
	assert_eq!(expected.encode().unwrap(), QUERY_V2);
	assert_eq!(expected.encode_latest().unwrap(), QUERY_V2);
	assert_eq!(
		CellActionEnvelope::decode_mode(QUERY_V2).unwrap(),
		CellMethodMode::Query,
	);
}

#[test]
fn query_downgrade_is_rejected_instead_of_becoming_a_command() {
	let query = CellActionEnvelope::decode(QUERY_V2).unwrap();
	let error = VersionedCellActionEnvelope::wrap_latest(query.into())
		.serialize_with_embedded_version(1)
		.unwrap_err();
	assert!(error.to_string().contains("v1 does not support queries"));
}

#[test]
fn empty_fields_and_fixed_width_timestamps_have_golden_encodings() {
	let v1 = b"\x01\x00\x00\x00\x08\x07\x06\x05\x04\x03\x02\x01";
	let v2 = b"\x02\x00\x00\x00\x00\x08\x07\x06\x05\x04\x03\x02\x01";
	let expected = CellActionEnvelope {
		mode: CellMethodMode::Command,
		operation_id: String::new(),
		input: Vec::new(),
		started_at: 0x0102_0304_0506_0708,
	};
	assert_eq!(CellActionEnvelope::decode(v1).unwrap(), expected);
	assert_eq!(CellActionEnvelope::decode(v2).unwrap(), expected);
	assert_eq!(expected.encode().unwrap(), v2);
	assert_eq!(
		VersionedCellActionEnvelope::wrap_latest(expected.into())
			.serialize_with_embedded_version(1)
			.unwrap(),
		v1,
	);
}

#[test]
fn multi_byte_lengths_have_golden_encodings() {
	// Each 128-byte field is prefixed by 0x80 0x01, not a fixed-width integer.
	let mut v1 = b"\x01\x00\x80\x01".to_vec();
	v1.extend([b'a'; 128]);
	v1.extend(b"\x80\x01");
	v1.extend([255; 128]);
	v1.extend(b"\x00\x00\x00\x00\x00\x00\x00\x80");
	let mut v2 = b"\x02\x00\x00".to_vec();
	v2.extend(&v1[2..]);
	let expected = CellActionEnvelope {
		mode: CellMethodMode::Command,
		operation_id: "a".repeat(128),
		input: vec![255; 128],
		started_at: i64::MIN,
	};
	assert_eq!(CellActionEnvelope::decode(&v1).unwrap(), expected);
	assert_eq!(CellActionEnvelope::decode(&v2).unwrap(), expected);
	assert_eq!(expected.encode_latest().unwrap(), v2);
	assert_eq!(
		VersionedCellActionEnvelope::wrap_latest(expected.into())
			.serialize_with_embedded_version(1)
			.unwrap(),
		v1,
	);
}

#[test]
fn malformed_envelopes_cannot_be_classified_as_queries() {
	for wire in [COMMAND_V1, COMMAND_V2, QUERY_V2] {
		for len in 0..wire.len() {
			assert!(CellActionEnvelope::decode(&wire[..len]).is_err());
			assert!(CellActionEnvelope::decode_mode(&wire[..len]).is_err());
		}
		let mut trailing = wire.to_vec();
		trailing.push(0);
		assert!(CellActionEnvelope::decode(&trailing).is_err());
		assert!(CellActionEnvelope::decode_mode(&trailing).is_err());
	}

	for header in [[0, 0], [3, 0], [0, 2], [255, 255]] {
		let mut unsupported = QUERY_V2.to_vec();
		unsupported[..2].copy_from_slice(&header);
		assert!(CellActionEnvelope::decode(&unsupported).is_err());
		assert!(CellActionEnvelope::decode_mode(&unsupported).is_err());
	}

	for tag in [2, 127] {
		let mut invalid_mode = QUERY_V2.to_vec();
		invalid_mode[2] = tag;
		assert!(CellActionEnvelope::decode(&invalid_mode).is_err());
		assert!(CellActionEnvelope::decode_mode(&invalid_mode).is_err());
	}
	let mut invalid_utf8 = QUERY_V2.to_vec();
	invalid_utf8[4] = 255;
	assert!(CellActionEnvelope::decode(&invalid_utf8).is_err());
	assert!(CellActionEnvelope::decode_mode(&invalid_utf8).is_err());
}

#[test]
fn public_serde_shape_is_unchanged() {
	let query = CellActionEnvelope::decode(QUERY_V2).unwrap();
	let json = r#"{"mode":"query","operation_id":"query-1","input":[0,255],"started_at":-2}"#;
	assert_eq!(serde_json::to_string(&query).unwrap(), json);
	assert_eq!(
		serde_json::from_str::<CellActionEnvelope>(json).unwrap(),
		query
	);
	assert_eq!(
		serde_json::to_string(&CellMethodMode::Command).unwrap(),
		r#""command""#,
	);
}
