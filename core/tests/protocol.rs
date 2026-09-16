//! Spec §11: control-stream framing.

use arborsync_core::meta::FileMetadata;
use arborsync_core::protocol::{
    Envelope, FrameError, MAX_CONTROL_FRAME, PROTOCOL_PREAMBLE, PROTOCOL_VERSION, ProtocolMessage,
    decode_control, encode_control,
};

#[test]
fn preamble_is_ascii_arborsync_v1() {
    assert_eq!(PROTOCOL_PREAMBLE, b"arborsync-v1");
}

#[test]
fn encode_is_length_prefixed_envelope_version_1() {
    let msg = ProtocolMessage::Disconnect {
        reason: "bye".into(),
    };
    let frame = encode_control(&msg).unwrap();
    assert!(frame.len() >= 4);
    let len = u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize;
    assert_eq!(len, frame.len() - 4);
    assert!(len <= MAX_CONTROL_FRAME);

    let (decoded, consumed) = decode_control(&frame).unwrap();
    assert_eq!(consumed, frame.len());
    assert_eq!(decoded, msg);
}

#[test]
fn decode_rejects_truncated_and_oversized_frames() {
    assert!(decode_control(&[0, 0]).is_err());
    let mut huge = (MAX_CONTROL_FRAME as u32 + 1).to_be_bytes().to_vec();
    huge.extend(std::iter::repeat_n(0, 8));
    assert!(decode_control(&huge).is_err());
}

#[test]
fn file_announce_round_trips_through_the_frame() {
    let msg = ProtocolMessage::FileAnnounce {
        checkout_id: "src".into(),
        path: "/src/foo.rs".into(),
        new: FileMetadata::file(3, 0, 0o100644, [9; 32]),
        basis: None,
    };
    let frame = encode_control(&msg).unwrap();
    let (decoded, _) = decode_control(&frame).unwrap();
    assert_eq!(decoded, msg);
}

#[test]
fn envelope_version_constant_is_one() {
    let env = Envelope {
        version: PROTOCOL_VERSION,
        msg: ProtocolMessage::Error {
            code: "x".into(),
            message: "y".into(),
        },
    };
    assert_eq!(env.version, 1);
}

#[test]
fn decode_reports_consumed_and_leaves_trailing_bytes() {
    let msg = ProtocolMessage::Disconnect { reason: "x".into() };
    let mut buf = encode_control(&msg).unwrap();
    buf.extend_from_slice(&[1, 2, 3]);
    let (decoded, consumed) = decode_control(&buf).unwrap();
    assert_eq!(decoded, msg);
    assert_eq!(consumed, buf.len() - 3);
}

#[test]
fn decode_rejects_unsupported_envelope_version() {
    let env = Envelope {
        version: 2,
        msg: ProtocolMessage::Disconnect { reason: "x".into() },
    };
    let payload = bincode::serde::encode_to_vec(&env, bincode::config::standard()).unwrap();
    let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&payload);
    assert_eq!(
        decode_control(&frame).unwrap_err(),
        FrameError::UnsupportedVersion(2)
    );
}
