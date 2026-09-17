use arborsync_core::hash::{ContentHash, DirNode, FileNode};
use arborsync_core::meta::FileMetadata;
use arborsync_core::path::{CanonicalPath, EntryName};
use arborsync_core::protocol::{
    BulkEncoding, BulkHeader, DirEntry, Envelope, FrameError, MAX_CONTROL_FRAME, PROTOCOL_PREAMBLE,
    PROTOCOL_VERSION, ProtocolMessage, decode_bulk, decode_control, encode_bulk, encode_control,
};
use arborsync_core::test_support::{name, p};

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
        path: p("/src/foo.rs"),
        new: FileMetadata::file(3, 0, 0o100644, ContentHash::from_bytes([9; 32])),
        basis: None,
    };
    let frame = encode_control(&msg).unwrap();
    let (decoded, _) = decode_control(&frame).unwrap();
    assert_eq!(decoded, msg);
}

#[test]
fn dir_list_response_round_trips_each_child_brand() {
    let msg = ProtocolMessage::DirListResponse {
        checkout_id: "src".into(),
        path: p("/src"),
        entries: vec![
            DirEntry::File {
                name: name("foo.rs"),
                node: FileNode::from_bytes([1; 32]),
            },
            DirEntry::Directory {
                name: name("nested"),
                node: DirNode::from_bytes([2; 32]),
            },
            DirEntry::Symlink {
                name: name("link"),
                node: FileNode::from_bytes([3; 32]),
            },
        ],
    };
    let frame = encode_control(&msg).unwrap();
    let (decoded, _) = decode_control(&frame).unwrap();
    assert_eq!(decoded, msg);
}

#[test]
fn wire_paths_and_names_are_parsed_not_trusted() {
    let bytes = bincode::serde::encode_to_vec("/src", bincode::config::standard()).unwrap();
    let (path, _): (CanonicalPath, usize) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
    assert_eq!(path, p("/src"));

    for bad in ["//src", "/src/", "src", "/src/../etc"] {
        let bytes = bincode::serde::encode_to_vec(bad, bincode::config::standard()).unwrap();
        let decoded: Result<(CanonicalPath, usize), _> =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard());
        assert!(decoded.is_err(), "{bad}");
    }

    for bad in ["a/b", "", ".."] {
        let bytes = bincode::serde::encode_to_vec(bad, bincode::config::standard()).unwrap();
        let decoded: Result<(EntryName, usize), _> =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard());
        assert!(decoded.is_err(), "{bad}");
    }
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

#[test]
fn decode_rejects_unknown_v2_variant_as_unsupported_version() {
    let mut payload = bincode::serde::encode_to_vec(2u16, bincode::config::standard()).unwrap();
    payload.extend(bincode::serde::encode_to_vec(15u32, bincode::config::standard()).unwrap());
    let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&payload);

    assert_eq!(
        decode_control(&frame).unwrap_err(),
        FrameError::UnsupportedVersion(2)
    );
}

#[test]
fn bulk_frame_is_header_then_exact_size_bytes() {
    let header = BulkHeader {
        path: p("/src/hello.txt"),
        checkout_id: "src".into(),
        want_hash: ContentHash::from_bytes([7; 32]),
        encoding: BulkEncoding::Whole,
        size: 5,
    };
    let frame = encode_bulk(&header, b"hello").unwrap();
    let (decoded, body, consumed) = decode_bulk(&frame).unwrap();
    assert_eq!(decoded, header);
    assert_eq!(body, b"hello");
    assert_eq!(consumed, frame.len());
}

#[test]
fn encode_bulk_rejects_a_body_that_does_not_match_size() {
    let header = BulkHeader {
        path: p("/src/hello.txt"),
        checkout_id: "src".into(),
        want_hash: ContentHash::from_bytes([7; 32]),
        encoding: BulkEncoding::Whole,
        size: 5,
    };
    assert_eq!(
        encode_bulk(&header, b"hi").unwrap_err(),
        FrameError::BodySize { got: 2, want: 5 }
    );
}

#[test]
fn decode_bulk_rejects_a_truncated_body() {
    let header = BulkHeader {
        path: p("/src/hello.txt"),
        checkout_id: "src".into(),
        want_hash: ContentHash::from_bytes([7; 32]),
        encoding: BulkEncoding::Whole,
        size: 5,
    };
    let mut frame = encode_bulk(&header, b"hello").unwrap();
    frame.pop();
    assert_eq!(decode_bulk(&frame).unwrap_err(), FrameError::Truncated);
}
