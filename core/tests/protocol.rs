use arborsync_core::hash::{ContentHash, DirNode, FileNode};
use arborsync_core::meta::FileMetadata;
use arborsync_core::path::{CanonicalPath, EntryName};
use arborsync_core::protocol::{
    BulkEncoding, BulkHeader, DirEntry, Envelope, FrameError, MAX_CONTROL_FRAME, PROTOCOL_PREAMBLE,
    PROTOCOL_VERSION, ProtocolMessage, decode_bulk, decode_control, encode_bulk, encode_bulk_chunks,
    encode_control,
    page_dir_list,
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

fn wide_dir_entries(n: usize) -> Vec<DirEntry> {
    (0..n)
        .map(|i| DirEntry::File {
            name: name(&format!("f{i:05}")),
            node: FileNode::from_bytes([1; 32]),
        })
        .collect()
}

fn wide_dir_list(n: usize) -> ProtocolMessage {
    ProtocolMessage::DirListResponse {
        checkout_id: "src".into(),
        path: p("/src"),
        after: None,
        entries: wide_dir_entries(n),
        more: false,
    }
}

#[test]
fn dir_list_response_for_long_names_fits_a_control_frame() {
    let pad = "x".repeat(236);
    let all: Vec<DirEntry> = (0..4000)
        .map(|i| DirEntry::File {
            name: name(&format!("f{i:04}{pad}")),
            node: FileNode::from_bytes([1; 32]),
        })
        .collect();
    assert_eq!(
        encode_control(&ProtocolMessage::DirListResponse {
            checkout_id: "src".into(),
            path: p("/src"),
            after: None,
            entries: all.clone(),
            more: false,
        })
        .unwrap_err(),
        FrameError::TooLarge
    );
    let page = page_dir_list("src".into(), p("/src"), None, all);
    encode_control(&page).expect("first long-name page must fit");
    match page {
        ProtocolMessage::DirListResponse { more, entries, .. } => {
            assert!(more, "4000 long names must need another page");
            assert!(!entries.is_empty());
        }
        other => panic!("expected DirListResponse, got {other:?}"),
    }
}

#[test]
fn dir_list_response_for_a_wide_directory_fits_a_control_frame() {
    assert_eq!(
        encode_control(&wide_dir_list(50_000)).unwrap_err(),
        FrameError::TooLarge
    );

    let mut after = None;
    let mut seen = Vec::new();
    let all = wide_dir_entries(50_000);
    loop {
        let page = page_dir_list("src".into(), p("/src"), after.clone(), all.clone());
        let ProtocolMessage::DirListResponse {
            entries,
            more,
            after: echoed,
            ..
        } = page
        else {
            panic!("expected DirListResponse, got {page:?}");
        };
        assert_eq!(echoed, after);
        encode_control(&ProtocolMessage::DirListResponse {
            checkout_id: "src".into(),
            path: p("/src"),
            after: echoed.clone(),
            entries: entries.clone(),
            more,
        })
        .expect("each DirList page must fit a control frame");
        assert!(!entries.is_empty());
        seen.extend(
            entries
                .iter()
                .map(|entry| entry.name().as_str().to_string()),
        );
        if !more {
            break;
        }
        after = Some(entries.last().unwrap().name().clone());
    }
    let expected: Vec<String> = all
        .iter()
        .map(|entry| entry.name().as_str().to_string())
        .collect();
    assert_eq!(seen, expected);
}

#[test]
fn dir_list_response_round_trips_each_child_brand() {
    let msg = ProtocolMessage::DirListResponse {
        checkout_id: "src".into(),
        path: p("/src"),
        after: None,
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
        more: false,
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
fn signature_request_for_an_80mib_basis_fits_a_control_frame() {
    use arborsync_core::meta::EntryKind;
    use arborsync_core::transfer::signature_request;

    let bytes = vec![b'B'; 80 << 20];
    let msg = signature_request(
        "src",
        p("/src/big.bin"),
        ContentHash::from_bytes([2; 32]),
        EntryKind::File,
        Some(&bytes),
    );
    let frame = encode_control(&msg).expect("80 MiB basis must still yield a sendable frame");
    assert!(
        frame.len() - 4 <= MAX_CONTROL_FRAME,
        "frame {} exceeds cap",
        frame.len()
    );
}

#[test]
fn signature_request_for_a_10mib_basis_still_asks_for_a_delta() {
    use arborsync_core::meta::EntryKind;
    use arborsync_core::transfer::signature_request;

    let bytes = vec![0u8; 10 << 20];
    let msg = signature_request(
        "src",
        p("/src/mid.bin"),
        ContentHash::from_bytes([3; 32]),
        EntryKind::File,
        Some(&bytes),
    );
    match &msg {
        ProtocolMessage::SignatureRequest { signature, .. } => {
            assert!(
                !signature.is_empty(),
                "a 10 MiB basis must keep a copia signature"
            );
        }
        other => panic!("expected SignatureRequest, got {other:?}"),
    }
    encode_control(&msg).expect("10 MiB signature must fit");
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

#[test]
fn decode_bulk_rejects_a_chunk_larger_than_the_cap() {
    let header = BulkHeader {
        path: p("/src/hello.txt"),
        checkout_id: "src".into(),
        want_hash: ContentHash::from_bytes([7; 32]),
        encoding: BulkEncoding::Whole,
        size: 4,
    };
    let payload =
        bincode::serde::encode_to_vec(&header, bincode::config::standard()).expect("header");
    let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
    frame.extend(payload);
    frame.extend_from_slice(&((16 * 1024 * 1024 + 1) as u32).to_be_bytes());
    assert_eq!(decode_bulk(&frame).unwrap_err(), FrameError::BulkTooLarge);
}

#[test]
fn decode_bulk_accepts_a_logical_body_past_1_gib_when_the_chunk_is_short() {
    let header = BulkHeader {
        path: p("/src/hello.txt"),
        checkout_id: "src".into(),
        want_hash: ContentHash::from_bytes([7; 32]),
        encoding: BulkEncoding::Whole,
        size: (1024 * 1024 * 1024) + 1,
    };
    let payload =
        bincode::serde::encode_to_vec(&header, bincode::config::standard()).expect("header");
    let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
    frame.extend(payload);
    frame.extend_from_slice(&4u32.to_be_bytes());
    frame.extend_from_slice(&[1, 2, 3, 4]);
    assert_eq!(decode_bulk(&frame).unwrap_err(), FrameError::Truncated);
}

#[test]
fn bulk_chunks_reassemble_to_the_original_bytes() {
    let body = b"abcdefghij";
    let header = BulkHeader {
        path: p("/src/hello.txt"),
        checkout_id: "src".into(),
        want_hash: ContentHash::from_bytes([7; 32]),
        encoding: BulkEncoding::Whole,
        size: body.len() as u64,
    };
    let frame = encode_bulk_chunks(&header, body, 4).unwrap();
    let (decoded, got, consumed) = decode_bulk(&frame).unwrap();
    assert_eq!(decoded, header);
    assert_eq!(got, body);
    assert_eq!(consumed, frame.len());
    assert!(frame.len() > 4 + body.len());
}
