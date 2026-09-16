use std::fs;

use arborsync_core::{KeyError, format_hex_key, parse_hex_key, read_static_key, write_static_key};

const AB_HEX: &str = "hex:abababababababababababababababababababababababababababababababab";

#[test]
fn format_hex_key_is_hex_prefix_and_64_lowercase_ab() {
    assert_eq!(format_hex_key(&[0xab; 32]), AB_HEX);
}

#[test]
fn parse_hex_key_round_trips_the_ab_literal() {
    assert_eq!(parse_hex_key(AB_HEX).unwrap(), [0xab; 32]);
}

#[test]
fn parse_hex_key_accepts_uppercase() {
    assert_eq!(
        parse_hex_key("hex:ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB")
            .unwrap(),
        [0xab; 32]
    );
}

#[test]
fn parse_hex_key_missing_prefix_fails() {
    assert!(parse_hex_key(&"ab".repeat(32)).is_err());
}

#[test]
fn write_static_key_is_32_bytes_mode_0600_and_second_call_already_exists() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k");

    let public = write_static_key(&path).unwrap();
    let raw = fs::read(&path).unwrap();
    assert_eq!(raw.len(), 32);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    assert_eq!(parse_hex_key(&format_hex_key(&public)).unwrap(), public);

    let err = write_static_key(&path).unwrap_err();
    assert!(matches!(err, KeyError::AlreadyExists { .. }));
}

#[test]
fn read_static_key_accepts_raw_bytes_and_hex_form() {
    let dir = tempfile::tempdir().unwrap();
    let raw_path = dir.path().join("raw.key");
    write_static_key(&raw_path).unwrap();
    let secret = read_static_key(&raw_path).unwrap();
    let raw: [u8; 32] = fs::read(&raw_path).unwrap().try_into().unwrap();
    assert_eq!(secret, raw);

    let hex_path = dir.path().join("hex.key");
    fs::write(&hex_path, format_hex_key(&secret)).unwrap();
    assert_eq!(read_static_key(&hex_path).unwrap(), secret);
}
