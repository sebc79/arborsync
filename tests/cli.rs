use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_arborsync"))
}

#[test]
fn help_lists_master_slave_and_keygen() {
    let output = bin().arg("--help").output().expect("run");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("master"));
    assert!(stdout.contains("slave"));
    assert!(stdout.contains("keygen"));
}

#[test]
fn keygen_help_mentions_out() {
    let output = bin().args(["keygen", "--help"]).output().expect("run");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--out"));
}

#[test]
fn keygen_without_out_fails() {
    let output = bin().arg("keygen").output().expect("run");
    assert!(!output.status.success());
}

#[test]
fn keygen_out_writes_32_bytes_and_prints_hex_public() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("arborsync-keygen-{unique}"));
    fs::create_dir_all(&dir).unwrap();
    let key = dir.join("k");
    let output = bin()
        .args(["keygen", "--out"])
        .arg(&key)
        .output()
        .expect("run");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let printed = stdout.trim();
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(printed.starts_with("hex:"));
    assert_eq!(printed.len(), 4 + 64);
    assert!(printed.as_bytes()[4..].iter().all(u8::is_ascii_hexdigit));
    assert_eq!(fs::read(&key).unwrap().len(), 32);
    let _ = fs::remove_dir_all(&dir);
}
