use std::fs;
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arborsync_core::test_support::SyncSandbox;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_arborsync"))
}

fn master_sandbox() -> (SyncSandbox, std::path::PathBuf) {
    let sandbox = SyncSandbox::new();
    let key = sandbox.path().join("master/master.key");
    let keygen = bin().args(["keygen", "--out"]).arg(&key).output().unwrap();
    assert!(keygen.status.success());
    let config = sandbox.write_master_config(Vec::new());
    (sandbox, config)
}

fn keygen_stdout(out: &std::path::Path) -> String {
    let output = bin().args(["keygen", "--out"]).arg(out).output().unwrap();
    assert!(
        output.status.success(),
        "keygen --out {}: {}",
        out.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("keygen stdout")
        .trim()
        .to_string()
}

fn stderr_after_kill(mut child: std::process::Child) -> String {
    sleep(Duration::from_millis(1500));
    if let Some(status) = child.try_wait().unwrap() {
        let output = child.wait_with_output().unwrap();
        panic!(
            "daemon exited with {status}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    child.kill().unwrap();
    let output = child.wait_with_output().unwrap();
    String::from_utf8_lossy(&output.stderr).into_owned()
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
fn recompute_rewrites_an_empty_master_index_then_matches() {
    let (_sandbox, config) = master_sandbox();
    let first = bin()
        .args(["recompute", "--config"])
        .arg(&config)
        .output()
        .expect("run");
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(
        String::from_utf8(first.stdout).expect("stdout").trim(),
        "recomputed directory hashes"
    );
    let second = bin()
        .args(["recompute", "--config"])
        .arg(&config)
        .output()
        .expect("run");
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(
        String::from_utf8(second.stdout).expect("stdout").trim(),
        "directory hashes already match"
    );
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

#[test]
fn master_keeps_running_on_a_valid_config() {
    let (sandbox, config) = master_sandbox();
    let mut child = bin()
        .arg("master")
        .arg("--config")
        .arg(&config)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn master");

    sleep(Duration::from_millis(1500));
    if let Some(status) = child.try_wait().unwrap() {
        let output = child.wait_with_output().unwrap();
        panic!(
            "master exited with {status}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    child.kill().unwrap();
    child.wait().unwrap();
    drop(sandbox);
}

#[test]
fn master_reads_the_config_path_from_the_environment() {
    let (sandbox, config) = master_sandbox();
    let mut child = bin()
        .arg("master")
        .env("ARBORSYNC_CONFIG", &config)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn master");

    sleep(Duration::from_millis(1500));
    if let Some(status) = child.try_wait().unwrap() {
        let output = child.wait_with_output().unwrap();
        panic!(
            "master exited with {status}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    child.kill().unwrap();
    child.wait().unwrap();
    drop(sandbox);
}

#[test]
fn slave_reports_the_missing_key_path_it_could_not_read() {
    let sandbox = SyncSandbox::new();
    let local = sandbox.add_checkout("dev-alice", "src");
    let config = sandbox.write_slave_config(
        "dev-alice",
        vec![arborsync_core::config::CheckoutConfig {
            id: "src".into(),
            central: "/src".into(),
            local: local.to_string_lossy().into_owned(),
        }],
        vec!["hex:".to_string() + &"11".repeat(32)],
    );
    let output = bin()
        .arg("slave")
        .arg("--config")
        .arg(&config)
        .output()
        .expect("run slave");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("slave.key"), "stderr={stderr}");
}

#[test]
fn slave_keeps_running_while_the_master_is_down() {
    let sandbox = SyncSandbox::new();
    let local = sandbox.add_checkout("dev-alice", "src");
    let key = sandbox.slave_root("dev-alice").join("slave.key");
    let keygen = bin().args(["keygen", "--out"]).arg(&key).output().unwrap();
    assert!(keygen.status.success());
    let config = sandbox.write_slave_config(
        "dev-alice",
        vec![arborsync_core::config::CheckoutConfig {
            id: "src".into(),
            central: "/src".into(),
            local: local.to_string_lossy().into_owned(),
        }],
        vec!["hex:".to_string() + &"11".repeat(32)],
    );
    let mut child = bin()
        .arg("slave")
        .arg("--config")
        .arg(&config)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn slave");

    sleep(Duration::from_millis(1500));
    if let Some(status) = child.try_wait().unwrap() {
        let output = child.wait_with_output().unwrap();
        panic!(
            "slave exited with {status}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    child.kill().unwrap();
    child.wait().unwrap();
    drop(sandbox);
}

#[test]
fn master_reports_the_missing_key_path_it_could_not_read() {
    let sandbox = SyncSandbox::new();
    let config = sandbox.write_master_config(Vec::new());
    let output = bin()
        .arg("master")
        .arg("--config")
        .arg(&config)
        .output()
        .expect("run master");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("master.key"), "stderr={stderr}");
}

#[test]
fn master_logs_the_public_key_keygen_printed() {
    let sandbox = SyncSandbox::new();
    let key = sandbox.path().join("master/master.key");
    let pin = keygen_stdout(&key);
    let config = sandbox.write_master_config(Vec::new());
    let child = bin()
        .arg("master")
        .arg("--config")
        .arg(&config)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn master");

    let stderr = stderr_after_kill(child);
    assert!(stderr.contains(&pin), "stderr={stderr}");
    drop(sandbox);
}

#[test]
fn slave_logs_the_public_key_keygen_printed() {
    let sandbox = SyncSandbox::new();
    let local = sandbox.add_checkout("dev-alice", "src");
    let key = sandbox.slave_root("dev-alice").join("slave.key");
    let pin = keygen_stdout(&key);
    let config = sandbox.write_slave_config(
        "dev-alice",
        vec![arborsync_core::config::CheckoutConfig {
            id: "src".into(),
            central: "/src".into(),
            local: local.to_string_lossy().into_owned(),
        }],
        vec!["hex:".to_string() + &"11".repeat(32)],
    );
    let child = bin()
        .arg("slave")
        .arg("--config")
        .arg(&config)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn slave");

    let stderr = stderr_after_kill(child);
    assert!(stderr.contains(&pin), "stderr={stderr}");
    drop(sandbox);
}
