use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arborsync_core::config::{CheckoutConfig, MasterConfig, SlaveAcl, SlaveConfig};
use arborsync_core::test_support::SyncSandbox;
use arborsync_core::tune::TuneSpec;

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
    assert!(stdout.contains("path"));
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

fn write_toml(path: &Path, text: &str) {
    fs::write(path, text).expect("write toml");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
}

fn spawn_logged(args: &[&str], log: &Path) -> Child {
    let stderr = File::create(log).unwrap_or_else(|err| panic!("create {}: {err}", log.display()));
    bin()
        .args(args)
        .env("ARBORSYNC_LOG_LEVEL", "info")
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .unwrap_or_else(|err| panic!("spawn {}: {err}", args.join(" ")))
}

fn wait_log(child: &mut Child, log: &Path, needle: &str, timeout: Duration) -> String {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!(
                "process exited {status} before {needle:?}\n{}",
                fs::read_to_string(log).unwrap_or_default()
            );
        }
        let text = fs::read_to_string(log).unwrap_or_default();
        if text.contains(needle) {
            return text;
        }
        if start.elapsed() > timeout {
            panic!(
                "timed out waiting for {needle:?} in {}\n{text}",
                log.display()
            );
        }
        sleep(Duration::from_millis(50));
    }
}

fn listen_addr(log: &str) -> String {
    for line in log.lines() {
        if let Some((_, addr)) = line.split_once("listening on ") {
            return addr.trim().to_string();
        }
    }
    panic!("no listen address in master log\n{log}");
}

fn sighup(child: &Child) {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    let rc = unsafe { kill(child.id() as i32, 1) };
    assert_eq!(rc, 0, "SIGHUP");
}

struct KillOnDrop(Option<Child>);

impl KillOnDrop {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn inner(&mut self) -> &mut Child {
        self.0.as_mut().expect("child")
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct ConnectedPair {
    _sandbox: SyncSandbox,
    master: KillOnDrop,
    slave: KillOnDrop,
    master_log: PathBuf,
    slave_log: PathBuf,
    master_cfg_path: PathBuf,
    slave_cfg_path: PathBuf,
    master_cfg: MasterConfig,
    slave_cfg: SlaveConfig,
}

impl ConnectedPair {
    fn start() -> Self {
        let sandbox = SyncSandbox::new();
        let local = sandbox.add_checkout("dev-alice", "src");
        let master_key = sandbox.path().join("master/master.key");
        let slave_key = sandbox.slave_root("dev-alice").join("slave.key");
        let master_pub = keygen_stdout(&master_key);
        let slave_pub = keygen_stdout(&slave_key);
        fs::create_dir_all(sandbox.central_root().join("src")).expect("central src");

        let master_cfg = MasterConfig {
            central_root: sandbox.central_root().to_string_lossy().into_owned(),
            listen_addr: "127.0.0.1:0".into(),
            master_key_path: master_key.to_string_lossy().into_owned(),
            db_path: sandbox.master_db().to_string_lossy().into_owned(),
            log_level: "info".into(),
            watcher_debounce_ms: 200,
            rescan_interval_seconds: 3600,
            status_interval_seconds: 0,
            max_checkouts_per_slave: 100,
            max_connections: 100,
            max_connection_attempts_per_minute: 60,
            slaves: vec![SlaveAcl {
                id: "dev-alice".into(),
                public_keys: vec![slave_pub],
                allowed_prefixes: vec!["/src".into()],
            }],
            tune: TuneSpec::default(),
        };
        let master_cfg_path = sandbox.master_config_path();
        write_toml(
            &master_cfg_path,
            &master_cfg.to_toml().expect("master toml"),
        );
        let master_log = sandbox.path().join("master.log");
        let mut master = KillOnDrop::new(spawn_logged(
            &[
                "master",
                "--config",
                master_cfg_path.to_str().expect("utf8"),
            ],
            &master_log,
        ));
        let master_text = wait_log(
            master.inner(),
            &master_log,
            "listening on ",
            Duration::from_secs(10),
        );

        let slave_cfg = SlaveConfig {
            slave_id: "dev-alice".into(),
            master_addr: listen_addr(&master_text),
            slave_key_path: slave_key.to_string_lossy().into_owned(),
            master_public_keys: vec![master_pub],
            db_path: sandbox.slave_db("dev-alice").to_string_lossy().into_owned(),
            log_level: "info".into(),
            max_checkouts_per_slave: 100,
            watcher_debounce_ms: 200,
            rescan_interval_seconds: 3600,
            status_interval_seconds: 0,
            peer_socket: None,
            checkouts: vec![CheckoutConfig {
                id: "src".into(),
                central: "/src".into(),
                local: local.to_string_lossy().into_owned(),
            }],
            tune: TuneSpec::default(),
        };
        let slave_cfg_path = sandbox.slave_root("dev-alice").join("slave.toml");
        write_toml(&slave_cfg_path, &slave_cfg.to_toml().expect("slave toml"));
        let slave_log = sandbox.path().join("slave.log");
        let mut slave = KillOnDrop::new(spawn_logged(
            &["slave", "--config", slave_cfg_path.to_str().expect("utf8")],
            &slave_log,
        ));
        wait_log(
            slave.inner(),
            &slave_log,
            "connected to ",
            Duration::from_secs(10),
        );

        Self {
            _sandbox: sandbox,
            master,
            slave,
            master_log,
            slave_log,
            master_cfg_path,
            slave_cfg_path,
            master_cfg,
            slave_cfg,
        }
    }
}

#[test]
fn sighup_closes_a_slave_removed_from_the_acl() {
    let mut pair = ConnectedPair::start();
    pair.master_cfg.slaves.clear();
    write_toml(
        &pair.master_cfg_path,
        &pair.master_cfg.to_toml().expect("master toml"),
    );
    sighup(pair.master.inner());
    wait_log(
        pair.master.inner(),
        &pair.master_log,
        "acl reload closed dev-alice",
        Duration::from_secs(10),
    );
    wait_log(
        pair.master.inner(),
        &pair.master_log,
        "unknown static key from ",
        Duration::from_secs(15),
    );
}

#[test]
fn config_watch_resubscribes_after_a_checkout_is_added() {
    let mut pair = ConnectedPair::start();
    let docs = pair._sandbox.add_checkout("dev-alice", "docs");
    pair.slave_cfg.checkouts.push(CheckoutConfig {
        id: "docs".into(),
        central: "/docs".into(),
        local: docs.to_string_lossy().into_owned(),
    });
    write_toml(
        &pair.slave_cfg_path,
        &pair.slave_cfg.to_toml().expect("slave toml"),
    );
    wait_log(
        pair.slave.inner(),
        &pair.slave_log,
        "resubscribe after reload",
        Duration::from_secs(10),
    );
}

fn path_stdout(config: &Path, arg: &str) -> String {
    let output = bin()
        .args(["path", "--config"])
        .arg(config)
        .arg(arg)
        .output()
        .expect("run path");
    assert!(
        output.status.success(),
        "path {arg}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("path stdout")
}

#[test]
fn path_dump_shows_a_master_file_on_disk_and_in_the_index() {
    let (sandbox, config) = master_sandbox();
    let host = sandbox.central_root().join("src/hello.txt");
    fs::create_dir_all(host.parent().unwrap()).unwrap();
    fs::write(&host, b"hello").unwrap();
    let child = bin()
        .args(["master", "--config"])
        .arg(&config)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn master");
    let _ = stderr_after_kill(child);

    let dumped = path_stdout(&config, host.to_str().unwrap());
    assert!(dumped.contains("role=master\n"), "{dumped}");
    assert!(dumped.contains("snapshot=live\n"), "{dumped}");
    assert!(dumped.contains("checkout=-\n"), "{dumped}");
    assert!(dumped.contains("central=/src/hello.txt\n"), "{dumped}");
    assert!(dumped.contains("disk=file "), "{dumped}");
    assert!(dumped.contains("index=file "), "{dumped}");
    assert!(dumped.contains("disk_index=match\n"), "{dumped}");

    let by_canonical = path_stdout(&config, "/src/hello.txt");
    assert!(
        by_canonical.contains(&format!("host={}\n", host.display())),
        "{by_canonical}"
    );

    let missing = path_stdout(&config, "/src/missing.txt");
    assert!(missing.contains("disk=absent\n"), "{missing}");
    assert!(missing.contains("index=absent\n"), "{missing}");
    assert!(missing.contains("disk_index=both_absent\n"), "{missing}");

    let outside = bin()
        .args(["path", "--config"])
        .arg(&config)
        .arg("not-a-canonical-path")
        .output()
        .expect("run path");
    assert!(!outside.status.success());
}

#[test]
fn path_dump_copies_the_index_while_the_master_holds_it() {
    let (sandbox, config) = master_sandbox();
    let host = sandbox.central_root().join("src/hello.txt");
    fs::create_dir_all(host.parent().unwrap()).unwrap();
    fs::write(&host, b"hello").unwrap();
    let log = sandbox.path().join("master.log");
    let mut child = spawn_logged(&["master", "--config", config.to_str().unwrap()], &log);
    wait_log(&mut child, &log, "master watching", Duration::from_secs(10));
    let dumped = path_stdout(&config, host.to_str().unwrap());
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(dumped.contains("snapshot=copy\n"), "{dumped}");
    assert!(dumped.contains("disk_index=match\n"), "{dumped}");
    assert!(dumped.contains("central=/src/hello.txt\n"), "{dumped}");
}

#[test]
fn path_dump_shows_a_slave_checkout_file() {
    let sandbox = SyncSandbox::new();
    let local = sandbox.add_checkout("dev-alice", "src");
    let host = local.join("hello.txt");
    fs::write(&host, b"hello").unwrap();
    let key = sandbox.slave_root("dev-alice").join("slave.key");
    let keygen = bin().args(["keygen", "--out"]).arg(&key).output().unwrap();
    assert!(keygen.status.success());
    let config = sandbox.write_slave_config(
        "dev-alice",
        vec![CheckoutConfig {
            id: "src".into(),
            central: "/src".into(),
            local: local.to_string_lossy().into_owned(),
        }],
        vec!["hex:".to_string() + &"11".repeat(32)],
    );
    let child = bin()
        .args(["slave", "--config"])
        .arg(&config)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn slave");
    let _ = stderr_after_kill(child);

    let dumped = path_stdout(&config, host.to_str().unwrap());
    assert!(dumped.contains("role=slave\n"), "{dumped}");
    assert!(dumped.contains("checkout=src\n"), "{dumped}");
    assert!(dumped.contains("central=/src/hello.txt\n"), "{dumped}");
    assert!(dumped.contains("disk_index=match\n"), "{dumped}");

    let by_canonical = path_stdout(&config, "/src/hello.txt");
    assert!(by_canonical.contains("checkout=src\n"), "{by_canonical}");
    assert!(
        by_canonical.contains(&format!("host={}\n", host.display())),
        "{by_canonical}"
    );
}
