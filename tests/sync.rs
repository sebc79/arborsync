use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use arborsync_core::config::{CheckoutConfig, MasterConfig, SlaveAcl, SlaveConfig};
use arborsync_core::test_support::SyncSandbox;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_arborsync"))
}

fn write_toml(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("config parent");
    }
    fs::write(path, text).expect("write toml");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
}

fn keygen(out: &Path) -> String {
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).expect("key parent");
    }
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

fn wait_bytes(path: &Path, expected: &[u8], timeout: Duration, logs: impl Fn() -> String) {
    let start = Instant::now();
    loop {
        if fs::read(path).ok().as_deref() == Some(expected) {
            return;
        }
        if start.elapsed() > timeout {
            panic!(
                "timed out waiting for {} to become {:?}\n{}",
                path.display(),
                expected,
                logs()
            );
        }
        sleep(Duration::from_millis(50));
    }
}

struct KillOnDrop(Option<Child>);

impl KillOnDrop {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn inner(&mut self) -> &mut Child {
        self.0.as_mut().expect("child")
    }

    fn into_inner(mut self) -> Child {
        self.0.take().expect("child")
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

struct DaemonPair {
    _sandbox: SyncSandbox,
    master: Child,
    slave: Child,
    master_log: PathBuf,
    slave_log: PathBuf,
    central_root: PathBuf,
    local: PathBuf,
}

impl DaemonPair {
    fn start() -> Self {
        Self::start_with_local(|_| {})
    }

    fn start_with_local(prefill: impl FnOnce(&Path)) -> Self {
        let sandbox = SyncSandbox::new();
        let local = sandbox.add_checkout("dev-alice", "src");
        let master_key = sandbox.path().join("master/master.key");
        let slave_key = sandbox.slave_root("dev-alice").join("slave.key");
        let master_pub = keygen(&master_key);
        let slave_pub = keygen(&slave_key);

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
        };
        fs::create_dir_all(sandbox.central_root().join("src")).expect("central src");
        let master_cfg_path = sandbox.master_config_path();
        write_toml(
            &master_cfg_path,
            &master_cfg.to_toml().expect("master toml"),
        );

        let master_log = sandbox.path().join("master.log");
        let slave_log = sandbox.path().join("slave.log");
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
        let master_addr = listen_addr(&master_text);

        let slave_cfg = SlaveConfig {
            slave_id: "dev-alice".into(),
            master_addr,
            slave_key_path: slave_key.to_string_lossy().into_owned(),
            master_public_keys: vec![master_pub],
            db_path: sandbox.slave_db("dev-alice").to_string_lossy().into_owned(),
            log_level: "info".into(),
            max_checkouts_per_slave: 100,
            watcher_debounce_ms: 200,
            rescan_interval_seconds: 3600,
            status_interval_seconds: 0,
            checkouts: vec![CheckoutConfig {
                id: "src".into(),
                central: "/src".into(),
                local: local.to_string_lossy().into_owned(),
            }],
        };
        let slave_cfg_path = sandbox.slave_root("dev-alice").join("slave.toml");
        write_toml(&slave_cfg_path, &slave_cfg.to_toml().expect("slave toml"));
        prefill(&local);

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
            central_root: sandbox.central_root(),
            _sandbox: sandbox,
            master: master.into_inner(),
            slave: slave.into_inner(),
            master_log,
            slave_log,
            local,
        }
    }

    fn log_tails(&self) -> String {
        format!(
            "master log:\n{}\nslave log:\n{}",
            fs::read_to_string(&self.master_log).unwrap_or_default(),
            fs::read_to_string(&self.slave_log).unwrap_or_default()
        )
    }

    fn kill_both(&mut self) {
        let _ = self.master.kill();
        let _ = self.slave.kill();
        let _ = self.master.wait();
        let _ = self.slave.wait();
    }
}

impl Drop for DaemonPair {
    fn drop(&mut self) {
        self.kill_both();
    }
}

#[test]
fn slave_write_after_connect_appears_on_master() {
    let mut pair = DaemonPair::start();
    fs::write(pair.local.join("hello.txt"), b"from-slave").expect("write slave hello.txt");
    wait_bytes(
        &pair.central_root.join("src/hello.txt"),
        b"from-slave",
        Duration::from_secs(20),
        || pair.log_tails(),
    );
    pair.kill_both();
}

#[test]
fn leftover_nested_drop_appears_on_master() {
    let mut pair = DaemonPair::start_with_local(|local| {
        for dir_n in 0..16 {
            let dir = local.join(format!("d{dir_n:02}"));
            fs::create_dir_all(&dir).expect("leftover dir");
            for file_n in 0..16 {
                fs::write(
                    dir.join(format!("f{file_n:02}.txt")),
                    format!("d{dir_n:02}-f{file_n:02}"),
                )
                .expect("leftover file");
            }
        }
    });
    wait_bytes(
        &pair.central_root.join("src/d15/f15.txt"),
        b"d15-f15",
        Duration::from_secs(60),
        || pair.log_tails(),
    );
    pair.kill_both();
}

#[test]
fn master_write_after_connect_appears_on_slave() {
    let mut pair = DaemonPair::start();
    let dest = pair.central_root.join("src/from-master.txt");
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).expect("central src");
    }
    fs::write(&dest, b"from-master").expect("write master from-master.txt");
    wait_bytes(
        &pair.local.join("from-master.txt"),
        b"from-master",
        Duration::from_secs(20),
        || pair.log_tails(),
    );
    pair.kill_both();
}
