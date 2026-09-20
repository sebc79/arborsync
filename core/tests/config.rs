use std::fs;
use std::path::Path;
use std::sync::Mutex;

use arborsync_core::config::{MasterReload, ReloadError, SlaveReload, log_level_filter};
use arborsync_core::path::PathError;
use arborsync_core::test_support::p;
use arborsync_core::{ConfigError, LoadedMaster, LoadedSlave, WorkerCountSpec};

static HOME_LOCK: Mutex<()> = Mutex::new(());

const AA_PIN: &str = "hex:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BB_PIN: &str = "hex:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const AA_BYTES: [u8; 32] = [0xaa; 32];

fn master_toml(slaves: &str) -> String {
    format!(
        r#"
central_root = "/central"
listen_addr = "127.0.0.1:8443"
master_key_path = "/etc/arborsync/master.key"
db_path = "/var/lib/arborsync/index.redb"
log_level = "info"
watcher_debounce_ms = 200

{slaves}
"#
    )
}

fn valid_master() -> String {
    master_toml(&format!(
        r#"
[[slaves]]
id = "dev-alice"
public_keys = ["{AA_PIN}"]
allowed_prefixes = ["/src", "/docs"]
"#
    ))
}

fn slave_toml(body: &str) -> String {
    format!(
        r#"
slave_id = "dev-alice"
master_addr = "master.example.com:8443"
slave_key_path = "/etc/arborsync/slave.key"
master_public_keys = ["{BB_PIN}"]
db_path = "/var/cache/arborsync/cache.redb"
log_level = "info"
watcher_debounce_ms = 200
max_checkouts_per_slave = 100

{body}
"#
    )
}

#[test]
fn valid_master_parses_and_acl_lookup_hits_the_row() {
    let loaded = LoadedMaster::parse(&valid_master()).unwrap();
    let row = loaded.acl_for_public_key(&AA_BYTES).expect("acl row");
    assert_eq!(row.id(), "dev-alice");
    assert_eq!(row.public_keys(), &[AA_BYTES]);
    assert_eq!(row.allowed_prefixes(), &[p("/src"), p("/docs")]);
    assert!(row.allows_central(&p("/src/project1")));
    assert!(!row.allows_central(&p("/src2")));
}

#[test]
fn empty_slaves_parses() {
    let loaded = LoadedMaster::parse(&master_toml("")).unwrap();
    assert!(loaded.acl_for_public_key(&AA_BYTES).is_none());
}

#[test]
fn invalid_pin_hex_zz_fails() {
    let toml = master_toml(
        r#"
[[slaves]]
id = "dev-alice"
public_keys = ["hex:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"]
allowed_prefixes = ["/src"]
"#,
    );
    assert!(LoadedMaster::parse(&toml).is_err());
}

#[test]
fn invalid_pin_missing_hex_prefix_fails() {
    let toml = master_toml(
        r#"
[[slaves]]
id = "dev-alice"
public_keys = ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
allowed_prefixes = ["/src"]
"#,
    );
    assert!(LoadedMaster::parse(&toml).is_err());
}

#[test]
fn invalid_pin_wrong_length_fails() {
    let toml = master_toml(
        r#"
[[slaves]]
id = "dev-alice"
public_keys = ["hex:aaaa"]
allowed_prefixes = ["/src"]
"#,
    );
    assert!(LoadedMaster::parse(&toml).is_err());
}

#[test]
fn debounce_199_and_501_fail() {
    let low = valid_master().replace("watcher_debounce_ms = 200", "watcher_debounce_ms = 199");
    let high = valid_master().replace("watcher_debounce_ms = 200", "watcher_debounce_ms = 501");
    assert!(LoadedMaster::parse(&low).is_err());
    assert!(LoadedMaster::parse(&high).is_err());
}

#[test]
fn debounce_200_and_500_pass() {
    let high = valid_master().replace("watcher_debounce_ms = 200", "watcher_debounce_ms = 500");
    LoadedMaster::parse(&valid_master()).unwrap();
    LoadedMaster::parse(&high).unwrap();
}

#[test]
fn omitted_status_interval_defaults_to_5() {
    let master = LoadedMaster::parse(&valid_master()).unwrap();
    assert_eq!(master.status_interval_seconds(), 5);
    let slave = LoadedSlave::parse(&slave_toml("checkouts = []")).unwrap();
    assert_eq!(slave.status_interval_seconds(), 5);
}

#[test]
fn status_interval_0_is_ok() {
    let master = valid_master().replace(
        "watcher_debounce_ms = 200",
        "watcher_debounce_ms = 200\nstatus_interval_seconds = 0",
    );
    assert_eq!(
        LoadedMaster::parse(&master)
            .unwrap()
            .status_interval_seconds(),
        0
    );
    let slave = slave_toml("checkouts = []").replace(
        "watcher_debounce_ms = 200",
        "watcher_debounce_ms = 200\nstatus_interval_seconds = 0",
    );
    assert_eq!(
        LoadedSlave::parse(&slave)
            .unwrap()
            .status_interval_seconds(),
        0
    );
}

#[test]
fn status_interval_3601_is_out_of_range() {
    let master = valid_master().replace(
        "watcher_debounce_ms = 200",
        "watcher_debounce_ms = 200\nstatus_interval_seconds = 3601",
    );
    match LoadedMaster::parse(&master) {
        Err(ConfigError::StatusIntervalOutOfRange { value: 3601 }) => {}
        other => panic!("expected StatusIntervalOutOfRange, got {other:?}"),
    }
    let slave = slave_toml("checkouts = []").replace(
        "watcher_debounce_ms = 200",
        "watcher_debounce_ms = 200\nstatus_interval_seconds = 3601",
    );
    match LoadedSlave::parse(&slave) {
        Err(ConfigError::StatusIntervalOutOfRange { value: 3601 }) => {}
        other => panic!("expected StatusIntervalOutOfRange, got {other:?}"),
    }
}

#[test]
fn bad_log_level_fails() {
    let toml = valid_master().replace(r#"log_level = "info""#, r#"log_level = "verbose""#);
    assert!(LoadedMaster::parse(&toml).is_err());
}

#[test]
fn good_log_levels_pass() {
    for level in ["error", "warn", "info", "debug", "trace"] {
        let toml = valid_master().replace(
            r#"log_level = "info""#,
            &format!(r#"log_level = "{level}""#),
        );
        LoadedMaster::parse(&toml).expect(level);
    }
}

#[test]
fn relative_prefix_fails() {
    let toml = master_toml(&format!(
        r#"
[[slaves]]
id = "dev-alice"
public_keys = ["{AA_PIN}"]
allowed_prefixes = ["src"]
"#
    ));
    assert!(LoadedMaster::parse(&toml).is_err());
}

#[test]
fn parent_dot_prefix_is_an_error_not_a_rewrite() {
    let toml = master_toml(&format!(
        r#"
[[slaves]]
id = "dev-alice"
public_keys = ["{AA_PIN}"]
allowed_prefixes = ["/src/../etc"]
"#
    ));
    assert!(matches!(
        LoadedMaster::parse(&toml).unwrap_err(),
        ConfigError::BadPrefix {
            source: PathError::DotComponent(_),
            ..
        }
    ));
}

#[test]
fn doubled_slash_prefix_is_non_normalized() {
    let toml = master_toml(&format!(
        r#"
[[slaves]]
id = "dev-alice"
public_keys = ["{AA_PIN}"]
allowed_prefixes = ["//src"]
"#
    ));
    assert!(matches!(
        LoadedMaster::parse(&toml).unwrap_err(),
        ConfigError::NonNormalizedPrefix { .. }
    ));
}

#[test]
fn src2_sibling_and_src_are_valid_distinct_prefixes() {
    let toml = master_toml(&format!(
        r#"
[[slaves]]
id = "dev-alice"
public_keys = ["{AA_PIN}"]
allowed_prefixes = ["/src", "/src2"]
"#
    ));
    let loaded = LoadedMaster::parse(&toml).unwrap();
    let row = loaded.acl_for_public_key(&AA_BYTES).unwrap();
    assert_eq!(row.allowed_prefixes(), &[p("/src"), p("/src2")]);
}

#[test]
fn duplicate_acl_id_fails() {
    let toml = master_toml(&format!(
        r#"
[[slaves]]
id = "dev-alice"
public_keys = ["{AA_PIN}"]
allowed_prefixes = ["/src"]

[[slaves]]
id = "dev-alice"
public_keys = ["{BB_PIN}"]
allowed_prefixes = ["/docs"]
"#
    ));
    assert!(LoadedMaster::parse(&toml).is_err());
}

#[test]
fn same_public_key_on_two_acl_rows_fails() {
    let toml = master_toml(&format!(
        r#"
[[slaves]]
id = "dev-alice"
public_keys = ["{AA_PIN}"]
allowed_prefixes = ["/src"]

[[slaves]]
id = "backup-1"
public_keys = ["{AA_PIN}"]
allowed_prefixes = ["/"]
"#
    ));
    assert!(LoadedMaster::parse(&toml).is_err());
}

#[test]
fn empty_public_keys_fails() {
    let toml = master_toml(
        r#"
[[slaves]]
id = "dev-alice"
public_keys = []
allowed_prefixes = ["/src"]
"#,
    );
    assert!(LoadedMaster::parse(&toml).is_err());
}

#[test]
fn slave_id_with_space_fails() {
    let toml = slave_toml("checkouts = []")
        .replace(r#"slave_id = "dev-alice""#, r#"slave_id = "has space""#);
    assert!(LoadedSlave::parse(&toml).is_err());
}

#[test]
fn slave_id_dev_alice_passes() {
    LoadedSlave::parse(&slave_toml("checkouts = []")).unwrap();
}

#[test]
fn valid_slave_with_two_non_overlapping_locals_parses() {
    let toml = slave_toml(
        r#"
checkouts = [
    { id = "src",  central = "/src",  local = "/opt/a" },
    { id = "docs", central = "/docs", local = "/opt/c" },
]
"#,
    );
    let loaded = LoadedSlave::parse(&toml).unwrap();
    assert_eq!(loaded.checkouts().len(), 2);
    assert_eq!(loaded.checkouts()[0].local(), Path::new("/opt/a"));
    assert_eq!(loaded.checkouts()[1].local(), Path::new("/opt/c"));
}

#[test]
fn overlapping_locals_fail() {
    let toml = slave_toml(
        r#"
checkouts = [
    { id = "a", central = "/src", local = "/opt/a" },
    { id = "b", central = "/docs", local = "/opt/a/b" },
]
"#,
    );
    assert!(LoadedSlave::parse(&toml).is_err());
}

#[test]
fn identical_locals_fail() {
    let toml = slave_toml(
        r#"
checkouts = [
    { id = "a", central = "/src", local = "/opt/a" },
    { id = "b", central = "/docs", local = "/opt/a" },
]
"#,
    );
    assert!(LoadedSlave::parse(&toml).is_err());
}

#[test]
fn sibling_locals_opt_a_and_opt_ab_pass() {
    let toml = slave_toml(
        r#"
checkouts = [
    { id = "a", central = "/src", local = "/opt/a" },
    { id = "b", central = "/docs", local = "/opt/ab" },
]
"#,
    );
    LoadedSlave::parse(&toml).unwrap();
}

#[test]
fn central_overlap_src_and_root_pass() {
    let toml = slave_toml(
        r#"
checkouts = [
    { id = "src", central = "/src", local = "/opt/src" },
    { id = "bak", central = "/",    local = "/backup/central" },
]
"#,
    );
    LoadedSlave::parse(&toml).unwrap();
}

#[test]
fn duplicate_checkout_id_fails() {
    let toml = slave_toml(
        r#"
checkouts = [
    { id = "src", central = "/src",  local = "/opt/a" },
    { id = "src", central = "/docs", local = "/opt/b" },
]
"#,
    );
    assert!(LoadedSlave::parse(&toml).is_err());
}

#[test]
fn too_many_checkouts_fails() {
    let toml = slave_toml(
        r#"
checkouts = [
    { id = "a", central = "/src",  local = "/opt/a" },
    { id = "b", central = "/docs", local = "/opt/b" },
]
"#,
    )
    .replace(
        "max_checkouts_per_slave = 100",
        "max_checkouts_per_slave = 1",
    );
    assert!(LoadedSlave::parse(&toml).is_err());
}

#[test]
fn empty_checkouts_pass() {
    LoadedSlave::parse(&slave_toml("checkouts = []")).unwrap();
}

#[test]
fn load_reads_a_temp_file_parse_would_accept() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("master.toml");
    fs::write(&path, valid_master()).unwrap();
    chmod(&path, 0o600);
    let loaded = LoadedMaster::load(&path).unwrap();
    let row = loaded.acl_for_public_key(&AA_BYTES).unwrap();
    assert_eq!(row.id(), "dev-alice");
}

#[test]
fn load_rejects_0644_and_accepts_0600() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("master.toml");
    fs::write(&path, valid_master()).unwrap();
    chmod(&path, 0o644);
    match LoadedMaster::load(&path) {
        Err(ConfigError::InsecureMode { mode, .. }) => assert_eq!(mode, 0o644),
        other => panic!("expected InsecureMode, got {other:?}"),
    }
    chmod(&path, 0o600);
    LoadedMaster::load(&path).unwrap();
}

fn chmod(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

#[test]
fn owning_local_finds_the_checkout_for_a_host_path() {
    let toml = slave_toml(
        r#"
checkouts = [
    { id = "src", central = "/src", local = "/opt/src" },
    { id = "bak", central = "/",    local = "/backup/central" },
]
"#,
    );
    let loaded = LoadedSlave::parse(&toml).unwrap();
    assert_eq!(
        loaded
            .owning_local(Path::new("/opt/src/foo.rs"))
            .unwrap()
            .id()
            .as_str(),
        "src"
    );
    assert_eq!(
        loaded
            .owning_local(Path::new("/backup/central/src/foo.rs"))
            .unwrap()
            .id()
            .as_str(),
        "bak"
    );
    assert!(loaded.owning_local(Path::new("/opt/other")).is_none());
}

#[test]
fn tilde_host_paths_expand_from_home() {
    let _guard = HOME_LOCK.lock().unwrap();
    let home = tempfile::tempdir().unwrap();
    let previous_home = std::env::var_os("HOME");
    unsafe { std::env::set_var("HOME", home.path()) };

    let toml = r#"
central_root = "~"
listen_addr = "127.0.0.1:8443"
master_key_path = "~/master.key"
db_path = "/var/lib/arborsync/index.redb"
log_level = "info"
watcher_debounce_ms = 200
"#;
    let loaded = LoadedMaster::parse(toml).unwrap();
    assert_eq!(loaded.central_root(), home.path());
    assert_eq!(loaded.master_key_path(), home.path().join("master.key"));
    match previous_home {
        Some(value) => unsafe { std::env::set_var("HOME", value) },
        None => unsafe { std::env::remove_var("HOME") },
    }
}

fn src_checkout() -> &'static str {
    r#"
checkouts = [
    { id = "src", central = "/src", local = "/opt/src" },
]
"#
}

fn plan_master(current: &str, next: &str) -> Result<MasterReload, ReloadError> {
    LoadedMaster::parse(current)
        .unwrap()
        .plan_reload(&LoadedMaster::parse(next).unwrap())
}

fn plan_slave(current: &str, next: &str) -> Result<SlaveReload, ReloadError> {
    LoadedSlave::parse(current)
        .unwrap()
        .plan_reload(&LoadedSlave::parse(next).unwrap())
}

#[test]
fn master_reload_rejects_listen_addr_change() {
    let cases = [
        (
            "listen_addr",
            r#"listen_addr = "127.0.0.1:8443""#,
            r#"listen_addr = "127.0.0.1:9443""#,
        ),
        (
            "db_path",
            r#"db_path = "/var/lib/arborsync/index.redb""#,
            r#"db_path = "/var/lib/arborsync/other.redb""#,
        ),
        (
            "central_root",
            r#"central_root = "/central""#,
            r#"central_root = "/other""#,
        ),
        (
            "master_key_path",
            r#"master_key_path = "/etc/arborsync/master.key""#,
            r#"master_key_path = "/etc/arborsync/other.key""#,
        ),
    ];
    for (field, from, to) in cases {
        match plan_master(&valid_master(), &valid_master().replace(from, to)) {
            Err(ReloadError::RestartRequired { fields }) => {
                assert!(
                    fields.contains(&field.to_string()),
                    "{field} missing from {fields:?}"
                );
            }
            other => panic!("{field}: expected RestartRequired, got {other:?}"),
        }
    }
}

#[test]
fn master_reload_adds_and_removes_acl_rows() {
    let next = master_toml(&format!(
        r#"
[[slaves]]
id = "backup-1"
public_keys = ["{BB_PIN}"]
allowed_prefixes = ["/"]
"#
    ))
    .replace(r#"log_level = "info""#, r#"log_level = "debug""#);
    let plan = plan_master(&valid_master(), &next).unwrap();
    assert!(plan.drop_peers.is_empty());
    assert_eq!(plan.drop_slave_ids, vec!["dev-alice".to_string()]);
    assert_eq!(plan.log_level, "debug");
}

#[test]
fn slave_reload_rejects_master_addr_change() {
    match plan_slave(
        &slave_toml("checkouts = []"),
        &slave_toml("checkouts = []").replace("master.example.com:8443", "other.example.com:8443"),
    ) {
        Err(ReloadError::RestartRequired { fields }) => {
            assert!(fields.contains(&"master_addr".to_string()));
        }
        other => panic!("expected RestartRequired, got {other:?}"),
    }
}

#[test]
fn slave_reload_add_and_remove_checkouts() {
    let plan = plan_slave(
        &slave_toml(src_checkout()),
        &slave_toml(
            r#"
checkouts = [
    { id = "src",  central = "/src",  local = "/opt/src" },
    { id = "docs", central = "/docs", local = "/opt/docs" },
]
"#,
        ),
    )
    .unwrap();
    assert_eq!(plan.added, vec!["docs".to_string()]);
    assert!(plan.removed.is_empty());
    assert_eq!(plan.resubscribe, true);
}

#[test]
fn slave_reload_id_or_central_change_is_remove_and_add() {
    let plan = plan_slave(
        &slave_toml(src_checkout()),
        &slave_toml(
            r#"
checkouts = [
    { id = "src", central = "/docs", local = "/opt/src" },
]
"#,
        ),
    )
    .unwrap();
    assert_eq!(plan.removed, vec!["src".to_string()]);
    assert_eq!(plan.added, vec!["src".to_string()]);
    assert_eq!(plan.resubscribe, true);
}

#[test]
fn log_level_filter_maps_known_levels_and_rejects_verbose() {
    assert_eq!(log_level_filter("debug"), Some(log::LevelFilter::Debug));
    assert_eq!(log_level_filter("verbose"), None);
}

#[test]
fn slave_reload_log_only_does_not_resubscribe() {
    let plan = plan_slave(
        &slave_toml(src_checkout()),
        &slave_toml(src_checkout()).replace(r#"log_level = "info""#, r#"log_level = "debug""#),
    )
    .unwrap();
    assert!(plan.added.is_empty());
    assert!(plan.removed.is_empty());
    assert_eq!(plan.resubscribe, false);
    assert_eq!(plan.log_level, "debug");
}

fn resolved_nproc() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().min(256).max(1))
        .unwrap_or(1)
}

#[test]
fn omitted_tune_defaults_to_nproc_and_inflight_4() {
    let master = LoadedMaster::parse(&valid_master()).unwrap();
    assert_eq!(
        master.tune().hashing_workers_spec(),
        &WorkerCountSpec::Nproc
    );
    assert_eq!(master.tune().hashing_workers().get(), resolved_nproc());
    assert_eq!(master.tune().fulfill_cap(), None);

    let slave = LoadedSlave::parse(&slave_toml("checkouts = []")).unwrap();
    assert_eq!(slave.tune().hashing_workers_spec(), &WorkerCountSpec::Nproc);
    assert_eq!(slave.tune().hashing_workers().get(), resolved_nproc());
    assert_eq!(slave.tune().fulfill_cap().unwrap().get(), 4);
}

#[test]
fn nproc_string_resolves_via_available_parallelism() {
    let master = valid_master()
        + r#"
[tune.hashing]
workers = "nproc"
"#;
    let loaded = LoadedMaster::parse(&master).unwrap();
    assert_eq!(
        loaded.tune().hashing_workers_spec(),
        &WorkerCountSpec::Nproc
    );
    assert_eq!(loaded.tune().hashing_workers().get(), resolved_nproc());
}

#[test]
fn workers_0_is_out_of_range() {
    let master = valid_master()
        + r#"
[tune.hashing]
workers = 0
"#;
    match LoadedMaster::parse(&master) {
        Err(ConfigError::Toml { source, .. }) => {
            let text = source.to_string();
            assert!(
                text.contains("tune.hashing.workers 0"),
                "expected workers 0 reject, got {text}"
            );
        }
        other => panic!("expected Toml, got {other:?}"),
    }
    let slave = slave_toml("checkouts = []")
        + r#"
[tune.hashing]
workers = 0
"#;
    match LoadedSlave::parse(&slave) {
        Err(ConfigError::Toml { source, .. }) => {
            let text = source.to_string();
            assert!(
                text.contains("tune.hashing.workers 0"),
                "expected workers 0 reject, got {text}"
            );
        }
        other => panic!("expected Toml, got {other:?}"),
    }
}

#[test]
fn inflight_0_is_out_of_range() {
    let slave = slave_toml("checkouts = []")
        + r#"
[tune.fulfill_parked]
inflight = 0
"#;
    match LoadedSlave::parse(&slave) {
        Err(ConfigError::InflightOutOfRange { value: 0 }) => {}
        other => panic!("expected InflightOutOfRange, got {other:?}"),
    }
}

#[test]
fn tune_origin_bytes_is_unknown() {
    let master = valid_master()
        + r#"
[tune.origin_bytes]
"#;
    match LoadedMaster::parse(&master) {
        Err(ConfigError::Toml { source, .. }) => {
            assert!(
                source.to_string().contains("origin_bytes"),
                "expected unknown field origin_bytes, got {source}"
            );
        }
        other => panic!("expected Toml, got {other:?}"),
    }
}

#[test]
fn master_fulfill_parked_is_not_on_role() {
    let master = valid_master()
        + r#"
[tune.fulfill_parked]
inflight = 8
"#;
    match LoadedMaster::parse(&master) {
        Err(ConfigError::TuneNotOnRole {
            field,
            role: "master",
        }) => assert_eq!(field, "tune.fulfill_parked"),
        other => panic!("expected TuneNotOnRole, got {other:?}"),
    }
}
