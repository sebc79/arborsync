use std::fs;
use std::path::Path;
use std::sync::Mutex;

use arborsync_core::path::PathError;
use arborsync_core::test_support::p;
use arborsync_core::{ConfigError, LoadedMaster, LoadedSlave};

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
    let loaded = LoadedMaster::load(&path).unwrap();
    let row = loaded.acl_for_public_key(&AA_BYTES).unwrap();
    assert_eq!(row.id(), "dev-alice");
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
