use std::process::Command;

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
