//! Small subprocess checks for command-line argument handling.

mod common;

use std::process::Command;

use common::{TemporaryFile, fixture_path, reference_fixture};

#[test]
fn head_cli_prints_requested_rows() {
    let output = Command::new(env!("CARGO_BIN_EXE_acta"))
        .args([
            "head",
            fixture_path("minimal/minimal.acta")
                .to_str()
                .expect("UTF-8 path"),
            "-n",
            "1",
        ])
        .output()
        .expect("acta command should run");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("UTF-8 output"),
        "time\n1000000us\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn head_cli_accepts_rows_before_the_path() {
    let output = Command::new(env!("CARGO_BIN_EXE_acta"))
        .args([
            "head",
            "--rows",
            "1",
            fixture_path("minimal/minimal.acta")
                .to_str()
                .expect("UTF-8 path"),
        ])
        .output()
        .expect("acta command should run");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("UTF-8 output"),
        "time\n1000000us\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn head_cli_returns_nonzero_for_a_bad_file() {
    let mut bytes = reference_fixture();
    bytes[0] ^= 1;
    let file = TemporaryFile::new("cli-corrupt", &bytes);
    let output = Command::new(env!("CARGO_BIN_EXE_acta"))
        .args(["head", file.path().to_str().expect("UTF-8 path")])
        .output()
        .expect("acta command should run");

    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .expect("UTF-8 error")
            .contains("corruption")
    );
}
