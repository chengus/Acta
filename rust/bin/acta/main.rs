//! The `acta` command-line tool.
//!
//! Argument handling is deliberately hand-written. One command with one
//! argument does not justify a parsing framework, and the command keeps its
//! argument handling small and explicit.

mod head;
mod inspect;

use std::path::Path;
use std::process::ExitCode;

const USAGE: &str = "\
Usage:
  acta inspect <file>   Print the schema and block metadata of an Acta v0.2 file
  acta head <file>      Print the first logical rows of an Acta v0.2 file

Options:
  -n, --rows <count>    Number of logical rows for head (default: 10)
  -h, --help            Print this message
  -V, --version         Print the version of this crate
";

/// Distinguishes a wrong command line from a file this crate cannot read.
const USAGE_ERROR: u8 = 2;

fn main() -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("inspect") => inspect_command(arguments),
        Some("head") => head_command(arguments),
        Some("-h" | "--help") => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        Some("-V" | "--version") => {
            println!("acta {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Some(command) => usage_error(&format!("unknown command `{command}`")),
        None => usage_error("no command given"),
    }
}

fn inspect_command(mut arguments: impl Iterator<Item = String>) -> ExitCode {
    let Some(path) = arguments.next() else {
        return usage_error("inspect needs the path of an Acta file");
    };
    if let Some(unexpected) = arguments.next() {
        return usage_error(&format!("inspect takes one path, found `{unexpected}`"));
    }

    match inspect::inspect_path(Path::new(&path)) {
        Ok(report) => {
            print!("{report}");
            ExitCode::SUCCESS
        }
        // The error already names its kind, so an unsupported version reads
        // differently from corruption without the caller decoding a status.
        Err(error) => {
            eprintln!("acta: {path}: {error}");
            ExitCode::FAILURE
        }
    }
}

fn head_command(mut arguments: impl Iterator<Item = String>) -> ExitCode {
    let mut rows = 10_usize;
    let mut seen_rows = false;
    let mut path = None;
    while let Some(argument) = arguments.next() {
        if argument == "-n" || argument == "--rows" {
            if seen_rows {
                return usage_error("head accepts only one row-count option");
            }
            let Some(value) = arguments.next() else {
                return usage_error("head row count is missing");
            };
            rows = match value.parse() {
                Ok(rows) => rows,
                Err(_) => return usage_error("head row count must be a nonnegative integer"),
            };
            seen_rows = true;
        } else if argument == "-h" || argument == "--help" {
            print!("{USAGE}");
            return ExitCode::SUCCESS;
        } else if argument.starts_with('-') {
            return usage_error("head found an unexpected option");
        } else if path.is_some() {
            return usage_error("head accepts only one path");
        } else {
            path = Some(argument);
        }
    }
    let Some(path) = path else {
        return usage_error("head needs the path of an Acta file");
    };

    match head::head_path(Path::new(&path), rows) {
        Ok(output) => {
            print!("{output}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("acta: {path}: {error}");
            ExitCode::FAILURE
        }
    }
}

fn usage_error(message: &str) -> ExitCode {
    eprintln!("acta: {message}");
    eprint!("{USAGE}");
    ExitCode::from(USAGE_ERROR)
}
