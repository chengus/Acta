//! The checked-in v0.2 compatibility fixtures must all classify correctly.

mod common;

use common::{FIXTURES, fixture, fixture_path};

#[test]
fn every_fixture_validates() {
    for (relative_path, _) in FIXTURES {
        acta::validate(fixture_path(relative_path))
            .unwrap_or_else(|error| panic!("fixture {relative_path} failed validation: {error}"));
    }
}

#[test]
fn every_fixture_declares_format_version_0_2() {
    for (relative_path, _) in FIXTURES {
        let report = acta::validate(fixture_path(relative_path)).unwrap();
        assert_eq!(report.format_version(), (0, 2), "{relative_path}");
    }
}

#[test]
fn every_fixture_declares_its_documented_feature_flags() {
    for (relative_path, feature_flags) in FIXTURES {
        let report = acta::validate(fixture_path(relative_path)).unwrap();
        assert_eq!(report.feature_flags(), *feature_flags, "{relative_path}");
    }
}

#[test]
fn every_fixture_holds_a_schema_frame_and_one_data_frame() {
    for (relative_path, _) in FIXTURES {
        let report = acta::validate(fixture_path(relative_path)).unwrap();
        assert_eq!(report.frame_count(), 2, "{relative_path}");
    }
}

#[test]
fn every_fixture_is_valid_through_its_final_byte() {
    for (relative_path, _) in FIXTURES {
        let report = acta::validate(fixture_path(relative_path)).unwrap();
        let length = fixture(relative_path).len() as u64;
        assert_eq!(report.last_good_offset(), length, "{relative_path}");
        assert_eq!(report.file_size(), length, "{relative_path}");
    }
}

#[test]
fn no_fixture_reports_an_incomplete_tail() {
    for (relative_path, _) in FIXTURES {
        let report = acta::validate(fixture_path(relative_path)).unwrap();
        assert!(!report.incomplete_tail(), "{relative_path}");
    }
}
