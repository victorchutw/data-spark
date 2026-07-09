//! Thin binary entry point. All load logic lives in the `data_spark` library
//! target so it can be unit-tested in process; see ADR-0028.

fn main() -> std::process::ExitCode {
    data_spark::run()
}
