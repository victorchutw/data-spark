//! Thin binary entry point. All load logic lives in the `data_spark` library
//! target so it can be unit-tested in process; see ADR-0028.

fn main() -> std::process::ExitCode {
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("__spike-mssql") {
        return data_spark::spike_mssql::run(argv[2..].to_vec());
    }
    data_spark::run()
}
