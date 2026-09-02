# Contributing

## Build and test

Use the stable Rust toolchain. Build the project and run the default test suite
with locked dependencies:

```bash
cargo build --locked
cargo test --locked
```

CI also requires formatting and lint checks:

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
```

## Server-backed tests

The default `cargo test --locked` command does not require SQL Server. Tests
that open a live TDS connection carry `#[ignore = "needs SQL Server"]` and
appear in Cargo's `ignored` count; while no such tests exist, that count is
zero. The `#[ignore]` attribute is reserved for tests that need a live server,
so all other tests remain in the default suite.

Run the complete suite, including server-backed tests, with:

```bash
cargo test --locked -- --include-ignored
```

The test process never starts a server. To provide the default local SQL Server,
start the same container used by CI:

```bash
docker run -d --name mssql -p 1433:1433 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=DataSparkTest123 mcr.microsoft.com/mssql/server:2022-latest
```

Wait until the server accepts connections before running the complete suite.
The server-backed tests use these defaults, which can be overridden when
pointing them at another SQL Server:

| Environment variable | Default |
| --- | --- |
| `DATA_SPARK_TEST_MSSQL_HOST` | `localhost` |
| `DATA_SPARK_TEST_MSSQL_PORT` | `1433` |
| `DATA_SPARK_TEST_MSSQL_USER` | `sa` |
| `DATA_SPARK_TEST_MSSQL_PASSWORD` | `DataSparkTest123` |

See the [documentation index](docs/README.md) for the versioned contracts,
guides, and architecture decision records. The live-server testing decision is
recorded in [ADR-0066](docs/adr/0066-test-server-backed-destinations-against-a-real-server-behind-an-ignore-gate.md).
