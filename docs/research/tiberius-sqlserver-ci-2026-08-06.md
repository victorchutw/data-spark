# Tiberius Driver and SQL Server CI Viability Research

Date: 2026-08-06

This note resolves the research questions of issue #107 for the SQL Server
destination cycle: whether [Tiberius](https://github.com/prisma/tiberius) is a
viable SQL Server connector for data-spark, and whether CI can run a real SQL
Server. Every claim links to the source that owns it; facts that only an
experiment can settle are collected at the end as prototype candidates.

## Baseline: what "single binary" means in this repo today

- The release job builds with `cargo build --release --locked --bin data-spark`
  on `ubuntu-latest` with no target override and no `.cargo/config`, so the
  target is the host default `x86_64-unknown-linux-gnu` (glibc). The asset is
  one stripped file, `data-spark-linux-x86_64` (`.github/workflows/release.yml`).
- DuckDB is compiled in via the `bundled` feature: "libduckdb-sys will use the
  cc crate to compile DuckDB from source and link against that"
  ([duckdb-rs README](https://github.com/duckdb/duckdb-rs/blob/main/README.md)).
- So the promise is a **single self-contained file** with no third-party
  runtime dependencies to install — not a fully static musl binary. A SQL
  Server connector keeps the promise as long as it adds no *system* library
  requirement (see TLS below).
- CI/runner budget observed on this repo (private): after the disk-free step,
  run 30605688262 reports `/dev/root 72G, 37G used, 36G avail` — ~36 GiB free
  for everything the job does.

## 1. Bulk insert

**API.** `Client::bulk_insert(table)` opens a TDS bulk load: "Execute a `BULK
INSERT` statement, efficiantly storing a large number of rows to a specified
table. Note: make sure the input row follows the same schema as the table,
otherwise calling `send()` will return an error"
([`Client::bulk_insert`](https://docs.rs/tiberius/latest/tiberius/struct.Client.html#method.bulk_insert)).
The returned [`BulkLoadRequest`](https://docs.rs/tiberius/latest/tiberius/struct.BulkLoadRequest.html)
buffers rows: `send(row)` "Adds a new row to the bulk insert, flushing only
when having a full packet of data", and `finalize()` "Ends the bulk load,
flushing all pending data to the wire … for the data to actually be available
in the table". `finalize` returns `Result<ExecuteResult>` (total rows), so the
error surface is **whole-operation**: a bad row fails the bulk load; there is
no per-row error report. One `bulk_insert` call is one `INSERT BULK` batch;
tiberius exposes no `ROWS_PER_BATCH`-style knobs
([open issue #302](https://github.com/prisma/tiberius/issues/302)), so chunked
writes mean one bulk-load request per chunk.

**Types.** The wire enum
[`ColumnData`](https://docs.rs/tiberius/latest/tiberius/enum.ColumnData.html)
covers everything data-spark's schema needs: `I64` (BIGINT), `F64` (FLOAT),
`Bit` (BIT), `String` (N/VARCHAR), `Numeric` (DECIMAL), and — gated behind the
default `tds73` feature — `Date`, `Time`, `DateTime2`, `DateTimeOffset`. Every
variant wraps `Option`, so NULLs are `None`. With the `chrono` feature the
driver maps `NaiveDate`/`NaiveTime`/`NaiveDateTime`/`DateTime<Utc|FixedOffset>`
to the TDS date/time types
([tiberius::time::chrono](https://docs.rs/tiberius/latest/tiberius/time/chrono/index.html)).

**Known sharp edges** (tiberius issue tracker, all open on 2026-08-06):

- No column list: bulk insert preflights the table and expects full-schema rows
  in table column order
  ([#311](https://github.com/prisma/tiberius/issues/311)).
- Large `NVARCHAR`/`VARCHAR` values fail with server error 4816 "Invalid column
  type from bcp client" ([#322](https://github.com/prisma/tiberius/issues/322))
  — directly relevant to an `NVARCHAR(MAX)` mapping policy.
- A `DATE` column immediately before a `TIME` column breaks the BCP encoding
  ([#410](https://github.com/prisma/tiberius/issues/410)); `MONEY` is
  unsupported ([#358](https://github.com/prisma/tiberius/issues/358), not a
  type we emit).

**Fallback: parameterized INSERT.** SQL Server caps parameters at **2,100 per
stored procedure** (RPC calls go through `sp_executesql`)
([Maximum capacity specifications](https://learn.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server)),
so a 10-column table yields at most ⌊2,100 / 10⌋ = **210 rows per statement**
(a couple less in practice for the statement's own parameters). Independently,
`INSERT … VALUES` caps at **1,000 rows** (error 10738); the documented
workarounds are multiple INSERTs, a derived-table `SELECT … FROM (VALUES …)`
(no row cap, still parameter-capped), or bulk import
([Table Value Constructor](https://learn.microsoft.com/en-us/sql/t-sql/queries/table-value-constructor-transact-sql)).
At data-spark's current 10k-row chunk size that is ~50 statements per chunk —
workable as a fallback, but bulk load is the primary path.

## 2. Async containment

Tiberius is async and **runtime-independent**: the client is generic over
`AsyncRead + AsyncWrite` (futures-io traits), and "when wanting to use Tiberius
with Tokio, their `TcpStream` needs to be wrapped in Tokio's `Compat` module"
([crate docs](https://docs.rs/tiberius/latest/tiberius/)). Tokio is only a
dev-dependency of tiberius itself; the library's direct deps are
`futures-util`, `asynchronous-codec`, `async-trait`, and the chosen TLS stack
([Cargo.toml](https://github.com/prisma/tiberius/blob/main/Cargo.toml)).

A sync binary can contain the runtime entirely inside the connector:

- Own a `tokio::runtime::Runtime` (current-thread flavor) next to the `Client`
  and drive every operation with `Runtime::block_on`, which "Runs a future to
  completion on the Tokio runtime. This is the runtime's entry point"
  ([tokio Runtime docs](https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html#method.block_on)).
- Two lifetime rules from the same docs bound the design: `block_on` "panics if
  … called within an asynchronous execution context" (irrelevant while the rest
  of the binary stays sync), and "Once the runtime has been dropped, any
  outstanding I/O resources bound to it will no longer function" — so the
  runtime and the connection must live and die together, e.g. one struct owning
  `(Runtime, Client)` per load. Keeping one connection across many `block_on`
  calls on the *same* runtime is the intended pattern.
- Dependency additions: `tokio` (features `rt`, `net`, `time`), `tokio-util`
  (`compat`), `futures-util`, plus tiberius. The exact dependency-tree and
  binary-size delta is empirical — prototype candidate.

## 3. TLS and static linking

Tiberius defaults: `default = ["tds73", "winauth", "native-tls"]`
([Cargo.toml](https://github.com/prisma/tiberius/blob/main/Cargo.toml)).

- **`native-tls`** on Linux links against the system OpenSSL
  ([README](https://github.com/prisma/tiberius#encryption-tls--ssl)) — a
  runtime system-library dependency the current binary does not have, i.e. it
  would weaken the portability promise.
- **`rustls`** swaps in a pure-Rust TLS stack (`tokio-rustls`,
  `rustls-pemfile`, `rustls-native-certs`) with no system library requirement —
  this keeps the single-file promise. The README recommends it where the OS
  stack is problematic.
- **`vendored-openssl`** statically compiles OpenSSL at build time — a heavier
  alternative that also keeps the promise.

Encryption semantics: "When compiled using the default features, a TLS
encryption will be available and by default, used for all traffic"
([crate docs](https://docs.rs/tiberius/latest/tiberius/)); levels are
Required/Off/NotSupported ([README](https://github.com/prisma/tiberius)). The
mssql container ships a self-signed certificate, so CI connections need
`trust_cert()` ("on production, it is not a good idea to do this" —
[crate docs](https://docs.rs/tiberius/latest/tiberius/)).

Known TLS risks in the released crate:

- The released rustls stack is old (`tokio-rustls 0.24`); an update request is
  open ([#329](https://github.com/prisma/tiberius/issues/329)) and `cargo
  audit` flags three rustls-webpki RUSTSEC advisories against it
  ([#417](https://github.com/prisma/tiberius/issues/417)).
- rustls has rejected some self-signed certificates outright
  ("invalid peer certificate: UnsupportedCertVersion",
  [#327](https://github.com/prisma/tiberius/issues/327)) — the
  container-handshake path must be proven in a prototype.
- If the server declines the requested encryption level the client **panics**
  instead of returning an error
  ([#425](https://github.com/prisma/tiberius/issues/425)); the TDS decoder has
  further panic-instead-of-`Err` sites
  ([#424](https://github.com/prisma/tiberius/issues/424)). A connector wrapper
  must assume some failure modes abort via panic, not `Err`.
- TDS 8.0 "strict" encryption (SQL Server 2022's new mode) is not supported
  ([#412](https://github.com/prisma/tiberius/issues/412)); classic TDS 7.x
  encryption is what tiberius speaks, which SQL Server 2022 still accepts.

## 4. Transactions and DDL over TDS

Tiberius has **no transaction API** — `Client`'s methods are `connect`,
`execute`, `query`, `simple_query`, `bulk_insert`, `close`
([Client docs](https://docs.rs/tiberius/latest/tiberius/struct.Client.html)).
Transactions are driven with raw T-SQL (`BEGIN TRAN` / `COMMIT` / `ROLLBACK`)
over the same connection; `simple_query` "Execute[s] multiple queries,
delimited with `;`" and must not carry user input (same source). Connection
state (open transaction) lives on the connection, which the containment design
already holds for the whole load.

The staging-table patterns are all legal inside one explicit transaction — the
documented exclusions are database-level statements, e.g. "The CREATE DATABASE
statement must run in autocommit mode … and isn't allowed in an explicit or
implicit transaction"
([CREATE DATABASE](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-database-transact-sql),
likewise [ALTER DATABASE](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-database-transact-sql));
table-level DDL carries no such restriction:

- `sp_rename` renames tables: `EXECUTE sp_rename 'Sales.SalesTerritory',
  'SalesTerr';` requires ALTER permission on the object; the doc's caution
  ("Changing any part of an object name can break scripts and stored
  procedures") targets renaming referenced objects, which a private staging
  table is not
  ([sp_rename](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-rename-transact-sql)).
- `ALTER SCHEMA … TRANSFER` moves a table between schemas, "uses a schema level
  lock", and — important caveat — "All permissions associated with the
  securable are dropped when the securable is moved to the new schema"; it
  needs CONTROL on the table and ALTER on the target schema
  ([ALTER SCHEMA](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-schema-transact-sql)).
  A rename-based swap (`sp_rename`) avoids the permission-drop side effect
  within one schema.
- `MERGE` for upsert: Microsoft's own guidance is that "specifying the
  `HOLDLOCK` will prevent against unique key violations" (`HOLDLOCK` = the
  SERIALIZABLE hint), and more broadly "At scale, `MERGE` might introduce
  complicated concurrency issues … plan to thoroughly test any `MERGE`
  statement before deploying to production"
  ([MERGE, "Concurrency considerations"](https://learn.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql)).
  `MERGE <target> WITH (HOLDLOCK)` inside the terminal transaction matches the
  staged terminal upsert of ADR-0057.

## 5. Server compatibility and crate health

- Versions: "SQL Server 2005–2022"; 2017/2019/2022 are CI-tested, older ones
  "should work"; the default `tds73` feature targets TDS 7.3+ (2008+) and must
  be disabled for 2005 ([README](https://github.com/prisma/tiberius)). The
  2025 container tag exists on Microsoft's side but is not in tiberius's tested
  matrix.
- Auth ([`AuthMethod`](https://docs.rs/tiberius/latest/tiberius/enum.AuthMethod.html)):
  `SqlServer` (SQL auth — ungated), `Windows` (Windows OS + `winauth` feature),
  `Integrated` (Windows; on Unix needs `integrated-auth-gssapi`, which pulls
  GSSAPI/Kerberos C libraries — a system dependency to avoid), `AADToken`
  (ungated). SQL auth is the CI/first-slice path.
- Crate health: 0.12.3 is the newest crates.io release, published
  **2024-07-19**, ~4.8M total downloads
  ([crates.io API](https://crates.io/api/v1/crates/tiberius)). The GitHub repo
  is alive but slow (last push 2026-03-06, 134 open issues,
  [gh api](https://api.github.com/repos/prisma/tiberius)); users have asked for
  a release for over a year
  ([#321 "Repo Status"](https://github.com/prisma/tiberius/issues/321)).
  Practical consequence: fixes merged after mid-2024 are unreleased, so the
  choice between crates.io 0.12.3 and a pinned git revision is a real decision
  for the cycle.

## 6. CI: a real SQL Server on GitHub Actions

- **Runner (this repo is private):** standard `ubuntu-latest` for private
  repositories is 2 vCPU / 8 GB RAM / 14 GB SSD advertised
  ([GitHub-hosted runners](https://docs.github.com/en/actions/reference/runners/github-hosted-runners#standard-github-hosted-runners-for-private-repositories));
  observed actual root filesystem on this repo's runs is 72 GiB with **36 GiB
  free** after the existing disk-free step (run 30605688262).
- **Image:** `mcr.microsoft.com/mssql/server:2022-latest` is ~1.6 GB
  compressed (2017: ~1.33 GB) per Microsoft's container repo discussion
  ([microsoft/mssql-docker #809](https://github.com/microsoft/mssql-docker/issues/809)
  — secondary source; exact pull/extract cost is a prototype measurement).
  Fits the 36 GiB budget with a wide margin.
- **Container requirements:** "At least 2 GB of disk space. At least 2 GB of
  RAM."
  ([Docker quickstart](https://learn.microsoft.com/en-us/sql/linux/install-upgrade/quickstart-install-docker))
  — comfortable inside 8 GB alongside cargo.
- **Licensing:** `ACCEPT_EULA` is a "Required setting for the SQL Server
  image"; `MSSQL_PID` defaults the container to **Developer edition**, "the
  freely licensed Developer Edition of SQL Server for non-production use"
  ([environment variables](https://learn.microsoft.com/en-us/sql/linux/configure/environment-variables)).
  CI testing is squarely non-production use, so Developer + `ACCEPT_EULA=Y` is
  license-clean.
- **Readiness:** "The server is ready for connections once the SQL Server
  error logs display the message: `SQL Server is now ready for client
  connections`" (quickstart, above). The `sa` password must satisfy the policy
  (8+ chars, three of four character classes).
- **Health-check tooling:** since SQL Server 2022 CU14 and 2019 CU28 "the
  container images include the new mssql-tools18 package. The previous
  directory `/opt/mssql-tools/bin` is being phased out" — health probes must
  use `/opt/mssql-tools18/bin/sqlcmd` (with `-C` to trust the self-signed
  cert) on current images (quickstart, above).
- **Wiring options:** GitHub Actions service containers map ports to
  `localhost` for runner-machine jobs and accept docker-create options
  (`--health-cmd`, `--health-interval`, `--health-timeout`,
  `--health-retries`) so "the workflow waits for the … container to be fully
  operational before proceeding"
  ([PostgreSQL service containers example](https://docs.github.com/en/actions/tutorials/use-containerized-services/create-postgresql-service-containers));
  service/job containers require Linux runners
  ([about service containers](https://docs.github.com/en/actions/how-tos/use-containerized-services/about-service-containers)).
  The alternative — `docker run` + an explicit sqlcmd wait loop in a step —
  uses the same image and readiness signal; choosing between them is
  ticket #111's decision.

## Verdict

| Question | Verdict |
| --- | --- |
| 1. Bulk insert | **Viable with caveats** — API fits chunked writes and covers all needed types, but full-schema/column-order rows, the large-NVARCHAR bug (#322), and the DATE-before-TIME bug (#410) must shape the writer and be pinned by a prototype. Parameterized INSERT (≈210 rows/statement at 10 columns) is a workable fallback. |
| 2. Async containment | **Viable** — current-thread runtime owned by the connector, `(Runtime, Client)` lifetime-coupled, rest of the binary stays sync. |
| 3. Static linking | **Viable with `rustls`** — pure-Rust TLS keeps the single-file promise; default `native-tls` would add a system OpenSSL dependency. Released rustls stack is old (RUSTSEC flags, #417). |
| 4. Transactions/DDL | **Viable** — raw T-SQL transactions over one connection; staging + `sp_rename` swap and `MERGE WITH (HOLDLOCK)` are all legal in explicit transactions; `ALTER SCHEMA TRANSFER` drops permissions on transfer (prefer same-schema rename swap). |
| 5. Server compatibility | **Viable** — 2017/2019/2022 CI-tested, SQL auth ungated; no TDS 8.0 strict mode; crate release staleness is the real risk (crates.io vs pinned git rev decision). |
| 6. CI | **Viable** — ~1.6 GB image vs 36 GiB observed free disk; 2 GB RAM vs 8 GB runner; Developer edition + `ACCEPT_EULA` is license-clean for CI; health-gating via service-container options or an explicit sqlcmd wait loop. |

## Facts that need a prototype to pin down

1. Binary-size and dependency-tree delta of tiberius + tokio (rt/net/time) +
   tokio-util + rustls on top of the current build.
2. The bulk-insert type matrix against a real container: BIGINT, FLOAT, BIT,
   NVARCHAR (short and MAX-sized values, re #322), DECIMAL, DATETIME2,
   DATETIMEOFFSET, NULLs — and column-order sensitivity (#410, #311).
3. rustls handshake against the container's self-signed certificate with
   `trust_cert()` (re #327), and behavior when encryption is declined (#425 —
   panic containment).
4. Error surface probing for retry classification: which failures return `Err`
   vs panic (#424), and what a mid-bulk-load connection drop looks like.
5. Image pull + extract + startup-to-ready seconds on the actual private-repo
   runner (secondary sources say ~1.6 GB compressed; no primary source states
   startup time).
6. Whether crates.io 0.12.3 suffices or the cycle should pin a git revision
   for post-2024 fixes.
