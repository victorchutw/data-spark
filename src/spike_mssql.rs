//! Throwaway spike for issue #115 — lives on a spike branch, never merged.
//!
//! Probes tiberius against a real containerized SQL Server to pin the
//! empirical facts listed in docs/research/tiberius-sqlserver-ci-2026-08-06.md:
//! TLS handshake behavior, bulk-insert type matrix, column-order sensitivity,
//! Err-vs-panic error surface, and mid-bulk connection drops.
//!
//! Invoked as `data-spark __spike-mssql <scenario>`. Every scenario runs under
//! `catch_unwind` so the harness reports OK / ERR / PANIC distinctly.

use std::borrow::Cow;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use tiberius::numeric::Numeric;
use tiberius::{AuthMethod, Client, ColumnData, Config, EncryptionLevel, TokenRow};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const HOST: &str = "127.0.0.1";
const PORT: u16 = 1433;
const PASSWORD: &str = "Sp1ke!Passw0rd";
const OP_TIMEOUT: Duration = Duration::from_secs(60);

type MssqlClient = Client<Compat<TcpStream>>;
type TdsError = tiberius::error::Error;

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio current-thread runtime")
}

fn base_config() -> Config {
    let mut config = Config::new();
    config.host(HOST);
    config.port(PORT);
    config.authentication(AuthMethod::sql_server("sa", PASSWORD));
    config
}

async fn connect(config: Config) -> Result<MssqlClient, TdsError> {
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Client::connect(config, tcp.compat_write()).await
}

/// Standard client for data scenarios: Required encryption + trust_cert,
/// database `spike`.
async fn connect_spike_db(database: &str) -> Result<MssqlClient, TdsError> {
    let mut config = base_config();
    config.encryption(EncryptionLevel::Required);
    config.trust_cert();
    config.database(database);
    connect(config).await
}

fn run_scenario(name: &str, body: impl FnOnce() -> Result<String, String>) {
    let outcome = catch_unwind(AssertUnwindSafe(body));
    match outcome {
        Ok(Ok(detail)) => println!("SPIKE {name}: OK | {detail}"),
        Ok(Err(detail)) => println!("SPIKE {name}: ERR | {detail}"),
        Err(panic) => {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "<non-string panic payload>".to_string());
            println!("SPIKE {name}: PANIC | {msg}");
        }
    }
}

fn block<F, T>(fut: F) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, TdsError>>,
{
    let rt = runtime();
    match rt.block_on(async { tokio::time::timeout(OP_TIMEOUT, fut).await }) {
        Err(_) => Err(format!("timed out after {}s", OP_TIMEOUT.as_secs())),
        Ok(Err(e)) => Err(format!("{e:?}")),
        Ok(Ok(v)) => Ok(v),
    }
}

async fn product_version(client: &mut MssqlClient) -> Result<String, TdsError> {
    let row = client
        .simple_query("SELECT CONVERT(varchar(128), SERVERPROPERTY('ProductVersion'))")
        .await?
        .into_row()
        .await?;
    Ok(row
        .and_then(|r| r.get::<&str, _>(0).map(str::to_string))
        .unwrap_or_else(|| "?".to_string()))
}

// ---------------------------------------------------------------------------
// Connection / TLS probes
// ---------------------------------------------------------------------------

fn probe_connect(mutate: impl FnOnce(&mut Config)) -> Result<String, String> {
    let mut config = base_config();
    mutate(&mut config);
    block(async move {
        let mut client = connect(config).await?;
        let version = product_version(&mut client).await?;
        client.close().await?;
        Ok(format!("connected; ProductVersion={version}"))
    })
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

fn setup() -> Result<String, String> {
    block(async {
        let mut config = base_config();
        config.encryption(EncryptionLevel::Required);
        config.trust_cert();
        let mut client = connect(config).await?;
        client
            .simple_query("IF DB_ID('spike') IS NULL CREATE DATABASE spike")
            .await?
            .into_results()
            .await?;
        client.close().await?;

        let mut client = connect_spike_db("spike").await?;
        let ddl = "
            IF OBJECT_ID('dbo.type_matrix') IS NOT NULL DROP TABLE dbo.type_matrix;
            CREATE TABLE dbo.type_matrix (
                c_bigint BIGINT NULL,
                c_float FLOAT NULL,
                c_bit BIT NULL,
                c_nvarchar NVARCHAR(50) NULL,
                c_nvarchar_max NVARCHAR(MAX) NULL,
                c_decimal DECIMAL(18,4) NULL,
                c_datetime2 DATETIME2 NULL,
                c_dtoffset DATETIMEOFFSET NULL
            );
            IF OBJECT_ID('dbo.order_probe') IS NOT NULL DROP TABLE dbo.order_probe;
            CREATE TABLE dbo.order_probe (c_bigint BIGINT NULL, c_nvarchar NVARCHAR(50) NULL);
            IF OBJECT_ID('dbo.date_time_probe') IS NOT NULL DROP TABLE dbo.date_time_probe;
            CREATE TABLE dbo.date_time_probe (d DATE NULL, t TIME NULL);
            IF OBJECT_ID('dbo.pk_probe') IS NOT NULL DROP TABLE dbo.pk_probe;
            CREATE TABLE dbo.pk_probe (id BIGINT NOT NULL PRIMARY KEY);
            IF OBJECT_ID('dbo.kill_probe') IS NOT NULL DROP TABLE dbo.kill_probe;
            CREATE TABLE dbo.kill_probe (a BIGINT NULL, b BIGINT NULL);
        ";
        client.simple_query(ddl).await?.into_results().await?;
        client.close().await?;
        Ok("spike database and probe tables ready".to_string())
    })
}

// ---------------------------------------------------------------------------
// Type matrix
// ---------------------------------------------------------------------------

fn dt2(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32, nano: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, mo, d)
        .unwrap()
        .and_hms_nano_opt(h, mi, s, nano)
        .unwrap()
}

fn dtoffset(secs_east: i32) -> DateTime<FixedOffset> {
    let offset = FixedOffset::east_opt(secs_east).unwrap();
    dt2(2026, 8, 6, 12, 34, 56, 123_456_700)
        .and_local_timezone(offset)
        .unwrap()
}

fn typical_row() -> TokenRow<'static> {
    let mut row = TokenRow::new();
    row.push(ColumnData::I64(Some(42)));
    row.push(ColumnData::F64(Some(1.5)));
    row.push(ColumnData::Bit(Some(true)));
    row.push(ColumnData::String(Some(Cow::from("short-value"))));
    row.push(ColumnData::String(Some(Cow::from("max-col-short"))));
    row.push(ColumnData::Numeric(Some(Numeric::new_with_scale(
        123_456_789, 4,
    ))));
    row.push(datetime2_col(Some(dt2(2026, 8, 6, 12, 34, 56, 123_456_700))));
    row.push(dtoffset_col(Some(dtoffset(8 * 3600))));
    row
}

fn null_row() -> TokenRow<'static> {
    let mut row = TokenRow::new();
    row.push(ColumnData::I64(None));
    row.push(ColumnData::F64(None));
    row.push(ColumnData::Bit(None));
    row.push(ColumnData::String(None));
    row.push(ColumnData::String(None));
    row.push(ColumnData::Numeric(None));
    row.push(datetime2_col(None));
    row.push(dtoffset_col(None));
    row
}

fn extreme_row() -> TokenRow<'static> {
    let mut row = TokenRow::new();
    row.push(ColumnData::I64(Some(i64::MAX)));
    row.push(ColumnData::F64(Some(f64::MIN)));
    row.push(ColumnData::Bit(Some(false)));
    row.push(ColumnData::String(Some(Cow::from("x".repeat(50)))));
    row.push(ColumnData::String(Some(Cow::from(""))));
    row.push(ColumnData::Numeric(Some(Numeric::new_with_scale(-1, 4))));
    row.push(datetime2_col(Some(dt2(1, 1, 1, 0, 0, 0, 0))));
    row.push(dtoffset_col(Some(
        dt2(2026, 8, 6, 0, 0, 0, 0)
            .and_local_timezone(FixedOffset::west_opt(12 * 3600).unwrap())
            .unwrap(),
    )));
    row
}

/// chrono NaiveDateTime -> DATETIME2 wire value via tiberius's chrono feature.
fn datetime2_col(value: Option<NaiveDateTime>) -> ColumnData<'static> {
    use tiberius::IntoSql;
    match value {
        Some(v) => v.into_sql(),
        None => Option::<NaiveDateTime>::None.into_sql(),
    }
}

/// chrono DateTime<FixedOffset> -> DATETIMEOFFSET wire value.
fn dtoffset_col(value: Option<DateTime<FixedOffset>>) -> ColumnData<'static> {
    use tiberius::IntoSql;
    match value {
        Some(v) => v.into_sql(),
        None => Option::<DateTime<FixedOffset>>::None.into_sql(),
    }
}

fn matrix_batch() -> Result<String, String> {
    block(async {
        let mut client = connect_spike_db("spike").await?;
        client
            .simple_query("TRUNCATE TABLE dbo.type_matrix")
            .await?
            .into_results()
            .await?;

        let mut req = client.bulk_insert("dbo.type_matrix").await?;
        req.send(typical_row()).await?;
        req.send(null_row()).await?;
        req.send(extreme_row()).await?;
        let res = req.finalize().await?;
        let total = res.total();

        let rows = client
            .query(
                "SELECT c_bigint,
                        CONVERT(varchar(64), c_float, 3),
                        c_bit,
                        c_nvarchar,
                        LEN(c_nvarchar_max),
                        CONVERT(varchar(64), c_decimal),
                        CONVERT(varchar(64), c_datetime2, 121),
                        CONVERT(varchar(64), c_dtoffset, 121)
                 FROM dbo.type_matrix ORDER BY c_bigint",
                &[],
            )
            .await?
            .into_first_result()
            .await?;

        let mut lines = Vec::new();
        for row in &rows {
            let bigint: Option<i64> = row.get(0);
            let float_s: Option<&str> = row.get(1);
            let bit: Option<bool> = row.get(2);
            let nv: Option<&str> = row.get(3);
            let max_len: Option<i64> = row.get(4);
            let dec_s: Option<&str> = row.get(5);
            let dt2_s: Option<&str> = row.get(6);
            let dto_s: Option<&str> = row.get(7);
            lines.push(format!(
                "row[bigint={bigint:?} float={float_s:?} bit={bit:?} nvarchar={nv:?} max_len={max_len:?} decimal={dec_s:?} datetime2={dt2_s:?} dtoffset={dto_s:?}]"
            ));
        }
        client.close().await?;
        Ok(format!(
            "finalize total={total}; readback {} rows: {}",
            rows.len(),
            lines.join(" ")
        ))
    })
}

/// One bulk request per NVARCHAR(MAX) payload length, so a failing length
/// cannot poison the others (bulk errors are whole-operation).
fn matrix_nvarchar_len(len: usize) -> Result<String, String> {
    block(async {
        let mut client = connect_spike_db("spike").await?;
        client
            .simple_query("TRUNCATE TABLE dbo.type_matrix")
            .await?
            .into_results()
            .await?;

        let payload = "\u{4e2d}".repeat(len); // non-ASCII to exercise UCS-2 width
        let mut row = TokenRow::new();
        row.push(ColumnData::I64(Some(len as i64)));
        row.push(ColumnData::F64(None));
        row.push(ColumnData::Bit(None));
        row.push(ColumnData::String(None));
        row.push(ColumnData::String(Some(Cow::from(payload))));
        row.push(ColumnData::Numeric(None));
        row.push(datetime2_col(None));
        row.push(dtoffset_col(None));

        let mut req = client.bulk_insert("dbo.type_matrix").await?;
        req.send(row).await?;
        let res = req.finalize().await?;

        let back = client
            .simple_query("SELECT LEN(c_nvarchar_max) FROM dbo.type_matrix")
            .await?
            .into_row()
            .await?;
        let stored_len: Option<i64> = back.and_then(|r| r.get(0));
        client.close().await?;
        Ok(format!(
            "sent len={len}; finalize total={}; stored LEN={stored_len:?}",
            res.total()
        ))
    })
}

/// Push columns in the wrong order (String where BIGINT is, I64 where
/// NVARCHAR is) to see whether bulk load errors or corrupts (#311).
fn matrix_column_order() -> Result<String, String> {
    block(async {
        let mut client = connect_spike_db("spike").await?;
        client
            .simple_query("TRUNCATE TABLE dbo.order_probe")
            .await?
            .into_results()
            .await?;

        let mut row = TokenRow::new();
        row.push(ColumnData::String(Some(Cow::from("i-am-not-a-bigint"))));
        row.push(ColumnData::I64(Some(7)));

        let mut req = client.bulk_insert("dbo.order_probe").await?;
        let send_result = req.send(row).await;
        let send_desc = match &send_result {
            Ok(_) => "send=Ok".to_string(),
            Err(e) => format!("send=Err({e:?})"),
        };
        let fin_desc = match req.finalize().await {
            Ok(res) => format!("finalize=Ok(total={})", res.total()),
            Err(e) => format!("finalize=Err({e:?})"),
        };

        let rows = client
            .query(
                "SELECT c_bigint, c_nvarchar FROM dbo.order_probe",
                &[],
            )
            .await?
            .into_first_result()
            .await?;
        let landed: Vec<String> = rows
            .iter()
            .map(|r| {
                format!(
                    "(c_bigint={:?}, c_nvarchar={:?})",
                    r.get::<i64, _>(0),
                    r.get::<&str, _>(1)
                )
            })
            .collect();
        client.close().await?;
        Ok(format!(
            "{send_desc}; {fin_desc}; landed rows: [{}]",
            landed.join(", ")
        ))
    })
}

/// DATE column immediately before TIME column (#410).
fn matrix_date_before_time() -> Result<String, String> {
    use tiberius::IntoSql;
    block(async {
        let mut client = connect_spike_db("spike").await?;
        client
            .simple_query("TRUNCATE TABLE dbo.date_time_probe")
            .await?
            .into_results()
            .await?;

        let mut row = TokenRow::new();
        row.push(NaiveDate::from_ymd_opt(2026, 8, 6).unwrap().into_sql());
        row.push(NaiveTime::from_hms_opt(12, 34, 56).unwrap().into_sql());

        let mut req = client.bulk_insert("dbo.date_time_probe").await?;
        let send_desc = match req.send(row).await {
            Ok(_) => "send=Ok".to_string(),
            Err(e) => format!("send=Err({e:?})"),
        };
        let fin_desc = match req.finalize().await {
            Ok(res) => format!("finalize=Ok(total={})", res.total()),
            Err(e) => format!("finalize=Err({e:?})"),
        };

        let back = client
            .simple_query(
                "SELECT CONVERT(varchar(32), d, 23), CONVERT(varchar(32), t, 121) FROM dbo.date_time_probe",
            )
            .await?
            .into_first_result()
            .await?;
        let landed: Vec<String> = back
            .iter()
            .map(|r| {
                format!(
                    "(d={:?}, t={:?})",
                    r.get::<&str, _>(0),
                    r.get::<&str, _>(1)
                )
            })
            .collect();
        client.close().await?;
        Ok(format!(
            "{send_desc}; {fin_desc}; landed rows: [{}]",
            landed.join(", ")
        ))
    })
}

/// Duplicate-PK bulk load: where does the error surface, and is the batch
/// atomic (COUNT afterwards)?
fn bulk_dup_key() -> Result<String, String> {
    block(async {
        let mut client = connect_spike_db("spike").await?;
        client
            .simple_query("TRUNCATE TABLE dbo.pk_probe")
            .await?
            .into_results()
            .await?;

        let mut req = client.bulk_insert("dbo.pk_probe").await?;
        let mut send_desc = String::from("sends=Ok");
        for value in [1i64, 1i64] {
            let mut row = TokenRow::new();
            row.push(ColumnData::I64(Some(value)));
            if let Err(e) = req.send(row).await {
                send_desc = format!("send=Err({e:?})");
                break;
            }
        }
        let fin_desc = match req.finalize().await {
            Ok(res) => format!("finalize=Ok(total={})", res.total()),
            Err(e) => format!("finalize=Err({e:?})"),
        };

        let count_row = client
            .simple_query("SELECT COUNT(*) FROM dbo.pk_probe")
            .await?
            .into_row()
            .await?;
        let count: Option<i32> = count_row.and_then(|r| r.get(0));
        client.close().await?;
        Ok(format!(
            "{send_desc}; {fin_desc}; rows landed after failure={count:?}"
        ))
    })
}

// ---------------------------------------------------------------------------
// Connection-drop probes
// ---------------------------------------------------------------------------

/// Bulk-load rows while a second connection KILLs the session mid-stream.
fn bulk_kill_mid() -> Result<String, String> {
    block(async {
        let mut client = connect_spike_db("spike").await?;
        client
            .simple_query("TRUNCATE TABLE dbo.kill_probe")
            .await?
            .into_results()
            .await?;
        let spid_row = client
            .simple_query("SELECT @@SPID")
            .await?
            .into_row()
            .await?;
        let spid: i16 = spid_row
            .and_then(|r| r.get(0))
            .ok_or(TdsError::Protocol(Cow::from("no spid")))?;

        let killer = std::thread::spawn(move || {
            let rt = runtime();
            rt.block_on(async move {
                tokio::time::sleep(Duration::from_millis(700)).await;
                let mut admin = connect_spike_db("master").await?;
                admin
                    .simple_query(format!("KILL {spid}"))
                    .await?
                    .into_results()
                    .await?;
                admin.close().await?;
                Ok::<_, TdsError>(())
            })
        });

        let started = Instant::now();
        let mut req = client.bulk_insert("dbo.kill_probe").await?;
        let mut outcome = String::new();
        let mut sent: u64 = 0;
        for i in 0..50_000_000u64 {
            let mut row = TokenRow::new();
            row.push(ColumnData::I64(Some(i as i64)));
            row.push(ColumnData::I64(Some((i * 2) as i64)));
            match req.send(row).await {
                Ok(_) => sent += 1,
                Err(e) => {
                    outcome = format!(
                        "send #{i} => Err({e:?}) after {:?}",
                        started.elapsed()
                    );
                    break;
                }
            }
        }
        if outcome.is_empty() {
            outcome = match req.finalize().await {
                Ok(res) => format!("all sends Ok; finalize=Ok(total={})", res.total()),
                Err(e) => format!(
                    "all {sent} sends Ok; finalize=Err({e:?}) after {:?}",
                    started.elapsed()
                ),
            };
        }
        let kill_result = killer.join();
        Ok(format!(
            "{outcome}; killer thread => {:?}",
            kill_result.map(|r| r.map(|_| "killed").map_err(|e| format!("{e:?}")))
        ))
    })
}

/// Endless bulk sender for external container kills: keeps sending until the
/// connection dies, then reports how the failure surfaced.
fn bulk_endless() -> Result<String, String> {
    block(async {
        let mut client = connect_spike_db("spike").await?;
        client
            .simple_query("TRUNCATE TABLE dbo.kill_probe")
            .await?
            .into_results()
            .await?;
        let started = Instant::now();
        let mut req = client.bulk_insert("dbo.kill_probe").await?;
        for i in 0..u64::MAX {
            let mut row = TokenRow::new();
            row.push(ColumnData::I64(Some(i as i64)));
            row.push(ColumnData::I64(Some((i * 2) as i64)));
            if let Err(e) = req.send(row).await {
                return Ok(format!(
                    "send #{i} => Err({e:?}) after {:?}",
                    started.elapsed()
                ));
            }
        }
        Ok("unreachable".to_string())
    })
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn run(args: Vec<String>) -> ExitCode {
    let scenario = args.first().map(String::as_str).unwrap_or("");
    match scenario {
        "connect-required-trust" => run_scenario("connect-required-trust", || {
            probe_connect(|c| {
                c.encryption(EncryptionLevel::Required);
                c.trust_cert();
            })
        }),
        "connect-required-notrust" => run_scenario("connect-required-notrust", || {
            probe_connect(|c| {
                c.encryption(EncryptionLevel::Required);
            })
        }),
        "connect-off-trust" => run_scenario("connect-off-trust", || {
            probe_connect(|c| {
                c.encryption(EncryptionLevel::Off);
                c.trust_cert();
            })
        }),
        "connect-notsupported" => run_scenario("connect-notsupported", || {
            probe_connect(|c| {
                c.encryption(EncryptionLevel::NotSupported);
            })
        }),
        "connect-wrong-password" => run_scenario("connect-wrong-password", || {
            probe_connect(|c| {
                c.encryption(EncryptionLevel::Required);
                c.trust_cert();
                c.authentication(AuthMethod::sql_server("sa", "definitely-wrong"));
            })
        }),
        "connect-wrong-port" => run_scenario("connect-wrong-port", || {
            probe_connect(|c| {
                c.encryption(EncryptionLevel::Required);
                c.trust_cert();
                c.port(14330);
            })
        }),
        "setup" => run_scenario("setup", setup),
        "matrix-batch" => run_scenario("matrix-batch", matrix_batch),
        "matrix-nvarchar-len" => {
            let len: usize = args
                .get(1)
                .and_then(|s| s.parse().ok())
                .expect("usage: matrix-nvarchar-len <chars>");
            run_scenario(&format!("matrix-nvarchar-len-{len}"), || {
                matrix_nvarchar_len(len)
            })
        }
        "matrix-column-order" => run_scenario("matrix-column-order", matrix_column_order),
        "matrix-date-before-time" => {
            run_scenario("matrix-date-before-time", matrix_date_before_time)
        }
        "bulk-dup-key" => run_scenario("bulk-dup-key", bulk_dup_key),
        "bulk-kill-mid" => run_scenario("bulk-kill-mid", bulk_kill_mid),
        "bulk-endless" => run_scenario("bulk-endless", bulk_endless),
        other => {
            eprintln!("unknown spike scenario: {other:?}");
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}
