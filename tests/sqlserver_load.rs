//! Live CLI contracts. The caller supplies SQL Server (ADR-0066).
use assert_cmd::Command;
use serde_json::{json, Value};
use std::fs;
use tempfile::TempDir;
use tiberius::{AuthMethod, Client, Config, Row};
use tokio::{net::TcpStream, runtime::Runtime};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

struct Server {
    runtime: Runtime,
    client: Client<Compat<TcpStream>>,
    table: String,
    rounding: bool,
}

fn setting(name: &str, default: &str) -> String {
    std::env::var(format!("DATA_SPARK_TEST_MSSQL_{name}")).unwrap_or_else(|_| default.into())
}

impl Server {
    fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut config = Config::new();
        config.host(setting("HOST", "localhost"));
        config.port(setting("PORT", "1433").parse().unwrap());
        config.database("tempdb");
        config.authentication(AuthMethod::sql_server(
            setting("USER", "sa"),
            setting("PASSWORD", "DataSparkTest123"),
        ));
        config.trust_cert();
        let client = runtime.block_on(async {
            let tcp = TcpStream::connect(config.get_addr()).await.unwrap();
            Client::connect(config, tcp.compat_write()).await.unwrap()
        });
        Self {
            runtime,
            client,
            table: format!("data_spark_{}", uuid::Uuid::new_v4().simple()),
            rounding: false,
        }
    }

    fn query(&mut self, sql: &str) -> Vec<Row> {
        self.runtime.block_on(async {
            self.client
                .simple_query(sql.replace("$table", &format!("[dbo].[{}]", self.table)))
                .await
                .unwrap()
                .into_first_result()
                .await
                .unwrap()
        })
    }

    fn load(&self, records: Value, options: &str, exit_code: i32) -> Value {
        let work = TempDir::new().unwrap();
        let source = records
            .as_array()
            .unwrap()
            .iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>();
        fs::write(work.path().join("source.jsonl"), source).unwrap();
        let destination = json!({"connector":"sqlserver", "host":setting("HOST", "localhost"),
            "port":setting("PORT", "1433").parse::<u16>().unwrap(), "database":"tempdb", "schema":"dbo",
            "user":setting("USER", "sa"), "password_env":"DATA_SPARK_LIVE_TEST_PASSWORD", "trust_server_certificate":true, "accept_datetime_rounding":self.rounding});
        fs::write(work.path().join("load.yml"), format!(
            "version: 1\nsource:\n  connector: local_file\n  path: source.jsonl\n  format: jsonl\ndestination: {destination}\ndataset: {}\nload_mode: full_refresh\n{options}", self.table)).unwrap();
        Command::cargo_bin("data-spark")
            .unwrap()
            .current_dir(work.path())
            .env(
                "DATA_SPARK_LIVE_TEST_PASSWORD",
                setting("PASSWORD", "DataSparkTest123"),
            )
            .args(["load", "--output-dir", "artifacts", "load.yml"])
            .assert()
            .code(exit_code);
        let run = fs::read_dir(work.path().join("artifacts"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let report: Value =
            serde_json::from_str(&fs::read_to_string(run.join("load-report.json")).unwrap())
                .unwrap();
        assert_eq!(report["destination_summary"]["host"], destination["host"]);
        assert!(!report
            .to_string()
            .contains(&setting("PASSWORD", "DataSparkTest123")));
        report
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.query("DROP TABLE IF EXISTS $table");
    }
}

#[test]
#[ignore = "needs SQL Server"]
fn full_refresh_bootstraps_exact_types_and_round_trips_values() {
    let mut server = Server::new();
    let report = server.load(json!([
        {"id":9223372036854775807_i64,"ratio":0.5,"active":true,"name":"","wall":"2024-02-29T12:34:56.123456","instant":"2024-02-29T20:34:56.123456+08:00","amount":"-123.456"},
        {"id":-7,"ratio":null,"active":false,"name":null,"wall":null,"instant":null,"amount":null}
    ]), "schema:\n  overrides:\n  - name: id\n    nullable: false\n  - name: active\n    nullable: false\n  - name: wall\n    type: timestamp\n  - name: instant\n    type: timestamptz\n  - name: amount\n    type: decimal(18,3)\n", 0);
    assert_eq!(
        report["destination_write"],
        json!({"atomicity":"atomic","strategy":"transactional_delete_insert"})
    );
    assert_eq!(report["row_counts"]["written"], 2);
    let rows = server.query("SELECT id, ratio, active, name, CONVERT(varchar(30),wall,126), CONVERT(varchar(30),instant,126), CONVERT(varchar(30),amount) FROM $table ORDER BY id DESC");
    assert_eq!(rows[0].get::<i64, _>(0), Some(i64::MAX));
    assert_eq!(rows[0].get::<f64, _>(1), Some(0.5));
    assert_eq!(rows[0].get::<bool, _>(2), Some(true));
    assert_eq!(rows[0].get::<&str, _>(3), Some(""));
    assert_eq!(rows[1].get::<&str, _>(3), None);
    assert_eq!(
        rows[0].get::<&str, _>(4),
        Some("2024-02-29T12:34:56.123456")
    );
    assert_eq!(
        rows[0].get::<&str, _>(5),
        Some("2024-02-29T12:34:56.123456")
    );
    assert_eq!(rows[0].get::<&str, _>(6), Some("-123.456"));
    let columns = server.query("SELECT c.name,t.name,c.precision,c.scale,c.max_length,c.is_nullable FROM sys.columns c JOIN sys.types t ON t.user_type_id=c.user_type_id WHERE c.object_id=OBJECT_ID('$table') ORDER BY c.column_id");
    let actual = columns
        .iter()
        .map(|row| {
            (
                row.get::<&str, _>(0).unwrap(),
                row.get::<&str, _>(1).unwrap(),
                row.get::<u8, _>(2).unwrap(),
                row.get::<u8, _>(3).unwrap(),
                row.get::<i16, _>(4).unwrap(),
                row.get::<bool, _>(5).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            ("id", "bigint", 19, 0, 8, false),
            ("ratio", "float", 53, 0, 8, true),
            ("active", "bit", 1, 0, 1, false),
            ("name", "nvarchar", 0, 0, -1, true),
            ("wall", "datetime2", 26, 6, 8, true),
            ("instant", "datetime2", 26, 6, 8, true),
            ("amount", "decimal", 18, 3, 9, true)
        ]
    );
}

#[test]
#[ignore = "needs SQL Server"]
fn full_refresh_preserves_object_index_and_uses_existing_column_order() {
    let mut server = Server::new();
    server.query("CREATE TABLE $table (second BIGINT NULL, first BIGINT NULL); CREATE INDEX existing_index ON $table(first); INSERT INTO $table VALUES(90,80)");
    let id = server.query("SELECT OBJECT_ID('$table')")[0]
        .get::<i32, _>(0)
        .unwrap();
    for first in [1, 3] {
        let report = server.load(
            json!([{ "first": first, "second": first + 1 }]),
            "execution:\n  chunk_rows: 1\n  parallelism: 4\n",
            0,
        );
        assert_eq!(report["execution"]["parallelism"], 1);
        assert_eq!(report["execution"]["connector_parallelism_limit"], 1);
        let rows = server.query("SELECT first, second FROM $table");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<i64, _>(0), Some(first));
        assert_eq!(rows[0].get::<i64, _>(1), Some(first + 1));
        assert_eq!(
            server.query("SELECT OBJECT_ID('$table')")[0].get::<i32, _>(0),
            Some(id)
        );
        assert_eq!(server.query("SELECT COUNT(*) FROM sys.indexes WHERE object_id=OBJECT_ID('$table') AND name='existing_index'")[0].get::<i32,_>(0), Some(1));
    }
}

#[test]
#[ignore = "needs SQL Server"]
fn zero_survivors_commit_an_empty_full_refresh() {
    let mut server = Server::new();
    server.query("CREATE TABLE $table (id BIGINT NOT NULL); INSERT INTO $table VALUES(99)");
    let report = server.load(json!([{ "id": "bad" }]), "schema:\n  overrides:\n  - name: id\n    type: int64\n    nullable: false\nreject_threshold: 1\n", 0);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert_eq!(
        report["destination_write"],
        json!({"atomicity":"atomic","strategy":"transactional_delete_insert"})
    );
    assert_eq!(
        server.query("SELECT COUNT(*) FROM $table")[0].get::<i32, _>(0),
        Some(0)
    );
}

#[test]
#[ignore = "needs SQL Server"]
fn incompatible_existing_table_fails_before_deleting_any_records() {
    let mut server = Server::new();
    server.query(
        "CREATE TABLE $table (name VARCHAR(100) NULL); INSERT INTO $table VALUES('original')",
    );
    let report = server.load(json!([{ "name": "replacement" }]), "", 1);
    assert_eq!(
        report["error_summary"]["code"],
        "incompatible_destination_table"
    );
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(
        server.query("SELECT name FROM $table")[0].get::<&str, _>(0),
        Some("original")
    );
}

#[test]
#[ignore = "needs SQL Server"]
fn unrepresentable_text_rolls_back_all_chunks_without_rejecting_records() {
    let mut server = Server::new();
    server.query(
        "CREATE TABLE $table (name NVARCHAR(MAX) NULL); INSERT INTO $table VALUES(N'original')",
    );
    let report = server.load(
        json!([{ "name": "first chunk" }, { "name": "x".repeat(32768) }]),
        "execution:\n  chunk_rows: 1\n",
        1,
    );
    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(
        server.query("SELECT name FROM $table")[0].get::<&str, _>(0),
        Some("original")
    );
}

#[test]
#[ignore = "needs SQL Server"]
fn full_refresh_supplies_extra_nullable_default_and_identity_columns() {
    let mut server = Server::new();
    server.query("CREATE TABLE $table (sequence INT IDENTITY NOT NULL, name NVARCHAR(MAX) NULL, optional INT NULL, defaulted INT NOT NULL DEFAULT 42, money_value MONEY NULL, small_money SMALLMONEY NULL, variant SQL_VARIANT NULL)");
    server.load(json!([{ "name": "Ada" }]), "", 0);
    let rows = server.query("SELECT sequence, name, optional, defaulted FROM $table");
    assert_eq!(rows[0].get::<i32, _>(0), Some(1));
    assert_eq!(rows[0].get::<&str, _>(1), Some("Ada"));
    assert_eq!(rows[0].get::<i32, _>(2), None);
    assert_eq!(rows[0].get::<i32, _>(3), Some(42));
    let rows = server.query("SELECT COUNT(*) FROM $table WHERE money_value IS NULL AND small_money IS NULL AND variant IS NULL");
    assert_eq!(rows[0].get::<i32, _>(0), Some(1));
    let report = server.load(
        json!([{ "name": "replacement" }, { "name": "x".repeat(32768) }]),
        "execution:\n  chunk_rows: 1\n",
        1,
    );
    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert_eq!(report["row_counts"]["written"], 0);
    let rows = server.query("SELECT sequence, name, defaulted FROM $table");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32, _>(0), Some(1));
    assert_eq!(rows[0].get::<&str, _>(1), Some("Ada"));
    assert_eq!(rows[0].get::<i32, _>(2), Some(42));
}

#[test]
#[ignore = "needs SQL Server"]
fn full_refresh_writes_accepted_narrow_and_widened_columns() {
    let mut server = Server::new();
    server.query("CREATE TABLE $table (id INT NULL, amount DECIMAL(28,3) NULL, name NVARCHAR(10) NULL, instant DATETIME2(7) NULL)");
    server.load(json!([{ "id": 123, "amount": "-1.234", "name":"Ada", "instant":"2024-02-29T12:34:56.123456Z" }]), "schema:\n  overrides:\n  - name: amount\n    type: decimal(18,3)\n  - name: instant\n    type: timestamptz\n", 0);
    let rows = server.query("SELECT id, CONVERT(varchar(40),amount), name, CONVERT(varchar(40),instant,126) FROM $table");
    assert_eq!(rows[0].get::<i32, _>(0), Some(123));
    assert_eq!(rows[0].get::<&str, _>(1), Some("-1.234"));
    assert_eq!(rows[0].get::<&str, _>(2), Some("Ada"));
    assert_eq!(
        rows[0].get::<&str, _>(3),
        Some("2024-02-29T12:34:56.1234560")
    );
}

#[test]
#[ignore = "needs SQL Server"]
fn a_failed_bootstrap_rolls_back_the_new_table_too() {
    let mut server = Server::new();
    let report = server.load(json!([{ "name": "x".repeat(32768) }]), "", 1);
    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(
        server.query("SELECT OBJECT_ID('$table')")[0].get::<i32, _>(0),
        None
    );
}

#[test]
#[ignore = "needs SQL Server"]
fn out_of_range_instants_roll_back_prior_chunks_without_rejection() {
    let mut server = Server::new();
    server.query(
        "CREATE TABLE $table (instant DATETIME2(6) NULL); INSERT INTO $table VALUES('2000-01-01')",
    );
    let report = server.load(json!([{ "instant": "2024-02-29T12:34:56Z" }, { "instant": "0001-01-01T00:00:00+01:00" }]), "schema:\n  overrides:\n  - name: instant\n    type: timestamptz\nexecution:\n  chunk_rows: 1\n", 1);
    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(
        server.query("SELECT CONVERT(varchar(30),instant,126) FROM $table")[0].get::<&str, _>(0),
        Some("2000-01-01T00:00:00")
    );
}

#[test]
#[ignore = "needs SQL Server"]
fn decimal_scale_38_round_trips_through_the_pinned_driver() {
    let mut server = Server::new();
    let amount = "-0.00000000000000000000000000000000000001";
    server.load(
        json!([{ "amount": amount }]),
        "schema:\n  overrides:\n  - name: amount\n    type: decimal(38,38)\n",
        0,
    );
    assert_eq!(
        server.query("SELECT CONVERT(varchar(50),amount) FROM $table")[0].get::<&str, _>(0),
        Some(amount)
    );
}

#[test]
#[ignore = "needs SQL Server"]
fn datetime_rounding_opt_in_writes_legacy_and_lower_precision_columns() {
    let mut server = Server::new();
    server.rounding = true;
    server.query("CREATE TABLE $table (legacy DATETIME NULL, millis DATETIME2(3) NULL)");
    server.load(json!([{ "legacy":"2024-02-29T12:34:56.123456", "millis":"2024-02-29T23:59:59.999999" }]), "schema:\n  overrides:\n  - name: legacy\n    type: timestamp\n  - name: millis\n    type: timestamp\n", 0);
    let rows = server.query(
        "SELECT CONVERT(varchar(40),legacy,126), CONVERT(varchar(40),millis,126) FROM $table",
    );
    assert_eq!(rows[0].get::<&str, _>(0), Some("2024-02-29T12:34:56.123"));
    assert_eq!(rows[0].get::<&str, _>(1), Some("2024-03-01T00:00:00"));
}
