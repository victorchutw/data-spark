//! A synchronous SQL Server load session with a private current-thread runtime.

use super::{create_table_ddl, quote_identifier, table, write_failure, BulkRowPlan};
use crate::connector::{
    AbandonedWrite, DestinationWrite, DestinationWriteFacts, DestinationWriteFailure,
    DestinationWriter, LoadMode, SqlServerConfig, SqlServerEncryption, Transience,
};
use crate::LoadFailure;
use arrow_array::RecordBatch;
use std::{sync::Mutex, time::Duration};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::{net::TcpStream, runtime::Runtime};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

type SqlClient = Client<Compat<TcpStream>>;
const FULL_REFRESH_STRATEGY: &str = "transactional_delete_insert";
const MERGE_STRATEGY: &str = "transactional_merge";
const APPEND_STRATEGY: &str = "bulk_insert";

fn client_config(address: &SqlServerConfig, password: String) -> Config {
    let mut config = Config::new();
    config.host(&address.host);
    config.port(address.port);
    config.database(&address.database);
    config.application_name("data-spark");
    config.authentication(AuthMethod::sql_server(&address.user, password));
    if address.encryption == SqlServerEncryption::Optional {
        config.encryption(EncryptionLevel::Off);
    }
    if address.trust_server_certificate {
        config.trust_cert();
    }
    config
}

fn failure(operation: &str, error: impl std::fmt::Display) -> LoadFailure {
    write_failure(format!("SQL Server {operation} failed: {error}"))
}

async fn execute(client: &mut SqlClient, sql: &str) -> Result<(), LoadFailure> {
    client
        .simple_query(sql)
        .await
        .map_err(|error| failure("statement", error))?
        .into_results()
        .await
        .map_err(|error| failure("statement", error))?;
    Ok(())
}

async fn inspect(
    client: &mut SqlClient,
    address: &SqlServerConfig,
) -> Result<table::TableShape, LoadFailure> {
    let rows = client
        .simple_query(table::introspection_query(
            &address.schema,
            &address.dataset,
        ))
        .await
        .map_err(|error| failure("introspection", error))?
        .into_first_result()
        .await
        .map_err(|error| failure("introspection", error))?;
    table::TableShape::from_catalog_rows(rows)
}

pub(crate) struct Writer {
    address: SqlServerConfig,
    mode: LoadMode,
    merge_keys: Vec<String>,
    stage: String,
    runtime: Runtime,
    session: Mutex<Session>,
}

struct Session {
    client: SqlClient,
    plan: Option<BulkRowPlan>,
    shape: Option<table::TableShape>,
    transaction: bool,
    identity_insert: bool,
    committed_chunks: u64,
    written_records: u64,
}

impl Writer {
    pub(crate) fn begin(
        address: SqlServerConfig,
        mode: LoadMode,
        merge_keys: Vec<String>,
    ) -> Result<Self, DestinationWriteFailure> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| failure("runtime initialization", error))?;
        let password = std::env::var(&address.password_env)
            .ok()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                write_failure("SQL Server credential reference is no longer available".into())
            })?;
        let config = client_config(&address, password);
        let mut client = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(15), async {
                let tcp = TcpStream::connect((address.host.as_str(), address.port))
                    .await
                    .map_err(|error| failure("connect", error))?;
                tcp.set_nodelay(true)
                    .map_err(|error| failure("connect", error))?;
                Client::connect(config, tcp.compat_write())
                    .await
                    .map_err(|error| failure("connect", error))
            })
            .await
            .map_err(|_| write_failure("SQL Server connect timed out after 15 seconds".into()))?
        })?;
        // After connecting, append and merge reject an absent table before any write.
        // The dataset schema becomes available with the first chunk.
        let shape = if mode != LoadMode::FullRefresh {
            let shape = runtime.block_on(inspect(&mut client, &address))?;
            if shape.columns.is_empty() {
                return Err(write_failure(format!(
                    "SQL Server table {}.{} must exist before {}",
                    quote_identifier(&address.schema),
                    quote_identifier(&address.dataset),
                    mode.as_str()
                ))
                .into());
            }
            Some(shape)
        } else {
            None
        };
        Ok(Self {
            address,
            mode,
            merge_keys,
            stage: format!("data_spark_merge_stage_{}", uuid::Uuid::new_v4().simple()),
            runtime,
            session: Mutex::new(Session {
                client,
                plan: None,
                shape,
                transaction: false,
                identity_insert: false,
                committed_chunks: 0,
                written_records: 0,
            }),
        })
    }

    fn table_name(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.address.schema),
            quote_identifier(&self.address.dataset)
        )
    }

    async fn prepare(&self, session: &mut Session, batch: &RecordBatch) -> Result<(), LoadFailure> {
        let shape = match session.shape.take() {
            Some(shape) => shape,
            None => inspect(&mut session.client, &self.address).await?,
        };
        let dataset = batch.schema();
        if !shape.columns.is_empty() {
            shape.validate(
                &dataset,
                self.mode,
                &self.merge_keys,
                self.address.accept_datetime_rounding,
            )?;
        }
        let plan = if shape.columns.is_empty() || self.mode == LoadMode::Merge {
            BulkRowPlan::new(
                &dataset,
                &dataset
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>(),
            )?
        } else {
            BulkRowPlan::for_table(&dataset, &shape)?
        };
        if self.mode == LoadMode::Merge {
            execute(&mut session.client, "BEGIN TRAN").await?;
            session.transaction = true;
            execute(
                &mut session.client,
                &create_table_ddl(&dataset, &self.address.schema, &self.stage)?,
            )
            .await?;
            session.identity_insert = shape
                .columns
                .iter()
                .any(|column| column.identity && self.merge_keys.contains(&column.name));
        }
        // The port exposes the resolved schema only with the first chunk.
        // Validate before any write. Full refresh creates within its transaction
        // so even a failed first load leaves no destination object behind.
        if self.mode == LoadMode::FullRefresh {
            execute(&mut session.client, "BEGIN TRAN").await?;
            session.transaction = true;
            if shape.columns.is_empty() {
                execute(
                    &mut session.client,
                    &create_table_ddl(&dataset, &self.address.schema, &self.address.dataset)?,
                )
                .await?;
            }
            execute(
                &mut session.client,
                &format!("DELETE FROM {}", self.table_name()),
            )
            .await?;
        }
        session.plan = Some(plan);
        Ok(())
    }

    fn stage_name(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.address.schema),
            quote_identifier(&self.stage)
        )
    }

    fn merge_failure(&self, failure: LoadFailure) -> DestinationWriteFailure {
        DestinationWriteFailure {
            failure,
            facts: DestinationWriteFacts::atomic(MERGE_STRATEGY),
            written_records: 0,
            committed_chunks: 0,
            transience: Transience::Terminal,
        }
    }

    async fn merge(&self, session: &mut Session) -> Result<DestinationWriteFacts, LoadFailure> {
        let stage = self.stage_name();
        let target = self.table_name();
        let keys = self
            .merge_keys
            .iter()
            .map(|key| quote_identifier(key))
            .collect::<Vec<_>>();
        let duplicates = session
            .client
            .simple_query(format!(
                "SELECT TOP (1) 1 FROM {stage} GROUP BY {} HAVING COUNT_BIG(*) > 1",
                keys.join(", ")
            ))
            .await
            .map_err(|error| failure("duplicate-key gate", error))?
            .into_first_result()
            .await
            .map_err(|error| failure("duplicate-key gate", error))?;
        if !duplicates.is_empty() {
            return Err(LoadFailure {
                code: "duplicate_merge_keys",
                message: "surviving records contain duplicate merge key tuples".into(),
            });
        }
        let predicate = keys
            .iter()
            .map(|key| format!("target.{key} = source.{key}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        // HOLDLOCK retains the counted key ranges until MERGE commits; EXISTS
        // counts each staged record once even if several target records match.
        let counts = session.client.simple_query(format!(
            "SELECT COUNT_BIG(*), COUNT_BIG(CASE WHEN matched = 1 THEN 1 END) FROM \
             (SELECT CASE WHEN EXISTS (SELECT 1 FROM {target} AS target WITH (UPDLOCK, HOLDLOCK) WHERE {predicate}) \
             THEN 1 ELSE 0 END AS matched FROM {stage} AS source) AS counted"
        )).await.map_err(|error| failure("merge counts", error))?
            .into_first_result().await.map_err(|error| failure("merge counts", error))?;
        let staged = counts[0].get::<i64, _>(0).expect("COUNT_BIG is non-null") as u64;
        let updated = counts[0].get::<i64, _>(1).expect("COUNT_BIG is non-null") as u64;
        let columns = session.plan.as_ref().expect("prepared plan").column_names();
        let updates = columns
            .iter()
            .filter(|name| !self.merge_keys.iter().any(|key| key == **name))
            .map(|name| {
                let name = quote_identifier(name);
                format!("{name} = source.{name}")
            })
            .collect::<Vec<_>>();
        let update = if updates.is_empty() {
            String::new()
        } else {
            format!("WHEN MATCHED THEN UPDATE SET {}", updates.join(", "))
        };
        let names = columns
            .iter()
            .map(|name| quote_identifier(name))
            .collect::<Vec<_>>();
        let values = names
            .iter()
            .map(|name| format!("source.{name}"))
            .collect::<Vec<_>>();
        if session.identity_insert {
            execute(
                &mut session.client,
                &format!("SET IDENTITY_INSERT {target} ON"),
            )
            .await?;
        }
        execute(&mut session.client, &format!(
            "MERGE INTO {target} WITH (HOLDLOCK) AS target USING {stage} AS source ON {predicate} \
             {update} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});", names.join(", "), values.join(", ")
        )).await?;
        if session.identity_insert {
            execute(
                &mut session.client,
                &format!("SET IDENTITY_INSERT {target} OFF"),
            )
            .await?;
        }
        execute(&mut session.client, &format!("DROP TABLE {stage}")).await?;
        Ok(DestinationWriteFacts::atomic(MERGE_STRATEGY)
            .with_merge_counts(updated, staged - updated))
    }

    fn rollback(&self, session: &mut Session) {
        if session.transaction {
            // A failed bulk send may leave the protocol unusable. Attempt
            // explicit rollback, bounded so dropping the client can still
            // close the connection and let SQL Server roll back the session.
            let _ = self.runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(15), async {
                    execute(&mut session.client, "IF @@TRANCOUNT > 0 ROLLBACK TRAN").await
                })
                .await
            });
            session.transaction = false;
        }
    }
}

impl DestinationWriter for Writer {
    fn write_chunk(&self, batch: &RecordBatch) -> Result<(), DestinationWriteFailure> {
        let mut session = self.session.lock().expect("SQL Server session lock");
        if session.plan.is_none() {
            if let Err(error) = self.runtime.block_on(self.prepare(&mut session, batch)) {
                let staged_merge = self.mode == LoadMode::Merge && session.transaction;
                self.rollback(&mut session);
                return Err(if staged_merge {
                    self.merge_failure(error)
                } else {
                    error.into()
                });
            }
        }
        let result = self.runtime.block_on(async {
            let Session { client, plan, .. } = &mut *session;
            let plan = plan.as_ref().expect("prepared plan");
            let rows = plan.rows(batch)?;
            if batch.num_rows() != 0 {
                let name = if self.mode == LoadMode::Merge {
                    self.stage_name()
                } else {
                    self.table_name()
                };
                let mut request = client
                    .bulk_insert_with_columns(&name, &plan.column_names())
                    .await
                    .map_err(|error| failure("bulk begin", error))?;
                for row in rows {
                    request
                        .send(row?)
                        .await
                        .map_err(|error| failure("bulk send", error))?;
                }
                request
                    .finalize()
                    .await
                    .map_err(|error| failure("bulk finalize", error))?;
            }
            Ok::<_, LoadFailure>(())
        });
        if self.mode == LoadMode::Append {
            result.map_err(|failure| DestinationWriteFailure {
                failure,
                facts: DestinationWriteFacts::best_effort(APPEND_STRATEGY),
                written_records: session.written_records,
                committed_chunks: session.committed_chunks,
                transience: Transience::Terminal,
            })?;
            session.committed_chunks += 1;
            session.written_records += batch.num_rows() as u64;
            Ok(())
        } else {
            result.map_err(|error| {
                self.rollback(&mut session);
                if self.mode == LoadMode::Merge {
                    self.merge_failure(error)
                } else {
                    error.into()
                }
            })
        }
    }

    fn commit(self: Box<Self>) -> Result<DestinationWrite, DestinationWriteFailure> {
        if self.mode == LoadMode::Append {
            return Ok(DestinationWrite {
                bytes_written: None,
                facts: DestinationWriteFacts::best_effort(APPEND_STRATEGY),
            });
        }
        let mut session = self.session.lock().expect("SQL Server session lock");
        let result = self.runtime.block_on(async {
            let facts = if self.mode == LoadMode::Merge {
                self.merge(&mut session).await?
            } else {
                DestinationWriteFacts::atomic(FULL_REFRESH_STRATEGY)
            };
            execute(&mut session.client, "COMMIT TRAN").await?;
            Ok::<_, LoadFailure>(facts)
        });
        match result {
            Ok(facts) => {
                session.transaction = false;
                Ok(DestinationWrite {
                    bytes_written: None,
                    facts,
                })
            }
            Err(error) => {
                self.rollback(&mut session);
                Err(if self.mode == LoadMode::Merge {
                    self.merge_failure(error)
                } else {
                    error.into()
                })
            }
        }
    }

    fn abandon(self: Box<Self>) -> AbandonedWrite {
        let mut session = self.session.lock().expect("SQL Server session lock");
        self.rollback(&mut session);
        AbandonedWrite {
            committed_chunks: session.committed_chunks,
            written_records: session.written_records,
            facts: if session.committed_chunks > 0 {
                DestinationWriteFacts::best_effort(APPEND_STRATEGY)
            } else {
                DestinationWriteFacts::not_applicable()
            },
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.rollback(&mut self.session.lock().expect("SQL Server session lock"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::Transience;

    #[test]
    fn client_posture_honors_encryption_and_certificate_trust_independently() {
        for (encryption, expected) in [
            (SqlServerEncryption::Required, "Required"),
            (SqlServerEncryption::Optional, "Off"),
        ] {
            for trust in [false, true] {
                let address = SqlServerConfig {
                    host: "example.test".into(),
                    port: 14330,
                    database: "analytics".into(),
                    schema: "dbo".into(),
                    user: "loader".into(),
                    password_env: "UNUSED".into(),
                    encryption,
                    trust_server_certificate: trust,
                    accept_datetime_rounding: false,
                    dataset: "records".into(),
                };
                // Tiberius exposes setters but no posture getters. Its Debug
                // representation permits a pure check with a synthetic credential.
                let config = client_config(&address, "test-only".into());
                assert_eq!(config.get_addr(), "example.test:14330");
                let config = format!("{config:?}");
                assert!(config.contains(&format!("encryption: {expected}")));
                assert!(config.contains(if trust {
                    "trust: TrustAll"
                } else {
                    "trust: Default"
                }));
                assert!(config.contains("application_name: Some(\"data-spark\")"));
                assert!(config.contains("database: Some(\"analytics\")"));
                assert!(config.contains("SqlServer"));
            }
        }
    }

    #[test]
    fn driver_error_shapes_remain_terminal_write_failures() {
        for error in [
            tiberius::error::Error::from(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
            tiberius::error::Error::BulkInput("value exceeds bulk representation".into()),
            tiberius::error::Error::Conversion("invalid value".into()),
        ] {
            let failure: DestinationWriteFailure = failure("bulk send", error).into();
            assert_eq!(failure.failure.code, "destination_write_failed");
            assert_eq!(failure.transience, Transience::Terminal);
            assert_eq!(failure.written_records, 0);
            assert_eq!(failure.committed_chunks, 0);
        }
        let failure: DestinationWriteFailure = LoadFailure {
            code: "incompatible_destination_table",
            message: "column name: VARCHAR is incompatible".into(),
        }
        .into();
        assert_eq!(failure.failure.code, "incompatible_destination_table");
        assert_eq!(failure.transience, Transience::Terminal);
    }
}
