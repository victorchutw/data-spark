//! A synchronous full-refresh session with a private current-thread runtime.

use super::{create_table_ddl, quote_identifier, table, write_failure, BulkRowPlan};
use crate::connector::{
    AbandonedWrite, DestinationWrite, DestinationWriteFacts, DestinationWriteFailure,
    DestinationWriter, LoadMode, SqlServerConfig, SqlServerEncryption,
};
use crate::LoadFailure;
use arrow_array::RecordBatch;
use std::{sync::Mutex, time::Duration};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::{net::TcpStream, runtime::Runtime};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

type SqlClient = Client<Compat<TcpStream>>;
const STRATEGY: &str = "transactional_delete_insert";

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

pub(crate) struct FullRefreshWriter {
    address: SqlServerConfig,
    runtime: Runtime,
    session: Mutex<Session>,
}

struct Session {
    client: SqlClient,
    plan: Option<BulkRowPlan>,
    transaction: bool,
}

impl FullRefreshWriter {
    pub(crate) fn begin(address: SqlServerConfig) -> Result<Self, DestinationWriteFailure> {
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
        let client = runtime.block_on(async {
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
        Ok(Self {
            address,
            runtime,
            session: Mutex::new(Session {
                client,
                plan: None,
                transaction: false,
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
        let rows = session
            .client
            .simple_query(table::introspection_query(
                &self.address.schema,
                &self.address.dataset,
            ))
            .await
            .map_err(|error| failure("introspection", error))?
            .into_first_result()
            .await
            .map_err(|error| failure("introspection", error))?;
        let shape = table::TableShape::from_catalog_rows(rows)?;
        let dataset = batch.schema();
        let plan = if shape.columns.is_empty() {
            BulkRowPlan::new(
                &dataset,
                &dataset
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>(),
            )?
        } else {
            shape.validate(
                &dataset,
                LoadMode::FullRefresh,
                &[],
                self.address.accept_datetime_rounding,
            )?;
            BulkRowPlan::for_table(&dataset, &shape)?
        };
        // The port exposes the resolved schema only with the first chunk.
        // Validate before BEGIN/DELETE; create within the same transaction
        // so even a failed first load leaves no destination object behind.
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
        session.plan = Some(plan);
        Ok(())
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

impl DestinationWriter for FullRefreshWriter {
    fn write_chunk(&self, batch: &RecordBatch) -> Result<(), DestinationWriteFailure> {
        let mut session = self.session.lock().expect("SQL Server session lock");
        let result = self.runtime.block_on(async {
            if session.plan.is_none() {
                self.prepare(&mut session, batch).await?;
            }
            let Session { client, plan, .. } = &mut *session;
            let rows = plan.as_ref().expect("prepared plan").rows(batch)?;
            if batch.num_rows() != 0 {
                let name = self.table_name();
                let mut request = client
                    .bulk_insert(&name)
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
        result.map_err(|error| {
            self.rollback(&mut session);
            error.into()
        })
    }

    fn commit(self: Box<Self>) -> Result<DestinationWrite, DestinationWriteFailure> {
        let mut session = self.session.lock().expect("SQL Server session lock");
        let result = self
            .runtime
            .block_on(execute(&mut session.client, "COMMIT TRAN"));
        if let Err(error) = result {
            self.rollback(&mut session);
            return Err(error.into());
        }
        session.transaction = false;
        Ok(DestinationWrite {
            bytes_written: None,
            facts: DestinationWriteFacts::atomic(STRATEGY),
        })
    }

    fn abandon(self: Box<Self>) -> AbandonedWrite {
        self.rollback(&mut self.session.lock().expect("SQL Server session lock"));
        AbandonedWrite {
            committed_chunks: 0,
            written_records: 0,
            facts: DestinationWriteFacts::not_applicable(),
        }
    }
}

impl Drop for FullRefreshWriter {
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
