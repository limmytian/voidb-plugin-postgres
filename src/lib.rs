mod agent_session;
mod capabilities;
mod cli_plugin;
mod config;
mod connection;
pub mod service;

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

use voidb_core::connection::{ConnectionConfig, DatabaseType};
use voidb_core::database::types::*;
use voidb_core::database::DatabaseAdapter;
use voidb_core::error::VoidbError;
use voidb_core::plugin::NativePlugin;
use voidb_core::plugin::native::CreateAdapterFuture;

pub use cli_plugin::create_postgres_cli_plugin;
pub use capabilities::{invoke_postgres_capability, postgres_capabilities};
pub use agent_session::PostgresAgentSessionFactory;
pub use config::PostgresConfig;
pub use connection::PostgresConnectionProvider;

// Re-export service layer for external consumers
pub use service::{PostgresCommand, PostgresEvent, PostgresService};

pub struct PostgresAdapter {
    client: Arc<Mutex<Client>>,
    connection_id: String,
}

impl PostgresAdapter {
    pub fn build_connection_string(config: &ConnectionConfig) -> Result<String, VoidbError> {
        use crate::config::PostgresConfig;

        // Read configuration from plugin_config
        let postgres_config: PostgresConfig = config.plugin_config
            .as_ref()
            .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
            .and_then(|pc| {
                serde_json::from_value(pc.clone())
                    .map_err(|e| VoidbError::Connection(format!("Invalid PostgreSQL config: {}", e)))
            })?;

        Ok(postgres_config.to_connection_string())
    }

    fn map_err(e: tokio_postgres::Error) -> VoidbError {
        VoidbError::Query(e.to_string())
    }

    pub fn decode_value(row: &tokio_postgres::Row, idx: usize) -> CellValue {
        use tokio_postgres::types::Type;

        let col_type = row.columns()[idx].type_();

        match *col_type {
            Type::BOOL => row.try_get::<_, Option<bool>>(idx)
                .ok().flatten()
                .map(CellValue::Bool)
                .unwrap_or(CellValue::Null),

            Type::INT2 => row.try_get::<_, Option<i16>>(idx)
                .ok().flatten()
                .map(|v| CellValue::Int(v as i64))
                .unwrap_or(CellValue::Null),

            Type::INT4 | Type::OID => row.try_get::<_, Option<i32>>(idx)
                .ok().flatten()
                .map(|v| CellValue::Int(v as i64))
                .unwrap_or(CellValue::Null),

            Type::INT8 => row.try_get::<_, Option<i64>>(idx)
                .ok().flatten()
                .map(CellValue::Int)
                .unwrap_or(CellValue::Null),

            Type::FLOAT4 => row.try_get::<_, Option<f32>>(idx)
                .ok().flatten()
                .map(|v| CellValue::Float(v as f64))
                .unwrap_or(CellValue::Null),

            Type::FLOAT8 | Type::NUMERIC => row.try_get::<_, Option<f64>>(idx)
                .ok().flatten()
                .map(CellValue::Float)
                .unwrap_or(CellValue::Null),

            Type::JSON | Type::JSONB => row.try_get::<_, Option<serde_json::Value>>(idx)
                .ok().flatten()
                .map(|v| CellValue::Json(v.to_string()))
                .unwrap_or(CellValue::Null),

            Type::UUID => row.try_get::<_, Option<uuid::Uuid>>(idx)
                .ok().flatten()
                .map(|v| CellValue::Uuid(v.to_string()))
                .unwrap_or(CellValue::Null),

            Type::TIMESTAMPTZ | Type::TIMESTAMP => row.try_get::<_, Option<chrono::NaiveDateTime>>(idx)
                .ok().flatten()
                .map(|v| CellValue::DateTime(v.format("%Y-%m-%d %H:%M:%S").to_string()))
                .unwrap_or(CellValue::Null),

            Type::DATE => row.try_get::<_, Option<chrono::NaiveDate>>(idx)
                .ok().flatten()
                .map(|v| CellValue::Date(v.format("%Y-%m-%d").to_string()))
                .unwrap_or(CellValue::Null),

            Type::TIME | Type::TIMETZ => row.try_get::<_, Option<chrono::NaiveTime>>(idx)
                .ok().flatten()
                .map(|v| CellValue::Time(v.format("%H:%M:%S").to_string()))
                .unwrap_or(CellValue::Null),

            Type::BYTEA => row.try_get::<_, Option<Vec<u8>>>(idx)
                .ok().flatten()
                .map(CellValue::Blob)
                .unwrap_or(CellValue::Null),

            _ => row.try_get::<_, Option<String>>(idx)
                .ok().flatten()
                .map(CellValue::Text)
                .unwrap_or(CellValue::Null),
        }
    }
}

#[async_trait]
impl DatabaseAdapter for PostgresAdapter {
    async fn connect(config: &ConnectionConfig) -> Result<Box<dyn DatabaseAdapter>, VoidbError> {
        let conn_str = Self::build_connection_string(config)?;

        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| VoidbError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(Box::new(Self {
            client: Arc::new(Mutex::new(client)),
            connection_id: config.name.clone(),
        }))
    }

    async fn ping(&self) -> Result<(), VoidbError> {
        let client = self.client.lock().await;
        client.simple_query("SELECT 1").await.map_err(Self::map_err)?;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), VoidbError> {
        Ok(())
    }

    fn db_type(&self) -> DatabaseType {
        DatabaseType::PostgreSQL
    }

    fn connection_id(&self) -> String {
        self.connection_id.clone()
    }

    async fn list_databases(&self) -> Result<Vec<String>, VoidbError> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname",
                &[],
            )
            .await
            .map_err(Self::map_err)?;

        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<TableInfo>, VoidbError> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT t.table_name, t.table_type,
                        obj_description((quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass, 'pg_class') as comment,
                        (SELECT reltuples::bigint FROM pg_class WHERE oid = (quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass) as row_est
                 FROM information_schema.tables t
                 WHERE t.table_schema = $1
                   AND t.table_type = 'BASE TABLE'
                 ORDER BY t.table_name",
                &[&schema],
            )
            .await
            .map_err(Self::map_err)?;

        Ok(rows
            .iter()
            .map(|r| TableInfo {
                name: r.get::<_, String>(0),
                table_type: r.get::<_, String>(1),
                comment: r.try_get::<_, Option<String>>(2).ok().flatten(),
                row_count: r.try_get::<_, Option<i64>>(3).ok().flatten().map(|v| v as u64),
            })
            .collect())
    }

    async fn list_views(&self, schema: &str) -> Result<Vec<ViewInfo>, VoidbError> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT table_name, view_definition
                 FROM information_schema.views
                 WHERE table_schema = $1
                 ORDER BY table_name",
                &[&schema],
            )
            .await
            .map_err(Self::map_err)?;

        Ok(rows
            .iter()
            .map(|r| ViewInfo {
                name: r.get::<_, String>(0),
                definition: r.try_get::<_, Option<String>>(1).ok().flatten(),
            })
            .collect())
    }

    async fn describe_table(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<TableSchema, VoidbError> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT c.column_name, c.data_type, c.is_nullable, c.column_default,
                        c.character_maximum_length,
                        CASE WHEN pk.column_name IS NOT NULL THEN 'PRI' ELSE '' END as col_key
                 FROM information_schema.columns c
                 LEFT JOIN (
                     SELECT ku.column_name
                     FROM information_schema.table_constraints tc
                     JOIN information_schema.key_column_usage ku
                         ON tc.constraint_name = ku.constraint_name
                         AND tc.table_schema = ku.table_schema
                     WHERE tc.constraint_type = 'PRIMARY KEY'
                       AND tc.table_schema = $1
                       AND tc.table_name = $2
                 ) pk ON c.column_name = pk.column_name
                 WHERE c.table_schema = $1 AND c.table_name = $2
                 ORDER BY c.ordinal_position",
                &[&schema, &table],
            )
            .await
            .map_err(Self::map_err)?;

        let columns = rows
            .iter()
            .map(|r| {
                let nullable_str: String = r.get(2);
                let key: String = r.get(5);
                ColumnInfo {
                    name: r.get::<_, String>(0),
                    data_type: r.get::<_, String>(1),
                    nullable: nullable_str == "YES",
                    is_primary_key: key == "PRI",
                    default_value: r.try_get::<_, Option<String>>(3).ok().flatten(),
                    max_length: r.try_get::<_, Option<i32>>(4).ok().flatten().map(|v| v as u64),
                    extra: String::new(),
                }
            })
            .collect();

        let indexes = self.list_indexes(schema, table).await?;
        let foreign_keys = self.list_foreign_keys(schema, table).await?;

        Ok(TableSchema {
            database: schema.to_string(),
            table_name: table.to_string(),
            columns,
            indexes,
            foreign_keys,
            engine: None,
            comment: None,
        })
    }

    async fn list_indexes(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>, VoidbError> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT i.relname as index_name,
                        a.attname as column_name,
                        ix.indisunique,
                        am.amname as index_type
                 FROM pg_index ix
                 JOIN pg_class t ON t.oid = ix.indrelid
                 JOIN pg_class i ON i.oid = ix.indexrelid
                 JOIN pg_namespace n ON n.oid = t.relnamespace
                 JOIN pg_am am ON am.oid = i.relam
                 JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                 WHERE n.nspname = $1 AND t.relname = $2
                 ORDER BY i.relname, a.attnum",
                &[&schema, &table],
            )
            .await
            .map_err(Self::map_err)?;

        let mut indexes: std::collections::HashMap<String, IndexInfo> =
            std::collections::HashMap::new();

        for row in &rows {
            let idx_name: String = row.get(0);
            let col_name: String = row.get(1);
            let unique: bool = row.get(2);
            let idx_type: String = row.get(3);

            indexes
                .entry(idx_name.clone())
                .or_insert_with(|| IndexInfo {
                    name: idx_name,
                    columns: Vec::new(),
                    unique,
                    index_type: idx_type,
                })
                .columns
                .push(col_name);
        }

        Ok(indexes.into_values().collect())
    }

    async fn list_foreign_keys(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>, VoidbError> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT tc.constraint_name,
                        kcu.column_name,
                        ccu.table_name AS referenced_table,
                        ccu.column_name AS referenced_column,
                        rc.update_rule,
                        rc.delete_rule
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.key_column_usage kcu
                     ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                 JOIN information_schema.constraint_column_usage ccu
                     ON tc.constraint_name = ccu.constraint_name
                     AND tc.table_schema = ccu.table_schema
                 JOIN information_schema.referential_constraints rc
                     ON tc.constraint_name = rc.constraint_name
                     AND tc.table_schema = rc.constraint_schema
                 WHERE tc.constraint_type = 'FOREIGN KEY'
                   AND tc.table_schema = $1
                   AND tc.table_name = $2
                 ORDER BY tc.constraint_name, kcu.ordinal_position",
                &[&schema, &table],
            )
            .await
            .map_err(Self::map_err)?;

        let mut fks: std::collections::HashMap<String, ForeignKeyInfo> =
            std::collections::HashMap::new();

        for row in &rows {
            let fk_name: String = row.get(0);
            let col_name: String = row.get(1);
            let ref_table: String = row.get(2);
            let ref_col: String = row.get(3);
            let update_rule: String = row.get(4);
            let delete_rule: String = row.get(5);

            let fk = fks.entry(fk_name.clone()).or_insert_with(|| ForeignKeyInfo {
                name: fk_name,
                columns: Vec::new(),
                referenced_table: ref_table,
                referenced_columns: Vec::new(),
                on_update: update_rule,
                on_delete: delete_rule,
            });
            fk.columns.push(col_name);
            fk.referenced_columns.push(ref_col);
        }

        Ok(fks.into_values().collect())
    }

    async fn query_rows(
        &self,
        sql: &str,
        _params: &[QueryParam],
        offset: u64,
        limit: u64,
    ) -> Result<QueryResult, VoidbError> {
        let paginated = format!("{sql} LIMIT {limit} OFFSET {offset}");
        self.execute_query(&paginated, None).await
    }

    async fn execute(
        &self,
        sql: &str,
        _params: &[QueryParam],
    ) -> Result<ExecuteResult, VoidbError> {
        let client = self.client.lock().await;
        let affected = client.execute(sql, &[]).await.map_err(Self::map_err)?;
        Ok(ExecuteResult {
            rows_affected: affected,
            last_insert_id: None,
        })
    }

    async fn execute_query(
        &self,
        sql: &str,
        schema: Option<&str>,
    ) -> Result<QueryResult, VoidbError> {
        let start = Instant::now();
        let client = self.client.lock().await;

        if let Some(s) = schema
            && !s.is_empty()
        {
            client
                .execute(&format!("SET search_path TO {}", self.quote_identifier(s)), &[])
                .await
                .map_err(Self::map_err)?;
        }

        let rows = client.query(sql, &[]).await.map_err(Self::map_err)?;
        let execution_time = start.elapsed();

        if rows.is_empty() {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                execution_time,
                rows_affected: Some(0),
            });
        }

        let columns: Vec<ColumnInfo> = rows[0]
            .columns()
            .iter()
            .map(|col| ColumnInfo {
                name: col.name().to_string(),
                data_type: col.type_().name().to_string(),
                nullable: true,
                is_primary_key: false,
                default_value: None,
                max_length: None,
                extra: String::new(),
            })
            .collect();

        let result_rows: Vec<Row> = rows
            .iter()
            .map(|row| {
                let values = (0..columns.len())
                    .map(|i| Self::decode_value(row, i))
                    .collect();
                Row { values }
            })
            .collect();

        let row_count = result_rows.len() as u64;
        Ok(QueryResult {
            columns,
            rows: result_rows,
            execution_time,
            rows_affected: Some(row_count),
        })
    }

    async fn count_rows(
        &self,
        schema: &str,
        table: &str,
        filter: Option<&str>,
    ) -> Result<u64, VoidbError> {
        let client = self.client.lock().await;
        let sql = if let Some(f) = filter {
            format!(
                "SELECT COUNT(*) FROM {}.{} WHERE {}",
                self.quote_identifier(schema),
                self.quote_identifier(table),
                f
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM {}.{}",
                self.quote_identifier(schema),
                self.quote_identifier(table),
            )
        };

        let row = client.query_one(&sql, &[]).await.map_err(Self::map_err)?;
        let count: i64 = row.get(0);
        Ok(count as u64)
    }

    async fn get_create_table_sql(
        &self,
        _schema: &str,
        table: &str,
    ) -> Result<String, VoidbError> {
        Err(VoidbError::Other(format!(
            "PostgreSQL does not have SHOW CREATE TABLE. Use pg_dump for '{}'.",
            table
        )))
    }

    fn generate_create_table_sql(&self, schema: &TableSchema) -> String {
        let mut sql = format!(
            "CREATE TABLE {} (\n",
            self.quote_identifier(&schema.table_name)
        );

        let col_defs: Vec<String> = schema
            .columns
            .iter()
            .map(|col| {
                let mut def = format!("  {} {}", self.quote_identifier(&col.name), col.data_type);
                if !col.nullable {
                    def.push_str(" NOT NULL");
                }
                if let Some(ref default) = col.default_value {
                    def.push_str(&format!(" DEFAULT {}", default));
                }
                def
            })
            .collect();

        sql.push_str(&col_defs.join(",\n"));

        let pk_cols: Vec<&String> = schema
            .columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| &c.name)
            .collect();

        if !pk_cols.is_empty() {
            sql.push_str(",\n  PRIMARY KEY (");
            sql.push_str(
                &pk_cols
                    .iter()
                    .map(|c| self.quote_identifier(c))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            sql.push(')');
        }

        sql.push_str("\n)");
        sql
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

pub struct PostgresPlugin;

impl NativePlugin for PostgresPlugin {
    fn plugin_id(&self) -> &str {
        "postgres"
    }

    fn name(&self) -> &str {
        "PostgreSQL"
    }

    fn protocols(&self) -> &[&str] {
        &["postgres", "postgresql"]
    }

    fn default_port(&self) -> u16 {
        5432
    }

    fn create_adapter(&self, config: &ConnectionConfig) -> CreateAdapterFuture {
        let config = config.clone();
        Box::pin(async move { PostgresAdapter::connect(&config).await })
    }
}
