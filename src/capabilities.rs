#![allow(clippy::result_large_err)]

use serde_json::{Value, json};
use voidb_core::formatters::cell_value_to_json;
use voidb_core::{
    CapabilityDefinition, CapabilityError, CapabilityErrorCategory, CapabilityInvocation,
    CapabilityInvocationResult, CapabilityRiskLevel, CredentialClass, InvocationOutputPage,
    InvocationStatus, LocalPathError, LocalPathScope, Pagination, RedactionStatus,
    SQL_DEFAULT_ROW_LIMIT, SQL_MAX_ROW_LIMIT, SQL_RESULT_CONTRACT_VERSION, SqlCatalogScope,
    SqlDataFormat, SqlDialect, SqlInputScope, SqlIntrospectionSupport, TargetSystemFailure,
    build_sql_import_batch, encode_sql_export, parse_sql_import_page, redact_text_with_json,
    sql_allows_read_only_query, sql_catalog_entry, sql_catalogs_input_schema,
    sql_catalogs_output_schema, sql_column_metadata, sql_describe_table_input_schema,
    sql_describe_table_output_schema, sql_explain_input_schema, sql_explain_output_schema,
    sql_explain_statement, sql_export_input_schema, sql_export_output_schema,
    sql_export_page_query, sql_foreign_key_constraints, sql_import_input_schema,
    sql_import_output_schema, sql_introspection_capabilities, sql_mutation_gate,
    sql_mutation_preview, sql_relation, sql_scope, sql_statement_count,
    sql_statements_output_schema, sql_tables_input_schema, sql_tables_output_schema,
    sql_text_input_schema,
};

use crate::config::PostgresConfig;
use crate::service::{PostgresService, StatementResult};

const PLUGIN_ID: &str = "postgres";
const DEFAULT_SCHEMA: &str = "public";
const DEFAULT_ROW_LIMIT: usize = SQL_DEFAULT_ROW_LIMIT;
const MAX_ROW_LIMIT: usize = SQL_MAX_ROW_LIMIT;

pub fn postgres_capabilities() -> Vec<CapabilityDefinition> {
    vec![
        capability(
            "query",
            "Execute read-oriented PostgreSQL SQL and return row sets.",
            sql_text_input_schema(
                "Read-oriented SQL. Mutating statements must use postgres.exec.",
                SqlInputScope::none(),
            ),
            sql_statements_output_schema(false),
            vec!["connection.read", "sql.query"],
            false,
            false,
            false,
            Some(30_000),
        ),
        capability(
            "explain",
            "Inspect the query plan for one read-oriented PostgreSQL SQL statement.",
            sql_explain_input_schema(SqlInputScope::none()),
            sql_explain_output_schema(),
            vec!["connection.read", "sql.explain"],
            false,
            false,
            false,
            Some(30_000),
        ),
        capability(
            "exec",
            "Execute PostgreSQL SQL that may mutate schema or data.",
            sql_text_input_schema(
                "SQL that may mutate PostgreSQL schema or data.",
                SqlInputScope::none(),
            ),
            sql_statements_output_schema(true),
            vec!["connection.read", "sql.exec"],
            true,
            false,
            true,
            Some(30_000),
        ),
        capability(
            "catalogs",
            "List PostgreSQL databases and schemas using a bounded catalog.",
            sql_catalogs_input_schema(),
            sql_catalogs_output_schema(SqlCatalogScope::Schema),
            vec!["connection.read", "schema.list"],
            false,
            false,
            false,
            Some(30_000),
        ),
        capability(
            "tables",
            "List PostgreSQL tables and views for the selected schema.",
            sql_tables_input_schema(SqlInputScope::schema(false)),
            sql_tables_output_schema(SqlCatalogScope::Schema),
            vec!["connection.read", "schema.list"],
            false,
            false,
            false,
            Some(30_000),
        ),
        capability(
            "describe_table",
            "Describe PostgreSQL table columns, indexes, and foreign keys.",
            sql_describe_table_input_schema(SqlInputScope::schema(false)),
            sql_describe_table_output_schema(SqlCatalogScope::Schema),
            vec!["connection.read", "schema.describe"],
            false,
            false,
            false,
            Some(30_000),
        ),
        capability(
            "export_query",
            "Export one bounded PostgreSQL query page as CSV, JSON, or JSONL.",
            sql_export_input_schema(SqlInputScope::none()),
            sql_export_output_schema(),
            vec!["connection.read", "sql.export", "local.write"],
            false,
            true,
            false,
            Some(60_000),
        ),
        capability(
            "import_plan",
            "Preview one bounded PostgreSQL import batch from an approved local source.",
            sql_import_input_schema(SqlInputScope::schema(false)),
            sql_import_output_schema(false),
            vec!["connection.read", "sql.import.plan", "local.read"],
            false,
            false,
            false,
            Some(60_000),
        ),
        capability(
            "import_apply",
            "Apply one acknowledged PostgreSQL import batch from an approved local source.",
            sql_import_input_schema(SqlInputScope::schema(false)),
            sql_import_output_schema(true),
            vec!["connection.read", "sql.import.apply", "local.read"],
            true,
            false,
            true,
            Some(60_000),
        ),
    ]
}

pub async fn invoke_postgres_capability(
    config: &PostgresConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    if invocation.plugin_id != PLUGIN_ID {
        return Err(validation_error(
            "validation.plugin_mismatch",
            "Invocation plugin_id does not match PostgreSQL.",
            json!({ "expected": PLUGIN_ID, "actual": invocation.plugin_id }),
        ));
    }

    match invocation.capability_id.as_str() {
        "query" => invoke_query(config, invocation).await,
        "explain" => invoke_explain(config, invocation).await,
        "exec" => invoke_exec(config, invocation).await,
        "catalogs" => invoke_catalogs(config, invocation).await,
        "tables" => invoke_tables(config, invocation).await,
        "describe_table" => invoke_describe_table(config, invocation).await,
        "export_query" => invoke_export_query(config, invocation).await,
        "import_plan" => invoke_import(config, invocation, false).await,
        "import_apply" => invoke_import(config, invocation, true).await,
        other => Err(unavailable_error(
            "unavailable.capability_not_found",
            "PostgreSQL capability was not found.",
            json!({ "capability_id": other }),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn capability(
    id: &str,
    description: &str,
    input_schema: Value,
    output_schema: Value,
    permissions: Vec<&str>,
    destructive: bool,
    streaming: bool,
    supports_dry_run: bool,
    default_timeout_ms: Option<u64>,
) -> CapabilityDefinition {
    let (execution_mode, session_handoff) = sql_execution_metadata(id);
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema,
        output_schema,
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: sql_authorization_metadata(id),
        risk: CapabilityRiskLevel::from_destructive(destructive),
        destructive,
        streaming,
        execution_mode,
        session_handoff,
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run,
        default_timeout_ms,
    }
}

fn sql_execution_metadata(
    id: &str,
) -> (
    voidb_core::CapabilityExecutionMode,
    Option<voidb_core::CapabilitySessionHandoff>,
) {
    let purpose = match id {
        "query" => voidb_core::PluginSessionPurpose::DatabaseQuery,
        "exec" => voidb_core::PluginSessionPurpose::DatabaseTransaction,
        _ => return (voidb_core::CapabilityExecutionMode::Stateless, None),
    };
    (
        voidb_core::CapabilityExecutionMode::Both,
        Some(voidb_core::CapabilitySessionHandoff::new(
            purpose,
            [format!("{PLUGIN_ID}.{id}")],
        )),
    )
}

fn sql_authorization_metadata(id: &str) -> voidb_core::CapabilityAuthorizationMetadata {
    let metadata = match id {
        "query" => voidb_core::CapabilityAuthorizationMetadata::declared()
            .with_session_purposes(vec![voidb_core::PluginSessionPurpose::DatabaseQuery]),
        "exec" | "import_apply" => voidb_core::CapabilityAuthorizationMetadata::declared()
            .with_interactive_execute()
            .with_session_purposes(vec![voidb_core::PluginSessionPurpose::DatabaseTransaction]),
        _ => voidb_core::CapabilityAuthorizationMetadata::declared(),
    };
    match id {
        "query" | "explain" | "exec" => {
            metadata.with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(vec![
                voidb_core::CapabilityApprovalField::new(
                    "/sql",
                    "SQL statement",
                    voidb_core::CapabilityApprovalValueType::String,
                )
                .required()
                .with_risk_emphasis(if id == "exec" {
                    voidb_core::CapabilityApprovalRiskEmphasis::Destructive
                } else {
                    voidb_core::CapabilityApprovalRiskEmphasis::Normal
                }),
            ]))
        }
        "tables" => metadata.with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(vec![
            voidb_core::CapabilityApprovalField::new(
                "/schema",
                "Schema",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            ),
        ])),
        "describe_table" => {
            metadata.with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(vec![
                voidb_core::CapabilityApprovalField::new(
                    "/table",
                    "Table",
                    voidb_core::CapabilityApprovalValueType::ResourceId,
                )
                .required(),
                voidb_core::CapabilityApprovalField::new(
                    "/schema",
                    "Schema",
                    voidb_core::CapabilityApprovalValueType::ResourceId,
                ),
            ]))
        }
        "export_query" => local_transfer_authorization(metadata, true, false),
        "import_plan" => local_transfer_authorization(metadata, false, false),
        "import_apply" => local_transfer_authorization(metadata, false, true),
        _ => metadata,
    }
}

fn local_transfer_authorization(
    metadata: voidb_core::CapabilityAuthorizationMetadata,
    export: bool,
    destructive: bool,
) -> voidb_core::CapabilityAuthorizationMetadata {
    let mut fields = Vec::new();
    if export {
        fields.push(
            voidb_core::CapabilityApprovalField::new(
                "/sql",
                "SQL statement",
                voidb_core::CapabilityApprovalValueType::String,
            )
            .required(),
        );
    } else {
        fields.push(
            voidb_core::CapabilityApprovalField::new(
                "/table",
                "Target table",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required()
            .with_risk_emphasis(if destructive {
                voidb_core::CapabilityApprovalRiskEmphasis::Destructive
            } else {
                voidb_core::CapabilityApprovalRiskEmphasis::Normal
            }),
        );
    }
    fields.extend([
        voidb_core::CapabilityApprovalField::new(
            "/local_root",
            "Approved local SQL transfer root",
            voidb_core::CapabilityApprovalValueType::Path,
        )
        .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::PrivilegeEscalation),
        voidb_core::CapabilityApprovalField::new(
            "/local_path",
            "Relative local SQL transfer path",
            voidb_core::CapabilityApprovalValueType::Path,
        ),
    ]);
    metadata
        .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(fields))
        .with_note(
            "Local SQL transfer paths are root-relative, no-follow, and revalidated during every read or atomic no-replace write.",
        )
        .without_capability_wide()
}

async fn invoke_query(
    config: &PostgresConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let sql = required_string(&invocation.input, "sql")?;
    if !sql_allows_read_only_query(&sql, SqlDialect::Postgres) {
        return Err(policy_error(
            "policy.destructive_requires_exec_capability",
            "PostgreSQL query only accepts read-oriented SQL; use postgres.exec for mutating statements.",
            json!({ "capability_id": "query" }),
        ));
    }

    let service = service(config).await?;
    let statements = service
        .execute_query(&sql)
        .await
        .map_err(|error| target_error(config, "postgres.query_failed", error.to_string()))?;
    let statements = statements_output(config, statements, page_request(&invocation)?)?;
    Ok(result(
        invocation.id,
        statements.output,
        statements.summary,
        statements.page,
    ))
}

async fn invoke_explain(
    config: &PostgresConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let sql = required_string(&invocation.input, "sql")?;
    if optional_bool(&invocation.input, "analyze")?.unwrap_or(false) {
        return Err(policy_error(
            "policy.explain_analyze_disabled",
            "PostgreSQL explain does not run analyze by default because it may execute target work.",
            json!({ "capability_id": "explain", "analyze": true }),
        ));
    }
    if sql_statement_count(&sql, SqlDialect::Postgres) != 1 {
        return Err(validation_error(
            "validation.explain_single_statement_required",
            "PostgreSQL explain accepts exactly one SQL statement.",
            json!({ "capability_id": "explain" }),
        ));
    }
    if !sql_allows_read_only_query(&sql, SqlDialect::Postgres) {
        return Err(policy_error(
            "policy.destructive_requires_exec_capability",
            "PostgreSQL explain only accepts read-oriented SQL.",
            json!({ "capability_id": "explain" }),
        ));
    }

    let explain_sql = sql_explain_statement(&sql, SqlDialect::Postgres);
    let service = service(config).await?;
    let statements = service
        .execute_query(&explain_sql)
        .await
        .map_err(|error| target_error(config, "postgres.explain_failed", error.to_string()))?;
    let statements = explain_output(statements_output(
        config,
        statements,
        page_request(&invocation)?,
    )?);
    Ok(result(
        invocation.id,
        statements.output,
        statements.summary,
        statements.page,
    ))
}

async fn invoke_exec(
    config: &PostgresConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let sql = required_string(&invocation.input, "sql")?;
    if invocation.controls.dry_run {
        let mut preview = sql_mutation_preview(
            &sql,
            SqlDialect::Postgres,
            invocation.controls.acknowledgement.is_some(),
        );
        if let Some(object) = preview.as_object_mut() {
            object.insert("database".to_string(), json!(config.database));
        }
        return Ok(result(
            invocation.id,
            preview.clone(),
            json!({
                "dry_run": true,
                "operation": "exec",
                "database": config.database,
                "statement_count": preview["statement_count"],
                "mutation_gate": preview["mutation_gate"]
            }),
            None,
        ));
    }

    let mutation_gate = sql_mutation_gate(
        &sql,
        SqlDialect::Postgres,
        invocation.controls.acknowledgement.is_some(),
        false,
    );
    let service = service(config).await?;
    let statements = service
        .execute_query(&sql)
        .await
        .map_err(|error| target_error(config, "postgres.exec_failed", error.to_string()))?;
    let mut statements = statements_output(config, statements, page_request(&invocation)?)?;
    let rows_affected = statements.rows_affected;
    if let Some(object) = statements.output.as_object_mut() {
        object.insert("rows_affected".to_string(), json!(rows_affected));
        object.insert("mutation_gate".to_string(), mutation_gate.clone());
    }
    if let Some(object) = statements.summary.as_object_mut() {
        object.insert("rows_affected".to_string(), json!(rows_affected));
        object.insert("mutation_gate".to_string(), mutation_gate);
    }
    Ok(result(
        invocation.id,
        statements.output,
        statements.summary,
        statements.page,
    ))
}

async fn invoke_catalogs(
    config: &PostgresConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let page = page_request(&invocation)?;
    let (pattern, include_system) = catalog_filters(&invocation.input)?;
    let service = service(config).await?;
    let databases = service
        .list_databases()
        .await
        .map_err(|error| target_error(config, "postgres.catalogs_failed", error.to_string()))?;
    let schemas = service
        .list_schemas()
        .await
        .map_err(|error| target_error(config, "postgres.catalogs_failed", error.to_string()))?;
    let mut entries = databases
        .into_iter()
        .filter(|database| include_system || !is_postgres_system_database(database))
        .map(|database| {
            sql_catalog_entry(
                &database,
                "database",
                None,
                database == config.database,
                is_postgres_system_database(&database),
            )
        })
        .chain(
            schemas
                .into_iter()
                .filter(|schema| include_system || !is_postgres_system_schema(schema))
                .map(|schema| {
                    sql_catalog_entry(
                        &schema,
                        "schema",
                        Some(&config.database),
                        schema == DEFAULT_SCHEMA,
                        is_postgres_system_schema(&schema),
                    )
                }),
        )
        .filter(|entry| name_matches(entry, pattern.as_deref()))
        .collect::<Vec<_>>();
    let (entries, next_cursor) = page_values(&mut entries, page);
    let databases = entries
        .iter()
        .filter(|entry| entry["kind"] == "database")
        .cloned()
        .collect::<Vec<_>>();
    let schemas = entries
        .iter()
        .filter(|entry| entry["kind"] == "schema")
        .cloned()
        .collect::<Vec<_>>();
    let warnings = include_system.then(|| {
        json!({
            "code": "postgres.system_schema_scope",
            "message": "The PostgreSQL service omits internal schemas that are unsafe or not useful for application discovery."
        })
    });
    let output = json!({
        "contract_version": SQL_RESULT_CONTRACT_VERSION,
        "scope": sql_scope(
            SqlCatalogScope::Schema,
            Some(config.database.as_str()),
            Some(DEFAULT_SCHEMA)
        ),
        "capabilities": sql_introspection_capabilities(postgres_introspection_support()),
        "database": config.database,
        "schema": DEFAULT_SCHEMA,
        "databases": databases,
        "schemas": schemas,
        "next_cursor": next_cursor,
        "warnings": warnings.into_iter().collect::<Vec<_>>(),
    });
    Ok(result(
        invocation.id,
        output.clone(),
        json!({
            "database_count": output["databases"].as_array().map_or(0, Vec::len),
            "schema_count": output["schemas"].as_array().map_or(0, Vec::len),
            "next_cursor": output["next_cursor"]
        }),
        output["next_cursor"]
            .as_str()
            .map(|cursor| InvocationOutputPage {
                next_cursor: Some(cursor.to_string()),
            }),
    ))
}

async fn invoke_tables(
    config: &PostgresConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let schema = schema_or_default(&invocation.input)?;
    let page = page_request(&invocation)?;
    let (pattern, include_system) = catalog_filters(&invocation.input)?;
    let service = service(config).await?;
    let tables = service
        .list_tables(&schema)
        .await
        .map_err(|error| target_error(config, "postgres.tables_failed", error.to_string()))?;

    let mut relations = Vec::new();
    for table in tables {
        let relation_type = if table.table_type.to_ascii_uppercase().contains("VIEW") {
            "view"
        } else {
            "table"
        };
        let relation = sql_relation(
            &table.name,
            relation_type,
            &table.table_type,
            Some(&config.database),
            Some(&schema),
            non_negative_row_estimate(table.row_estimate),
            None,
        );
        if (include_system || !is_postgres_system_relation(&relation))
            && name_matches(&relation, pattern.as_deref())
        {
            relations.push(relation);
        }
    }
    let (relations, next_cursor) = page_values(&mut relations, page);
    let table_entries = relations
        .iter()
        .filter(|relation| relation["relation_type"] == "table")
        .map(|relation| {
            json!({
                "name": relation["name"],
                "table_type": relation["table_type"],
                "rows": relation["rows"],
                "comment": relation["comment"],
            })
        })
        .collect::<Vec<_>>();
    let views = relations
        .iter()
        .filter(|relation| relation["relation_type"] == "view")
        .filter_map(|relation| relation["name"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    let table_count = table_entries.len();
    let view_count = views.len();
    let output = json!({
        "contract_version": SQL_RESULT_CONTRACT_VERSION,
        "scope": sql_scope(
            SqlCatalogScope::Schema,
            Some(config.database.as_str()),
            Some(schema.as_str())
        ),
        "capabilities": sql_introspection_capabilities(postgres_introspection_support()),
        "database": config.database,
        "schema": schema,
        "tables": table_entries,
        "views": views,
        "relations": relations,
        "next_cursor": next_cursor,
        "warnings": [],
    });
    Ok(result(
        invocation.id,
        output.clone(),
        json!({
            "database": config.database,
            "schema": schema,
            "table_count": table_count,
            "view_count": view_count,
            "next_cursor": output["next_cursor"]
        }),
        output["next_cursor"]
            .as_str()
            .map(|cursor| InvocationOutputPage {
                next_cursor: Some(cursor.to_string()),
            }),
    ))
}

async fn invoke_describe_table(
    config: &PostgresConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let schema = schema_or_default(&invocation.input)?;
    let table = required_string(&invocation.input, "table")?;
    let service = service(config).await?;
    let (columns, indexes, foreign_keys) =
        service
            .describe_table(&schema, &table)
            .await
            .map_err(|error| {
                target_error(config, "postgres.describe_table_failed", error.to_string())
            })?;
    let constraints = sql_foreign_key_constraints(&foreign_keys);
    let output = json!({
        "scope": sql_scope(
            SqlCatalogScope::Schema,
            Some(config.database.as_str()),
            Some(schema.as_str())
        ),
        "capabilities": sql_introspection_capabilities(postgres_introspection_support()),
        "database": config.database,
        "schema": schema,
        "table": table,
        "contract_version": SQL_RESULT_CONTRACT_VERSION,
        "columns": columns.iter().enumerate().map(|(index, column)| {
            sql_column_metadata(column, index + 1, SqlDialect::Postgres)
        }).collect::<Vec<_>>(),
        "indexes": indexes.into_iter().map(|index| {
            json!({
                "name": index.name,
                "columns": index.columns,
                "unique": index.is_unique,
                "index_type": index.index_type
            })
        }).collect::<Vec<_>>(),
        "constraints": constraints,
        "foreign_keys": foreign_keys,
        "warnings": [],
    });
    Ok(result(
        invocation.id,
        output.clone(),
        json!({
            "database": output["database"],
            "schema": output["schema"],
            "table": output["table"],
            "column_count": output["columns"].as_array().map_or(0, Vec::len),
            "index_count": output["indexes"].as_array().map_or(0, Vec::len),
            "foreign_key_count": output["foreign_keys"].as_array().map_or(0, Vec::len)
        }),
        None,
    ))
}

async fn invoke_export_query(
    config: &PostgresConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let format = sql_data_format(&invocation.input)?;
    let include_header = optional_bool(&invocation.input, "include_header")?.unwrap_or(true);
    let destination = local_destination(&invocation.input)?;
    let page = page_request(&invocation)?;
    let sql = sql_export_page_query(
        &required_string(&invocation.input, "sql")?,
        SqlDialect::Postgres,
        page.cursor,
        page.limit,
    )
    .map_err(|reason| {
        validation_error(
            "validation.sql_export_query_invalid",
            "PostgreSQL export query is invalid.",
            json!({ "reason": reason }),
        )
    })?;
    let mut query_invocation = invocation.clone();
    query_invocation.capability_id = "query".to_string();
    query_invocation.input = json!({ "sql": sql });
    query_invocation.controls.page = Some(Pagination {
        limit: page.limit as u32,
        cursor: None,
    });
    let query = invoke_query(config, query_invocation).await?;
    let chunk = encode_sql_export(&query.output, format, include_header).map_err(|error| {
        validation_error(
            "validation.sql_export_invalid",
            "PostgreSQL export could not encode the bounded query page.",
            json!({ "reason": error }),
        )
    })?;
    enforce_inline_export_limit(&invocation, destination.is_none(), chunk.bytes.len())?;
    let truncated = query.output["truncated"].as_bool().unwrap_or(false);
    let next_cursor = truncated.then(|| page.cursor.saturating_add(chunk.rows.len()).to_string());
    let result_page = next_cursor.as_ref().map(|cursor| InvocationOutputPage {
        next_cursor: Some(cursor.clone()),
    });
    let (destination_written, local_scope_id, preview) =
        write_export_destination(&invocation.id, destination, &chunk.bytes)?;
    let output = json!({
        "format": format.as_str(),
        "columns": chunk.columns,
        "row_count": chunk.rows.len(),
        "bytes": chunk.bytes.len(),
        "truncated": truncated,
        "cursor": if page.cursor == 0 { Value::Null } else { json!(page.cursor.to_string()) },
        "next_cursor": next_cursor,
        "destination_written": destination_written,
        "local_scope_id": local_scope_id,
        "preview": preview,
        "progress": {
            "rows_completed": chunk.rows.len(),
            "bytes_completed": chunk.bytes.len(),
            "terminal": !truncated
        }
    });
    Ok(result(
        invocation.id,
        output.clone(),
        json!({
            "format": output["format"],
            "row_count": output["row_count"],
            "bytes": output["bytes"],
            "truncated": output["truncated"],
            "next_cursor": output["next_cursor"],
            "destination_written": output["destination_written"]
        }),
        result_page,
    ))
}

async fn invoke_import(
    config: &PostgresConfig,
    invocation: CapabilityInvocation,
    apply: bool,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    validate_import_policy(&invocation.input)?;
    let schema = schema_or_default(&invocation.input)?;
    let table = required_string(&invocation.input, "table")?;
    let format = sql_data_format(&invocation.input)?;
    let page = page_request(&invocation)?;
    let local_root = required_string(&invocation.input, "local_root")?;
    let local_path = required_string(&invocation.input, "local_path")?;
    let local_scope_id = format!("local-scope:{}", invocation.id);
    let local_scope = LocalPathScope::new(&local_root)
        .map_err(|error| local_path_error(&local_scope_id, "read_file", error))?;
    let file = local_scope
        .open_existing_file(&local_path)
        .map_err(|error| local_path_error(&local_scope_id, "read_file", error))?;
    let source_file_bytes = file.len();
    let requested_columns = optional_string_array(&invocation.input, "columns")?;
    let import = parse_sql_import_page(
        file,
        source_file_bytes,
        format,
        optional_bool(&invocation.input, "has_header")?.unwrap_or(true),
        requested_columns.as_deref(),
        page.cursor,
        page.limit,
    )
    .map_err(|error| {
        validation_error(
            "validation.sql_import_invalid",
            "PostgreSQL import source is invalid.",
            json!({ "reason": error }),
        )
    })?;
    let next_cursor = import.next_cursor();
    let dry_run = !apply || invocation.controls.dry_run;
    let rows_inserted = if apply && !invocation.controls.dry_run && !import.rows.is_empty() {
        let sql = build_sql_import_batch(
            SqlDialect::Postgres,
            None,
            Some(&schema),
            &table,
            &import.columns,
            &import.rows,
        )
        .map_err(|error| {
            validation_error(
                "validation.sql_import_mapping_invalid",
                "PostgreSQL import mapping is invalid.",
                json!({ "reason": error }),
            )
        })?;
        let service = service(config).await?;
        let statements = service
            .execute_query(&sql)
            .await
            .map_err(|error| target_error(config, "postgres.import_failed", error.to_string()))?;
        statements_output(
            config,
            statements,
            PageRequest {
                limit: 1,
                cursor: 0,
            },
        )?;
        import.rows.len()
    } else {
        0
    };
    let mut output = json!({
        "dry_run": dry_run,
        "would_execute": !import.rows.is_empty(),
        "destructive": true,
        "operation": if apply { "import_apply" } else { "import_plan" },
        "format": format.as_str(),
        "table": table,
        "columns": import.columns,
        "row_count": import.rows.len(),
        "source_file_bytes": source_file_bytes,
        "truncated": import.truncated,
        "cursor": if page.cursor == 0 { Value::Null } else { json!(page.cursor.to_string()) },
        "next_cursor": next_cursor,
        "transaction_scope": "batch",
        "row_error_policy": "abort",
        "local_scope_id": local_scope_id,
        "checks": [
            { "code": "local_scope_revalidated", "status": "passed" },
            { "code": "batch_bounded", "status": "passed", "limit": page.limit },
            { "code": "transaction_per_batch", "status": "passed" }
        ],
        "warnings": []
    });
    if apply {
        output["rows_inserted"] = json!(rows_inserted);
    }
    Ok(result(
        invocation.id,
        output.clone(),
        json!({
            "operation": output["operation"],
            "row_count": output["row_count"],
            "rows_inserted": rows_inserted,
            "truncated": output["truncated"],
            "next_cursor": output["next_cursor"],
            "dry_run": dry_run
        }),
        output["next_cursor"]
            .as_str()
            .map(|cursor| InvocationOutputPage {
                next_cursor: Some(cursor.to_string()),
            }),
    ))
}

fn postgres_introspection_support() -> SqlIntrospectionSupport {
    SqlIntrospectionSupport {
        supports_databases: true,
        supports_schemas: true,
        supports_views: true,
        supports_row_counts: true,
        supports_indexes: true,
        supports_foreign_keys: true,
        supports_constraints: true,
        supports_comments: false,
    }
}

async fn service(config: &PostgresConfig) -> Result<PostgresService, CapabilityError> {
    PostgresService::new_direct(config)
        .await
        .map_err(|error| target_error(config, "postgres.open_failed", error))
}

#[derive(Debug, Clone, Copy)]
struct PageRequest {
    limit: usize,
    cursor: usize,
}

struct StatementsOutput {
    output: Value,
    summary: Value,
    page: Option<InvocationOutputPage>,
    rows_affected: u64,
}

fn sql_data_format(input: &Value) -> Result<SqlDataFormat, CapabilityError> {
    let format = optional_string(input, "format")?.unwrap_or_else(|| "jsonl".to_string());
    SqlDataFormat::parse(&format).map_err(|reason| {
        validation_error(
            "validation.sql_transfer_format_invalid",
            "SQL transfer format is invalid.",
            json!({ "reason": reason }),
        )
    })
}

fn local_destination(input: &Value) -> Result<Option<(String, String)>, CapabilityError> {
    let root = optional_string(input, "local_root")?;
    let path = optional_string(input, "local_path")?;
    match (root, path) {
        (None, None) => Ok(None),
        (Some(root), Some(path)) => Ok(Some((root, path))),
        _ => Err(validation_error(
            "validation.local_destination_incomplete",
            "local_root and local_path must be supplied together.",
            json!({ "fields": ["local_root", "local_path"] }),
        )),
    }
}

fn write_export_destination(
    invocation_id: &str,
    destination: Option<(String, String)>,
    bytes: &[u8],
) -> Result<(bool, Value, Value), CapabilityError> {
    let Some((root, path)) = destination else {
        return Ok((
            false,
            Value::Null,
            Value::String(String::from_utf8_lossy(bytes).into_owned()),
        ));
    };
    let scope_id = format!("local-scope:{invocation_id}");
    let scope = LocalPathScope::new(root)
        .map_err(|error| local_path_error(&scope_id, "write_new_file", error))?;
    scope
        .write_new_file(path, bytes)
        .map_err(|error| local_path_error(&scope_id, "write_new_file", error))?;
    Ok((true, json!(scope_id), Value::Null))
}

fn enforce_inline_export_limit(
    invocation: &CapabilityInvocation,
    inline: bool,
    bytes: usize,
) -> Result<(), CapabilityError> {
    if inline
        && invocation
            .controls
            .max_output_bytes
            .is_some_and(|maximum| bytes as u64 > maximum)
    {
        return Err(policy_error(
            "policy.output_limit_exceeded",
            "Inline SQL export exceeds max_output_bytes; use a smaller page or an approved local destination.",
            json!({ "bytes": bytes, "maximum": invocation.controls.max_output_bytes }),
        ));
    }
    Ok(())
}

fn validate_import_policy(input: &Value) -> Result<(), CapabilityError> {
    let mode = optional_string(input, "mode")?.unwrap_or_else(|| "append".to_string());
    let row_error_policy =
        optional_string(input, "row_error_policy")?.unwrap_or_else(|| "abort".to_string());
    if mode != "append" || row_error_policy != "abort" {
        return Err(policy_error(
            "policy.sql_import_mode_not_supported",
            "SQL imports currently require append mode and abort-on-error policy.",
            json!({ "mode": mode, "row_error_policy": row_error_policy }),
        ));
    }
    Ok(())
}

fn optional_string_array(
    input: &Value,
    field: &str,
) -> Result<Option<Vec<String>>, CapabilityError> {
    let Some(value) = input.get(field) else {
        return Ok(None);
    };
    let values = value.as_array().ok_or_else(|| {
        validation_error(
            "validation.input_field_invalid",
            "Optional input field must be an array of strings.",
            json!({ "field": field }),
        )
    })?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    validation_error(
                        "validation.input_field_invalid",
                        "Optional input field must contain non-empty strings.",
                        json!({ "field": field }),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn local_path_error(scope_id: &str, access: &str, error: LocalPathError) -> CapabilityError {
    capability_error(
        error.category(),
        error.code(),
        error.safe_message(),
        json!({ "local_scope_id": scope_id, "access": access }),
        None,
        error.retryable(),
        RedactionStatus::Applied,
    )
}

fn catalog_filters(input: &Value) -> Result<(Option<String>, bool), CapabilityError> {
    let pattern = match input.get("pattern") {
        Some(value) if !value.is_string() => {
            return Err(validation_error(
                "validation.input_field_invalid",
                "Catalog pattern must be a string.",
                json!({ "field": "pattern" }),
            ));
        }
        Some(value) => value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_ascii_lowercase),
        None => None,
    };
    let include_system = optional_bool(input, "include_system")?.unwrap_or(false);
    Ok((pattern, include_system))
}

fn name_matches(value: &Value, pattern: Option<&str>) -> bool {
    let Some(pattern) = pattern else {
        return true;
    };
    value["name"]
        .as_str()
        .is_some_and(|name| name.to_ascii_lowercase().contains(pattern))
}

fn is_postgres_system_database(database: &str) -> bool {
    database == "template0" || database == "template1"
}

fn is_postgres_system_schema(schema: &str) -> bool {
    schema == "pg_catalog" || schema == "information_schema" || schema.starts_with("pg_")
}

fn is_postgres_system_relation(value: &Value) -> bool {
    value["schema"]
        .as_str()
        .is_some_and(is_postgres_system_schema)
}

fn page_values(values: &mut Vec<Value>, page: PageRequest) -> (Vec<Value>, Option<String>) {
    let source_len = values.len();
    let page_values = values
        .drain(..)
        .skip(page.cursor)
        .take(page.limit)
        .collect::<Vec<_>>();
    let next_offset = page.cursor.saturating_add(page_values.len());
    let next_cursor = (next_offset < source_len).then(|| next_offset.to_string());
    (page_values, next_cursor)
}

fn page_request(invocation: &CapabilityInvocation) -> Result<PageRequest, CapabilityError> {
    let Some(page) = invocation.controls.page.as_ref() else {
        return Ok(PageRequest {
            limit: DEFAULT_ROW_LIMIT,
            cursor: 0,
        });
    };

    if page.limit == 0 || page.limit as usize > MAX_ROW_LIMIT {
        return Err(validation_error(
            "validation.page_limit_invalid",
            "PostgreSQL page limit must be between 1 and the maximum row limit.",
            json!({ "limit": page.limit, "maximum": MAX_ROW_LIMIT }),
        ));
    }

    let cursor = page
        .cursor
        .as_deref()
        .map(|cursor| {
            cursor.parse::<usize>().map_err(|_| {
                validation_error(
                    "validation.cursor_invalid",
                    "PostgreSQL query cursor must be an unsigned integer row offset.",
                    json!({ "cursor": cursor }),
                )
            })
        })
        .transpose()?
        .unwrap_or(0);

    Ok(PageRequest {
        limit: page.limit as usize,
        cursor,
    })
}

fn statements_output(
    config: &PostgresConfig,
    statements: Vec<StatementResult>,
    page: PageRequest,
) -> Result<StatementsOutput, CapabilityError> {
    let mut output_statements = Vec::new();
    let mut rows_affected = 0_u64;
    let mut remaining_skip = page.cursor;
    let mut remaining_take = page.limit;
    let mut returned_row_count = 0_usize;
    let mut source_row_count = 0_usize;
    let mut has_more = false;

    for statement in statements {
        match statement {
            StatementResult::Select { columns, rows } => {
                let statement_source_row_count = rows.len();
                source_row_count += statement_source_row_count;
                let skip_for_statement = remaining_skip.min(statement_source_row_count);
                remaining_skip -= skip_for_statement;
                let available_rows = statement_source_row_count.saturating_sub(skip_for_statement);
                let take_for_statement = remaining_take.min(available_rows);
                remaining_take -= take_for_statement;
                returned_row_count += take_for_statement;
                if available_rows > take_for_statement {
                    has_more = true;
                }

                let rows = rows
                    .iter()
                    .skip(skip_for_statement)
                    .take(take_for_statement)
                    .map(|row| {
                        let mut object = serde_json::Map::new();
                        for (index, column) in columns.iter().enumerate() {
                            let value = row
                                .values
                                .get(index)
                                .map(cell_value_to_json)
                                .unwrap_or(Value::Null);
                            object.insert(column.name.clone(), value);
                        }
                        Value::Object(object)
                    })
                    .collect::<Vec<_>>();
                let column_metadata = columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        sql_column_metadata(column, index + 1, SqlDialect::Postgres)
                    })
                    .collect::<Vec<_>>();
                output_statements.push(json!({
                    "kind": "select",
                    "columns": column_metadata,
                    "rows": rows,
                    "row_count": take_for_statement,
                    "source_row_count": statement_source_row_count,
                    "truncated": available_rows > take_for_statement
                }));
            }
            StatementResult::Affected(affected) => {
                rows_affected = rows_affected.saturating_add(affected);
                output_statements.push(json!({
                    "kind": "affected",
                    "rows_affected": affected
                }));
            }
            StatementResult::Empty => {
                output_statements.push(json!({ "kind": "empty" }));
            }
            StatementResult::Error(error) => {
                return Err(target_error(config, "postgres.statement_failed", error));
            }
        }
    }

    let next_cursor = has_more.then(|| (page.cursor + returned_row_count).to_string());
    let output = json!({
        "statements": output_statements,
        "row_limit": page.limit,
        "batch_size": page.limit,
        "row_count": returned_row_count,
        "source_row_count": source_row_count,
        "truncated": has_more,
        "cursor": if page.cursor == 0 { Value::Null } else { json!(page.cursor.to_string()) },
        "next_cursor": next_cursor,
        "contract_version": SQL_RESULT_CONTRACT_VERSION,
        "warnings": []
    });
    let summary = json!({
        "statement_count": output["statements"].as_array().map_or(0, Vec::len),
        "row_count": returned_row_count,
        "source_row_count": source_row_count,
        "truncated": has_more,
        "next_cursor": output["next_cursor"],
        "contract_version": SQL_RESULT_CONTRACT_VERSION,
        "batch_size": page.limit
    });
    let page = has_more.then(|| InvocationOutputPage {
        next_cursor: output["next_cursor"].as_str().map(str::to_string),
    });

    Ok(StatementsOutput {
        output,
        summary,
        page,
        rows_affected,
    })
}

fn explain_output(mut statements: StatementsOutput) -> StatementsOutput {
    if let Some(object) = statements.output.as_object_mut() {
        object.insert("analyze".to_string(), json!(false));
        object.insert("format".to_string(), json!("rows"));
        object.insert("dialect".to_string(), json!(SqlDialect::Postgres.as_str()));
        object.insert("explained_statement_count".to_string(), json!(1));
    }
    if let Some(object) = statements.summary.as_object_mut() {
        object.insert("analyze".to_string(), json!(false));
        object.insert("format".to_string(), json!("rows"));
        object.insert("dialect".to_string(), json!(SqlDialect::Postgres.as_str()));
        object.insert("explained_statement_count".to_string(), json!(1));
    }
    statements
}

fn result(
    invocation_id: String,
    output: Value,
    output_summary: Value,
    page: Option<InvocationOutputPage>,
) -> CapabilityInvocationResult {
    CapabilityInvocationResult {
        invocation_id,
        status: InvocationStatus::Succeeded,
        output,
        output_summary,
        page,
    }
}

fn schema_or_default(input: &Value) -> Result<String, CapabilityError> {
    optional_string(input, "schema").map(|schema| schema.unwrap_or_else(|| DEFAULT_SCHEMA.into()))
}

fn required_string(input: &Value, field: &str) -> Result<String, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_string() => Err(validation_error(
            "validation.input_field_invalid",
            "Required input field must be a string.",
            json!({ "field": field }),
        )),
        Some(value) => value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                validation_error(
                    "validation.input_field_required",
                    "Required string input field is missing.",
                    json!({ "field": field }),
                )
            }),
        None => Err(validation_error(
            "validation.input_field_required",
            "Required string input field is missing.",
            json!({ "field": field }),
        )),
    }
}

fn optional_string(input: &Value, field: &str) -> Result<Option<String>, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_string() => Err(validation_error(
            "validation.input_field_invalid",
            "Optional input field must be a string.",
            json!({ "field": field }),
        )),
        Some(value) => Ok(value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)),
        None => Ok(None),
    }
}

fn optional_bool(input: &Value, field: &str) -> Result<Option<bool>, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_boolean() => Err(validation_error(
            "validation.input_field_invalid",
            "Optional input field must be a boolean.",
            json!({ "field": field }),
        )),
        Some(value) => Ok(value.as_bool()),
        None => Ok(None),
    }
}

fn non_negative_row_estimate(value: Option<i64>) -> Option<u64> {
    value.and_then(|value| u64::try_from(value).ok())
}

fn validation_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Validation,
        code,
        message,
        details,
        None,
        false,
        RedactionStatus::NotRequired,
    )
}

fn policy_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Policy,
        code,
        message,
        details,
        None,
        false,
        RedactionStatus::NotRequired,
    )
}

fn unavailable_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Unavailable,
        code,
        message,
        details,
        None,
        true,
        RedactionStatus::NotRequired,
    )
}

fn target_error(config: &PostgresConfig, code: &str, message: String) -> CapabilityError {
    let profile = serde_json::to_value(config).unwrap_or(Value::Null);
    let (message, redaction) = redact_text_with_json(&message, &profile);
    capability_error(
        CapabilityErrorCategory::TargetSystem,
        code,
        "PostgreSQL target operation failed.",
        Value::Null,
        Some(TargetSystemFailure {
            system: Some(PLUGIN_ID.to_string()),
            code: None,
            message: Some(message),
        }),
        false,
        redaction,
    )
}

fn capability_error(
    category: CapabilityErrorCategory,
    code: &str,
    message: &str,
    details: Value,
    target: Option<TargetSystemFailure>,
    retryable: bool,
    redaction: RedactionStatus,
) -> CapabilityError {
    CapabilityError {
        category,
        code: code.to_string(),
        message: message.to_string(),
        details,
        target,
        retryable,
        redaction,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use voidb_core::{
        ActorRef, ActorType, CapabilityErrorCategory, ConnectionInstancePurpose,
        ConnectionProfileRef, InstanceReusePolicy, InvocationConnectionTarget, InvocationControls,
        Pagination, RedactionStatus, validate_sql_capability_contract,
    };

    #[test]
    fn postgres_capabilities_conform_to_shared_sql_contract() {
        validate_sql_capability_contract(PLUGIN_ID, &postgres_capabilities()).unwrap();
    }

    #[test]
    fn exposes_postgres_capability_metadata() {
        let capabilities = postgres_capabilities();

        assert_eq!(capabilities.len(), 9);
        assert!(
            capabilities
                .iter()
                .any(|capability| capability.qualified_id() == "postgres.query")
        );
        assert!(
            capabilities
                .iter()
                .any(|capability| capability.qualified_id() == "postgres.explain"
                    && !capability.destructive)
        );
        assert!(
            capabilities
                .iter()
                .any(|capability| capability.id == "exec" && capability.destructive)
        );
        assert!(
            capabilities
                .iter()
                .any(|capability| capability.id == "tables" && !capability.destructive)
        );
    }

    #[tokio::test]
    async fn query_rejects_mutating_sql_before_connecting() {
        let error = invoke_postgres_capability(
            &config(),
            invocation("query", json!({ "sql": "drop table users" })),
        )
        .await
        .expect_err("policy error");

        assert_eq!(error.category, CapabilityErrorCategory::Policy);
        assert_eq!(error.code, "policy.destructive_requires_exec_capability");
    }

    #[tokio::test]
    async fn explain_rejects_analyze_before_connecting() {
        let error = invoke_postgres_capability(
            &config(),
            invocation("explain", json!({ "sql": "select 1", "analyze": true })),
        )
        .await
        .expect_err("policy error");

        assert_eq!(error.category, CapabilityErrorCategory::Policy);
        assert_eq!(error.code, "policy.explain_analyze_disabled");
        assert!(error.target.is_none());
    }

    #[tokio::test]
    async fn explain_rejects_mutating_sql_before_connecting() {
        let error = invoke_postgres_capability(
            &config(),
            invocation("explain", json!({ "sql": "drop table users" })),
        )
        .await
        .expect_err("policy error");

        assert_eq!(error.category, CapabilityErrorCategory::Policy);
        assert_eq!(error.code, "policy.destructive_requires_exec_capability");
        assert!(error.target.is_none());
    }

    #[tokio::test]
    async fn exec_dry_run_does_not_connect() {
        let mut invocation = invocation("exec", json!({ "sql": "drop table users" }));
        invocation.controls.dry_run = true;

        let result = invoke_postgres_capability(&config(), invocation)
            .await
            .expect("dry run");

        assert_eq!(result.status, InvocationStatus::Succeeded);
        assert_eq!(result.output["dry_run"], true);
        assert_eq!(result.output_summary["operation"], "exec");
    }

    #[test]
    fn schema_defaults_to_public() {
        assert_eq!(schema_or_default(&json!({})).unwrap(), "public");
        assert_eq!(
            schema_or_default(&json!({ "schema": "analytics" })).unwrap(),
            "analytics"
        );
    }

    #[test]
    fn invalid_page_cursor_is_validation_error() {
        let mut invocation = invocation("query", json!({ "sql": "select 1" }));
        invocation.controls.page = Some(Pagination {
            limit: 1,
            cursor: Some("later".into()),
        });

        let error = page_request(&invocation).expect_err("cursor error");

        assert_eq!(error.category, CapabilityErrorCategory::Validation);
        assert_eq!(error.code, "validation.cursor_invalid");
    }

    #[tokio::test]
    async fn invocation_plugin_must_match() {
        let error = invoke_postgres_capability(
            &config(),
            CapabilityInvocation {
                plugin_id: "mysql".into(),
                ..invocation("query", json!({ "sql": "select 1" }))
            },
        )
        .await
        .expect_err("plugin mismatch");

        assert_eq!(error.category, CapabilityErrorCategory::Validation);
    }

    #[test]
    fn target_error_redacts_profile_values() {
        let error = target_error(
            &config(),
            "postgres.statement_failed",
            "failed postgresql://app:pg-secret@pg.internal.example:5432/warehouse".into(),
        );

        assert_eq!(error.redaction, RedactionStatus::Applied);
        let message = error
            .target
            .as_ref()
            .and_then(|target| target.message.as_ref())
            .expect("target message");
        assert!(!message.contains("pg-secret"));
        assert!(!message.contains("pg.internal.example"));
        assert!(!message.contains("warehouse"));
    }

    fn config() -> PostgresConfig {
        PostgresConfig::new(
            "pg.internal.example".into(),
            5432,
            "app".into(),
            "pg-secret".into(),
            "warehouse".into(),
        )
    }

    fn invocation(capability_id: &str, input: Value) -> CapabilityInvocation {
        CapabilityInvocation {
            id: format!("invoke-{}", capability_id),
            plugin_id: PLUGIN_ID.into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::FromProfile {
                profile: ConnectionProfileRef::name("postgres-test"),
                purpose: ConnectionInstancePurpose::CapabilityInvocation,
                reuse: InstanceReusePolicy::Never,
                options: Value::Null,
            },
            input,
            controls: InvocationControls::default(),
            actor: Some(ActorRef {
                id: "agent:test".into(),
                actor_type: ActorType::Agent,
            }),
            requested_at: Utc::now(),
        }
    }
}
