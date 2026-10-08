//! Persistent one-connection PostgreSQL agent sessions.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use voidb_core::{
    AgentSessionCallRequest, AgentSessionCallResult, AgentSessionOpenContext, PluginAgentSession,
    PluginAgentSessionFactory, PluginSessionError, PluginSessionErrorCode, PluginSessionHealth,
    PluginSessionPurpose, RedactionTarget, SqlDialect, SqlSessionTransactionTracker,
    collect_redaction_targets, redact_text_with_targets, sql_allows_read_only_query,
};

use crate::{PostgresConfig, PostgresService, service::StatementResult};

pub struct PostgresAgentSessionFactory {
    config: PostgresConfig,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

impl PostgresAgentSessionFactory {
    pub fn new(config: PostgresConfig) -> Self {
        let redaction_targets = serde_json::to_value(&config)
            .map(|value| collect_redaction_targets(&value))
            .unwrap_or_default();
        Self {
            config,
            redaction_targets: Arc::new(redaction_targets),
        }
    }
}

#[async_trait]
impl PluginAgentSessionFactory for PostgresAgentSessionFactory {
    fn plugin_id(&self) -> &str {
        "postgres"
    }

    async fn open(
        &self,
        context: AgentSessionOpenContext,
    ) -> Result<Arc<dyn PluginAgentSession>, PluginSessionError> {
        if !matches!(
            context.binding.purpose,
            PluginSessionPurpose::DatabaseQuery | PluginSessionPurpose::DatabaseTransaction
        ) {
            return Err(error(
                PluginSessionErrorCode::Unsupported,
                "PostgreSQL persistent sessions require a database purpose.",
            ));
        }
        if context
            .binding
            .allowed_capabilities
            .iter()
            .any(|capability| {
                !matches!(
                    capability.as_str(),
                    "postgres.query" | "query" | "postgres.exec" | "exec"
                )
            })
        {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "PostgreSQL session capability scope is limited to query and exec.",
            ));
        }
        let service = PostgresService::new_direct(&self.config)
            .await
            .map_err(|_| {
                error(
                    PluginSessionErrorCode::OwnerUnavailable,
                    "PostgreSQL session connection could not be opened.",
                )
            })?;
        let transaction = SqlSessionTransactionTracker::from_open_input(
            SqlDialect::Postgres,
            &context.request.input,
        )
        .map_err(|reason| {
            PluginSessionError::new(
                PluginSessionErrorCode::PolicyDenied,
                format!("PostgreSQL session transaction options are invalid: {reason}"),
            )
        })?;
        if let Some(sql) = transaction.isolation_setup_sql() {
            let results = service.execute_query(sql).await.map_err(|_| {
                error(
                    PluginSessionErrorCode::OwnerUnavailable,
                    "PostgreSQL session isolation could not be configured.",
                )
            })?;
            if results_have_error(&results) {
                return Err(error(
                    PluginSessionErrorCode::Unsupported,
                    "PostgreSQL session isolation is unsupported by the target.",
                ));
            }
        }
        Ok(Arc::new(PostgresAgentSession {
            service: Mutex::new(Some(service)),
            transaction: Mutex::new(transaction),
            redaction_targets: Arc::clone(&self.redaction_targets),
        }))
    }
}

struct PostgresAgentSession {
    service: Mutex<Option<PostgresService>>,
    transaction: Mutex<SqlSessionTransactionTracker>,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

#[async_trait]
impl PluginAgentSession for PostgresAgentSession {
    async fn call(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        let sql = request
            .input
            .get("sql")
            .and_then(Value::as_str)
            .filter(|sql| !sql.trim().is_empty())
            .ok_or_else(|| error(PluginSessionErrorCode::PolicyDenied, "SQL is required."))?;
        match request.capability.as_str() {
            "postgres.query" if !sql_allows_read_only_query(sql, SqlDialect::Postgres) => {
                return Err(error(
                    PluginSessionErrorCode::PolicyDenied,
                    "postgres.query accepts read-oriented SQL only; use postgres.exec.",
                ));
            }
            "postgres.exec" if !request.destructive_acknowledged => {
                return Err(error(
                    PluginSessionErrorCode::PolicyDenied,
                    "postgres.exec requires destructive acknowledgement.",
                ));
            }
            "postgres.query" | "postgres.exec" => {}
            _ => {
                return Err(error(
                    PluginSessionErrorCode::PolicyDenied,
                    "PostgreSQL sessions accept postgres.query and postgres.exec.",
                ));
            }
        }
        let mut transaction = self.transaction.lock().await;
        let plan = transaction.plan(sql).map_err(|reason| {
            PluginSessionError::new(
                PluginSessionErrorCode::PolicyDenied,
                format!("PostgreSQL transaction transition is invalid: {reason}"),
            )
        })?;
        let guard = self.service.lock().await;
        let service = guard.as_ref().ok_or_else(|| {
            error(
                PluginSessionErrorCode::OwnerUnavailable,
                "PostgreSQL session is closed.",
            )
        })?;
        let results = service.execute_query(sql).await.map_err(|_| {
            error(
                PluginSessionErrorCode::OwnerUnavailable,
                "PostgreSQL session SQL failed.",
            )
        })?;
        if results_have_error(&results) {
            let rollback_succeeded = if transaction.failure_requires_rollback(&plan) {
                service
                    .execute_query("ROLLBACK")
                    .await
                    .is_ok_and(|results| !results_have_error(&results))
            } else {
                true
            };
            transaction.record_failure(&plan, rollback_succeeded);
        } else {
            transaction.record_success(plan);
        }
        bounded_results(
            request,
            results,
            transaction.status(),
            &self.redaction_targets,
        )
    }

    async fn health(&self) -> Result<PluginSessionHealth, PluginSessionError> {
        Ok(if self.service.lock().await.is_some() {
            PluginSessionHealth::Ready
        } else {
            PluginSessionHealth::Closed
        })
    }
    async fn cancel(&self, _call_id: &str) -> Result<(), PluginSessionError> {
        close_service(&self.transaction, &self.service).await;
        Ok(())
    }
    async fn close(&self, _reason: String) -> Result<(), PluginSessionError> {
        close_service(&self.transaction, &self.service).await;
        Ok(())
    }
}

async fn close_service(
    transaction: &Mutex<SqlSessionTransactionTracker>,
    service: &Mutex<Option<PostgresService>>,
) {
    let _transaction = transaction.lock().await;
    if let Some(service) = service.lock().await.take() {
        let _ = service.execute_query("ROLLBACK").await;
    }
}

fn bounded_results(
    request: AgentSessionCallRequest,
    results: Vec<StatementResult>,
    transaction: Value,
    redaction_targets: &[RedactionTarget],
) -> Result<AgentSessionCallResult, PluginSessionError> {
    let max_rows = request
        .input
        .get("max_rows")
        .and_then(Value::as_u64)
        .unwrap_or(100)
        .clamp(1, 1000) as usize;
    let statements = results.into_iter().map(|result| match result {
        StatementResult::Select { columns, rows } => { let source_count = rows.len(); json!({ "kind": "rows", "columns": columns.iter().map(|column| column.name.clone()).collect::<Vec<_>>(), "rows": rows.into_iter().take(max_rows).map(|row| row.values.into_iter().map(|value| voidb_core::formatters::cell_value_to_json(&value)).collect::<Vec<_>>()).collect::<Vec<_>>(), "source_count": source_count, "truncated": source_count > max_rows }) },
        StatementResult::Affected(rows) => json!({ "kind": "affected", "rows": rows }),
        StatementResult::Empty => json!({ "kind": "empty" }),
        StatementResult::Error(_) => json!({ "kind": "error", "message": "PostgreSQL statement failed; issue ROLLBACK before continuing the failed transaction." }),
    }).collect::<Vec<_>>();
    let encoded = serde_json::to_string(
        &json!({ "statements": statements, "max_rows": max_rows, "transaction": transaction }),
    )
    .map_err(|_| {
        error(
            PluginSessionErrorCode::OwnerUnavailable,
            "PostgreSQL session output could not be encoded.",
        )
    })?;
    let (redacted, _) = redact_text_with_targets(&encoded, redaction_targets);
    let output = serde_json::from_str(&redacted)
        .unwrap_or_else(|_| json!({ "statements": [], "redacted": true }));
    AgentSessionCallResult::bounded(request.call_id, output, request.output_limit_bytes)
}

fn results_have_error(results: &[StatementResult]) -> bool {
    results
        .iter()
        .any(|result| matches!(result, StatementResult::Error(_)))
}

fn error(code: PluginSessionErrorCode, message: &str) -> PluginSessionError {
    PluginSessionError::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn result_redaction_withholds_profile_password() {
        let factory = PostgresAgentSessionFactory::new(PostgresConfig::new(
            "invalid".into(),
            5432,
            "user".into(),
            "super-secret".into(),
            "db".into(),
        ));
        let request = AgentSessionCallRequest {
            session: voidb_core::AgentSessionRef::new("session", 1),
            call_id: "call".into(),
            capability: "postgres.query".into(),
            input: json!({"sql":"SELECT 1"}),
            destructive_acknowledged: false,
            timeout_ms: Some(1),
            output_limit_bytes: 4096,
        };
        let output = bounded_results(
            request,
            vec![StatementResult::Error("super-secret".into())],
            json!({ "state": "idle" }),
            &factory.redaction_targets,
        )
        .unwrap();
        assert!(!output.output.to_string().contains("super-secret"));
    }
}
