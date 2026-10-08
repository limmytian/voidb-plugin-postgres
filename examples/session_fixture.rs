//! Disposable PostgreSQL fixture proof for persistent agent sessions.

use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use chrono::{Duration, Utc};
use serde_json::{Value, json};
use voidb_core::{
    AgentSessionBinding, AgentSessionCallRequest, AgentSessionOpenContext, AgentSessionOpenRequest,
    AgentSessionRef, PluginAgentSession, PluginAgentSessionFactory, PluginSessionPurpose,
};
use voidb_plugin_postgres::{PostgresAgentSessionFactory, PostgresConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config = PostgresConfig::new(
        required("VOIDB_POSTGRES_SESSION_HOST")?,
        required("VOIDB_POSTGRES_SESSION_PORT")?.parse()?,
        required("VOIDB_POSTGRES_SESSION_USER")?,
        required("VOIDB_POSTGRES_SESSION_PASSWORD")?,
        required("VOIDB_POSTGRES_SESSION_DATABASE")?,
    );
    let factory = PostgresAgentSessionFactory::new(config);
    let session = factory
        .open(context())
        .await
        .map_err(|error| anyhow::anyhow!("open PostgreSQL session: {error}"))?;

    call(
        &session,
        "postgres.exec",
        "SET application_name='voidb-agent-session'; CREATE TEMP TABLE kept(value INT); BEGIN; INSERT INTO kept VALUES (7)",
        true,
    )
    .await?;
    let state = call(
        &session,
        "postgres.query",
        "SELECT current_setting('application_name')='voidb-agent-session', COUNT(*) FROM kept",
        false,
    )
    .await?;
    ensure!(
        state["statements"][0]["rows"][0][0] == true
            && as_u64(&state["statements"][0]["rows"][0][1]) == Some(1),
        "PostgreSQL session state was not retained: {state}"
    );
    call(&session, "postgres.exec", "ROLLBACK", true).await?;
    let rolled_back = call(
        &session,
        "postgres.query",
        "SELECT COUNT(*) FROM kept",
        false,
    )
    .await?;
    ensure!(
        as_u64(&rolled_back["statements"][0]["rows"][0][0]) == Some(0),
        "PostgreSQL rollback did not preserve the temp table while clearing rows: {rolled_back}"
    );

    call(&session, "postgres.exec", "BEGIN", true).await?;
    let failed = call(&session, "postgres.query", "SELECT 1 / 0", false).await?;
    ensure!(failed["statements"][0]["kind"] == "error");
    call(&session, "postgres.exec", "ROLLBACK", true).await?;
    let recovered = call(&session, "postgres.query", "SELECT 42", false).await?;
    ensure!(as_u64(&recovered["statements"][0]["rows"][0][0]) == Some(42));

    call(
        &session,
        "postgres.query",
        "SELECT pg_advisory_lock(850085)",
        false,
    )
    .await?;
    call(
        &session,
        "postgres.query",
        "SELECT pg_advisory_unlock(850085)",
        false,
    )
    .await?;
    session.close("fixture close".into()).await?;
    println!("postgres persistent agent session fixture passed");
    Ok(())
}

fn context() -> AgentSessionOpenContext {
    let purpose = PluginSessionPurpose::DatabaseTransaction;
    AgentSessionOpenContext {
        binding: AgentSessionBinding {
            grant_id: "postgres-fixture-grant".into(),
            profile_id: "postgres-fixture-profile".into(),
            plugin_id: "postgres".into(),
            purpose: purpose.clone(),
            allowed_capabilities: vec!["postgres.query".into(), "postgres.exec".into()],
            host_generation: 1,
        },
        request: AgentSessionOpenRequest {
            purpose,
            capabilities: vec!["postgres.query".into(), "postgres.exec".into()],
            lease_seconds: 60,
            concurrency: Default::default(),
            destructive_acknowledged: false,
            input: Value::Null,
        },
        lease_expires_at: Utc::now() + Duration::seconds(60),
    }
}

async fn call(
    session: &Arc<dyn PluginAgentSession>,
    capability: &str,
    sql: &str,
    acknowledged: bool,
) -> Result<Value> {
    session
        .call(AgentSessionCallRequest {
            session: AgentSessionRef::new("postgres-fixture-session", 1),
            call_id: format!("postgres-session-{capability}"),
            capability: capability.into(),
            input: json!({ "sql": sql, "max_rows": 100 }),
            destructive_acknowledged: acknowledged,
            timeout_ms: Some(10_000),
            output_limit_bytes: 128 * 1024,
        })
        .await
        .map(|result| result.output)
        .map_err(|error| anyhow::anyhow!("PostgreSQL session call failed: {error}"))
}

fn required(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} is required"))
}

fn as_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}
