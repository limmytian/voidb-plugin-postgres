//! PostgreSQL connection factory.
//!
//! Provides a factory function for creating `tokio_postgres::Client` instances
//! from `PostgresConfig`. Supports both NoTls and NativeTls connections based
//! on the `ssl_mode` configuration.
//!
//! # Security
//!
//! The connection string is NOT logged because it may contain passwords
//! (threat T-03-01). Only `host:port` is logged in tracing calls.

use tokio_postgres::Client;

use crate::config::PostgresConfig;

/// Create a new `tokio_postgres::Client` from a `PostgresConfig`.
///
/// SSL behavior depends on `config.ssl_mode`:
/// - `None` or `"disable"`: Connect with `NoTls`
/// - `"prefer"`: Connect with NativeTls but accept invalid certs
/// - Any other value (e.g., `"require"`, `"verify-ca"`, `"verify-full"`):
///   Connect with NativeTls using strict certificate validation
///
/// The spawned connection task logs errors but does not propagate them
/// back to the caller -- the client will simply fail on the next query
/// if the connection is lost.
///
/// # Errors
///
/// Returns an error string if the connection cannot be established.
pub async fn create_connection(config: &PostgresConfig) -> Result<Client, String> {
    let conn_str = config.to_connection_string();
    let ssl_mode = config
        .ssl_mode
        .as_deref()
        .unwrap_or("disable");

    // Log only host:port, never the connection string (T-03-01)
    tracing::info!(
        "Connecting to PostgreSQL at {}:{}",
        config.host,
        config.port
    );

    if ssl_mode == "disable" {
        // No TLS
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| format!("PostgreSQL connection failed: {}", e))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(client)
    } else {
        // TLS via native-tls
        let danger_accept_invalid = ssl_mode == "prefer";

        let tls_connector = native_tls::TlsConnector::builder()
            .danger_accept_invalid_certs(danger_accept_invalid)
            .build()
            .map_err(|e| format!("TLS connector build failed: {}", e))?;

        let connector = postgres_native_tls::MakeTlsConnector::new(tls_connector);

        let (client, connection) = tokio_postgres::connect(&conn_str, connector)
            .await
            .map_err(|e| format!("PostgreSQL TLS connection failed: {}", e))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL TLS connection error: {}", e);
            }
        });

        Ok(client)
    }
}
