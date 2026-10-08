//! PostgreSQL plugin configuration structures

use serde::{Deserialize, Serialize};

/// PostgreSQL connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// Database host
    pub host: String,
    /// Database port
    pub port: u16,
    /// Username
    pub username: String,
    /// Password
    pub password: String,
    /// Database name
    pub database: String,
    /// SSL mode
    #[serde(default)]
    pub ssl_mode: Option<String>,
}

impl PostgresConfig {
    pub fn new(host: String, port: u16, username: String, password: String, database: String) -> Self {
        Self {
            host,
            port,
            username,
            password,
            database,
            ssl_mode: None,
        }
    }

    pub fn with_ssl_mode(mut self, ssl_mode: String) -> Self {
        self.ssl_mode = Some(ssl_mode);
        self
    }

    /// Build connection string for tokio-postgres
    pub fn to_connection_string(&self) -> String {
        if self.password.is_empty() {
            format!(
                "host={} port={} user={} dbname={}",
                self.host, self.port, self.username, self.database
            )
        } else {
            format!(
                "host={} port={} user={} password={} dbname={}",
                self.host, self.port, self.username, self.password, self.database
            )
        }
    }
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            username: "postgres".to_string(),
            password: String::new(),
            database: "postgres".to_string(),
            ssl_mode: None,
        }
    }
}
