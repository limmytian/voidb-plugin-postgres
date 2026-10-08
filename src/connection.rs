/// PostgreSQL Connection Manager integration.
///
/// Implements ConnectionDataProvider and ConnectionDisplayProvider traits
/// for PostgreSQL plugin to provide connection metadata and display configuration.
use serde_json::{json, Value};
use std::collections::HashMap;

use voidb_core::connection::ConnectionConfig;
use voidb_core::plugin::{
    ConnectionDataProvider, ConnectionDisplayProvider, ConnectionSummary, DisplayLayout,
    ColumnDefinition, ColumnWidth, ColumnFormat, Alignment, Badge, BadgeStyle,
    FieldDefinition, FieldType, ValidationRule, QuickAction, DisplayConfig,
    ViewMode, DefaultColumn, SortOption, FilterOption,
};

/// PostgreSQL plugin connection data provider
pub struct PostgresConnectionProvider;

impl PostgresConnectionProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PostgresConnectionProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionDataProvider for PostgresConnectionProvider {
    fn plugin_id(&self) -> &str {
        "postgres"
    }

    fn get_connection_summary(&self, config: &ConnectionConfig) -> ConnectionSummary {
        use crate::config::PostgresConfig;

        let pg_config: PostgresConfig = config.plugin_config
            .as_ref()
            .and_then(|pc| serde_json::from_value(pc.clone()).ok())
            .unwrap_or_default();

        let host = &pg_config.host;
        let port = pg_config.port;
        let database = &pg_config.database;
        let user = &pg_config.username;

        let has_ssl = pg_config
            .ssl_mode
            .as_ref()
            .map(|s| s == "require" || s == "prefer")
            .unwrap_or(false);

        let mut badges = Vec::new();
        if has_ssl {
            badges.push(Badge {
                text: "SSL".to_string(),
                style: BadgeStyle::Success,
                icon: Some("🔒".to_string()),
            });
        }

        let mut metadata = HashMap::new();
        if let Some(ref ssl_mode) = pg_config.ssl_mode {
            metadata.insert("ssl_mode".to_string(), ssl_mode.clone());
        }

        ConnectionSummary {
            icon: "🐘".to_string(),
            display_name: config.name.clone(),
            type_label: "PostgreSQL".to_string(),
            primary_info: format!("{}:{}", host, port),
            secondary_info: if database.is_empty() {
                format!("user: {}", user)
            } else {
                format!("database: {} | user: {}", database, user)
            },
            tags: vec!["database".to_string(), "sql".to_string()],
            metadata,
            display_layout: DisplayLayout {
                columns: vec![],
                badges,
                inline_widgets: Vec::new(),
            },
        }
    }

    fn get_custom_fields(&self) -> Vec<FieldDefinition> {
        vec![
            FieldDefinition {
                name: "ssl_mode".to_string(),
                label: "SSL Mode".to_string(),
                field_type: FieldType::Select {
                    options: vec![
                        "disable".to_string(),
                        "prefer".to_string(),
                        "require".to_string(),
                    ],
                },
                default_value: Some(json!("prefer")),
                validation: ValidationRule::OneOf(vec![
                    "disable".to_string(),
                    "prefer".to_string(),
                    "require".to_string(),
                ]),
                help_text: Some("SSL/TLS connection mode".to_string()),
                required: false,
            },
            FieldDefinition {
                name: "connect_timeout".to_string(),
                label: "Connection Timeout".to_string(),
                field_type: FieldType::Number,
                default_value: Some(json!(10)),
                validation: ValidationRule::Range { min: 1, max: 300 },
                help_text: Some("Connection timeout in seconds (1-300)".to_string()),
                required: false,
            },
        ]
    }

    fn validate_custom_field(&self, name: &str, value: &Value) -> Result<(), String> {
        match name {
            "ssl_mode" => {
                let ssl_mode = value
                    .as_str()
                    .ok_or_else(|| "SSL mode must be a string".to_string())?;
                let valid_modes = ["disable", "prefer", "require"];
                if !valid_modes.contains(&ssl_mode) {
                    return Err(format!(
                        "Invalid SSL mode '{}'. Must be one of: {}",
                        ssl_mode,
                        valid_modes.join(", ")
                    ));
                }
                Ok(())
            }
            "connect_timeout" => {
                let timeout = value
                    .as_i64()
                    .ok_or_else(|| "Connect timeout must be a number".to_string())?;
                if !(1..=300).contains(&timeout) {
                    return Err(format!(
                        "Connect timeout must be between 1 and 300, got {}",
                        timeout
                    ));
                }
                Ok(())
            }
            _ => Err(format!("Unknown field: {}", name)),
        }
    }

    fn get_quick_actions(&self) -> Vec<QuickAction> {
        vec![
            QuickAction {
                id: "open_sql_editor".to_string(),
                label: "SQL Editor".to_string(),
                icon: Some("📝".to_string()),
                description: Some("Open SQL query editor".to_string()),
            },
            QuickAction {
                id: "show_databases".to_string(),
                label: "Show Databases".to_string(),
                icon: Some("📂".to_string()),
                description: Some("List all databases".to_string()),
            },
            QuickAction {
                id: "connection_info".to_string(),
                label: "Info".to_string(),
                icon: Some("ℹ\u{fe0f}".to_string()),
                description: Some("Show connection details".to_string()),
            },
        ]
    }
}

impl ConnectionDisplayProvider for PostgresConnectionProvider {
    fn plugin_id(&self) -> &str {
        "postgres"
    }

    fn get_display_config(&self) -> DisplayConfig {
        DisplayConfig {
            view_mode: ViewMode::Table,
            default_columns: vec![
                DefaultColumn::Status,
                DefaultColumn::Icon,
                DefaultColumn::Name,
                DefaultColumn::Type,
                DefaultColumn::Address,
                DefaultColumn::Database,
                DefaultColumn::User,
                DefaultColumn::LastUsed,
            ],
            custom_columns: vec![
                ColumnDefinition {
                    id: "ssl".to_string(),
                    label: "SSL".to_string(),
                    value: String::new(),
                    width: ColumnWidth::Fixed(5),
                    alignment: Alignment::Center,
                    format: ColumnFormat::Badge,
                },
            ],
            sort_options: vec![
                SortOption::Name,
                SortOption::LastUsed,
                SortOption::Status,
                SortOption::Custom("database".to_string()),
            ],
            filter_options: vec![
                FilterOption::ByStatus,
                FilterOption::ByTag,
                FilterOption::Custom {
                    id: "has_ssl".to_string(),
                    label: "With SSL".to_string(),
                    description: Some("Show only SSL-enabled connections".to_string()),
                },
            ],
        }
    }

    fn has_custom_rendering(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use voidb_core::connection::DatabaseType;

    #[test]
    fn test_get_connection_summary() {
        let provider = PostgresConnectionProvider::new();
        let config = ConnectionConfig {
            name: "test-pg".to_string(),
            db_type: DatabaseType::PostgreSQL,
            plugin_id: Some("postgres".to_string()),
            plugin_config: Some(json!({
                "host": "10.0.0.1",
                "port": 5432,
                "username": "admin",
                "password": "password",
                "database": "mydb",
                "ssl_mode": "require"
            })),
        };

        let summary = provider.get_connection_summary(&config);

        assert_eq!(summary.icon, "🐘");
        assert_eq!(summary.display_name, "test-pg");
        assert_eq!(summary.type_label, "PostgreSQL");
        assert_eq!(summary.primary_info, "10.0.0.1:5432");
        assert!(summary.secondary_info.contains("mydb"));
        assert!(summary.secondary_info.contains("admin"));
        assert_eq!(summary.display_layout.badges.len(), 1);
        assert_eq!(summary.display_layout.badges[0].text, "SSL");
    }

    #[test]
    fn test_validate_custom_field() {
        let provider = PostgresConnectionProvider::new();

        assert!(provider.validate_custom_field("ssl_mode", &json!("require")).is_ok());
        assert!(provider.validate_custom_field("ssl_mode", &json!("invalid")).is_err());
        assert!(provider.validate_custom_field("connect_timeout", &json!(10)).is_ok());
        assert!(provider.validate_custom_field("connect_timeout", &json!(500)).is_err());
        assert!(provider.validate_custom_field("connect_timeout", &json!(0)).is_err());
    }

    #[test]
    fn test_get_custom_fields() {
        let provider = PostgresConnectionProvider::new();
        let fields = provider.get_custom_fields();

        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "ssl_mode");
        assert_eq!(fields[1].name, "connect_timeout");
    }

    #[test]
    fn test_get_quick_actions() {
        let provider = PostgresConnectionProvider::new();
        let actions = provider.get_quick_actions();

        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0].id, "open_sql_editor");
        assert_eq!(actions[1].id, "show_databases");
        assert_eq!(actions[2].id, "connection_info");
    }
}
