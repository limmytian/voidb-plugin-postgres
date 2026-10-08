//! Centralized PostgreSQL value conversion utilities.
//!
//! This module provides pure functions for converting between `tokio_postgres::Row`
//! values and `voidb_core::CellValue`, as well as SQL generation helpers
//! (`escape_pg_value`, `build_pk_where`, `quote_ident`). These are shared by the
//! CRUD, data-loading, and schema inner services, eliminating the value-conversion
//! duplication that previously existed across `db_browser.rs`, `table_plugin.rs`,
//! and `lib.rs`.

use anyhow::Result;
use voidb_core::{CellValue, ColumnInfo, Row};

/// Decode a single column value from a `tokio_postgres::Row` to `CellValue`.
///
/// Uses the most robust conversion strategy: for temporal types (TIMESTAMP,
/// DATE, TIME), tries String decoding first with chrono fallback. This handles
/// edge cases like infinity timestamps that chrono cannot parse.
///
/// # Arguments
///
/// * `row` - The tokio_postgres Row containing the value.
/// * `idx` - Column index within the row.
pub fn decode_value(row: &tokio_postgres::Row, idx: usize) -> CellValue {
    use tokio_postgres::types::Type;

    let col_type = row.columns()[idx].type_();

    match *col_type {
        Type::BOOL => row
            .try_get::<_, Option<bool>>(idx)
            .ok()
            .flatten()
            .map(CellValue::Bool)
            .unwrap_or(CellValue::Null),

        Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .ok()
            .flatten()
            .map(|v| CellValue::Int(v as i64))
            .unwrap_or(CellValue::Null),

        Type::INT4 | Type::OID => row
            .try_get::<_, Option<i32>>(idx)
            .ok()
            .flatten()
            .map(|v| CellValue::Int(v as i64))
            .unwrap_or(CellValue::Null),

        Type::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .ok()
            .flatten()
            .map(CellValue::Int)
            .unwrap_or(CellValue::Null),

        Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(idx)
            .ok()
            .flatten()
            .map(|v| CellValue::Float(v as f64))
            .unwrap_or(CellValue::Null),

        Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .map(CellValue::Float)
            .unwrap_or(CellValue::Null),

        // NUMERIC: try f64 first -- tokio-postgres supports this for most
        // numeric values. Fall back to String for very large/precise decimals.
        Type::NUMERIC => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .map(CellValue::Float)
            .unwrap_or_else(|| {
                row.try_get::<_, Option<String>>(idx)
                    .ok()
                    .flatten()
                    .map(CellValue::Text)
                    .unwrap_or(CellValue::Null)
            }),

        Type::BYTEA => row
            .try_get::<_, Option<Vec<u8>>>(idx)
            .ok()
            .flatten()
            .map(CellValue::Blob)
            .unwrap_or(CellValue::Null),

        // Temporal types: try String first (handles infinity, -infinity),
        // fall back to chrono for well-formed values.
        Type::TIMESTAMP | Type::TIMESTAMPTZ => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .map(CellValue::DateTime)
            .unwrap_or_else(|| {
                row.try_get::<_, Option<chrono::NaiveDateTime>>(idx)
                    .ok()
                    .flatten()
                    .map(|dt| CellValue::DateTime(dt.to_string()))
                    .unwrap_or(CellValue::Null)
            }),

        Type::DATE => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .map(CellValue::Date)
            .unwrap_or_else(|| {
                row.try_get::<_, Option<chrono::NaiveDate>>(idx)
                    .ok()
                    .flatten()
                    .map(|d| CellValue::Date(d.to_string()))
                    .unwrap_or(CellValue::Null)
            }),

        Type::TIME | Type::TIMETZ => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .map(CellValue::Time)
            .unwrap_or_else(|| {
                row.try_get::<_, Option<chrono::NaiveTime>>(idx)
                    .ok()
                    .flatten()
                    .map(|t| CellValue::Time(t.to_string()))
                    .unwrap_or(CellValue::Null)
            }),

        Type::UUID => row
            .try_get::<_, Option<uuid::Uuid>>(idx)
            .ok()
            .flatten()
            .map(|u| CellValue::Uuid(u.to_string()))
            .unwrap_or_else(|| {
                row.try_get::<_, Option<String>>(idx)
                    .ok()
                    .flatten()
                    .map(CellValue::Uuid)
                    .unwrap_or(CellValue::Null)
            }),

        Type::JSON | Type::JSONB => row
            .try_get::<_, Option<serde_json::Value>>(idx)
            .ok()
            .flatten()
            .map(|v| CellValue::Json(v.to_string()))
            .unwrap_or_else(|| {
                row.try_get::<_, Option<String>>(idx)
                    .ok()
                    .flatten()
                    .map(CellValue::Json)
                    .unwrap_or(CellValue::Null)
            }),

        // VARCHAR, TEXT, CHAR, NAME, BPCHAR, and all other types
        _ => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .map(CellValue::Text)
            .unwrap_or(CellValue::Null),
    }
}

/// Extract column metadata from a `tokio_postgres::Row`.
///
/// Returns column names and type names based on the row's column metadata.
/// Note: nullable, primary key, default, and extra info are not available
/// from row metadata alone -- these are populated from information_schema queries.
pub fn extract_columns(row: &tokio_postgres::Row) -> Vec<ColumnInfo> {
    row.columns()
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
        .collect()
}

/// Convert a `tokio_postgres::Row` to a `voidb_core::Row`.
///
/// Calls `decode_value` for each column index 0..col_count.
pub fn convert_row(row: &tokio_postgres::Row, col_count: usize) -> Row {
    let values: Vec<CellValue> = (0..col_count).map(|i| decode_value(row, i)).collect();
    Row { values }
}

// ---------------------------------------------------------------------------
// SQL generation helpers
// ---------------------------------------------------------------------------

/// Escape a `CellValue` for use in raw PostgreSQL SQL statements.
///
/// This produces a SQL literal suitable for embedding in generated
/// UPDATE/DELETE/INSERT statements. PostgreSQL-specific conventions:
/// - Booleans use TRUE/FALSE (not 1/0)
/// - Blob uses `E'\\x{hex}'` syntax
/// - Strings are single-quote escaped with `''`
pub fn escape_pg_value(v: &CellValue) -> String {
    match v {
        CellValue::Null => "NULL".into(),
        CellValue::Bool(b) => {
            if *b {
                "TRUE".into()
            } else {
                "FALSE".into()
            }
        }
        CellValue::Int(i) => i.to_string(),
        CellValue::Float(f) => f.to_string(),
        CellValue::Text(s) => format!("'{}'", s.replace('\'', "''")),
        CellValue::Date(s) | CellValue::Time(s) | CellValue::DateTime(s) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        CellValue::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            format!("E'\\\\x{}'", hex)
        }
        CellValue::Json(s) => format!("'{}'", s.replace('\'', "''")),
        CellValue::Uuid(s) => format!("'{}'", s),
        CellValue::Unknown(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

/// Quote a PostgreSQL identifier (schema, table, or column name).
///
/// Wraps the name in double quotes and escapes embedded double quotes
/// by doubling them, per PostgreSQL identifier quoting rules.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build a WHERE clause from primary key columns and a data row.
///
/// Produces expressions like `"id" = 42 AND "name" = 'foo'`.
/// NULL PKs are handled with `IS NULL`.
///
/// # Errors
///
/// Returns an error if `pk_cols` is empty or a PK column is not found
/// in the column list.
pub fn build_pk_where(columns: &[ColumnInfo], row: &Row, pk_cols: &[String]) -> Result<String> {
    if pk_cols.is_empty() {
        return Err(anyhow::anyhow!("No primary key -- cannot identify row"));
    }

    let mut conditions = Vec::new();
    for pk in pk_cols {
        let idx = columns
            .iter()
            .position(|c| c.name == *pk)
            .ok_or_else(|| anyhow::anyhow!("PK column '{}' not found", pk))?;

        let val = &row.values[idx];
        if matches!(val, CellValue::Null) {
            conditions.push(format!("{} IS NULL", quote_ident(pk)));
        } else {
            conditions.push(format!("{} = {}", quote_ident(pk), escape_pg_value(val)));
        }
    }

    Ok(conditions.join(" AND "))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // === escape_pg_value tests ===

    #[test]
    fn test_escape_pg_value_null() {
        assert_eq!(escape_pg_value(&CellValue::Null), "NULL");
    }

    #[test]
    fn test_escape_pg_value_bool_true() {
        assert_eq!(escape_pg_value(&CellValue::Bool(true)), "TRUE");
    }

    #[test]
    fn test_escape_pg_value_bool_false() {
        assert_eq!(escape_pg_value(&CellValue::Bool(false)), "FALSE");
    }

    #[test]
    fn test_escape_pg_value_int() {
        assert_eq!(escape_pg_value(&CellValue::Int(42)), "42");
    }

    #[test]
    fn test_escape_pg_value_float() {
        let result = escape_pg_value(&CellValue::Float(12.5));
        assert!(result.starts_with("12.5"));
    }

    #[test]
    fn test_escape_pg_value_text_with_quotes() {
        assert_eq!(
            escape_pg_value(&CellValue::Text("it's".into())),
            "'it''s'"
        );
    }

    #[test]
    fn test_escape_pg_value_blob() {
        assert_eq!(
            escape_pg_value(&CellValue::Blob(vec![0xDE, 0xAD])),
            "E'\\\\xdead'"
        );
    }

    #[test]
    fn test_escape_pg_value_datetime() {
        assert_eq!(
            escape_pg_value(&CellValue::DateTime("2024-01-15 10:30:00".into())),
            "'2024-01-15 10:30:00'"
        );
    }

    // === quote_ident tests ===

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("my_table"), "\"my_table\"");
    }

    #[test]
    fn test_quote_ident_with_embedded_double_quote() {
        assert_eq!(quote_ident("my\"table"), "\"my\"\"table\"");
    }

    // === build_pk_where tests ===

    #[test]
    fn test_build_pk_where_single() {
        let columns = vec![ColumnInfo {
            name: "id".into(),
            data_type: "INT".into(),
            nullable: false,
            is_primary_key: true,
            default_value: None,
            max_length: None,
            extra: String::new(),
        }];
        let row = Row {
            values: vec![CellValue::Int(42)],
        };
        let result = build_pk_where(&columns, &row, &["id".into()]).unwrap();
        assert_eq!(result, "\"id\" = 42");
    }

    #[test]
    fn test_build_pk_where_composite() {
        let columns = vec![
            ColumnInfo {
                name: "a".into(),
                data_type: "INT".into(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                max_length: None,
                extra: String::new(),
            },
            ColumnInfo {
                name: "b".into(),
                data_type: "VARCHAR".into(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
                max_length: None,
                extra: String::new(),
            },
        ];
        let row = Row {
            values: vec![CellValue::Int(1), CellValue::Text("x".into())],
        };
        let result = build_pk_where(&columns, &row, &["a".into(), "b".into()]).unwrap();
        assert_eq!(result, "\"a\" = 1 AND \"b\" = 'x'");
    }

    #[test]
    fn test_build_pk_where_null_pk() {
        let columns = vec![ColumnInfo {
            name: "id".into(),
            data_type: "INT".into(),
            nullable: true,
            is_primary_key: true,
            default_value: None,
            max_length: None,
            extra: String::new(),
        }];
        let row = Row {
            values: vec![CellValue::Null],
        };
        let result = build_pk_where(&columns, &row, &["id".into()]).unwrap();
        assert_eq!(result, "\"id\" IS NULL");
    }

    #[test]
    fn test_build_pk_where_no_pks() {
        let columns = vec![ColumnInfo {
            name: "id".into(),
            data_type: "INT".into(),
            nullable: false,
            is_primary_key: true,
            default_value: None,
            max_length: None,
            extra: String::new(),
        }];
        let row = Row {
            values: vec![CellValue::Int(1)],
        };
        let result = build_pk_where(&columns, &row, &[]);
        assert!(result.is_err());
    }
}
