use crate::error::{LunarisError, LunarisResult};
use lunaris_common::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnType {
    Integer,
    Float,
    Boolean,
    Varchar(u16),
}

impl ColumnType {
    pub fn byte_size(&self) -> usize {
        match self {
            ColumnType::Integer => 8,
            ColumnType::Float => 8,
            ColumnType::Boolean => 1,
            ColumnType::Varchar(n) => 2 + *n as usize,
        }
    }
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::Integer => write!(f, "INTEGER"),
            ColumnType::Float => write!(f, "FLOAT"),
            ColumnType::Boolean => write!(f, "BOOLEAN"),
            ColumnType::Varchar(n) => write!(f, "VARCHAR({n})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub row_size: usize,
}

impl TableSchema {
    pub fn new(table_name: String, columns: Vec<ColumnDef>) -> Self {
        let bitmap_size = columns.len().div_ceil(8);
        let data_size: usize = columns.iter().map(|c| c.col_type.byte_size()).sum();
        let row_size = bitmap_size + data_size;
        Self {
            table_name,
            columns,
            row_size,
        }
    }

    pub fn find_column(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn bitmap_size(&self) -> usize {
        self.columns.len().div_ceil(8)
    }
}

pub fn serialize_row(schema: &TableSchema, values: &[Value]) -> LunarisResult<Vec<u8>> {
    if values.len() != schema.columns.len() {
        return Err(LunarisError::ValueCountMismatch {
            expected: schema.columns.len(),
            got: values.len(),
        });
    }

    let mut buf = vec![0u8; schema.row_size];
    let mut offset = schema.bitmap_size();
    for (i, (col, val)) in schema.columns.iter().zip(values.iter()).enumerate() {
        if *val == Value::Null {
            // Set bit i in the null bitmap
            buf[i / 8] |= 1 << (i % 8);
            offset += col.col_type.byte_size();
            continue;
        }

        match (&col.col_type, val) {
            (ColumnType::Integer, Value::Integer(v)) => {
                buf[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
                offset += 8;
            }
            (ColumnType::Float, Value::Float(v)) => {
                buf[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
                offset += 8;
            }
            (ColumnType::Boolean, Value::Boolean(v)) => {
                buf[offset] = if *v { 1 } else { 0 };
                offset += 1;
            }
            (ColumnType::Varchar(max_len), Value::Text(s)) => {
                let bytes = s.as_bytes();
                let len = bytes.len().min(*max_len as usize);
                buf[offset..offset + 2].copy_from_slice(&(len as u16).to_le_bytes());
                buf[offset + 2..offset + 2 + len].copy_from_slice(&bytes[..len]);
                // remaining bytes stay zero (padding)
                offset += 2 + *max_len as usize;
            }
            _ => {
                return Err(LunarisError::TypeMismatch {
                    expected: col.col_type.to_string(),
                    got: format!("{val:?}"),
                });
            }
        }
    }

    Ok(buf)
}

pub fn deserialize_row(schema: &TableSchema, data: &[u8]) -> LunarisResult<Vec<Value>> {
    let mut values = Vec::with_capacity(schema.columns.len());
    let mut offset = schema.bitmap_size();
    for (i, col) in schema.columns.iter().enumerate() {
        let is_null = (data[i / 8] >> (i % 8)) & 1 == 1;
        if is_null {
            values.push(Value::Null);
            offset += col.col_type.byte_size();
            continue;
        }

        let val = match &col.col_type {
            ColumnType::Integer => {
                let v = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                Value::Integer(v)
            }
            ColumnType::Float => {
                let v = f64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                Value::Float(v)
            }
            ColumnType::Boolean => {
                let v = data[offset] != 0;
                offset += 1;
                Value::Boolean(v)
            }
            ColumnType::Varchar(max_len) => {
                let len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
                let s = String::from_utf8_lossy(&data[offset + 2..offset + 2 + len]).to_string();
                offset += 2 + *max_len as usize;
                Value::Text(s)
            }
        };
        values.push(val);
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> TableSchema {
        TableSchema::new(
            "test".into(),
            vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: ColumnType::Integer,
                },
                ColumnDef {
                    name: "name".into(),
                    col_type: ColumnType::Varchar(20),
                },
                ColumnDef {
                    name: "active".into(),
                    col_type: ColumnType::Boolean,
                },
            ],
        )
    }

    #[test]
    fn test_roundtrip() {
        let schema = test_schema();
        let values = vec![
            Value::Integer(42),
            Value::Text("Alice".into()),
            Value::Boolean(true),
        ];
        let data = serialize_row(&schema, &values).unwrap();
        assert_eq!(data.len(), schema.row_size);
        let restored = deserialize_row(&schema, &data).unwrap();
        assert_eq!(restored, values);
    }

    #[test]
    fn test_null_values() {
        let schema = test_schema();
        let values = vec![Value::Integer(1), Value::Null, Value::Boolean(false)];
        let data = serialize_row(&schema, &values).unwrap();
        let restored = deserialize_row(&schema, &data).unwrap();
        assert_eq!(restored, values);
    }

    #[test]
    fn test_wrong_count() {
        let schema = test_schema();
        let result = serialize_row(&schema, &[Value::Integer(1)]);
        assert!(result.is_err());
    }
}
