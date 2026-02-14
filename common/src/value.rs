use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Text(String),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Boolean(v) => write!(f, "{v}"),
            Value::Text(v) => write!(f, "{v}"),
        }
    }
}

pub fn compare(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
        (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
        _ => None,
    }
}
