use thiserror::Error;

#[derive(Debug, Error)]
pub enum LunarisError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Page full: no space for {needed} bytes (available: {available})")]
    PageFull { needed: usize, available: usize },

    #[error("Row too large: {size} bytes (max: {max})")]
    RowTooLarge { size: usize, max: usize },

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Compile error: {0}")]
    Compile(String),

    #[error("VM error: {0}")]
    Vm(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Value count mismatch: expected {expected}, got {got}")]
    ValueCountMismatch { expected: usize, got: usize },

    #[error("Duplicate key: {0}")]
    DuplicateKey(u64),

    #[error("Null value for non-nullable column: {0}")]
    NullConstraint(String),
}

pub type LunarisResult<T> = Result<T, LunarisError>;
