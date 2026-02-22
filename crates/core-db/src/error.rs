use thiserror::Error;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("document not found: {0}")]
    DocumentNotFound(String),

    #[error("duplicate document: {0}")]
    DuplicateDocument(String),

    #[error("schema violation: {0}")]
    SchemaViolation(String),

    #[error("invalid field name: {0}")]
    InvalidFieldName(String),

    #[error("transaction conflict: {0}")]
    TransactionConflict(String),

    #[error("index error: {0}")]
    IndexError(String),
}
