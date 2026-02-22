use thiserror::Error;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("document not found: {0}")]
    DocumentNotFound(String),
    #[error("invalid operation: {0}")]
    InvalidOperation(String),
    #[error("schema violation: {0}")]
    SchemaViolation(String),
}
