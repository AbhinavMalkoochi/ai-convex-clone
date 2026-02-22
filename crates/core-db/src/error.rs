use thiserror::Error;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("document not found: {0}")]
    DocumentNotFound(String),
    #[error("schema violation: {0}")]
    SchemaViolation(String),
}
