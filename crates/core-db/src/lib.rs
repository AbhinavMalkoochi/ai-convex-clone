pub mod engine;
pub mod error;
pub mod schema;
pub mod types;

pub use engine::InMemoryEngine;
pub use error::{CoreError, CoreResult};
pub use schema::{Schema, SchemaField, SchemaType};
pub use types::{
    Document, DocumentId, NewDocument, Revision, TableName, TableState, Value, WriteOperation,
};
