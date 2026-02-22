use crate::error::{CoreError, CoreResult};
use crate::types::{Document, DocumentId, TableName};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct InMemoryEngine {
    tables: HashMap<TableName, HashMap<DocumentId, Document>>,
}

impl InMemoryEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_table(&mut self, table: &str) {
        self.tables.entry(table.to_owned()).or_default();
    }

    pub fn get(&self, table: &str, id: &str) -> CoreResult<Document> {
        let table_docs = self
            .tables
            .get(table)
            .ok_or_else(|| CoreError::TableNotFound(table.to_owned()))?;

        table_docs
            .get(id)
            .cloned()
            .ok_or_else(|| CoreError::DocumentNotFound(id.to_owned()))
    }
}
