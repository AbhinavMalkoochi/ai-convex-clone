use crate::error::{CoreError, CoreResult};
use crate::schema::Schema;
use crate::types::{
    Document, DocumentId, NewDocument, Revision, TableName, TableState, WriteOperation,
};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct Table {
    schema: Schema,
    documents: HashMap<DocumentId, Document>,
}

#[derive(Debug)]
pub struct InMemoryEngine {
    tables: HashMap<TableName, Table>,
    next_revision: u64,
}

impl Default for InMemoryEngine {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
            next_revision: 1,
        }
    }
}

impl InMemoryEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_table(&mut self, table: &str, schema: Schema) -> CoreResult<()> {
        if self.tables.contains_key(table) {
            return Err(CoreError::TableAlreadyExists(table.to_owned()));
        }

        self.tables.insert(
            table.to_owned(),
            Table {
                schema,
                documents: HashMap::new(),
            },
        );

        Ok(())
    }

    pub fn list_tables(&self) -> Vec<TableState> {
        let mut states: Vec<TableState> = self
            .tables
            .iter()
            .map(|(name, table)| TableState {
                name: name.clone(),
                document_count: table.documents.len(),
            })
            .collect();
        states.sort_by(|left, right| left.name.cmp(&right.name));
        states
    }

    pub fn get(&self, table: &str, id: &str) -> CoreResult<Document> {
        let table_data = self
            .tables
            .get(table)
            .ok_or_else(|| CoreError::TableNotFound(table.to_owned()))?;

        table_data
            .documents
            .get(id)
            .cloned()
            .ok_or_else(|| CoreError::DocumentNotFound(id.to_owned()))
    }

    pub fn list_documents(&self, table: &str) -> CoreResult<Vec<Document>> {
        let table_data = self
            .tables
            .get(table)
            .ok_or_else(|| CoreError::TableNotFound(table.to_owned()))?;

        let mut docs: Vec<Document> = table_data.documents.values().cloned().collect();
        docs.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(docs)
    }

    pub fn write_batch(
        &mut self,
        table: &str,
        ops: &[WriteOperation],
    ) -> CoreResult<Vec<Document>> {
        let existing = self
            .tables
            .get(table)
            .ok_or_else(|| CoreError::TableNotFound(table.to_owned()))?;

        let schema = existing.schema.clone();
        let mut documents = existing.documents.clone();
        let mut written_docs = Vec::new();

        for op in ops {
            match op {
                WriteOperation::Put(input) => {
                    schema.validate(&input.fields)?;
                    let id = resolve_document_id(input);
                    let document = Document {
                        id: id.clone(),
                        revision: self.next_revision(),
                        fields: input.fields.clone(),
                    };
                    documents.insert(id, document.clone());
                    written_docs.push(document);
                }
                WriteOperation::Delete(id) => {
                    let deleted = documents.remove(id);
                    if deleted.is_none() {
                        return Err(CoreError::DocumentNotFound(id.clone()));
                    }
                }
            }
        }

        if let Some(table_data) = self.tables.get_mut(table) {
            table_data.documents = documents;
        }

        Ok(written_docs)
    }

    fn next_revision(&mut self) -> Revision {
        let current = self.next_revision;
        self.next_revision += 1;
        Revision(current)
    }
}

fn resolve_document_id(input: &NewDocument) -> DocumentId {
    match &input.id {
        Some(explicit) => explicit.clone(),
        None => Uuid::now_v7().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryEngine;
    use crate::schema::{Schema, SchemaField, SchemaType};
    use crate::types::{NewDocument, WriteOperation};
    use std::collections::BTreeMap;

    fn users_schema() -> Schema {
        let mut fields = BTreeMap::new();
        fields.insert(
            "name".to_string(),
            SchemaField {
                required: true,
                field_type: SchemaType::String,
            },
        );
        Schema::with_fields(fields)
    }

    #[test]
    fn writes_and_reads_documents() {
        let mut engine = InMemoryEngine::new();
        engine
            .create_table("users", users_schema())
            .expect("table should be created");

        let mut doc_fields = BTreeMap::new();
        doc_fields.insert(
            "name".to_string(),
            serde_json::Value::String("Grace".to_string()),
        );

        let result = engine
            .write_batch(
                "users",
                &[WriteOperation::Put(NewDocument {
                    id: Some("user_1".to_string()),
                    fields: doc_fields,
                })],
            )
            .expect("write should succeed");

        assert_eq!(result.len(), 1);
        let fetched = engine.get("users", "user_1").expect("doc must exist");
        assert_eq!(fetched.id, "user_1");
        assert_eq!(fetched.revision.0, 1);
    }

    #[test]
    fn batch_is_atomic_on_validation_failure() {
        let mut engine = InMemoryEngine::new();
        engine
            .create_table("users", users_schema())
            .expect("table should be created");

        let mut good_fields = BTreeMap::new();
        good_fields.insert(
            "name".to_string(),
            serde_json::Value::String("Ada".to_string()),
        );

        let mut bad_fields = BTreeMap::new();
        bad_fields.insert("name".to_string(), serde_json::Value::Bool(false));

        let result = engine.write_batch(
            "users",
            &[
                WriteOperation::Put(NewDocument {
                    id: Some("user_1".to_string()),
                    fields: good_fields,
                }),
                WriteOperation::Put(NewDocument {
                    id: Some("user_2".to_string()),
                    fields: bad_fields,
                }),
            ],
        );

        assert!(result.is_err());
        let listed = engine
            .list_documents("users")
            .expect("listing should succeed");
        assert!(listed.is_empty());
    }
}
