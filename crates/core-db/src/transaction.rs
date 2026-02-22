use crate::document::Document;
use crate::error::{CoreError, CoreResult};
use crate::index::{IndexDefinition, IndexRegistry};
use crate::schema::{validate_document, SchemaDefinition};
use crate::table::Table;
use crate::values::{ConvexValue, DocumentId, TableName};
use std::collections::{BTreeMap, HashMap, HashSet};

/// An MVCC transaction providing snapshot isolation.
///
/// Created via `Database::begin()`. All reads see a consistent snapshot
/// taken at transaction creation time. Writes are buffered in the local
/// copy and applied atomically to the database on `commit()`.
///
/// Conflict detection uses optimistic concurrency control: if any document
/// in the read set or write set was modified by another committed
/// transaction after this transaction began, commit fails with
/// `CoreError::TransactionConflict`.
pub struct Transaction {
    /// Working copy of tables (snapshot + local mutations applied).
    pub(crate) tables: HashMap<TableName, Table>,
    /// Working copy of indexes.
    pub(crate) indexes: HashMap<TableName, IndexRegistry>,
    /// Schema at transaction start.
    pub(crate) schema: Option<SchemaDefinition>,
    /// Documents read during this transaction: (table, doc_id).
    pub(crate) read_set: HashSet<(TableName, String)>,
    /// Documents written during this transaction: (table, doc_id).
    pub(crate) write_set: HashSet<(TableName, String)>,
    /// Database version at the time this transaction began.
    pub(crate) begin_version: u64,
}

impl Transaction {
    /// Create a table in the transaction's working copy.
    pub fn create_table(&mut self, name: &str) {
        self.tables
            .entry(name.to_owned())
            .or_insert_with(|| Table::new(name));
        self.indexes.entry(name.to_owned()).or_default();
    }

    /// Check if a table exists.
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Get a reference to a table.
    pub fn table(&self, name: &str) -> CoreResult<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| CoreError::TableNotFound(name.to_owned()))
    }

    fn table_mut(&mut self, name: &str) -> CoreResult<&mut Table> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| CoreError::TableNotFound(name.to_owned()))
    }

    fn validate_fields(
        &self,
        table: &str,
        fields: &BTreeMap<String, ConvexValue>,
    ) -> CoreResult<()> {
        if let Some(schema) = &self.schema {
            if let Some(table_schema) = schema.get_table_schema(table) {
                validate_document(fields, table_schema)
                    .map_err(|msg| CoreError::SchemaViolation(format!("{table}: {msg}")))?;
            }
        }
        Ok(())
    }

    /// Insert a document, returning the generated DocumentId.
    pub fn insert(
        &mut self,
        table: &str,
        fields: BTreeMap<String, ConvexValue>,
    ) -> CoreResult<DocumentId> {
        self.validate_fields(table, &fields)?;
        let doc_id = DocumentId::generate(table);
        let doc = Document::new(doc_id.clone(), fields);
        if let Some(registry) = self.indexes.get_mut(table) {
            registry.on_insert(doc.id().id(), doc.fields());
        }
        self.table_mut(table)?.insert(doc)?;
        self.write_set
            .insert((table.to_owned(), doc_id.id().to_owned()));
        Ok(doc_id)
    }

    /// Get a document by ID, recording the read in the read set.
    pub fn get(&mut self, id: &DocumentId) -> CoreResult<&Document> {
        self.read_set
            .insert((id.table().to_owned(), id.id().to_owned()));
        self.tables
            .get(id.table())
            .ok_or_else(|| CoreError::TableNotFound(id.table().to_owned()))?
            .get(id.id())
    }

    /// Replace all user fields of an existing document.
    pub fn replace(
        &mut self,
        id: &DocumentId,
        fields: BTreeMap<String, ConvexValue>,
    ) -> CoreResult<()> {
        self.validate_fields(id.table(), &fields)?;
        self.read_set
            .insert((id.table().to_owned(), id.id().to_owned()));
        let old_fields = self.table(id.table())?.get(id.id())?.fields().clone();
        self.table_mut(id.table())?.replace(id.id(), fields)?;
        let new_fields = self.table(id.table())?.get(id.id())?.fields().clone();
        if let Some(registry) = self.indexes.get_mut(id.table()) {
            registry.on_update(id.id(), &old_fields, &new_fields);
        }
        self.write_set
            .insert((id.table().to_owned(), id.id().to_owned()));
        Ok(())
    }

    /// Patch (merge) specific fields into an existing document.
    pub fn patch(
        &mut self,
        id: &DocumentId,
        fields: BTreeMap<String, ConvexValue>,
    ) -> CoreResult<()> {
        self.read_set
            .insert((id.table().to_owned(), id.id().to_owned()));
        let old_fields = self.table(id.table())?.get(id.id())?.fields().clone();
        self.table_mut(id.table())?.patch(id.id(), fields)?;
        let new_fields = self.table(id.table())?.get(id.id())?.fields().clone();
        if let Some(registry) = self.indexes.get_mut(id.table()) {
            registry.on_update(id.id(), &old_fields, &new_fields);
        }
        // Re-validate after patching
        if let Some(schema) = &self.schema {
            if let Some(table_schema) = schema.get_table_schema(id.table()) {
                validate_document(&new_fields, table_schema)
                    .map_err(|msg| CoreError::SchemaViolation(format!("{}: {msg}", id.table())))?;
            }
        }
        self.write_set
            .insert((id.table().to_owned(), id.id().to_owned()));
        Ok(())
    }

    /// Delete a document.
    pub fn delete(&mut self, id: &DocumentId) -> CoreResult<Document> {
        self.read_set
            .insert((id.table().to_owned(), id.id().to_owned()));
        let doc = self.table_mut(id.table())?.delete(id.id())?;
        if let Some(registry) = self.indexes.get_mut(id.table()) {
            registry.on_remove(id.id(), doc.fields());
        }
        self.write_set
            .insert((id.table().to_owned(), id.id().to_owned()));
        Ok(doc)
    }

    /// List all documents in a table (marks the entire table as read).
    pub fn list(&mut self, table: &str) -> CoreResult<Vec<&Document>> {
        let tbl = self.table(table)?;
        // Record reads for all documents in the table
        let ids: Vec<String> = tbl.iter().map(|d| d.id().id().to_owned()).collect();
        for doc_id in ids {
            self.read_set.insert((table.to_owned(), doc_id));
        }
        Ok(self.table(table)?.list())
    }

    /// Count documents in a table.
    pub fn count(&self, table: &str) -> CoreResult<usize> {
        Ok(self.table(table)?.len())
    }

    /// Create a secondary index within this transaction.
    pub fn create_index(&mut self, definition: IndexDefinition) -> CoreResult<()> {
        let table_name = definition.table.clone();
        let idx_name = definition.name.clone();
        self.table(&table_name)?;

        let registry = self.indexes.entry(table_name.clone()).or_default();
        registry.add_index(definition)?;

        let docs: Vec<_> = self
            .tables
            .get(&table_name)
            .expect("table verified above")
            .iter()
            .map(|d| (d.id().id().to_owned(), d.fields().clone()))
            .collect();

        let idx = self
            .indexes
            .get_mut(&table_name)
            .expect("registry exists")
            .get_index_mut(&idx_name)?;
        for (doc_id, fields) in &docs {
            idx.insert(doc_id, fields);
        }
        Ok(())
    }

    /// Query an index by equality.
    pub fn query_index(
        &self,
        table: &str,
        index_name: &str,
        values: &[ConvexValue],
    ) -> CoreResult<Vec<&Document>> {
        let registry = self
            .indexes
            .get(table)
            .ok_or_else(|| CoreError::TableNotFound(table.to_owned()))?;
        let idx = registry.get_index(index_name)?;
        let doc_ids = idx.lookup(values);
        let tbl = self.table(table)?;
        doc_ids.into_iter().map(|id| tbl.get(id)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    fn setup_db() -> Database {
        let mut db = Database::new();
        db.create_table("users");
        db.create_table("messages");
        db
    }

    fn user_fields(name: &str, age: i64) -> BTreeMap<String, ConvexValue> {
        BTreeMap::from([
            ("name".to_string(), ConvexValue::from(name)),
            ("age".to_string(), ConvexValue::from(age)),
        ])
    }

    #[test]
    fn basic_transaction_commit() {
        let mut db = setup_db();
        let mut tx = db.begin();

        let id = tx.insert("users", user_fields("Alice", 30)).unwrap();
        // Data visible in transaction
        let doc = tx.get(&id).unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Alice")));

        // Data NOT visible in database yet
        assert_eq!(db.count("users").unwrap(), 0);

        // Commit
        db.commit(tx).unwrap();
        assert_eq!(db.count("users").unwrap(), 1);
        let doc = db.get(&id).unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Alice")));
    }

    #[test]
    fn transaction_replace_and_commit() {
        let mut db = setup_db();
        let id = db.insert("users", user_fields("Alice", 30)).unwrap();

        let mut tx = db.begin();
        tx.replace(&id, user_fields("Alicia", 31)).unwrap();
        db.commit(tx).unwrap();

        let doc = db.get(&id).unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Alicia")));
        assert_eq!(doc.get("age"), Some(&ConvexValue::from(31i64)));
    }

    #[test]
    fn transaction_patch_and_commit() {
        let mut db = setup_db();
        let id = db.insert("users", user_fields("Alice", 30)).unwrap();

        let mut tx = db.begin();
        tx.patch(
            &id,
            BTreeMap::from([("age".to_string(), ConvexValue::from(31i64))]),
        )
        .unwrap();
        db.commit(tx).unwrap();

        let doc = db.get(&id).unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Alice")));
        assert_eq!(doc.get("age"), Some(&ConvexValue::from(31i64)));
    }

    #[test]
    fn transaction_delete_and_commit() {
        let mut db = setup_db();
        let id = db.insert("users", user_fields("Alice", 30)).unwrap();

        let mut tx = db.begin();
        tx.delete(&id).unwrap();
        db.commit(tx).unwrap();

        assert_eq!(db.count("users").unwrap(), 0);
    }

    #[test]
    fn conflict_detection_write_write() {
        let mut db = setup_db();
        let id = db.insert("users", user_fields("Alice", 30)).unwrap();

        // Start transaction
        let mut tx = db.begin();
        tx.replace(&id, user_fields("Alicia", 30)).unwrap();

        // Concurrent write directly on database (simulates another committed tx)
        db.replace(&id, user_fields("Bob", 25)).unwrap();

        // Commit should fail — the document was modified after tx began
        let result = db.commit(tx);
        assert!(result.is_err());
        match result.unwrap_err() {
            CoreError::TransactionConflict(_) => {}
            other => panic!("expected TransactionConflict, got: {other}"),
        }
    }

    #[test]
    fn conflict_detection_read_write() {
        let mut db = setup_db();
        let id = db.insert("users", user_fields("Alice", 30)).unwrap();

        // Start transaction that reads
        let mut tx = db.begin();
        let _doc = tx.get(&id).unwrap();

        // Concurrent modification
        db.patch(
            &id,
            BTreeMap::from([("age".to_string(), ConvexValue::from(99i64))]),
        )
        .unwrap();

        // Commit should fail — a document we read was modified
        let result = db.commit(tx);
        assert!(result.is_err());
    }

    #[test]
    fn no_conflict_on_unrelated_documents() {
        let mut db = setup_db();
        let id1 = db.insert("users", user_fields("Alice", 30)).unwrap();
        let id2 = db.insert("users", user_fields("Bob", 25)).unwrap();

        // Transaction modifies id1
        let mut tx = db.begin();
        tx.replace(&id1, user_fields("Alicia", 31)).unwrap();

        // Concurrent modification of id2 (unrelated)
        db.replace(&id2, user_fields("Robert", 26)).unwrap();

        // Should succeed — no overlap
        db.commit(tx).unwrap();
        assert_eq!(
            db.get(&id1).unwrap().get("name"),
            Some(&ConvexValue::from("Alicia"))
        );
    }

    #[test]
    fn transaction_insert_new_document() {
        let mut db = setup_db();

        let mut tx = db.begin();
        let id = tx.insert("users", user_fields("NewUser", 20)).unwrap();
        db.commit(tx).unwrap();

        let doc = db.get(&id).unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("NewUser")));
    }

    #[test]
    fn dropped_transaction_has_no_effect() {
        let db = setup_db();
        let initial_count = db.count("users").unwrap();

        {
            let mut tx = db.begin();
            tx.insert("users", user_fields("Ghost", 0)).unwrap();
            // tx dropped here without commit
        }

        assert_eq!(db.count("users").unwrap(), initial_count);
    }

    #[test]
    fn multiple_writes_in_single_transaction() {
        let mut db = setup_db();

        let mut tx = db.begin();
        let id1 = tx.insert("users", user_fields("Alice", 30)).unwrap();
        let id2 = tx.insert("users", user_fields("Bob", 25)).unwrap();
        tx.patch(
            &id1,
            BTreeMap::from([("age".to_string(), ConvexValue::from(31i64))]),
        )
        .unwrap();
        db.commit(tx).unwrap();

        assert_eq!(db.count("users").unwrap(), 2);
        assert_eq!(
            db.get(&id1).unwrap().get("age"),
            Some(&ConvexValue::from(31i64))
        );
        assert_eq!(
            db.get(&id2).unwrap().get("name"),
            Some(&ConvexValue::from("Bob"))
        );
    }

    #[test]
    fn transaction_version_increments() {
        let mut db = setup_db();
        assert_eq!(db.version(), 0);

        let tx = db.begin();
        db.commit(tx).unwrap();
        assert_eq!(db.version(), 1);

        let mut tx = db.begin();
        tx.insert("users", user_fields("Alice", 30)).unwrap();
        db.commit(tx).unwrap();
        assert_eq!(db.version(), 2);
    }
}
