use crate::document::Document;
use crate::error::{CoreError, CoreResult};
use crate::table::Table;
use crate::values::{ConvexValue, DocumentId, TableName};
use std::collections::{BTreeMap, HashMap};

/// The top-level database holding multiple tables.
///
/// Provides CRUD operations that route to the correct table,
/// auto-generating DocumentIds and managing table lifecycle.
#[derive(Debug, Default)]
pub struct Database {
    tables: HashMap<TableName, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new table. No-op if the table already exists.
    pub fn create_table(&mut self, name: &str) {
        self.tables
            .entry(name.to_owned())
            .or_insert_with(|| Table::new(name));
    }

    /// Get a reference to a table, returning an error if it doesn't exist.
    pub fn table(&self, name: &str) -> CoreResult<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| CoreError::TableNotFound(name.to_owned()))
    }

    /// Get a mutable reference to a table.
    fn table_mut(&mut self, name: &str) -> CoreResult<&mut Table> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| CoreError::TableNotFound(name.to_owned()))
    }

    /// Check if a table exists.
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// List all table names.
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(String::as_str).collect()
    }

    /// Insert a new document into a table.
    /// Auto-generates a DocumentId (UUID v7) and sets _creationTime.
    /// The table must already exist.
    pub fn insert(
        &mut self,
        table: &str,
        fields: BTreeMap<String, ConvexValue>,
    ) -> CoreResult<DocumentId> {
        let doc_id = DocumentId::generate(table);
        let doc = Document::new(doc_id.clone(), fields);
        self.table_mut(table)?.insert(doc)?;
        Ok(doc_id)
    }

    /// Insert a document with a specific ID (useful for tests and migrations).
    pub fn insert_with_id(
        &mut self,
        id: DocumentId,
        fields: BTreeMap<String, ConvexValue>,
    ) -> CoreResult<()> {
        let table_name = id.table().to_owned();
        let doc = Document::new(id, fields);
        self.table_mut(&table_name)?.insert(doc)
    }

    /// Get a document by its full DocumentId.
    pub fn get(&self, id: &DocumentId) -> CoreResult<&Document> {
        self.table(id.table())?.get(id.id())
    }

    /// Replace all user fields of an existing document.
    pub fn replace(
        &mut self,
        id: &DocumentId,
        fields: BTreeMap<String, ConvexValue>,
    ) -> CoreResult<()> {
        self.table_mut(id.table())?.replace(id.id(), fields)
    }

    /// Patch (merge) specific fields into an existing document.
    pub fn patch(
        &mut self,
        id: &DocumentId,
        fields: BTreeMap<String, ConvexValue>,
    ) -> CoreResult<()> {
        self.table_mut(id.table())?.patch(id.id(), fields)
    }

    /// Delete a document by its full DocumentId.
    pub fn delete(&mut self, id: &DocumentId) -> CoreResult<Document> {
        self.table_mut(id.table())?.delete(id.id())
    }

    /// List all documents in a table.
    pub fn list(&self, table: &str) -> CoreResult<Vec<&Document>> {
        Ok(self.table(table)?.list())
    }

    /// Count documents in a table.
    pub fn count(&self, table: &str) -> CoreResult<usize> {
        Ok(self.table(table)?.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn create_and_list_tables() {
        let mut db = Database::new();
        assert!(!db.has_table("users"));

        db.create_table("users");
        assert!(db.has_table("users"));

        // Idempotent creation
        db.create_table("users");
        assert!(db.has_table("users"));
    }

    #[test]
    fn insert_and_get() {
        let mut db = setup_db();
        let id = db.insert("users", user_fields("Alice", 30)).unwrap();

        let doc = db.get(&id).unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Alice")));
        assert_eq!(doc.get("age"), Some(&ConvexValue::from(30i64)));
        assert_eq!(doc.id().table(), "users");
    }

    #[test]
    fn insert_into_nonexistent_table_fails() {
        let mut db = Database::new();
        let result = db.insert("ghosts", BTreeMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn get_nonexistent_document_fails() {
        let db = setup_db();
        let fake_id = DocumentId::new("users", "nonexistent");
        assert!(db.get(&fake_id).is_err());
    }

    #[test]
    fn replace_document() {
        let mut db = setup_db();
        let id = db.insert("users", user_fields("Alice", 30)).unwrap();

        db.replace(
            &id,
            BTreeMap::from([("name".to_string(), ConvexValue::from("Bob"))]),
        )
        .unwrap();

        let doc = db.get(&id).unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Bob")));
        assert_eq!(doc.get("age"), None); // replaced, not merged
    }

    #[test]
    fn patch_document() {
        let mut db = setup_db();
        let id = db.insert("users", user_fields("Alice", 30)).unwrap();

        db.patch(
            &id,
            BTreeMap::from([("age".to_string(), ConvexValue::from(31i64))]),
        )
        .unwrap();

        let doc = db.get(&id).unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Alice"))); // preserved
        assert_eq!(doc.get("age"), Some(&ConvexValue::from(31i64))); // updated
    }

    #[test]
    fn delete_document() {
        let mut db = setup_db();
        let id = db.insert("users", user_fields("Alice", 30)).unwrap();
        assert_eq!(db.count("users").unwrap(), 1);

        let deleted = db.delete(&id).unwrap();
        assert_eq!(deleted.get("name"), Some(&ConvexValue::from("Alice")));
        assert_eq!(db.count("users").unwrap(), 0);
    }

    #[test]
    fn delete_nonexistent_fails() {
        let mut db = setup_db();
        let fake_id = DocumentId::new("users", "nonexistent");
        assert!(db.delete(&fake_id).is_err());
    }

    #[test]
    fn list_documents() {
        let mut db = setup_db();
        db.insert("users", user_fields("Alice", 30)).unwrap();
        db.insert("users", user_fields("Bob", 25)).unwrap();

        let docs = db.list("users").unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn tables_are_isolated() {
        let mut db = setup_db();
        let user_id = db.insert("users", user_fields("Alice", 30)).unwrap();
        let msg_fields = BTreeMap::from([("text".to_string(), ConvexValue::from("hello"))]);
        db.insert("messages", msg_fields).unwrap();

        assert_eq!(db.count("users").unwrap(), 1);
        assert_eq!(db.count("messages").unwrap(), 1);

        // Can't get a user doc from messages table
        let wrong_table_id = DocumentId::new("messages", user_id.id());
        assert!(db.get(&wrong_table_id).is_err());
    }

    #[test]
    fn insert_with_explicit_id() {
        let mut db = setup_db();
        let id = DocumentId::new("users", "custom-id");
        db.insert_with_id(id.clone(), user_fields("Alice", 30))
            .unwrap();

        let doc = db.get(&id).unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Alice")));
        assert_eq!(doc.id().id(), "custom-id");
    }

    #[test]
    fn multiple_operations_sequence() {
        let mut db = setup_db();

        // Insert
        let id1 = db.insert("users", user_fields("Alice", 30)).unwrap();
        let id2 = db.insert("users", user_fields("Bob", 25)).unwrap();
        assert_eq!(db.count("users").unwrap(), 2);

        // Patch
        db.patch(
            &id1,
            BTreeMap::from([("age".to_string(), ConvexValue::from(31i64))]),
        )
        .unwrap();

        // Replace
        db.replace(&id2, user_fields("Charlie", 35)).unwrap();

        // Delete
        db.delete(&id1).unwrap();
        assert_eq!(db.count("users").unwrap(), 1);

        // Verify remaining doc
        let doc = db.get(&id2).unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Charlie")));
    }
}
