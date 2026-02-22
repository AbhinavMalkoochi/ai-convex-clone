use crate::document::Document;
use crate::error::{CoreError, CoreResult};
use crate::values::ConvexValue;
use std::collections::BTreeMap;

/// A single table storing documents indexed by their DocumentId.
///
/// Uses a BTreeMap for ordered storage, enabling efficient range scans
/// and ordered iteration (important for index support in later phases).
#[derive(Debug, Default)]
pub struct Table {
    name: String,
    docs: BTreeMap<String, Document>,
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            docs: BTreeMap::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Insert a new document. The document's ID must reference this table.
    /// Returns an error if a document with the same ID already exists.
    pub fn insert(&mut self, doc: Document) -> CoreResult<()> {
        let doc_id = doc.id().id().to_owned();
        if self.docs.contains_key(&doc_id) {
            return Err(CoreError::DuplicateDocument(doc.id().to_string()));
        }
        self.docs.insert(doc_id, doc);
        Ok(())
    }

    /// Get a document by its unique ID string (not the full DocumentId).
    pub fn get(&self, id: &str) -> CoreResult<&Document> {
        self.docs
            .get(id)
            .ok_or_else(|| CoreError::DocumentNotFound(format!("{}:{}", self.name, id)))
    }

    /// Get a mutable reference to a document.
    pub fn get_mut(&mut self, id: &str) -> CoreResult<&mut Document> {
        self.docs
            .get_mut(id)
            .ok_or_else(|| CoreError::DocumentNotFound(format!("{}:{}", self.name, id)))
    }

    /// Replace all user fields of an existing document.
    /// Preserves system fields (_id, _creationTime).
    pub fn replace(&mut self, id: &str, fields: BTreeMap<String, ConvexValue>) -> CoreResult<()> {
        let doc = self.get_mut(id)?;
        doc.replace_fields(fields);
        Ok(())
    }

    /// Patch (merge) fields into an existing document.
    /// Only the specified fields are updated; other fields are preserved.
    pub fn patch(&mut self, id: &str, fields: BTreeMap<String, ConvexValue>) -> CoreResult<()> {
        let doc = self.get_mut(id)?;
        for (key, value) in fields {
            doc.set(key, value)?;
        }
        Ok(())
    }

    /// Delete a document by ID. Returns the removed document.
    pub fn delete(&mut self, id: &str) -> CoreResult<Document> {
        self.docs
            .remove(id)
            .ok_or_else(|| CoreError::DocumentNotFound(format!("{}:{}", self.name, id)))
    }

    /// Iterate over all documents in insertion order (BTreeMap key order).
    pub fn iter(&self) -> impl Iterator<Item = &Document> {
        self.docs.values()
    }

    /// Collect all documents as a Vec (useful for queries).
    pub fn list(&self) -> Vec<&Document> {
        self.docs.values().collect()
    }

    /// Check if a document with the given ID exists.
    pub fn contains(&self, id: &str) -> bool {
        self.docs.contains_key(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::DocumentId;

    fn make_doc(table: &str, id: &str, name: &str) -> Document {
        let doc_id = DocumentId::new(table, id);
        let fields = BTreeMap::from([("name".to_string(), ConvexValue::from(name))]);
        Document::with_creation_time(doc_id, 1000.0, fields)
    }

    #[test]
    fn insert_and_get() {
        let mut table = Table::new("users");
        let doc = make_doc("users", "001", "Alice");
        table.insert(doc).unwrap();

        let retrieved = table.get("001").unwrap();
        assert_eq!(retrieved.get("name"), Some(&ConvexValue::from("Alice")));
    }

    #[test]
    fn insert_duplicate_fails() {
        let mut table = Table::new("users");
        table.insert(make_doc("users", "001", "Alice")).unwrap();
        let result = table.insert(make_doc("users", "001", "Bob"));
        assert!(result.is_err());
    }

    #[test]
    fn get_missing_fails() {
        let table = Table::new("users");
        let result = table.get("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn replace_document() {
        let mut table = Table::new("users");
        table.insert(make_doc("users", "001", "Alice")).unwrap();

        let new_fields = BTreeMap::from([
            ("name".to_string(), ConvexValue::from("Bob")),
            ("age".to_string(), ConvexValue::from(25i64)),
        ]);
        table.replace("001", new_fields).unwrap();

        let doc = table.get("001").unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Bob")));
        assert_eq!(doc.get("age"), Some(&ConvexValue::from(25i64)));
    }

    #[test]
    fn patch_document() {
        let mut table = Table::new("users");
        let doc_id = DocumentId::new("users", "001");
        let fields = BTreeMap::from([
            ("name".to_string(), ConvexValue::from("Alice")),
            ("age".to_string(), ConvexValue::from(30i64)),
        ]);
        table
            .insert(Document::with_creation_time(doc_id, 1000.0, fields))
            .unwrap();

        // Patch only the age field
        let patch = BTreeMap::from([("age".to_string(), ConvexValue::from(31i64))]);
        table.patch("001", patch).unwrap();

        let doc = table.get("001").unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Alice"))); // unchanged
        assert_eq!(doc.get("age"), Some(&ConvexValue::from(31i64))); // updated
    }

    #[test]
    fn delete_document() {
        let mut table = Table::new("users");
        table.insert(make_doc("users", "001", "Alice")).unwrap();

        let deleted = table.delete("001").unwrap();
        assert_eq!(deleted.get("name"), Some(&ConvexValue::from("Alice")));
        assert!(table.is_empty());
    }

    #[test]
    fn delete_missing_fails() {
        let mut table = Table::new("users");
        assert!(table.delete("nonexistent").is_err());
    }

    #[test]
    fn list_and_count() {
        let mut table = Table::new("users");
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);

        table.insert(make_doc("users", "001", "Alice")).unwrap();
        table.insert(make_doc("users", "002", "Bob")).unwrap();

        assert_eq!(table.len(), 2);
        assert!(!table.is_empty());
        assert_eq!(table.list().len(), 2);
    }

    #[test]
    fn contains() {
        let mut table = Table::new("users");
        table.insert(make_doc("users", "001", "Alice")).unwrap();

        assert!(table.contains("001"));
        assert!(!table.contains("002"));
    }

    #[test]
    fn iter_over_documents() {
        let mut table = Table::new("users");
        table.insert(make_doc("users", "001", "Alice")).unwrap();
        table.insert(make_doc("users", "002", "Bob")).unwrap();

        let names: Vec<&str> = table
            .iter()
            .filter_map(|doc| doc.get("name")?.as_str())
            .collect();
        assert_eq!(names, vec!["Alice", "Bob"]); // BTreeMap orders by key
    }
}
