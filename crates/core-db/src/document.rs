use crate::values::{ConvexValue, DocumentId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A document in the database.
///
/// Every document has system-managed fields (`_id`, `_creationTime`) that are
/// set automatically on creation. User-defined fields are stored separately
/// and cannot overwrite system fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    id: DocumentId,
    creation_time: f64,
    fields: BTreeMap<String, ConvexValue>,
}

impl Document {
    /// Create a new document with auto-generated creation timestamp.
    pub fn new(id: DocumentId, fields: BTreeMap<String, ConvexValue>) -> Self {
        let creation_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as f64;
        Self {
            id,
            creation_time,
            fields,
        }
    }

    /// Create a document with an explicit creation time (useful for tests).
    pub fn with_creation_time(
        id: DocumentId,
        creation_time: f64,
        fields: BTreeMap<String, ConvexValue>,
    ) -> Self {
        Self {
            id,
            creation_time,
            fields,
        }
    }

    pub fn id(&self) -> &DocumentId {
        &self.id
    }

    pub fn creation_time(&self) -> f64 {
        self.creation_time
    }

    pub fn fields(&self) -> &BTreeMap<String, ConvexValue> {
        &self.fields
    }

    pub fn fields_mut(&mut self) -> &mut BTreeMap<String, ConvexValue> {
        &mut self.fields
    }

    /// Get a field value by name. Returns None for system fields and missing fields.
    pub fn get(&self, field: &str) -> Option<&ConvexValue> {
        self.fields.get(field)
    }

    /// Set a field value. Rejects system field names (_id, _creationTime).
    pub fn set(
        &mut self,
        field: String,
        value: ConvexValue,
    ) -> Result<(), crate::error::CoreError> {
        if field.starts_with('_') {
            return Err(crate::error::CoreError::InvalidFieldName(format!(
                "cannot set system field: {field}"
            )));
        }
        self.fields.insert(field, value);
        Ok(())
    }

    /// Remove a field, returning its previous value if it existed.
    pub fn remove(&mut self, field: &str) -> Option<ConvexValue> {
        self.fields.remove(field)
    }

    /// Replace all user fields at once (used for full document replacement).
    pub fn replace_fields(&mut self, fields: BTreeMap<String, ConvexValue>) {
        self.fields = fields;
    }

    /// Convert to a ConvexValue::Object including system fields.
    /// This is the representation clients see when querying.
    pub fn to_value(&self) -> ConvexValue {
        let mut map = BTreeMap::new();
        map.insert("_id".to_string(), ConvexValue::String(self.id.to_string()));
        map.insert(
            "_creationTime".to_string(),
            ConvexValue::Float64(self.creation_time),
        );
        for (k, v) in &self.fields {
            map.insert(k.clone(), v.clone());
        }
        ConvexValue::Object(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_document() {
        let id = DocumentId::new("users", "test-id");
        let fields = BTreeMap::from([
            ("name".to_string(), ConvexValue::from("Alice")),
            ("age".to_string(), ConvexValue::from(30i64)),
        ]);
        let doc = Document::new(id.clone(), fields);

        assert_eq!(doc.id(), &id);
        assert!(doc.creation_time() > 0.0);
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Alice")));
        assert_eq!(doc.get("age"), Some(&ConvexValue::from(30i64)));
        assert_eq!(doc.get("missing"), None);
    }

    #[test]
    fn set_and_remove_fields() {
        let id = DocumentId::new("users", "test-id");
        let mut doc = Document::new(id, BTreeMap::new());

        doc.set("name".into(), ConvexValue::from("Bob")).unwrap();
        assert_eq!(doc.get("name"), Some(&ConvexValue::from("Bob")));

        let old = doc.remove("name");
        assert_eq!(old, Some(ConvexValue::from("Bob")));
        assert_eq!(doc.get("name"), None);
    }

    #[test]
    fn reject_system_field_names() {
        let id = DocumentId::new("users", "test-id");
        let mut doc = Document::new(id, BTreeMap::new());

        let result = doc.set("_id".into(), ConvexValue::from("hacked"));
        assert!(result.is_err());

        let result = doc.set("_creationTime".into(), ConvexValue::from(0i64));
        assert!(result.is_err());
    }

    #[test]
    fn to_value_includes_system_fields() {
        let id = DocumentId::new("users", "test-id");
        let fields = BTreeMap::from([("name".to_string(), ConvexValue::from("Alice"))]);
        let doc = Document::with_creation_time(id, 1000.0, fields);

        let value = doc.to_value();
        if let ConvexValue::Object(map) = &value {
            assert_eq!(
                map.get("_id"),
                Some(&ConvexValue::String("users:test-id".into()))
            );
            assert_eq!(
                map.get("_creationTime"),
                Some(&ConvexValue::Float64(1000.0))
            );
            assert_eq!(map.get("name"), Some(&ConvexValue::String("Alice".into())));
        } else {
            panic!("expected Object");
        }
    }

    #[test]
    fn replace_fields() {
        let id = DocumentId::new("users", "test-id");
        let mut doc = Document::new(
            id,
            BTreeMap::from([("old".to_string(), ConvexValue::from("data"))]),
        );

        doc.replace_fields(BTreeMap::from([(
            "new".to_string(),
            ConvexValue::from("data"),
        )]));
        assert_eq!(doc.get("old"), None);
        assert_eq!(doc.get("new"), Some(&ConvexValue::from("data")));
    }

    #[test]
    fn with_explicit_creation_time() {
        let id = DocumentId::new("users", "test-id");
        let doc = Document::with_creation_time(id, 12345.0, BTreeMap::new());
        assert_eq!(doc.creation_time(), 12345.0);
    }
}
