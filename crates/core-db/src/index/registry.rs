use crate::error::{CoreError, CoreResult};
use crate::index::Index;
use crate::values::ConvexValue;
use std::collections::BTreeMap;

/// Composite index key: a vector of ConvexValues, one per indexed field.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexValue(pub Vec<ConvexValue>);

/// Defines which fields an index covers.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexDefinition {
    pub name: String,
    pub table: String,
    pub fields: Vec<String>,
}

/// Manages all indexes for a single table.
///
/// Automatically maintains indexes when documents are inserted, updated, or deleted.
#[derive(Debug, Default, Clone)]
pub struct IndexRegistry {
    indexes: BTreeMap<String, Index>,
}

impl IndexRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new index. Returns error if an index with this name already exists.
    pub fn add_index(&mut self, definition: IndexDefinition) -> CoreResult<()> {
        if self.indexes.contains_key(&definition.name) {
            return Err(CoreError::IndexError(format!(
                "index already exists: {}",
                definition.name
            )));
        }
        let name = definition.name.clone();
        self.indexes.insert(name, Index::new(definition));
        Ok(())
    }

    /// Remove an index by name.
    pub fn remove_index(&mut self, name: &str) -> CoreResult<()> {
        self.indexes
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CoreError::IndexError(format!("index not found: {name}")))
    }

    /// Get an index by name for querying.
    pub fn get_index(&self, name: &str) -> CoreResult<&Index> {
        self.indexes
            .get(name)
            .ok_or_else(|| CoreError::IndexError(format!("index not found: {name}")))
    }

    /// Get a mutable reference to an index by name.
    pub fn get_index_mut(&mut self, name: &str) -> CoreResult<&mut Index> {
        self.indexes
            .get_mut(name)
            .ok_or_else(|| CoreError::IndexError(format!("index not found: {name}")))
    }

    /// List all index names in this registry.
    pub fn index_names(&self) -> Vec<&str> {
        self.indexes.keys().map(String::as_str).collect()
    }

    /// Notify all indexes that a document was inserted.
    pub fn on_insert(&mut self, doc_id: &str, fields: &BTreeMap<String, ConvexValue>) {
        for index in self.indexes.values_mut() {
            index.insert(doc_id, fields);
        }
    }

    /// Notify all indexes that a document was removed.
    pub fn on_remove(&mut self, doc_id: &str, fields: &BTreeMap<String, ConvexValue>) {
        for index in self.indexes.values_mut() {
            index.remove(doc_id, fields);
        }
    }

    /// Notify all indexes that a document's fields changed.
    pub fn on_update(
        &mut self,
        doc_id: &str,
        old_fields: &BTreeMap<String, ConvexValue>,
        new_fields: &BTreeMap<String, ConvexValue>,
    ) {
        for index in self.indexes.values_mut() {
            index.update(doc_id, old_fields, new_fields);
        }
    }

    /// Rebuild all indexes from a full set of documents.
    /// Call this after adding a new index to an existing table.
    pub fn rebuild_all<'a>(
        &mut self,
        docs: impl Iterator<Item = (&'a str, &'a BTreeMap<String, ConvexValue>)>,
    ) {
        // Collect docs so we can iterate multiple times (once per index)
        let docs: Vec<_> = docs.collect();
        for index in self.indexes.values_mut() {
            for &(doc_id, fields) in &docs {
                index.insert(doc_id, fields);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user_fields(name: &str, age: i64) -> BTreeMap<String, ConvexValue> {
        BTreeMap::from([
            ("name".to_string(), ConvexValue::from(name)),
            ("age".to_string(), ConvexValue::from(age)),
        ])
    }

    #[test]
    fn add_and_query_index() {
        let mut registry = IndexRegistry::new();
        registry
            .add_index(IndexDefinition {
                name: "by_name".to_string(),
                table: "users".to_string(),
                fields: vec!["name".to_string()],
            })
            .unwrap();

        registry.on_insert("001", &user_fields("Alice", 30));
        registry.on_insert("002", &user_fields("Bob", 25));

        let idx = registry.get_index("by_name").unwrap();
        assert_eq!(idx.lookup(&[ConvexValue::from("Alice")]), vec!["001"]);
        assert_eq!(idx.lookup(&[ConvexValue::from("Bob")]), vec!["002"]);
    }

    #[test]
    fn duplicate_index_fails() {
        let mut registry = IndexRegistry::new();
        let def = IndexDefinition {
            name: "by_name".to_string(),
            table: "users".to_string(),
            fields: vec!["name".to_string()],
        };
        registry.add_index(def.clone()).unwrap();
        assert!(registry.add_index(def).is_err());
    }

    #[test]
    fn remove_index() {
        let mut registry = IndexRegistry::new();
        registry
            .add_index(IndexDefinition {
                name: "by_name".to_string(),
                table: "users".to_string(),
                fields: vec!["name".to_string()],
            })
            .unwrap();

        registry.remove_index("by_name").unwrap();
        assert!(registry.get_index("by_name").is_err());
    }

    #[test]
    fn auto_maintain_on_insert_remove() {
        let mut registry = IndexRegistry::new();
        registry
            .add_index(IndexDefinition {
                name: "by_name".to_string(),
                table: "users".to_string(),
                fields: vec!["name".to_string()],
            })
            .unwrap();

        let fields = user_fields("Alice", 30);
        registry.on_insert("001", &fields);
        assert_eq!(
            registry
                .get_index("by_name")
                .unwrap()
                .lookup(&[ConvexValue::from("Alice")]),
            vec!["001"]
        );

        registry.on_remove("001", &fields);
        assert!(registry
            .get_index("by_name")
            .unwrap()
            .lookup(&[ConvexValue::from("Alice")])
            .is_empty());
    }

    #[test]
    fn auto_maintain_on_update() {
        let mut registry = IndexRegistry::new();
        registry
            .add_index(IndexDefinition {
                name: "by_name".to_string(),
                table: "users".to_string(),
                fields: vec!["name".to_string()],
            })
            .unwrap();

        let old = user_fields("Alice", 30);
        let new = user_fields("Alicia", 30);
        registry.on_insert("001", &old);
        registry.on_update("001", &old, &new);

        let idx = registry.get_index("by_name").unwrap();
        assert!(idx.lookup(&[ConvexValue::from("Alice")]).is_empty());
        assert_eq!(idx.lookup(&[ConvexValue::from("Alicia")]), vec!["001"]);
    }

    #[test]
    fn multiple_indexes_maintained() {
        let mut registry = IndexRegistry::new();
        registry
            .add_index(IndexDefinition {
                name: "by_name".to_string(),
                table: "users".to_string(),
                fields: vec!["name".to_string()],
            })
            .unwrap();
        registry
            .add_index(IndexDefinition {
                name: "by_age".to_string(),
                table: "users".to_string(),
                fields: vec!["age".to_string()],
            })
            .unwrap();

        registry.on_insert("001", &user_fields("Alice", 30));

        assert_eq!(
            registry
                .get_index("by_name")
                .unwrap()
                .lookup(&[ConvexValue::from("Alice")]),
            vec!["001"]
        );
        assert_eq!(
            registry
                .get_index("by_age")
                .unwrap()
                .lookup(&[ConvexValue::from(30i64)]),
            vec!["001"]
        );
    }

    #[test]
    fn rebuild_indexes() {
        let mut registry = IndexRegistry::new();
        registry
            .add_index(IndexDefinition {
                name: "by_name".to_string(),
                table: "users".to_string(),
                fields: vec!["name".to_string()],
            })
            .unwrap();

        let f1 = user_fields("Alice", 30);
        let f2 = user_fields("Bob", 25);
        let docs = vec![("001", &f1), ("002", &f2)];

        registry.rebuild_all(docs.into_iter());

        let idx = registry.get_index("by_name").unwrap();
        assert_eq!(idx.lookup(&[ConvexValue::from("Alice")]), vec!["001"]);
        assert_eq!(idx.lookup(&[ConvexValue::from("Bob")]), vec!["002"]);
    }

    #[test]
    fn list_index_names() {
        let mut registry = IndexRegistry::new();
        registry
            .add_index(IndexDefinition {
                name: "by_age".to_string(),
                table: "users".to_string(),
                fields: vec!["age".to_string()],
            })
            .unwrap();
        registry
            .add_index(IndexDefinition {
                name: "by_name".to_string(),
                table: "users".to_string(),
                fields: vec!["name".to_string()],
            })
            .unwrap();

        let mut names = registry.index_names();
        names.sort();
        assert_eq!(names, vec!["by_age", "by_name"]);
    }
}
