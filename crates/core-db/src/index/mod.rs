mod registry;

pub use registry::{IndexDefinition, IndexRegistry, IndexValue};

use crate::values::ConvexValue;
use std::collections::{BTreeMap, BTreeSet};

/// A single secondary index backed by a BTreeMap.
///
/// Maps composite key values (from one or more document fields) to the set
/// of document IDs that have those values. Supports equality lookups,
/// range scans, and ordered iteration.
#[derive(Debug, Clone)]
pub struct Index {
    definition: IndexDefinition,
    /// Maps (indexed field values) → set of document ID strings.
    entries: BTreeMap<IndexValue, BTreeSet<String>>,
}

impl Index {
    pub fn new(definition: IndexDefinition) -> Self {
        Self {
            definition,
            entries: BTreeMap::new(),
        }
    }

    pub fn definition(&self) -> &IndexDefinition {
        &self.definition
    }

    /// Insert a document's entry into the index.
    pub fn insert(&mut self, doc_id: &str, fields: &BTreeMap<String, ConvexValue>) {
        let key = self.extract_key(fields);
        self.entries
            .entry(key)
            .or_default()
            .insert(doc_id.to_owned());
    }

    /// Remove a document's entry from the index.
    pub fn remove(&mut self, doc_id: &str, fields: &BTreeMap<String, ConvexValue>) {
        let key = self.extract_key(fields);
        if let Some(ids) = self.entries.get_mut(&key) {
            ids.remove(doc_id);
            if ids.is_empty() {
                self.entries.remove(&key);
            }
        }
    }

    /// Update a document's entry (remove old, insert new).
    pub fn update(
        &mut self,
        doc_id: &str,
        old_fields: &BTreeMap<String, ConvexValue>,
        new_fields: &BTreeMap<String, ConvexValue>,
    ) {
        self.remove(doc_id, old_fields);
        self.insert(doc_id, new_fields);
    }

    /// Exact equality lookup: find all document IDs matching the given field values.
    pub fn lookup(&self, values: &[ConvexValue]) -> Vec<&str> {
        let key = IndexValue(values.to_vec());
        self.entries
            .get(&key)
            .map(|ids| ids.iter().map(String::as_str).collect())
            .unwrap_or_default()
    }

    /// Range scan: find all document IDs where the indexed values fall within the range.
    /// Both bounds are optional (None means unbounded).
    pub fn range(&self, lower: Option<&[ConvexValue]>, upper: Option<&[ConvexValue]>) -> Vec<&str> {
        use std::ops::Bound;

        let lower_bound = match lower {
            Some(vals) => Bound::Included(IndexValue(vals.to_vec())),
            None => Bound::Unbounded,
        };
        let upper_bound = match upper {
            Some(vals) => Bound::Excluded(IndexValue(vals.to_vec())),
            None => Bound::Unbounded,
        };

        self.entries
            .range((lower_bound, upper_bound))
            .flat_map(|(_, ids)| ids.iter().map(String::as_str))
            .collect()
    }

    /// Iterate all entries in index order.
    pub fn scan(&self) -> Vec<(&IndexValue, &str)> {
        self.entries
            .iter()
            .flat_map(|(key, ids)| ids.iter().map(move |id| (key, id.as_str())))
            .collect()
    }

    /// Extract the composite key from document fields based on the index definition.
    fn extract_key(&self, fields: &BTreeMap<String, ConvexValue>) -> IndexValue {
        let values: Vec<ConvexValue> = self
            .definition
            .fields
            .iter()
            .map(|field_name| fields.get(field_name).cloned().unwrap_or(ConvexValue::Null))
            .collect();
        IndexValue(values)
    }

    /// Number of unique key combinations in the index.
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }

    /// Total number of entries (document references) in the index.
    pub fn entry_count(&self) -> usize {
        self.entries.values().map(BTreeSet::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index(fields: &[&str]) -> Index {
        Index::new(IndexDefinition {
            name: "test_idx".to_string(),
            table: "users".to_string(),
            fields: fields.iter().map(|s| s.to_string()).collect(),
        })
    }

    fn user_fields(name: &str, age: i64) -> BTreeMap<String, ConvexValue> {
        BTreeMap::from([
            ("name".to_string(), ConvexValue::from(name)),
            ("age".to_string(), ConvexValue::from(age)),
        ])
    }

    #[test]
    fn insert_and_lookup() {
        let mut idx = make_index(&["name"]);
        idx.insert("001", &user_fields("Alice", 30));
        idx.insert("002", &user_fields("Bob", 25));
        idx.insert("003", &user_fields("Alice", 28));

        let results = idx.lookup(&[ConvexValue::from("Alice")]);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&"001"));
        assert!(results.contains(&"003"));

        let results = idx.lookup(&[ConvexValue::from("Bob")]);
        assert_eq!(results, vec!["002"]);

        let results = idx.lookup(&[ConvexValue::from("Charlie")]);
        assert!(results.is_empty());
    }

    #[test]
    fn remove_from_index() {
        let mut idx = make_index(&["name"]);
        idx.insert("001", &user_fields("Alice", 30));
        idx.insert("002", &user_fields("Alice", 25));

        idx.remove("001", &user_fields("Alice", 30));
        let results = idx.lookup(&[ConvexValue::from("Alice")]);
        assert_eq!(results, vec!["002"]);

        idx.remove("002", &user_fields("Alice", 25));
        let results = idx.lookup(&[ConvexValue::from("Alice")]);
        assert!(results.is_empty());
        assert_eq!(idx.key_count(), 0); // cleanup empty entries
    }

    #[test]
    fn update_entry() {
        let mut idx = make_index(&["name"]);
        let old = user_fields("Alice", 30);
        let new = user_fields("Alicia", 30);
        idx.insert("001", &old);

        idx.update("001", &old, &new);
        assert!(idx.lookup(&[ConvexValue::from("Alice")]).is_empty());
        assert_eq!(idx.lookup(&[ConvexValue::from("Alicia")]), vec!["001"]);
    }

    #[test]
    fn compound_index() {
        let mut idx = make_index(&["name", "age"]);
        idx.insert("001", &user_fields("Alice", 30));
        idx.insert("002", &user_fields("Alice", 25));
        idx.insert("003", &user_fields("Bob", 30));

        // Exact compound lookup
        let results = idx.lookup(&[ConvexValue::from("Alice"), ConvexValue::from(30i64)]);
        assert_eq!(results, vec!["001"]);

        let results = idx.lookup(&[ConvexValue::from("Alice"), ConvexValue::from(25i64)]);
        assert_eq!(results, vec!["002"]);
    }

    #[test]
    fn range_scan() {
        let mut idx = make_index(&["age"]);
        idx.insert("001", &user_fields("Alice", 20));
        idx.insert("002", &user_fields("Bob", 25));
        idx.insert("003", &user_fields("Charlie", 30));
        idx.insert("004", &user_fields("Diana", 35));

        // Range: age >= 25 and age < 35
        let results = idx.range(
            Some(&[ConvexValue::from(25i64)]),
            Some(&[ConvexValue::from(35i64)]),
        );
        assert!(results.contains(&"002"));
        assert!(results.contains(&"003"));
        assert!(!results.contains(&"001"));
        assert!(!results.contains(&"004"));
    }

    #[test]
    fn unbounded_range() {
        let mut idx = make_index(&["age"]);
        idx.insert("001", &user_fields("Alice", 20));
        idx.insert("002", &user_fields("Bob", 30));

        // All entries (unbounded both sides)
        let results = idx.range(None, None);
        assert_eq!(results.len(), 2);

        // Lower bounded only: age >= 25
        let results = idx.range(Some(&[ConvexValue::from(25i64)]), None);
        assert_eq!(results, vec!["002"]);

        // Upper bounded only: age < 25
        let results = idx.range(None, Some(&[ConvexValue::from(25i64)]));
        assert_eq!(results, vec!["001"]);
    }

    #[test]
    fn scan_ordered() {
        let mut idx = make_index(&["age"]);
        idx.insert("001", &user_fields("Alice", 30));
        idx.insert("002", &user_fields("Bob", 20));
        idx.insert("003", &user_fields("Charlie", 25));

        let scan: Vec<&str> = idx.scan().into_iter().map(|(_, id)| id).collect();
        assert_eq!(scan, vec!["002", "003", "001"]); // ordered by age: 20, 25, 30
    }

    #[test]
    fn missing_field_indexes_as_null() {
        let mut idx = make_index(&["email"]);
        idx.insert("001", &user_fields("Alice", 30)); // no email field

        let results = idx.lookup(&[ConvexValue::Null]);
        assert_eq!(results, vec!["001"]);
    }

    #[test]
    fn entry_counts() {
        let mut idx = make_index(&["name"]);
        idx.insert("001", &user_fields("Alice", 30));
        idx.insert("002", &user_fields("Alice", 25));
        idx.insert("003", &user_fields("Bob", 30));

        assert_eq!(idx.key_count(), 2); // "Alice" and "Bob"
        assert_eq!(idx.entry_count(), 3); // 3 document references
    }
}
