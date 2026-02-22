use serde::{Deserialize, Serialize};
use std::fmt;

/// A unique identifier for a document within a table.
///
/// Combines a table name with a UUID v7 (time-ordered) string.
/// The table reference enables type-safe foreign key relationships.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocumentId {
    table: String,
    id: String,
}

impl DocumentId {
    pub fn new(table: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            id: id.into(),
        }
    }

    /// Generate a new DocumentId with a UUID v7 (time-ordered).
    pub fn generate(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            id: uuid::Uuid::now_v7().to_string(),
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.table, self.id)
    }
}

impl PartialOrd for DocumentId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DocumentId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.table.cmp(&other.table).then(self.id.cmp(&other.id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_id_new() {
        let id = DocumentId::new("users", "abc-123");
        assert_eq!(id.table(), "users");
        assert_eq!(id.id(), "abc-123");
    }

    #[test]
    fn test_document_id_generate() {
        let id = DocumentId::generate("users");
        assert_eq!(id.table(), "users");
        assert!(!id.id().is_empty());
    }

    #[test]
    fn test_document_id_display() {
        let id = DocumentId::new("users", "abc-123");
        assert_eq!(id.to_string(), "users:abc-123");
    }

    #[test]
    fn test_document_id_ordering() {
        let a = DocumentId::new("messages", "001");
        let b = DocumentId::new("messages", "002");
        let c = DocumentId::new("users", "001");
        assert!(a < b); // same table, different id
        assert!(b < c); // different table (messages < users)
    }

    #[test]
    fn test_document_id_equality() {
        let a = DocumentId::new("users", "abc");
        let b = DocumentId::new("users", "abc");
        let c = DocumentId::new("users", "xyz");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_document_id_generated_uniqueness() {
        let a = DocumentId::generate("users");
        let b = DocumentId::generate("users");
        assert_ne!(a, b);
    }
}
