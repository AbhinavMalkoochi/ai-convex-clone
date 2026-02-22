use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type TableName = String;
pub type Value = serde_json::Value;
pub type DocumentId = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Revision(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Document {
    pub id: DocumentId,
    pub revision: Revision,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewDocument {
    pub id: Option<DocumentId>,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableState {
    pub name: TableName,
    pub document_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WriteOperation {
    Put(NewDocument),
    Delete(DocumentId),
}
