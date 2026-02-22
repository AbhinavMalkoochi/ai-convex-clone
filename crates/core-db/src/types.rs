use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type TableName = String;
pub type Value = serde_json::Value;
pub type DocumentId = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Document {
    pub id: DocumentId,
    pub revision: u64,
    pub fields: BTreeMap<String, Value>,
}
