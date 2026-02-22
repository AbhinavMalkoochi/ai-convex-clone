use crate::error::{CoreError, CoreResult};
use crate::types::Value;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaType {
    String,
    Number,
    Boolean,
    Object,
    Array,
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaField {
    pub required: bool,
    pub field_type: SchemaType,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    pub fields: BTreeMap<String, SchemaField>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct WireSchemaField {
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: String,
}

pub type WireCollectionSchema = BTreeMap<String, WireSchemaField>;
pub type WireDatabaseSchema = BTreeMap<String, WireCollectionSchema>;

impl Schema {
    pub fn with_fields(fields: BTreeMap<String, SchemaField>) -> Self {
        Self { fields }
    }

    pub fn from_wire(collection: &WireCollectionSchema) -> CoreResult<Self> {
        let mut fields = BTreeMap::new();

        for (name, wire) in collection {
            let field_type = SchemaType::try_from(wire.field_type.as_str())?;
            fields.insert(
                name.clone(),
                SchemaField {
                    required: wire.required,
                    field_type,
                },
            );
        }

        Ok(Self { fields })
    }

    pub fn validate(&self, input: &BTreeMap<String, Value>) -> CoreResult<()> {
        for (field_name, field) in &self.fields {
            if field.required && !input.contains_key(field_name) {
                return Err(CoreError::SchemaViolation(format!(
                    "missing required field: {}",
                    field_name
                )));
            }
        }

        for (key, value) in input {
            if let Some(expected) = self.fields.get(key) {
                if !matches_schema_type(&expected.field_type, value) {
                    return Err(CoreError::SchemaViolation(format!(
                        "field '{}' expected {:?} but got {}",
                        key,
                        expected.field_type,
                        value_type_name(value)
                    )));
                }
            }
        }

        Ok(())
    }
}

impl TryFrom<&str> for SchemaType {
    type Error = CoreError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "string" => Ok(Self::String),
            "number" => Ok(Self::Number),
            "boolean" => Ok(Self::Boolean),
            "object" => Ok(Self::Object),
            "array" => Ok(Self::Array),
            "null" => Ok(Self::Null),
            _ => Err(CoreError::SchemaViolation(format!(
                "unsupported schema type: {}",
                value
            ))),
        }
    }
}

fn matches_schema_type(schema_type: &SchemaType, value: &Value) -> bool {
    match schema_type {
        SchemaType::String => value.is_string(),
        SchemaType::Number => value.is_number(),
        SchemaType::Boolean => value.is_boolean(),
        SchemaType::Object => value.is_object(),
        SchemaType::Array => value.is_array(),
        SchemaType::Null => value.is_null(),
    }
}

fn value_type_name(value: &Value) -> &'static str {
    if value.is_null() {
        "null"
    } else if value.is_boolean() {
        "boolean"
    } else if value.is_number() {
        "number"
    } else if value.is_string() {
        "string"
    } else if value.is_array() {
        "array"
    } else {
        "object"
    }
}

#[cfg(test)]
mod tests {
    use super::{Schema, SchemaField, SchemaType, WireCollectionSchema, WireSchemaField};
    use std::collections::BTreeMap;

    #[test]
    fn parses_wire_schema_fields() {
        let mut wire = WireCollectionSchema::new();
        wire.insert(
            "name".to_string(),
            WireSchemaField {
                required: true,
                field_type: "string".to_string(),
            },
        );

        let parsed = Schema::from_wire(&wire).expect("wire schema should parse");
        assert_eq!(parsed.fields.len(), 1);
    }

    #[test]
    fn validates_required_and_typed_fields() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "name".to_string(),
            SchemaField {
                required: true,
                field_type: SchemaType::String,
            },
        );

        let schema = Schema::with_fields(fields);

        let mut ok_doc = BTreeMap::new();
        ok_doc.insert(
            "name".to_string(),
            serde_json::Value::String("Ada".to_string()),
        );

        assert!(schema.validate(&ok_doc).is_ok());

        let mut bad_doc = BTreeMap::new();
        bad_doc.insert("name".to_string(), serde_json::Value::Bool(true));

        assert!(schema.validate(&bad_doc).is_err());
    }
}
