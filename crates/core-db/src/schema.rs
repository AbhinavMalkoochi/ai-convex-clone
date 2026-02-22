use crate::error::{CoreError, CoreResult};
use crate::types::Value;
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

impl Schema {
    pub fn with_fields(fields: BTreeMap<String, SchemaField>) -> Self {
        Self { fields }
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
    use super::{Schema, SchemaField, SchemaType};
    use std::collections::BTreeMap;

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
