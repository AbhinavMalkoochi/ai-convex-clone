use crate::values::ConvexValue;
use std::collections::BTreeMap;

/// Describes the expected type of a document field.
///
/// Mirrors Convex's schema type system, supporting primitives,
/// nested objects, arrays with element types, optional fields,
/// union types, literal values, and `Id` references to other tables.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    /// Any string value.
    String,
    /// Any number (Int64 or Float64).
    Number,
    /// A boolean value.
    Boolean,
    /// A null value.
    Null,
    /// Raw bytes.
    Bytes,
    /// A reference to a document in the specified table.
    Id(String),
    /// An array where every element matches the inner type.
    Array(Box<FieldType>),
    /// An object with a defined set of fields.
    Object(BTreeMap<String, FieldDefinition>),
    /// A value that may be one of several types.
    Union(Vec<FieldType>),
    /// A specific literal string value.
    LiteralString(String),
    /// A specific literal number value.
    LiteralNumber(f64),
    /// A specific literal boolean value.
    LiteralBool(bool),
    /// Accepts any value (opts out of validation for this field).
    Any,
}

/// A single field definition within a table schema.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDefinition {
    pub field_type: FieldType,
    pub optional: bool,
}

impl FieldDefinition {
    pub fn required(field_type: FieldType) -> Self {
        Self {
            field_type,
            optional: false,
        }
    }

    pub fn optional(field_type: FieldType) -> Self {
        Self {
            field_type,
            optional: true,
        }
    }
}

/// Schema for a single table, defining the expected shape of its documents.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    /// Field definitions. Only user fields — system fields (_id, _creationTime) are implicit.
    pub fields: BTreeMap<String, FieldDefinition>,
    /// Whether to reject documents with fields not listed in the schema.
    pub strict: bool,
}

impl TableSchema {
    /// Create a strict schema (rejects unknown fields).
    pub fn strict(fields: BTreeMap<String, FieldDefinition>) -> Self {
        Self {
            fields,
            strict: true,
        }
    }

    /// Create a permissive schema (allows extra fields).
    pub fn permissive(fields: BTreeMap<String, FieldDefinition>) -> Self {
        Self {
            fields,
            strict: false,
        }
    }
}

/// Database-level schema definition mapping table names to their schemas.
#[derive(Debug, Clone, Default)]
pub struct SchemaDefinition {
    pub tables: BTreeMap<String, TableSchema>,
}

impl SchemaDefinition {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn define_table(&mut self, name: impl Into<String>, schema: TableSchema) {
        self.tables.insert(name.into(), schema);
    }

    pub fn get_table_schema(&self, table: &str) -> Option<&TableSchema> {
        self.tables.get(table)
    }
}

/// Validate a document's fields against a table schema.
///
/// Returns `Ok(())` if the document is valid, or a descriptive error message.
pub fn validate_document(
    fields: &BTreeMap<String, ConvexValue>,
    schema: &TableSchema,
) -> Result<(), String> {
    // Check all required fields are present
    for (field_name, definition) in &schema.fields {
        if !definition.optional && !fields.contains_key(field_name) {
            return Err(format!("missing required field: `{field_name}`"));
        }
    }

    // Check each provided field
    for (field_name, value) in fields {
        // Reject system field names in user data
        if field_name.starts_with('_') {
            return Err(format!(
                "field names cannot start with underscore: `{field_name}`"
            ));
        }

        match schema.fields.get(field_name) {
            Some(definition) => {
                validate_value(value, &definition.field_type, field_name)?;
            }
            None if schema.strict => {
                return Err(format!("unknown field `{field_name}` in strict schema"));
            }
            None => {} // permissive: extra fields are allowed
        }
    }

    Ok(())
}

/// Validate a single value against a field type, recursively.
fn validate_value(value: &ConvexValue, expected: &FieldType, path: &str) -> Result<(), String> {
    match expected {
        FieldType::Any => Ok(()),
        FieldType::Null => match value {
            ConvexValue::Null => Ok(()),
            _ => Err(type_error(path, "null", value)),
        },
        FieldType::String => match value {
            ConvexValue::String(_) => Ok(()),
            _ => Err(type_error(path, "string", value)),
        },
        FieldType::Number => match value {
            ConvexValue::Int64(_) | ConvexValue::Float64(_) => Ok(()),
            _ => Err(type_error(path, "number", value)),
        },
        FieldType::Boolean => match value {
            ConvexValue::Boolean(_) => Ok(()),
            _ => Err(type_error(path, "boolean", value)),
        },
        FieldType::Bytes => match value {
            ConvexValue::Bytes(_) => Ok(()),
            _ => Err(type_error(path, "bytes", value)),
        },
        FieldType::Id(table) => match value {
            ConvexValue::String(s) if s.starts_with(&format!("{table}:")) => Ok(()),
            ConvexValue::String(_) => Err(format!(
                "field `{path}`: expected Id reference to table `{table}`, got different reference"
            )),
            _ => Err(type_error(path, &format!("Id<{table}>"), value)),
        },
        FieldType::Array(element_type) => match value {
            ConvexValue::Array(items) => {
                for (i, item) in items.iter().enumerate() {
                    let item_path = format!("{path}[{i}]");
                    validate_value(item, element_type, &item_path)?;
                }
                Ok(())
            }
            _ => Err(type_error(path, "array", value)),
        },
        FieldType::Object(field_defs) => match value {
            ConvexValue::Object(obj) => {
                // Check required fields
                for (key, def) in field_defs {
                    if !def.optional && !obj.contains_key(key) {
                        return Err(format!("field `{path}.{key}`: required but missing"));
                    }
                }
                // Validate present fields
                for (key, val) in obj {
                    let nested_path = format!("{path}.{key}");
                    if let Some(def) = field_defs.get(key) {
                        validate_value(val, &def.field_type, &nested_path)?;
                    }
                    // Nested objects are always permissive for extra fields
                }
                Ok(())
            }
            _ => Err(type_error(path, "object", value)),
        },
        FieldType::Union(variants) => {
            for variant in variants {
                if validate_value(value, variant, path).is_ok() {
                    return Ok(());
                }
            }
            let type_names: Vec<&str> = variants.iter().map(|v| field_type_name(v)).collect();
            Err(format!(
                "field `{path}`: expected one of [{}], got {}",
                type_names.join(", "),
                value.type_name()
            ))
        }
        FieldType::LiteralString(expected_val) => match value {
            ConvexValue::String(s) if s == expected_val => Ok(()),
            ConvexValue::String(s) => Err(format!(
                "field `{path}`: expected literal \"{expected_val}\", got \"{s}\""
            )),
            _ => Err(type_error(
                path,
                &format!("literal \"{expected_val}\""),
                value,
            )),
        },
        FieldType::LiteralNumber(expected_val) => match value {
            ConvexValue::Float64(f) if (f - expected_val).abs() < f64::EPSILON => Ok(()),
            ConvexValue::Int64(i) if (*i as f64 - expected_val).abs() < f64::EPSILON => Ok(()),
            _ => Err(type_error(path, &format!("literal {expected_val}"), value)),
        },
        FieldType::LiteralBool(expected_val) => match value {
            ConvexValue::Boolean(b) if b == expected_val => Ok(()),
            _ => Err(type_error(path, &format!("literal {expected_val}"), value)),
        },
    }
}

fn type_error(path: &str, expected: &str, got: &ConvexValue) -> String {
    format!(
        "field `{path}`: expected {expected}, got {}",
        got.type_name()
    )
}

fn field_type_name(ft: &FieldType) -> &'static str {
    match ft {
        FieldType::String => "string",
        FieldType::Number => "number",
        FieldType::Boolean => "boolean",
        FieldType::Null => "null",
        FieldType::Bytes => "bytes",
        FieldType::Id(_) => "id",
        FieldType::Array(_) => "array",
        FieldType::Object(_) => "object",
        FieldType::Union(_) => "union",
        FieldType::LiteralString(_) => "literal_string",
        FieldType::LiteralNumber(_) => "literal_number",
        FieldType::LiteralBool(_) => "literal_bool",
        FieldType::Any => "any",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convex_object;

    fn user_schema() -> TableSchema {
        TableSchema::strict(BTreeMap::from([
            (
                "name".to_string(),
                FieldDefinition::required(FieldType::String),
            ),
            (
                "age".to_string(),
                FieldDefinition::required(FieldType::Number),
            ),
            (
                "email".to_string(),
                FieldDefinition::optional(FieldType::String),
            ),
        ]))
    }

    #[test]
    fn valid_document_passes() {
        let schema = user_schema();
        let fields = BTreeMap::from([
            ("name".to_string(), ConvexValue::from("Alice")),
            ("age".to_string(), ConvexValue::from(30i64)),
        ]);
        assert!(validate_document(&fields, &schema).is_ok());
    }

    #[test]
    fn valid_with_optional_field() {
        let schema = user_schema();
        let fields = BTreeMap::from([
            ("name".to_string(), ConvexValue::from("Alice")),
            ("age".to_string(), ConvexValue::from(30i64)),
            ("email".to_string(), ConvexValue::from("alice@example.com")),
        ]);
        assert!(validate_document(&fields, &schema).is_ok());
    }

    #[test]
    fn missing_required_field_fails() {
        let schema = user_schema();
        let fields = BTreeMap::from([("name".to_string(), ConvexValue::from("Alice"))]);
        let err = validate_document(&fields, &schema).unwrap_err();
        assert!(err.contains("missing required field"));
        assert!(err.contains("age"));
    }

    #[test]
    fn wrong_type_fails() {
        let schema = user_schema();
        let fields = BTreeMap::from([
            ("name".to_string(), ConvexValue::from(123i64)), // should be string
            ("age".to_string(), ConvexValue::from(30i64)),
        ]);
        let err = validate_document(&fields, &schema).unwrap_err();
        assert!(err.contains("expected string"));
    }

    #[test]
    fn strict_rejects_unknown_fields() {
        let schema = user_schema();
        let fields = BTreeMap::from([
            ("name".to_string(), ConvexValue::from("Alice")),
            ("age".to_string(), ConvexValue::from(30i64)),
            ("unknown".to_string(), ConvexValue::from("value")),
        ]);
        let err = validate_document(&fields, &schema).unwrap_err();
        assert!(err.contains("unknown field"));
    }

    #[test]
    fn permissive_allows_unknown_fields() {
        let schema = TableSchema::permissive(BTreeMap::from([(
            "name".to_string(),
            FieldDefinition::required(FieldType::String),
        )]));
        let fields = BTreeMap::from([
            ("name".to_string(), ConvexValue::from("Alice")),
            ("extra".to_string(), ConvexValue::from("allowed")),
        ]);
        assert!(validate_document(&fields, &schema).is_ok());
    }

    #[test]
    fn rejects_underscore_prefixed_fields() {
        let schema = user_schema();
        let fields = BTreeMap::from([
            ("name".to_string(), ConvexValue::from("Alice")),
            ("age".to_string(), ConvexValue::from(30i64)),
            ("_secret".to_string(), ConvexValue::from("bad")),
        ]);
        let err = validate_document(&fields, &schema).unwrap_err();
        assert!(err.contains("underscore"));
    }

    #[test]
    fn nested_object_validation() {
        let address_fields = BTreeMap::from([
            (
                "street".to_string(),
                FieldDefinition::required(FieldType::String),
            ),
            (
                "city".to_string(),
                FieldDefinition::required(FieldType::String),
            ),
        ]);
        let schema = TableSchema::strict(BTreeMap::from([(
            "address".to_string(),
            FieldDefinition::required(FieldType::Object(address_fields)),
        )]));

        // Valid nested object
        let fields = BTreeMap::from([(
            "address".to_string(),
            convex_object! {
                "street" => "123 Main St",
                "city" => "Springfield",
            },
        )]);
        assert!(validate_document(&fields, &schema).is_ok());

        // Missing nested required field
        let fields = BTreeMap::from([(
            "address".to_string(),
            convex_object! { "street" => "123 Main St" },
        )]);
        let err = validate_document(&fields, &schema).unwrap_err();
        assert!(err.contains("city"));
    }

    #[test]
    fn array_validation() {
        let schema = TableSchema::strict(BTreeMap::from([(
            "tags".to_string(),
            FieldDefinition::required(FieldType::Array(Box::new(FieldType::String))),
        )]));

        // Valid array
        let fields = BTreeMap::from([(
            "tags".to_string(),
            ConvexValue::Array(vec![ConvexValue::from("rust"), ConvexValue::from("db")]),
        )]);
        assert!(validate_document(&fields, &schema).is_ok());

        // Invalid element in array
        let fields = BTreeMap::from([(
            "tags".to_string(),
            ConvexValue::Array(vec![ConvexValue::from("rust"), ConvexValue::from(42i64)]),
        )]);
        let err = validate_document(&fields, &schema).unwrap_err();
        assert!(err.contains("tags[1]"));
        assert!(err.contains("expected string"));
    }

    #[test]
    fn union_type_validation() {
        let schema = TableSchema::strict(BTreeMap::from([(
            "value".to_string(),
            FieldDefinition::required(FieldType::Union(vec![FieldType::String, FieldType::Number])),
        )]));

        // String variant
        let fields = BTreeMap::from([("value".to_string(), ConvexValue::from("hello"))]);
        assert!(validate_document(&fields, &schema).is_ok());

        // Number variant
        let fields = BTreeMap::from([("value".to_string(), ConvexValue::from(42i64))]);
        assert!(validate_document(&fields, &schema).is_ok());

        // Invalid variant
        let fields = BTreeMap::from([("value".to_string(), ConvexValue::from(true))]);
        let err = validate_document(&fields, &schema).unwrap_err();
        assert!(err.contains("expected one of"));
    }

    #[test]
    fn literal_validation() {
        let schema = TableSchema::strict(BTreeMap::from([(
            "status".to_string(),
            FieldDefinition::required(FieldType::Union(vec![
                FieldType::LiteralString("active".to_string()),
                FieldType::LiteralString("inactive".to_string()),
            ])),
        )]));

        let fields = BTreeMap::from([("status".to_string(), ConvexValue::from("active"))]);
        assert!(validate_document(&fields, &schema).is_ok());

        let fields = BTreeMap::from([("status".to_string(), ConvexValue::from("deleted"))]);
        assert!(validate_document(&fields, &schema).is_err());
    }

    #[test]
    fn id_reference_validation() {
        let schema = TableSchema::strict(BTreeMap::from([(
            "authorId".to_string(),
            FieldDefinition::required(FieldType::Id("users".to_string())),
        )]));

        // Valid reference
        let fields = BTreeMap::from([("authorId".to_string(), ConvexValue::from("users:abc123"))]);
        assert!(validate_document(&fields, &schema).is_ok());

        // Wrong table reference
        let fields =
            BTreeMap::from([("authorId".to_string(), ConvexValue::from("messages:abc123"))]);
        assert!(validate_document(&fields, &schema).is_err());

        // Not an ID at all
        let fields = BTreeMap::from([("authorId".to_string(), ConvexValue::from(42i64))]);
        assert!(validate_document(&fields, &schema).is_err());
    }

    #[test]
    fn number_accepts_both_int_and_float() {
        let schema = TableSchema::strict(BTreeMap::from([(
            "value".to_string(),
            FieldDefinition::required(FieldType::Number),
        )]));

        let fields = BTreeMap::from([("value".to_string(), ConvexValue::from(42i64))]);
        assert!(validate_document(&fields, &schema).is_ok());

        let fields = BTreeMap::from([("value".to_string(), ConvexValue::from(42.5f64))]);
        assert!(validate_document(&fields, &schema).is_ok());
    }

    #[test]
    fn schema_definition() {
        let mut schema_def = SchemaDefinition::new();
        schema_def.define_table("users", user_schema());

        assert!(schema_def.get_table_schema("users").is_some());
        assert!(schema_def.get_table_schema("messages").is_none());
    }
}
