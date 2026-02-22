use core_db::{InMemoryEngine, NewDocument, Schema, SchemaField, SchemaType, WriteOperation};
use std::collections::BTreeMap;

fn users_schema() -> Schema {
    let mut fields = BTreeMap::new();
    fields.insert(
        "name".to_string(),
        SchemaField {
            required: true,
            field_type: SchemaType::String,
        },
    );
    Schema::with_fields(fields)
}

#[test]
fn list_tables_and_delete_document() {
    let mut engine = InMemoryEngine::new();
    engine
        .create_table("users", users_schema())
        .expect("table should be created");

    let mut user = BTreeMap::new();
    user.insert(
        "name".to_string(),
        serde_json::Value::String("Lin".to_string()),
    );

    engine
        .write_batch(
            "users",
            &[WriteOperation::Put(NewDocument {
                id: Some("u_1".to_string()),
                fields: user,
            })],
        )
        .expect("insert should work");

    let tables = engine.list_tables();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "users");
    assert_eq!(tables[0].document_count, 1);

    engine
        .write_batch("users", &[WriteOperation::Delete("u_1".to_string())])
        .expect("delete should work");

    let users = engine.list_documents("users").expect("list should succeed");
    assert!(users.is_empty());
}
