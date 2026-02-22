use core_db::{InMemoryEngine, NewDocument, Schema, SchemaField, SchemaType, WriteOperation};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
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

fn bench_write_batch(c: &mut Criterion) {
    c.bench_function("write_single_document", |b| {
        b.iter(|| {
            let mut engine = InMemoryEngine::new();
            engine
                .create_table("users", users_schema())
                .expect("table creation should work");

            let mut fields = BTreeMap::new();
            fields.insert(
                "name".to_string(),
                serde_json::Value::String("bench".to_string()),
            );

            let ops = vec![WriteOperation::Put(NewDocument {
                id: Some("u_bench".to_string()),
                fields,
            })];

            black_box(
                engine
                    .write_batch("users", &ops)
                    .expect("write should succeed"),
            );
        })
    });
}

criterion_group!(benches, bench_write_batch);
criterion_main!(benches);
