#[derive(Debug, Clone)]
pub enum SchemaType {
    String,
    Number,
    Boolean,
    Object,
    Array,
    Null,
}

#[derive(Debug, Clone)]
pub struct SchemaField {
    pub required: bool,
    pub field_type: SchemaType,
}

#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub fields: std::collections::BTreeMap<String, SchemaField>,
}
