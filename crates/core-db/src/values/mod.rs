pub mod id;

pub use id::DocumentId;

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// Table name type alias for clarity.
pub type TableName = String;

/// Core value type mirroring Convex's type system.
///
/// Convex supports a richer set of types than JSON:
/// - Distinct integer (Int64) and floating-point (Float64) numbers
/// - Binary data (Bytes)
/// - All standard JSON types (Null, Boolean, String, Array, Object)
///
/// Values have a defined total ordering for index support:
/// Null < Numbers < Boolean < String < Bytes < Array < Object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConvexValue {
    Null,
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<ConvexValue>),
    Object(BTreeMap<String, ConvexValue>),
}

impl ConvexValue {
    /// Returns the type name as a string, useful for error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Int64(_) => "int64",
            Self::Float64(_) => "float64",
            Self::Boolean(_) => "boolean",
            Self::String(_) => "string",
            Self::Bytes(_) => "bytes",
            Self::Array(_) => "array",
            Self::Object(_) => "object",
        }
    }

    /// Returns the sort key for cross-type ordering in indexes.
    /// Int64 and Float64 share the same sort bucket so they compare numerically.
    fn type_order(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Int64(_) | Self::Float64(_) => 1,
            Self::Boolean(_) => 2,
            Self::String(_) => 3,
            Self::Bytes(_) => 4,
            Self::Array(_) => 5,
            Self::Object(_) => 6,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int64(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float64(n) => Some(*n),
            Self::Int64(n) => Some(*n as f64),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[ConvexValue]> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&BTreeMap<String, ConvexValue>> {
        match self {
            Self::Object(o) => Some(o),
            _ => None,
        }
    }
}

// Manual PartialEq: NaN != NaN for Float64, standard equality for everything else.
impl PartialEq for ConvexValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Float64(a), Self::Float64(b)) => a == b,
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Bytes(a), Self::Bytes(b)) => a == b,
            (Self::Array(a), Self::Array(b)) => a == b,
            (Self::Object(a), Self::Object(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ConvexValue {}

// Total ordering for index support. Numbers compare cross-type (Int64 vs Float64).
impl PartialOrd for ConvexValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ConvexValue {
    fn cmp(&self, other: &Self) -> Ordering {
        let type_ord = self.type_order().cmp(&other.type_order());
        if type_ord != Ordering::Equal {
            return type_ord;
        }
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Int64(a), Self::Int64(b)) => a.cmp(b),
            (Self::Float64(a), Self::Float64(b)) => a.total_cmp(b),
            (Self::Int64(a), Self::Float64(b)) => (*a as f64).total_cmp(b),
            (Self::Float64(a), Self::Int64(b)) => a.total_cmp(&(*b as f64)),
            (Self::Boolean(a), Self::Boolean(b)) => a.cmp(b),
            (Self::String(a), Self::String(b)) => a.cmp(b),
            (Self::Bytes(a), Self::Bytes(b)) => a.cmp(b),
            (Self::Array(a), Self::Array(b)) => a.cmp(b),
            (Self::Object(a), Self::Object(b)) => {
                let mut a_iter = a.iter();
                let mut b_iter = b.iter();
                loop {
                    match (a_iter.next(), b_iter.next()) {
                        (Some((ka, va)), Some((kb, vb))) => {
                            let ord = ka.cmp(kb).then_with(|| va.cmp(vb));
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                        (Some(_), None) => return Ordering::Greater,
                        (None, Some(_)) => return Ordering::Less,
                        (None, None) => return Ordering::Equal,
                    }
                }
            }
            _ => Ordering::Equal,
        }
    }
}

// ---------------------------------------------------------------------------
// From conversions for ergonomic value construction
// ---------------------------------------------------------------------------

impl From<i64> for ConvexValue {
    fn from(v: i64) -> Self {
        Self::Int64(v)
    }
}

impl From<f64> for ConvexValue {
    fn from(v: f64) -> Self {
        Self::Float64(v)
    }
}

impl From<bool> for ConvexValue {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl From<String> for ConvexValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for ConvexValue {
    fn from(v: &str) -> Self {
        Self::String(v.to_owned())
    }
}

impl From<BTreeMap<String, ConvexValue>> for ConvexValue {
    fn from(v: BTreeMap<String, ConvexValue>) -> Self {
        Self::Object(v)
    }
}

// ---------------------------------------------------------------------------
// JSON interop — convert between ConvexValue and serde_json::Value
// ---------------------------------------------------------------------------

impl From<serde_json::Value> for ConvexValue {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(b) => Self::Boolean(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Self::Int64(i)
                } else {
                    Self::Float64(n.as_f64().unwrap_or(f64::NAN))
                }
            }
            serde_json::Value::String(s) => Self::String(s),
            serde_json::Value::Array(arr) => Self::Array(arr.into_iter().map(Self::from).collect()),
            serde_json::Value::Object(obj) => {
                Self::Object(obj.into_iter().map(|(k, v)| (k, Self::from(v))).collect())
            }
        }
    }
}

impl From<ConvexValue> for serde_json::Value {
    fn from(v: ConvexValue) -> Self {
        match v {
            ConvexValue::Null => serde_json::Value::Null,
            ConvexValue::Boolean(b) => serde_json::Value::Bool(b),
            ConvexValue::Int64(i) => serde_json::json!(i),
            ConvexValue::Float64(f) => serde_json::json!(f),
            ConvexValue::String(s) => serde_json::Value::String(s),
            ConvexValue::Bytes(b) => serde_json::json!({ "$bytes": b }),
            ConvexValue::Array(arr) => {
                serde_json::Value::Array(arr.into_iter().map(serde_json::Value::from).collect())
            }
            ConvexValue::Object(obj) => serde_json::Value::Object(
                obj.into_iter()
                    .map(|(k, v)| (k, serde_json::Value::from(v)))
                    .collect(),
            ),
        }
    }
}

/// Helper macro for constructing ConvexValue::Object inline.
///
/// # Example
/// ```
/// use core_db::convex_object;
/// use core_db::values::ConvexValue;
///
/// let obj = convex_object! {
///     "name" => "Alice",
///     "age" => 30i64
/// };
/// ```
#[macro_export]
macro_rules! convex_object {
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut map = std::collections::BTreeMap::new();
        $(
            map.insert($key.to_string(), $crate::values::ConvexValue::from($value));
        )*
        $crate::values::ConvexValue::Object(map)
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_names() {
        assert_eq!(ConvexValue::Null.type_name(), "null");
        assert_eq!(ConvexValue::Int64(42).type_name(), "int64");
        assert_eq!(ConvexValue::Float64(2.72).type_name(), "float64");
        assert_eq!(ConvexValue::Boolean(true).type_name(), "boolean");
        assert_eq!(ConvexValue::String("hello".into()).type_name(), "string");
        assert_eq!(ConvexValue::Bytes(vec![1]).type_name(), "bytes");
        assert_eq!(ConvexValue::Array(vec![]).type_name(), "array");
        assert_eq!(ConvexValue::Object(BTreeMap::new()).type_name(), "object");
    }

    #[test]
    fn equality() {
        assert_eq!(ConvexValue::Int64(42), ConvexValue::Int64(42));
        assert_ne!(ConvexValue::Int64(42), ConvexValue::Float64(42.0));
        assert_eq!(
            ConvexValue::String("abc".into()),
            ConvexValue::String("abc".into())
        );
        assert_ne!(ConvexValue::Null, ConvexValue::Boolean(false));
    }

    #[test]
    fn cross_type_ordering() {
        assert!(ConvexValue::Null < ConvexValue::Int64(0));
        assert!(ConvexValue::Int64(0) < ConvexValue::Boolean(false));
        assert!(ConvexValue::Boolean(true) < ConvexValue::String("".into()));
        assert!(ConvexValue::String("z".into()) < ConvexValue::Bytes(vec![]));
        assert!(ConvexValue::Bytes(vec![]) < ConvexValue::Array(vec![]));
        assert!(ConvexValue::Array(vec![]) < ConvexValue::Object(BTreeMap::new()));
    }

    #[test]
    fn numeric_ordering() {
        assert!(ConvexValue::Int64(1) < ConvexValue::Int64(2));
        assert!(ConvexValue::Float64(1.0) < ConvexValue::Float64(2.0));
        // Cross-type numeric comparison
        assert!(ConvexValue::Int64(1) < ConvexValue::Float64(1.5));
        assert!(ConvexValue::Float64(0.5) < ConvexValue::Int64(1));
    }

    #[test]
    fn string_ordering() {
        assert!(ConvexValue::String("abc".into()) < ConvexValue::String("abd".into()));
        assert!(ConvexValue::String("".into()) < ConvexValue::String("a".into()));
    }

    #[test]
    fn object_ordering() {
        let a = convex_object! { "a" => 1i64 };
        let b = convex_object! { "a" => 2i64 };
        let c = convex_object! { "b" => 1i64 };
        assert!(a < b); // same key, different value
        assert!(a < c); // different key ("a" < "b")
    }

    #[test]
    fn from_conversions() {
        assert_eq!(ConvexValue::from(42i64), ConvexValue::Int64(42));
        assert_eq!(ConvexValue::from(2.72f64), ConvexValue::Float64(2.72));
        assert_eq!(ConvexValue::from(true), ConvexValue::Boolean(true));
        assert_eq!(
            ConvexValue::from("hello"),
            ConvexValue::String("hello".into())
        );
    }

    #[test]
    fn json_roundtrip() {
        let original = convex_object! {
            "name" => "Alice",
            "age" => 30i64,
            "active" => true,
        };
        let json: serde_json::Value = original.clone().into();
        let restored = ConvexValue::from(json);
        assert_eq!(original, restored);
    }

    #[test]
    fn json_roundtrip_nested() {
        let original = convex_object! {
            "tags" => ConvexValue::Array(vec![
                ConvexValue::from("rust"),
                ConvexValue::from("database"),
            ]),
            "metadata" => convex_object! {
                "version" => 1i64,
            },
        };
        let json: serde_json::Value = original.clone().into();
        let restored = ConvexValue::from(json);
        assert_eq!(original, restored);
    }

    #[test]
    fn accessor_methods() {
        assert_eq!(ConvexValue::String("hi".into()).as_str(), Some("hi"));
        assert_eq!(ConvexValue::Int64(42).as_i64(), Some(42));
        assert_eq!(ConvexValue::Float64(2.72).as_f64(), Some(2.72));
        assert_eq!(ConvexValue::Int64(42).as_f64(), Some(42.0));
        assert_eq!(ConvexValue::Boolean(true).as_bool(), Some(true));
        assert!(ConvexValue::Null.is_null());
        assert_eq!(ConvexValue::Null.as_str(), None);
        assert_eq!(ConvexValue::Null.as_i64(), None);
    }

    #[test]
    fn convex_object_macro() {
        let obj = convex_object! {
            "x" => 1i64,
            "y" => "hello",
        };
        if let ConvexValue::Object(map) = &obj {
            assert_eq!(map.get("x"), Some(&ConvexValue::Int64(1)));
            assert_eq!(map.get("y"), Some(&ConvexValue::String("hello".into())));
        } else {
            panic!("expected Object");
        }
    }
}
