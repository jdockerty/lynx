use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Namespace where data should be written.
    pub namespace: String,
    /// Name of the event which is being recorded.
    pub name: String,
    /// Timestamp that the event occurred.
    pub timestamp: i64,
    /// Optional precision of the provided timestamp. When this is not provided,
    /// nanosecond precision is assumed.
    pub precision: Option<Precision>,
    /// Value associated with the event.
    pub value: i64,
    /// Arbitrary key-value metadata associated with the event.
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Precision {
    Nanosecond,
    Microsecond,
}
