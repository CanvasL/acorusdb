#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MemValue {
    Value(String),
    Tombstone,
}
