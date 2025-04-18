use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use std::cmp::Ordering;

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub schema: Vec<FieldDef>,
    pub data: HashMap<Value, Value>,
    pub indexes: HashMap<String, Index>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub field_name: String,
    pub index_type: FieldDataType,
    pub index_data: IndexStore,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum IndexStore {
    Text(BTreeMap<String, Vec<Value>>),
    Number(BTreeMap<IndexNumber, Vec<Value>>),
    Boolean(BTreeMap<bool, Vec<Value>>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum IndexNumber {
    Int(i64),
    Float(OrderedFloat),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct OrderedFloat(pub f64);

impl Eq for OrderedFloat {}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

impl Eq for IndexNumber {}

impl Ord for IndexNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (IndexNumber::Int(a), IndexNumber::Int(b)) => a.cmp(b),
            (IndexNumber::Float(a), IndexNumber::Float(b)) => a.cmp(b),
            (IndexNumber::Int(a), IndexNumber::Float(b)) => OrderedFloat(*a as f64).cmp(b),
            (IndexNumber::Float(a), IndexNumber::Int(b)) => a.cmp(&OrderedFloat(*b as f64)),
        }
    }
}

impl PartialOrd for IndexNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum FieldDataType {
    TEXT,
    NUMBER,
    BOOLEAN,
    SERIAL,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FieldDef {
    pub name: String,
    pub data_type: Option<FieldDataType>,
    pub primary_key: bool,
    pub serial: Option<SerialState>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SerialState {
    pub next_val: u32,
}
