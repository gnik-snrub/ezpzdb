use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;


#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub schema: Vec<FieldDef>,
    pub data: HashMap<Value, Value>,
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
