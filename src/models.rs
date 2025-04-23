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
    pub indexed_column: String,
    pub index_type: FieldDataType,
    pub index_data: IndexStore,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum IndexStore {
    Text(BTreeMap<String, Vec<Value>>),
    #[serde(with = "index_number_map")]
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

pub mod index_number_map {
    use serde_json::Value;
    use std::collections::BTreeMap;
    use serde::{Serializer, Deserializer};
    use serde::ser::SerializeMap;
    use serde::de::{self, MapAccess, Visitor};
    use std::fmt;
    use crate::models::{IndexNumber, OrderedFloat};

    pub fn serialize<S>(
        map: &BTreeMap<IndexNumber, Vec<Value>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_map(Some(map.len()))?;

        for (index, values) in map {
            let key = match index {
                IndexNumber::Int(n) => n.to_string(),
                IndexNumber::Float(f) => f.0.to_string(),
            };
            state.serialize_entry(&key, values)?;
        }

        state.end()
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<IndexNumber, Vec<Value>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct IndexMapVisitor;

        impl<'de> Visitor<'de> for IndexMapVisitor {
            type Value = BTreeMap<IndexNumber, Vec<Value>>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a BTreeMap<String, Vec<Value>> where keys are numbers stored as strings")
            }

            fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut map = BTreeMap::new();

                while let Some((key, value)) = access.next_entry::<String, Vec<Value>>()? {
                    let parsed_key = if let Ok(i) = key.parse::<i64>() {
                        IndexNumber::Int(i)
                    } else if let Ok(f) = key.parse::<f64>() {
                        IndexNumber::Float(OrderedFloat(f))
                    } else {
                        return Err(de::Error::custom(format!("Invalid key for IndexNumber: {}", key)));
                    };

                    map.insert(parsed_key, value);
                }

                Ok(map)
            }
        }

        deserializer.deserialize_map(IndexMapVisitor)
    }
}
