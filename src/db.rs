use core::num;
use std::any::type_name;
use std::string;
use std::{any::Any, fs};
use std::collections::{HashMap, HashSet};
use serde_json::{self, json, Number, Value};

pub fn save(store: &HashMap<String, Value>) {
    let store_bin = serde_json::to_string(&store).unwrap();

    fs::write("store.bin", store_bin).unwrap();
}

pub fn load() -> HashMap<String, Value> {
    let store_bin = fs::read_to_string("store.bin");
    match store_bin {
        Ok(json_str) => serde_json::from_str(&json_str).unwrap_or_else(|e| {
            eprintln!("Failed to parse JSON: {}", e);
            HashMap::new()
        }),
        Err(e) => {
            eprintln!("Failed to read file: {}", e);
            HashMap::new()
        }
    }
}
