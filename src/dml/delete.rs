use std::collections::HashMap;

use serde_json::Value;

use crate::{dql::select::{build_query, evaluate_query}, models::Table, storage::save_to_disk};

pub fn delete(mut table: Table, delete_query_tokens: Vec<String>) {
    let query = build_query(delete_query_tokens);

    let filtered_store: HashMap<Value, Value> = evaluate_query(&table, &query);

    if filtered_store.is_empty() {
        println!("Entry not found");
        return;
    }

    for item in &filtered_store {
        println!("Removing item: {:?}", item);
        table.data.remove(&item.0);
    }
    save_to_disk(&table.name, &table);
}
