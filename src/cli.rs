use clap::{Parser, Subcommand};
use serde_json::{self, json, Number, Value};
use std::collections::{HashMap, HashSet};
use crate::query::{build_query, evaluate_query};
use crate::db::{save, load};

// Simple key-value store CLI
#[derive(Parser, Debug)]
#[command(name = "Ezpz Database")]
#[command(about = "Simple SQL-like database CLI", long_about = None)]
struct Cli {
    // Key to get or set
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Clone, Debug)]
enum Command {
    Read,
    Get {
        key: String,
        field: Option<String>,
    },
    Set {
        key: String,
        field: String,
        value: String,
    },
    Delete {
        key: String,
    },
    Filter {
        field: String,
        value: String,
    },
    Search {
        #[arg(trailing_var_arg = true, num_args(1..))]
        query: Vec<String>,
    },
}


pub fn ezpzdb_cli() {
    let cli = Cli::parse();
    let mut store: HashMap<String, Value> = load();

    match cli {
        Cli { command: Some(Command::Read) } => {
            println!("{:?}", store);
        }
        Cli { command: Some(Command::Get { key, field }) } => {
            match store.get(&key) {
                Some(value) => match field {
                    Some(field_value) => println!("{:?}", value.get(&field_value).unwrap_or(&Value::Null)),
                    None => println!("{:?}", value),
                }
                None => println!("Key not found"),
            }
        }
        Cli { command: Some(Command::Set { key, field, value }) } => {
            let mut set = json!({});
            if let Some(record) = store.get_mut(&key) {
                if let Some(obj) = record.as_object_mut() {
                    obj.insert(field, json!(value));
                    set = json!(obj);
                } else {
                    println!("Record is not an object");
                }
            } else {
                set = json!({field: json!(value)});
            }
            store.insert(key, set);

            save(&store);
        }
        Cli { command: Some(Command::Delete { key }) } => {
            store.remove(&key);
            save(&store);
        }
        Cli { command: Some(Command::Filter { field, value }) } => {
            let mut filtered_store = HashMap::new();
            for (k, v) in store {
                if v.get(&field).unwrap_or(&Value::Null) == &Value::String(value.clone()) {
                    filtered_store.insert(k, v);
                }
            }
            if filtered_store.is_empty() {
                println!("No records found");
            } else {
                println!("{:?}", filtered_store);
            }
        }
        Cli { command: Some(Command::Search { query }) } => {
            // FROM not yet implemented, as the current implementation only allows for one table
            let built_query = build_query(query);

            let mut filtered_store = evaluate_query(&store, &built_query);
            if filtered_store.is_empty() {
                println!("No records found");
            } else {
                if built_query.select != vec!["*".to_string()] {
                    for (_k, v) in filtered_store.iter_mut() {
                        if let Some(obj) = v.as_object_mut() {
                            obj.retain(|field_key, _field_value| {
                                built_query.select.contains(field_key)
                            })
                        }
                    }
                }
                println!("{:?}", filtered_store);

                // List missing fields
                let all_fields: HashSet<String> = store.values().filter_map(|v| v.as_object()).flat_map(|obj| obj.keys().cloned()).collect();
                for field in &built_query.select {
                    if !all_fields.contains(field) {
                        println!("Field not found: {}", field);
                    }
                }
            }
        }
        _ => {
            println!("No command provided");
        }
    }
}
