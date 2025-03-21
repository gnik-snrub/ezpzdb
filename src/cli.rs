use clap::{Parser, Subcommand};
use serde_json::{self, json, Number, Value};
use std::collections::{HashMap, HashSet};
use std::process::exit;
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
    Read {
        table: String,
    },
    Get {
        table: String,
        key: String,
        field: Option<String>,
    },
    Set {
        table: String,
        key: String,
        field: String,
        value: String,
    },
    Delete {
        table: String,
        key: String,
        //field: Option<String>,
    },
    Filter {
        table: String,
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

    match cli {
        Cli { command: Some(Command::Read { table}) } => {
            let store: HashMap<String, Value> = load(&table);
            print_to_cli(store);
        }
        Cli { command: Some(Command::Get { table, key, field }) } => {
            let store: HashMap<String, Value> = load(&table);
            match store.get(&key) {
                Some(value) => match field {
                    Some(field_value) => println!("{:?}", value.get(&field_value).unwrap_or(&Value::Null)),
                    None => println!("{:?}", value),
                }
                None => println!("Key not found"),
            }
        }
        Cli { command: Some(Command::Set { table, key, field, value }) } => {
            let mut store: HashMap<String, Value> = load(&table);
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

            save(&table, &store);
        }
        Cli { command: Some(Command::Delete { table, key }) } => {
            let mut store: HashMap<String, Value> = load(&table);
            store.remove(&key);
            save(&table, &store);
        }
        Cli { command: Some(Command::Filter { table, field, value }) } => {
            let store: HashMap<String, Value> = load(&table);
            let mut filtered_store = HashMap::new();
            for (k, v) in store {
                if v.get(&field).unwrap_or(&Value::Null) == &Value::String(value.clone()) {
                    filtered_store.insert(k, v);
                }
            }
            if filtered_store.is_empty() {
                println!("No records found");
            } else {
                print_to_cli(filtered_store);
            }
        }
        Cli { command: Some(Command::Search { query }) } => {
            // FROM not yet implemented, as the current implementation only allows for one table
            let built_query = build_query(query);
            let store: HashMap<String, Value> = load(&built_query.from);

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

                print_to_cli(filtered_store);

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

fn print_to_cli(data: HashMap<String, Value>) {
    let mut first_line = String::from(" | ");
    match data.values().next() {
        Some(entry) => {
            let value = serde_json::to_value(entry).unwrap();
            if let Value::Object(map) = value {
                let field_names: Vec<String> = map.keys().cloned().collect();
                for field in field_names {
                    first_line.push_str(field.as_str());
                    first_line.push_str(" | ")
                }
                println!("{}", first_line);
            }
        },
        None => {
            println!("Entry not found");
        }
    }
    for entry in data.values() {
        let mut entry_text = String::from(" | ");
        let value = serde_json::to_value(entry).unwrap();
        if let Value::Object(map) = value {
            let field_names: Vec<&Value> = map.values().collect();
            for field in field_names {
                entry_text.push_str(field.as_str().unwrap());
                entry_text.push_str(" | ")
            }
            println!("{}", entry_text);
        }
    }
}
