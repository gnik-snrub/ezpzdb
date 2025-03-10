use clap::{Parser, Subcommand};
use std::fs;
use std::collections::HashMap;
use serde_json::{self, Value, json};

// Simple key-value store CLI
#[derive(Parser, Debug)]
#[command(name = "Ezpz Database")]
#[command(about = "Simple key-value store CLI", long_about = None)]
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


fn save(store: &HashMap<String, Value>) {
    let store_bin = serde_json::to_string(&store).unwrap();

    fs::write("store.bin", store_bin).unwrap();
}

fn load() -> HashMap<String, Value> {
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

fn main() {
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
            let mut select_tokens = vec![];
            let mut from_tokens = vec![];
            let mut where_tokens = vec![];

            let mut current_token = CurrentToken::None;

            for q in query {
                if q == "SELECT" {
                    current_token = CurrentToken::Select;
                } else if q == "FROM" {
                    current_token = CurrentToken::From;
                } else if q == "WHERE" {
                    current_token = CurrentToken::Where;
                } else {
                    match current_token {
                        CurrentToken::Select => select_tokens.push(q),
                        CurrentToken::From => from_tokens.push(q),
                        CurrentToken::Where => where_tokens.push(q),
                        CurrentToken::None => {}
                    }
                }
            }

            println!("Select: {:?}, From: {:?}, Where: {:?}", select_tokens, from_tokens, where_tokens);
            let query = Query {
                select: select_tokens,
                from: from_tokens.join(""),
                where_clause: match where_tokens[1].as_str() {
                    "=" => Some(Condition::Equals(where_tokens[0].to_string(), SearchValue::Text(where_tokens[2].to_string()))),
                    "!=" => Some(Condition::NotEquals(where_tokens[0].to_string(), SearchValue::Text(where_tokens[2].to_string()))),
                    ">" => Some(Condition::GreaterThan(where_tokens[0].to_string(), SearchValue::Text(where_tokens[2].to_string()))),
                    "<" => Some(Condition::LessThan(where_tokens[0].to_string(), SearchValue::Text(where_tokens[2].to_string()))),
                    _ => None
                }
            };
            println!("Query: {:?}", query);
        }
        _ => {
            println!("No command provided");
        }
    }
}

enum CurrentToken {
    Select,
    From,
    Where,
    None,
}

#[derive(Debug)]
struct Query {
    select: Vec<String>,
    from: String,
    where_clause: Option<Condition>
}

#[derive(Debug)]
enum Condition {
    Equals(String, SearchValue),
    NotEquals(String, SearchValue),
    GreaterThan(String, SearchValue),
    LessThan(String, SearchValue),
}

#[derive(Debug)]
enum SearchValue {
    Integer(i32),
    Float(f64),
    Text(String),
    Boolean(bool),
}
