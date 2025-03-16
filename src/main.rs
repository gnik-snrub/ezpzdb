use clap::{Parser, Subcommand};
use core::num;
use std::any::type_name;
use std::string;
use std::{any::Any, fs};
use std::collections::HashMap;
use serde_json::{self, json, Number, Value};

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
            }
        }
        _ => {
            println!("No command provided");
        }
    }
}

fn build_query(query_tokens: Vec<String>) -> Query {
    let mut select_tokens = vec![];
    let mut from_tokens = vec![];
    let mut where_tokens: Vec<WhereClause> = vec![];

    let mut temp_where_tokens: Vec<String> = vec![];

    let mut current_token: TokenOption = TokenOption::CurrentToken(CurrentToken::None);

    for q in query_tokens {
        if temp_where_tokens.len() > 3 {
            finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);
        } else if temp_where_tokens.len() > 2 && !(temp_where_tokens.contains(&String::from("AND")) || temp_where_tokens.contains(&String::from("OR"))) {
            finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);
        }
        match q.as_str() {
            "SELECT" => current_token = TokenOption::CurrentToken(CurrentToken::Select),
            "FROM" => current_token = TokenOption::CurrentToken(CurrentToken::From),
            "WHERE" => current_token = TokenOption::CurrentToken(CurrentToken::Where),
            "AND" => {
                finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);
                temp_where_tokens.push(q);
            },
            "OR" => {
                finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);
                temp_where_tokens.push(q);
            }
            _ => {
                match current_token {
                    TokenOption::CurrentToken(CurrentToken::Select) => select_tokens.push(q),
                    TokenOption::CurrentToken(CurrentToken::From) => from_tokens.push(q),
                    TokenOption::CurrentToken(CurrentToken::Where) => temp_where_tokens.push(q),
                    _ => {}
                }
            }
        }
    }
    finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);

    let query = Query {
        select: select_tokens,
        from: from_tokens.join(""),
        where_clause: match where_tokens.len() {
            0 => None,
            _ => Some(where_tokens),
        }
    };
    query
}

fn finalize_where_clause(temp_where_tokens: &mut Vec<String>, where_tokens: &mut Vec<WhereClause>) {
    if !temp_where_tokens.is_empty() {
        where_tokens.push(build_where_clause(temp_where_tokens.clone()));
        temp_where_tokens.clear();
    }
}

enum TokenOption {
    CurrentToken(CurrentToken),
    Connector(Connector),
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
    where_clause: Option<Vec<WhereClause>>,
}

fn build_where_clause(mut where_tokens: Vec<String>) -> WhereClause {
    let connector = if (where_tokens[0] == String::from("AND")) || (where_tokens[0] == String::from("OR")) {
        let found = where_tokens.remove(0);
        match found.as_str() {
            "AND" => Some(Connector::AND),
            _ => Some(Connector::OR)
        }
    } else {
        None
    };
    WhereClause {
        left_hand: where_tokens.get(0).cloned().unwrap_or_default(),
        operator: match where_tokens.get(1).map(|s| s.as_str()) {
            Some("=") => Condition::Equals,
            Some("!=") => Condition::NotEquals,
            Some(">") => Condition::GreaterThan,
            Some("<") => Condition::LessThan,
            _ => Condition::Invalid
        },
        right_hand: match where_tokens.get(2) {
            Some(t) if t.parse::<i32>().is_ok() => RightHandType::Integer(i32::from(t.parse::<i32>().unwrap())),
            Some(t) if t.parse::<f64>().is_ok() => RightHandType::Float(f64::from(t.parse::<f64>().unwrap())),
            Some(t) if t.parse::<bool>().is_ok() => RightHandType::Boolean(bool::from(t.parse::<bool>().unwrap())),
            _ => {RightHandType::String(where_tokens[2].clone())},
        },
        connector,
    }
}

#[derive(Debug)]
struct WhereClause {
    left_hand: String,
    operator: Condition,
    right_hand: RightHandType,
    connector: Option<Connector>,
}

#[derive(Debug)]
enum Connector {
    AND,
    OR,
}

#[derive(Debug)]
enum RightHandType {
    String(String),
    Integer(i32),
    Float(f64),
    Boolean(bool),
}

#[derive(Debug)]
enum Condition {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    Invalid
}

fn evaluate_query(row: &HashMap<String, Value>, query: &Query) -> HashMap<String, Value> {
    let clauses = match &query.where_clause {
        Some(clauses) => clauses,
        None => return row.clone(),
    };

    let output = row.clone().into_iter().filter(|v| {
        let mut result = evaluate_clause(v, &clauses[0]);

        for clause in clauses.iter().skip(1) {
            let clause_result = evaluate_clause(v, clause);
            match clause.connector {
                Some(Connector::AND) => result = result && clause_result,
                Some(Connector::OR) => result = result || clause_result,
                None => { result = result && clause_result}
            }
        }

        result
    } ).collect();
    output
}

fn evaluate_clause(data: &(String, Value), clause: &WhereClause) -> bool {
    let left_hand = data.1.get(&clause.left_hand).unwrap_or(&Value::Null);
    let mut right_hand = match &clause.right_hand {
        RightHandType::Integer(i) => Value::Number(Number::from(*i)),
        RightHandType::Float(f) => Value::Number(Number::from_f64(*f).expect("Invalid f64 value")),
        RightHandType::Boolean(b) => Value::Bool(*b),
        RightHandType::String(s) => Value::String(s.clone())
    };
    match clause.operator {
        Condition::Equals => left_hand == &right_hand,
        Condition::NotEquals => left_hand != &right_hand,
        Condition::GreaterThan => compare(left_hand, &right_hand.take(), |l, r| l > r),
        Condition::LessThan => compare(left_hand, &right_hand.take(), |l, r| l < r),
        Condition::Invalid => false
    }
}

fn compare(left: &Value, right: &Value, cmp: impl Fn(f64, f64) -> bool) -> bool {
    if let (Some(l), Some(r)) = (left.as_str().unwrap().parse::<f64>().ok(), right.as_f64()) {
        cmp(l, r)
    } else {
        false
    }
}
