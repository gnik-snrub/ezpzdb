use serde_json::Value;

use crate::{dql::select::{build_query, evaluate_query}, models::{FieldDataType, Table}, storage::save_to_disk};

pub fn update(mut table: Table, mut tokens: Vec<String>) {
    // Split tokens into "set" and "query" tokens
    let query_position = tokens.iter().position(|t| t == &"where" || t == &"WHERE");
    let query_tokens: Vec<String> = match query_position {
        Some(p) => {
            tokens.split_off(p)
        }
        None => {
            println!("Error: No query entered");
            return;
        }
    };

    if tokens.len() < 1 {
        println!("Error: No changes entered");
        return;
    }

    let default_query_options = vec!["*".to_string(), "FROM".to_string(), table.name.clone()];
    let mut final_query = Vec::with_capacity(query_tokens.len() + default_query_options.len());
    final_query.extend(default_query_options.into_iter());
    final_query.extend(query_tokens.into_iter());


    let query = build_query(final_query);

    let mut update_rows = evaluate_query(&table, &query);

    let field_position = table.schema.iter().position(|f| f.name == tokens[1]);
    let field = match field_position {
        None => {
            println!("Column not found");
            return;
        },
        Some(f) => &table.schema[f]
    };
    let change = UpdateChange {
        left_hand: tokens[1].clone(),
        right_hand: build_right_hand_change(&field.data_type, &tokens[3]),
    };

    for (key, value) in update_rows.iter_mut() {
        value[&change.left_hand] = change.right_hand.clone();
        table.data.insert(key.clone(), value.clone());
    }

    save_to_disk(&table.name, &table);
    // run over set tokens, and apply changes
}

struct UpdateChange {
    left_hand: String,
    right_hand: Value,
}

fn build_right_hand_change(data_type: &Option<FieldDataType>, value: &String) -> Value {
    match data_type {
        Some(FieldDataType::TEXT) => {
            Value::String(value.to_string())
        },
        Some(FieldDataType::NUMBER) => {
            if value.parse::<i64>().is_ok() {
                Value::Number(value.parse::<i64>().unwrap().into())
            } else if value.parse::<f64>().is_ok() {
                Value::Number(serde_json::Number::from_f64(value.parse::<f64>().unwrap()).unwrap())
            } else {
                println!("Error: Could not parse value as number");
                return Value::Null
            }
        },
        Some(FieldDataType::BOOLEAN) => {
            if value.parse::<bool>().is_ok() {
                return Value::Bool(value.parse::<bool>().unwrap())
            }
            println!("Error: Could not parse value as boolean");
            Value::Null
        },
        Some(FieldDataType::SERIAL) => {
            println!("Error: Cannot manually change serial columns");
            Value::Null
        }
        None => {
            println!("Error: Schema column data type not found");
            Value::Null
        }
    }
}
