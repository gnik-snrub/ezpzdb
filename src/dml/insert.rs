use std::collections::HashMap;
use serde_json::Value;

use crate::{models::{FieldDataType, FieldDef, Table}, storage::save_to_disk};

pub fn insert(mut table: Table, new_data_tokens: Vec<String>) {
    let (new_key, new_row) = generate_row_data(&mut table.schema, new_data_tokens.into());

    if new_key == Value::Null {
        eprintln!("Error: Key is missing");
        return
    }

    if table.data.contains_key(&new_key) {
        eprintln!("Error: Key is already in use");
        return
    }

    let json_row: serde_json::Map<String, Value> = new_row.into_iter().collect();
    table.data.insert(new_key, Value::Object(json_row));
    save_to_disk(&table.name, &table);
}

fn generate_row_data(schema: &mut Vec<FieldDef>, new_data_tokens: Vec<String>) -> (Value, HashMap<String, Value>) {
    let mut row_data_result: HashMap<String, Value> = HashMap::new();
    let mut row_key: Value = Value::Null;
    let mut field_name: String;
    let mut field_value: Value = Value::Null;
    for field in schema.iter_mut() {
        field_name = field.name.clone();
        let field_index = new_data_tokens.iter().position(|f| **f == field.name);
        let value_token = match field_index {
            Some(v) => new_data_tokens[v + 1].clone(),
            None => String::from("")
        };
        match field.data_type.as_ref().unwrap() {
            FieldDataType::NUMBER => {
                let value = match value_token.parse::<i64>() {
                    Ok(i) => Value::Number(i.into()),
                    Err(_) => match value_token.parse::<f64>() {
                        Ok(f) => Value::Number(serde_json::Number::from_f64(f).unwrap()),
                        Err(_) => Value::Null
                    }
                };
                if field.primary_key {
                    row_key = value.clone();
                }
                field_value = value;
            },
            FieldDataType::BOOLEAN => {
                let val = if value_token.to_lowercase() == "true" {
                    Value::Bool(true)
                } else {
                    Value::Bool(false)
                };
                if field.primary_key {
                    row_key = val.clone();
                }
                field_value = val;
            },
            FieldDataType::TEXT => {
                if field.primary_key {
                    row_key = Value::String(value_token.clone());
                }
                field_value = Value::String(value_token.clone());
            },
            FieldDataType::SERIAL => {
                match field.serial.as_mut() {
                    Some(next) => {
                        let curr = next.next_val;
                        next.next_val += 1;
                        if field.primary_key {
                            row_key = Value::from(curr);
                        }
                        field_value = Value::from(curr);
                    },
                    None => {eprintln!("Error: SerialState not found");}
                }
            },
        }
        row_data_result.insert(field_name, field_value.clone());
    }
    (row_key, row_data_result)
} 
