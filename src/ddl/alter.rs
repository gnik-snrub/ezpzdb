use std::cmp::Ordering;

use serde_json::Value;

use crate::{models::{FieldDataType, FieldDef, SerialState, Table}, storage::save_to_disk};


pub fn alter(mut table: Table, action: String, tokens: Vec<String>) {

    match action.as_str() {
        "add" | "ADD" => {
            let field_names: Vec<&String> = table.schema.iter().map(|f| &f.name).collect();
            let col_name = &tokens[0];
            let col_type = &tokens[1].to_uppercase();
            // 1 - Check to see if new column already in table
            // 2 - If exists, early return
            if field_names.contains(&&col_name) {
                println!("Column already exists in table");
                return
            }

            // 3 - If not exists, add new column to schema
            let mut new_field = FieldDef {
                name: col_name.clone(),
                data_type: match col_type.as_str() {
                    "TEXT" => Some(FieldDataType::TEXT),
                    "NUMBER" => Some(FieldDataType::NUMBER),
                    "BOOLEAN" => Some(FieldDataType::BOOLEAN),
                    "SERIAL" => Some(FieldDataType::SERIAL),
                    _ => Some(FieldDataType::TEXT),
                },
                primary_key: false,
                serial: if col_type.as_str() == "SERIAL" {
                    Some(SerialState { next_val: 1 })
                } else {
                    None
                }
            };
            table.schema.push(new_field.clone());

            // 4 - Sort rows, ordered by primary key
            let mut rows: Vec<(&Value, &mut Value)> = table.data.iter_mut().map(|f| (f.0, f.1)).collect();
            rows.sort_by(|c, n| {
                match (c.0, n.0) {
                    (Value::Number(curr), Value::Number(next)) => {
                        let curr_i = curr.as_f64().unwrap();
                        let next_i = next.as_f64().unwrap();
                        if curr_i < next_i {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    },
                    (Value::String(curr), Value::String(next)) => {
                        curr.cmp(next)
                    },
                    (Value::Bool(curr), Value::Bool(next)) => {
                        if *curr && !next {
                            Ordering::Greater
                        } else if !curr && *next {
                            Ordering::Less
                        } else {
                            Ordering::Equal
                        }
                    },
                    _ => { Ordering::Equal }
                }
            });

            // Create updated set of data
            for row in &mut rows {
                if let Value::Object(map) = row.1 {
                    match &new_field.data_type {
                        Some(FieldDataType::TEXT) => {
                            map.insert(new_field.name.clone(), Value::String("".to_string()));
                        },
                        Some(FieldDataType::NUMBER) => {
                            map.insert(new_field.name.clone(), Value::Number(0.into()));
                        },
                        Some(FieldDataType::BOOLEAN) => {
                            map.insert(new_field.name.clone(), Value::Bool(false));
                        },
                        Some(FieldDataType::SERIAL) => {
                            match new_field.serial.as_mut() {
                                Some(next) => {
                                    let curr = next.next_val;
                                    next.next_val += 1;
                                    if new_field.primary_key {
                                        map.insert(new_field.name.clone(), Value::from(curr));
                                    }
                                    row.1[&new_field.name] = Value::from(curr);
                                },
                                None => {eprintln!("Error: SerialState not found");}
                            }
                        },
                        None => {}
                    }
                }
            }

            // Write new table to disk
            println!("Adding {} column", col_name);
            save_to_disk(&table.name, &table);
        },
        "modify" | "MODIFY" => {
            println!("modify");
        },
        "drop" | "DROP" => {
            println!("drop");
        },
        "rename" | "RENAME" => {
            println!("rename");
        },
        _ => {
            println!("not found");
        }
    }

}
