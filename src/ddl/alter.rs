use std::cmp::Ordering;

use serde_json::Value;

use crate::{models::{FieldDataType, FieldDef, SerialState, Table}, storage::save_to_disk};


pub fn alter(mut table: Table, action: String, tokens: Vec<String>) {

    match action.as_str() {
        "add" | "ADD" => {
            if tokens.len() <= 1 {
                println!("Missing parameters");
                return;
            }

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
            if tokens.len() <= 1 {
                println!("Missing parameters");
                return;
            }

            let col_name = &tokens[0];
            let new_type = &tokens[1];

            let col_index = table.schema.iter().position(|f| &f.name == col_name);
            if let Some(i) = col_index {
                match new_type.as_str() {
                    "TEXT" => {
                        if table.schema[i].data_type == Some(FieldDataType::TEXT) {
                            println!("Schema field {} already set to TEXT", col_name);
                            return;
                        } else {
                            table.schema[i].data_type = Some(FieldDataType::TEXT);
                        }
                    },
                    "NUMBER" => {
                        if table.schema[i].data_type == Some(FieldDataType::NUMBER) {
                            println!("Schema field {} already set to NUMBER", col_name);
                            return;
                        } else {
                            table.schema[i].data_type = Some(FieldDataType::NUMBER);
                        }
                    },
                    "BOOLEAN" => {
                        if table.schema[i].data_type == Some(FieldDataType::BOOLEAN) {
                            println!("Schema field {} already set to BOOLEAN", col_name);
                            return;
                        } else {
                            table.schema[i].data_type = Some(FieldDataType::BOOLEAN);
                        }
                    },
                    "SERIAL" => {
                        if table.schema[i].data_type == Some(FieldDataType::SERIAL) {
                            println!("Schema field {} already set to SERIAL", col_name);
                            return;
                        } else {
                            table.schema[i].data_type = Some(FieldDataType::SERIAL);
                        }
                    },
                    _ => {
                        println!("{} is an invalid data type", new_type);
                        return;
                    }
                }
                if new_type.as_str() == "SERIAL" {
                    table.schema[i].serial = Some(SerialState { next_val: 1 });
                }
            } else {
                println!("Column not found");
            }

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
                if let (Some(map), Some(i)) = (row.1.as_object_mut(), col_index) {
                    if table.schema[i].data_type.is_none() {
                        println!("Error in columns existing data type");
                        return;
                    }
                    let field = &mut table.schema[i];
                    let val = map.get_mut(&field.name);
                    match field.data_type {
                        Some(FieldDataType::TEXT) => {
                            if let Some(v) = &val {
                                map[&field.name] = Value::String(v.to_string().trim().to_string());
                            }
                        },
                        Some(FieldDataType::NUMBER) => {
                            if let Some(v) = &val {
                                if v.to_string().parse::<i64>().is_ok() {
                                    map[&field.name] = Value::Number(v.to_string().parse::<i64>().unwrap().into());
                                } else {
                                    map[&field.name] = Value::Number(0.into());
                                }
                            }
                        },
                        Some(FieldDataType::BOOLEAN) => {
                            if let Value::String(v) = &val.unwrap() {
                                if v.to_string().parse::<bool>().is_ok() {
                                    map[&field.name] = Value::Bool(v.to_string().parse::<bool>().unwrap());
                                } else {
                                    map[&field.name] = Value::Bool(false);
                                }
                            }
                        },
                        Some(FieldDataType::SERIAL) => {
                            match field.serial.as_mut() {
                                Some(next) => {
                                    let curr = next.next_val;
                                    next.next_val += 1;
                                    if field.primary_key {
                                        map.insert(field.name.clone(), Value::from(curr));
                                    }
                                    row.1[&field.name] = Value::from(curr);
                                },
                                None => {eprintln!("Error: SerialState not found");}
                            }
                        },
                        None => {}
                    }
                }
            }
            println!("Modified column {} to use {} data type", col_name, new_type);
            save_to_disk(&table.name, &table);
        },
        "drop" | "DROP" => {
            if tokens.len() <= 0 {
                println!("Missing parameters");
                return;
            }

            let col_name = &tokens[0];

            let primary_key_name = table.schema.iter().find(|f| f.primary_key);
            if let Some(key) = primary_key_name {
                if &key.name == col_name {
                    println!("Cannot drop primary key");
                    return;
                }
            }

            let col_index = table.schema.iter().position(|f| &f.name == col_name);
            if let Some(i) = col_index {
                table.schema.remove(i);
            } else {
                println!("Column not found");
                return;
            }

            for row in table.data.iter_mut() {
                if let Value::Object(map) = row.1 {
                    map.remove(col_name);
                }
            }

            println!("Dropping {} column", col_name);
            save_to_disk(&table.name, &table);
        },
        "rename" | "RENAME" => {
            if tokens.len() <= 1 {
                println!("Missing parameters");
                return;
            }

            let col_name = &tokens[0];
            let new_name = &tokens[1];

            let col_index = table.schema.iter().position(|f| &f.name == col_name);
            if let Some(i) = col_index {
                table.schema[i].name = new_name.clone();
            } else {
                println!("Column not found");
                return;
            }

            for row in table.data.iter_mut() {
                if let Value::Object(map) = row.1 {
                    let val = map.get(col_name).unwrap();
                    map.insert(new_name.clone(), val.clone());
                    map.remove(col_name);
                }
            }
            println!("Renamed column {} to {}", col_name, new_name);
            save_to_disk(&table.name, &table);
        },
        _ => {
            println!("not found");
        }
    }

}
