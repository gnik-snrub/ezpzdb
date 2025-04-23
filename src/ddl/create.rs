use std::{collections::{BTreeMap, HashMap, VecDeque}, fs::write};
use directories::UserDirs;
use serde_json::Value;

use crate::{models::{FieldDataType, FieldDef, Index, IndexNumber, IndexStore, OrderedFloat, SerialState, Table}, storage::{load_from_disk, save_to_disk}};

pub enum CreateData {
    Table { name: String, schema: Vec<String> },
    Index { table: String, column: String },
    // Not yet implemented the following
    //Database
}

pub fn create(create_data: CreateData) {
    match create_data {
        CreateData::Table {name, schema }=> {
            if let Some(dirs) = UserDirs::new() {
                let mut file_name = String::from(name.clone());
                file_name.push_str(".db");
                let path = dirs.home_dir().join("Documents/ezpzdb/").join(file_name);

                let fields = generate_schema(schema.clone().into());
                let new_table = Table { name, schema: fields, data: HashMap::new(), indexes: HashMap::new()};
                let schema_json = serde_json::to_string_pretty(&new_table);
                match schema_json {
                    Ok(sj) => {
                        let new_file = write(path, sj);
                        match new_file {
                            Ok(_) => { println!("New table created"); },
                            Err(_) => { panic!("Error creating table"); }
                        }
                    },
                    Err(_) => { panic!("Error: Schema could not convert to JSON"); }
                }

            } else {
                panic!("No home directory found");
            }
        },
        CreateData::Index { table, column } => {
            let mut table_from_disk = load_from_disk(&table);
            let column_position = table_from_disk.schema.iter().position(|p| p.name == column);
            if let Some(pos) = column_position {
                let index_type;
                let column_type = &table_from_disk.schema[pos].data_type;
                // assign FieldDataType to index_type, according to column type
                match column_type {
                    Some(FieldDataType::TEXT) => {
                        index_type = FieldDataType::TEXT
                    },
                    Some(FieldDataType::NUMBER) | Some(FieldDataType::SERIAL) => {
                        index_type = FieldDataType::NUMBER
                    },
                    Some(FieldDataType::BOOLEAN) => {
                        index_type = FieldDataType::BOOLEAN
                    },
                    None => {
                        println!("Error in schema column data");
                        return;
                    }
                }

                // Go through table data, adding to BTreeMap
                let mut index_data = match index_type {
                    FieldDataType::TEXT => {
                        IndexStore::Text(BTreeMap::new())
                    },
                    FieldDataType::NUMBER | FieldDataType::SERIAL => {
                        IndexStore::Number(BTreeMap::new())
                    },
                    FieldDataType::BOOLEAN => {
                        IndexStore::Boolean(BTreeMap::new())
                    },
                };

                for (key, row) in table_from_disk.data.iter() {
                    match &mut index_data {
                        IndexStore::Text(btree) => {
                            match &row[&column] {
                                Value::String(data) => {
                                    btree.entry(data.clone()).or_default().push(key.clone());
                                }
                                _ => {
                                    println!("Error: schema and column types dont match");
                                    return;
                                }
                            };
                        },
                        IndexStore::Number(btree) => {
                            match &row[&column] {
                                Value::Number(data) => {
                                    match (&index_type, data) {
                                        (FieldDataType::NUMBER, _) | (FieldDataType::SERIAL, _) => {
                                            if data.is_i64() {
                                                let int = data.as_i64().unwrap();
                                                let index_number = IndexNumber::Int(int);
                                                btree.entry(index_number).or_default().push(key.clone());
                                            } else if data.is_f64() {
                                                let float = data.as_f64().unwrap();
                                                let index_number = IndexNumber::Float(OrderedFloat(float));
                                                btree.entry(index_number).or_default().push(key.clone());
                                            } else {
                                                println!("Error: Unexpected numeric type: {:?}", data);
                                            }
                                        },
                                        _ => {
                                            println!("Error: Schema and data types don't match");
                                            return;
                                        }
                                    }
                                }
                                other => {
                                    println!("Error: schema and column types dont match. Got: {:?}", other);
                                    return;
                                }
                            };
                        },
                        IndexStore::Boolean(btree) => {
                            match &row[&column] {
                                Value::Bool(data) => {
                                    btree.entry(*data).or_default().push(key.clone());
                                }
                                _ => {
                                    println!("Error: schema and column types dont match");
                                    return;
                                }
                            };
                        },
                    }
                }

                table_from_disk.indexes.insert(column.clone(), Index { indexed_column: column, index_type: index_type.clone(), index_data });
                save_to_disk(&table_from_disk.name, &table_from_disk);
            }
        }
    }
}

fn generate_schema(mut schema_tokens: VecDeque<String>) -> Vec<FieldDef> {
    let mut schema_result: Vec<FieldDef> = vec![];
    while let Some(token) = schema_tokens.pop_front() {
        let name = token;
        let mut data_type = None;
        let mut serial = None;
        let mut primary_key = false;

        while let Some(attr) = schema_tokens.front() {
            match attr.as_str() {
                "TEXT" | "NUMBER" | "BOOLEAN" => {
                    if data_type.is_some() {
                        panic!("Invalid: Multiple types specified");
                    }
                    data_type = match attr.as_str() {
                        "TEXT" => Some(FieldDataType::TEXT),
                        "NUMBER" => Some(FieldDataType::NUMBER),
                        "BOOLEAN" => Some(FieldDataType::BOOLEAN),
                        _ => break,
                    }
                },
                "SERIAL" => {
                    data_type = Some(FieldDataType::SERIAL);
                    serial = Some(SerialState { next_val: 1 });
                },
                "KEY" => {
                    if schema_result.iter().any(|f| f.primary_key) {
                        panic!("Invalid: Multiple primary keys set");
                    }
                    primary_key = true;
                }
                _ => break,
            }
            schema_tokens.pop_front();
        }
        schema_result.push(FieldDef { name, data_type, primary_key, serial });
    }
    schema_result
}
