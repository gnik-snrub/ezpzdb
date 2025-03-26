use std::{collections::VecDeque, fs::write};
use directories::UserDirs;
use serde::{Serialize, Deserialize};

pub enum CreateData {
    Table { name: String, schema: Vec<String> },
    // Not yet implemented the following
    //Index,
    //Database
}

pub fn create(create_data: CreateData) {
    match create_data {
        CreateData::Table {name, schema }=> {
            if let Some(dirs) = UserDirs::new() {
                let mut file_name = String::from(name);
                file_name.push_str(".db");
                let path = dirs.home_dir().join("Documents/ezpzdb/").join(file_name);

                let fields = generate_schema(schema.clone().into());
                let schema_json = serde_json::to_string_pretty(&fields);
                match schema_json {
                    Ok(sj) => {
                        let new_table = format!(
                            "{{\n\t\"schema\": {}, \n\t\"table\": []\n}}",
                            sj
                        );
                        let new_file = write(path, new_table);
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
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum FieldDataType {
    TEXT,
    NUMBER,
    BOOLEAN,
    SERIAL,
}

#[derive(Debug, Serialize, Deserialize)]
struct FieldDef {
    name: String,
    data_type: Option<FieldDataType>,
    primary_key: bool,
    serial: Option<SerialState>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SerialState {
    next_val: u32,
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
