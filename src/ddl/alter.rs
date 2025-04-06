use std::cmp::Ordering;

use serde_json::Value;

use crate::models::{FieldDataType, FieldDef, SerialState, Table};


pub fn alter(mut table: Table, action: String, tokens: Vec<String>) {

    match action.as_str() {
        "add" | "ADD" => {
            let field_names: Vec<&String> = table.schema.iter().map(|f| &f.name).collect();
            let col_name = &tokens[0];
            let col_type = &tokens[1];
            // 1 - Check to see if new column already in table
            // 2 - If exists, early return
            if field_names.contains(&&col_name) {
                println!("Column already exists in table");
                return
            }

            // 3 - If not exists, add new column to schema
            let new_field = FieldDef {
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
            table.schema.push(new_field);

            // 4 - Loop through rows, ordered by primary key
            let mut rows: Vec<(&Value, &Value)> = table.data.iter().map(|f| (f.0, f.1)).collect();
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
            for row in rows {
                // 4 - a - Add field to row
                // 4 - b - Set default value for column, based on type
                // DEFAULTS: 
                //  TEXT = ""
                //  NUMBER = 0
                //  BOOLEAN = false
                //  SERIAL = { add next_val in order }
            }
        },
    }
}
