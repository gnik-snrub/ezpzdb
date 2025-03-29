use std::{collections::HashMap, fs};
use directories::UserDirs;

use crate::models::Table;

pub fn save_to_disk(table: &String, store: &Table) {
    if let Some(dirs) = UserDirs::new() {
        let store_bin = serde_json::to_string_pretty(&store).unwrap();

        let mut file_name = String::from(table);
        file_name.push_str(".db");
        let save_path = dirs.home_dir().join("Documents/ezpzdb/").join(file_name);
        fs::write(save_path, store_bin).unwrap();
    } else {
        println!("No home directory found");
    }
}

pub fn load_from_disk(table: &String) -> Table {
    if let Some(dirs) = UserDirs::new() {
        let mut file_name = String::from(table);
        file_name.push_str(".db");
        let load_path = dirs.home_dir().join("Documents/ezpzdb/").join(file_name);
        let store = fs::read_to_string(load_path);
        match store {
            Ok(file) => {
                let table = serde_json::from_str::<Table>(&file);
                if table.is_ok() {
                    table.unwrap()
                } else {
                    eprintln!("Error: Could not build table from file");
                    Table {
                        name: "".to_string(),
                        schema: vec![],
                        data: HashMap::new()
                    }
                }
            },
            Err(_) => {
                eprintln!("Error: Could not read file");
                Table {
                    name: "".to_string(),
                    schema: vec![],
                    data: HashMap::new()
                }
            }
        }
    } else {
        println!("Error: No home directory found");
        Table {
            name: "".to_string(),
            schema: vec![],
            data: HashMap::new()
        }
    }
}
