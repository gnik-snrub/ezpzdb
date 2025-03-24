use std::{collections::HashMap, fs};
use directories::UserDirs;
use serde_json::Value;

pub fn save_to_disk(table: &String, store: &HashMap<String, Value>) {
    if let Some(dirs) = UserDirs::new() {
        let store_bin = serde_json::to_string(&store).unwrap();

        let mut file_name = String::from(table);
        file_name.push_str(".db");
        let save_path = dirs.home_dir().join("Documents/ezpzdb/").join(table);
        fs::write(save_path, store_bin).unwrap();
    } else {
        println!("No home directory found");
    }
}

pub fn load_from_disk(table: &String) -> HashMap<String, Value> {
    if let Some(dirs) = UserDirs::new() {
        let mut file_name = String::from(table);
        file_name.push_str(".db");
        let load_path = dirs.home_dir().join("Documents/ezpzdb/").join(file_name);
        let store = fs::read_to_string(load_path);
        match store {
            Ok(json_str) => serde_json::from_str(&json_str).unwrap_or_else(|e| {
                eprintln!("Failed to parse JSON: {}", e);
                HashMap::new()
            }),
            Err(e) => {
                eprintln!("Failed to read file: {}", e);
                HashMap::new()
            }
        }
    } else {
        println!("No home directory found");
        HashMap::new()
    }
}
