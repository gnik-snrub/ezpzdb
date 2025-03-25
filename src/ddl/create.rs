use std::fs::write;
use directories::UserDirs;

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
                let mut new_table = String::new();

                // Builds 
                new_table += "{\n\t\"schema\": [";
                for (i, s) in schema.iter().enumerate() {
                    new_table += s.as_str();
                    if i < schema.len() - 1 {
                        new_table += ", ";
                    }
                }
                new_table += "],\n\t\"table\": [],\n}";

                let new_file = write(path, new_table);
                match new_file {
                    Ok(_) => { println!("New table created"); },
                    Err(_) => { eprintln!("Error creating table"); }
                }

            } else {
                println!("No home directory found");
            }
        },
    }
}
