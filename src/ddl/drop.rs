use std::fs;
use directories::UserDirs;

pub fn drop(name: String) {
    if let Some(dirs) = UserDirs::new() {
        let mut file_name = String::from(&name);
        file_name.push_str(".db");
        let path = dirs.home_dir().join("Documents/ezpzdb/").join(file_name);

        let success = fs::remove_file(path);
        match success {
            Ok(_) => { println!("Table {} removed successfully", name) },
            Err(_) => { panic!("Error: Could not remove table") }
        }
    } else {
        panic!("No home directory found");
    }
}
