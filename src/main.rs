use clap::{Parser, Subcommand};
use std::collections::HashMap;

// Simple key-value store CLI
#[derive(Parser, Debug)]
#[command(name = "Ezpz Database")]
#[command(about = "Simple key-value store CLI", long_about = None)]
struct Cli {
    // Key to get or set
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Clone, Debug)]
enum Command {
    Read,
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
}

fn save(store: &HashMap<String, String>) {
    let store_bin = bincode::serialize(&store).unwrap();

    std::fs::write("store.bin", store_bin).unwrap();
}

fn load() -> HashMap<String, String> {
    let store_bin = std::fs::read("store.bin").unwrap();
    bincode::deserialize(&store_bin).unwrap()
}

fn main() {
    let cli = Cli::parse();
    let mut store: HashMap<String, String> = load();

    match cli {
        Cli { command: Some(Command::Read) } => {
            println!("{:?}", store);
        }
        Cli { command: Some(Command::Get { key }) } => {
            match store.get(&key) {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
        }
        Cli { command: Some(Command::Set { key, value }) } => {
            store.insert(key, value);
            save(&store);
        }
        Cli { command: Some(Command::Delete { key }) } => {
            store.remove(&key);
            save(&store);
        }
        _ => {
            println!("No command provided");
        }
    }
}
