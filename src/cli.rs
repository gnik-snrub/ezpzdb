use clap::{Parser, Subcommand};
use serde_json::{self, Value};
use std::collections::HashMap;
use crate::ddl::create::{create, CreateData};
use crate::ddl::drop::drop;
use crate::dml::insert::insert;
use crate::dql::select::select;
use crate::models::Table;
use crate::storage::load_from_disk;

// Simple key-value store CLI
#[derive(Parser, Debug)]
#[command(name = "Ezpz Database")]
#[command(about = "Simple SQL-like database CLI", long_about = None)]
struct Cli {
    // Key to get or set
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Clone, Debug)]
enum Command {
    Search {
        #[arg(trailing_var_arg = true, num_args(1..))]
        query: Vec<String>,
    },
    Create {
        name: String,
        #[arg(trailing_var_arg = true, num_args(1..))]
        schema: Vec<String>,
    },
    Drop {
        name: String,
    },
    Insert {
        table: String,
        tokens: Vec<String>,
    },
}


pub fn ezpzdb_cli() {
    let cli = Cli::parse();

    match cli {
        Cli { command: Some(Command::Search { query }) } => {
            let (query_results, missing_fields) = select(query);
            if query_results.is_empty() {
                println!("No records found");
            } else {
                print_to_cli(query_results);

                for field in missing_fields {
                    println!("Field not found: {}", field);
                }
            }
        }
        Cli { command: Some(Command::Create { name, schema }) } => {
            create(CreateData::Table {name, schema});
        }
        Cli { command: Some(Command::Drop { name }) } => {
            drop(name);
        }
        Cli { command: Some(Command::Insert { table, tokens }) } => {
            let table_data: Table = load_from_disk(&table);
            insert(table_data, tokens);
        }
        _ => {
            println!("No command provided");
        }
    }
}

fn print_to_cli(data: HashMap<String, Value>) {
    let mut headers: Vec<String> = vec![];
    let mut rows: Vec<Vec<Value>> = vec![];

    match data.values().next() {
        Some(entry) => {
            let value = serde_json::to_value(entry).unwrap();
            if let Value::Object(map) = value {
                for field in map.keys().cloned() {
                    headers.push(field);
                }
            }
        },
        None => {
            println!("No entries found");
        }
    }

    headers.sort();
    if let Some(pos) = headers.iter().position(|h| h == "key") {
        if pos != 0 {
            let key = headers.remove(pos);
            headers.insert(0, key);
        }
    };

    for entry in data.values() {
        let value = serde_json::to_value(entry).unwrap();
        if let Value::Object(map) = value {
            let mut row = vec![];

            for header in &headers {
                row.push(map.get(header).cloned().unwrap_or(Value::Null));
            }
            rows.push(row);
        }
    }

    let mut col_widths: HashMap<usize, usize> = HashMap::new();
    for (col, header) in headers.iter().enumerate() {
        let max_length = rows.iter()
            .map(|row| match &row[col] {
                Value::String(s) => s.len(),
                v => v.to_string().len(),
            })
            .max()
            .unwrap_or(0);
        col_widths.insert(col, max_length.max(header.len()));
    }

    print!("|");
    for (col, header) in headers.iter().enumerate() {
        print!("{:<width$} | ", header, width = col_widths[&col]);
    }
    println!();

    print!("|");
    for col in 0..headers.len() {
        print!("{:-<width$}-|-", "-", width = col_widths[&col]);
    }
    println!();

    for row in &rows {
        print!("|");
        for (col, value) in row.iter().enumerate() {
            match value {
                Value::String(s) => print!("{:<width$} | ", s, width = col_widths[&col]),
                _ => print!("{:-<width$} | ", value, width = col_widths[&col]),
            }
        }
        println!();
    }
}
