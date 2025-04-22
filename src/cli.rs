use clap::{Parser, Subcommand};
use rustyline::DefaultEditor;
use serde_json::{self, Value};
use std::collections::HashMap;
use crate::ddl::alter::alter;
use crate::ddl::create::{create, CreateData};
use crate::ddl::drop::drop;
use crate::dml::delete::delete;
use crate::dml::insert::insert;
use crate::dml::update::update;
use crate::dql::select::select;
use crate::models::{FieldDef, Table};
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
    Select {
        #[arg(trailing_var_arg = true, num_args(1..))]
        query: Vec<String>,
    },
    Create {
        create_type: String,
        #[arg(trailing_var_arg = true, num_args(1..))]
        tokens: Vec<String>,
    },
    Drop {
        name: String,
    },
    Alter {
        table: String,
        action: String,
        tokens: Vec<String>,
    },
    Insert {
        table: String,
        tokens: Vec<String>,
    },
    Delete {
        table: String,
        tokens: Vec<String>,
    },
    Update {
        table: String,
        tokens: Vec<String>,
    }
}


pub fn ezpzdb_cli() {
    let init: Vec<_> = std::env::args().collect();
    if init.len() <= 1 {
        let mut rl = DefaultEditor::new().unwrap();
        let exit_command = "quit".to_string();
        println!();
        println!("Welcome to the EZPZDB REPL interface!");
        println!();
        loop {
            let readline = rl.readline(">> ").unwrap();
            if readline.trim().is_empty() { continue }
            let splits: Vec<&str> = readline.split(" ").collect();
            if readline == exit_command {
                break;
            } else {
                let mut tokens = Vec::new();
                tokens.push("REPL");
                tokens.extend(splits);
                let cli = Cli::try_parse_from(tokens);
                if let Ok(valid) = cli {
                    run_command(valid);
                    println!();
                }
            }
        }
    } else {
        run_command(Cli::parse());
    }
}

fn run_command(tokens: Cli) {
    match tokens {
        Cli { command: Some(Command::Select { query }) } => {
            let select_results = select(query);
            if select_results.filtered.is_empty() {
                println!("No records found");
            } else {
                print_to_cli(select_results.filtered, select_results.schema);

                for field in select_results.missing {
                    println!("Field not found: {}", field);
                }
            }
        }
        Cli { command: Some(Command::Create { create_type, mut tokens }) } => {
            let create_data: CreateData;
            let other_tokens = &tokens.split_off(1);
            match create_type.as_str() {
                "TABLE" | "table" => {
                    create_data = CreateData::Table { name: tokens[0].clone(), schema: other_tokens.clone() };
                },
                "INDEX" | "index" => {
                    create_data = CreateData::Index { table: tokens[0].clone(), column: other_tokens[0].clone() };
                },
                _ => {
                    println!("Invalid create type entered");
                    return;
                }
            }
            create(create_data);
        }
        Cli { command: Some(Command::Drop { name }) } => {
            drop(name);
        }
        Cli { command: Some(Command::Alter { table, action, tokens }) } => {
            let table_data = load_from_disk(&table);
            alter(table_data, action, tokens);
        }
        Cli { command: Some(Command::Insert { table, tokens }) } => {
            let table_data: Table = load_from_disk(&table);
            insert(table_data, tokens);
        }
        Cli { command: Some(Command::Delete { table, tokens }) } => {
            let table_data: Table = load_from_disk(&table);
            delete(table_data, tokens);
        }
        Cli { command: Some(Command::Update { table, tokens }) } => {
            let table_data: Table = load_from_disk(&table);
            update(table_data, tokens);
        }
        _ => {
            println!("No command provided");
        }
    }
}

fn print_to_cli(data: HashMap<Value, Value>, schema: Vec<(bool, FieldDef)>) {
    // Sets column width, and label
    let mut cols: Vec<(String, usize)> = vec![];

    let primary_key = &schema.iter().find(|(_, field)| field.primary_key).unwrap().1;
    let mut sorted: Vec<&Value> = data.values().collect();
    sorted.sort_by(|curr, next| {
        let curr_state = match curr {
            Value::Object(c) => c.get(&primary_key.name).cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        };
        let next_state = match next {
            Value::Object(c) => c.get(&primary_key.name).cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        };
        let out = match (curr_state, next_state) {
            (Value::String(c), Value::String(n)) => c.cmp(&n),
            (Value::Number(c), Value::Number(n)) => c.as_i64().cmp(&n.as_i64()),
            _ => std::cmp::Ordering::Equal,
        };
        out
    });

    for (should_print, header) in schema.iter() {
        if !*should_print {
            continue
        }
        let max_length: usize = std::cmp::max(data.values()
            .map(|d| {
                match &d[&header.name] {
                    Value::String(s) => s.len(),
                    v => v.to_string().len(),
                }
            }
        )
        .max()
        .unwrap_or(3), 3);
        cols.push((header.name.clone(), max_length));
    }

    print!("|");
    for (_, width) in &cols {
        print!("{:-<width$}-|-", "-");
    }
    println!();

    print!("|");
    for (header, width) in &cols {
        print!("{:<width$} | ", header.to_uppercase());
    }
    println!();

    print!("|");
    for (_, width) in &cols {
        print!("{:-<width$}-|-", "-");
    }
    println!();

    for value in &sorted {
        print!("|");
        if let Value::Object(map) = value {
            for (col, f) in cols.iter().enumerate() {
                let width = cols[col].1;
                let print_val = map.get(&f.0).unwrap();
                match print_val {
                    Value::String(s) => print!("{:<width$} | ", s.trim()),
                    _ => print!("{:<width$} | ", print_val.to_string().trim()),
                }
            }
        }
        println!();
    }

    print!("|");
    for (_, width) in &cols {
        print!("{:-<width$}-|-", "-");
    }
    println!();
}
