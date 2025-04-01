
use serde_json::{self, Value};
use std::collections::{HashMap, HashSet};
use crate::{models::Table, storage::load_from_disk};

pub fn select(query: Vec<String> ) -> (HashMap<Value, Value>, Vec<String>) {
    let built_query: Query = build_query(query);
    let table: Table = load_from_disk(&built_query.from);

    let mut filtered_store: HashMap<Value, Value> = evaluate_query(&table.data, &built_query);
    if filtered_store.is_empty() {
        (filtered_store, vec![])
    } else {
        if built_query.select != vec!["*".to_string()] {
            for (_k, v) in filtered_store.iter_mut() {
                if let Some(obj) = v.as_object_mut() {
                    obj.retain(|field_key, _field_value| {
                        built_query.select.contains(field_key)
                    })
                }
            }
        }

        // List missing fields
        let all_fields: HashSet<String> = table.data.values().filter_map(|v| v.as_object()).flat_map(|obj| obj.keys().cloned()).collect();
        let mut missing_fields: Vec<String> = Vec::new();
        for field in &built_query.select {
            if !all_fields.contains(field) {
                missing_fields.push(field.clone());
            }
        }

        (filtered_store, missing_fields)
    }
}

pub fn build_query(query_tokens: Vec<String>) -> Query {
    let mut select_tokens = vec![];
    let mut from_tokens = vec![];
    let mut where_tokens: Vec<WhereClause> = vec![];

    let mut temp_where_tokens: Vec<String> = vec![];

    let mut current_token: TokenOption = TokenOption::CurrentToken(CurrentToken::None);

    for q in query_tokens {
        if temp_where_tokens.len() > 3 {
            finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);
        } else if temp_where_tokens.len() > 2 && !(temp_where_tokens.contains(&String::from("AND")) || temp_where_tokens.contains(&String::from("OR"))) {
            finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);
        }
        match q.as_str() {
            "SELECT" => current_token = TokenOption::CurrentToken(CurrentToken::Select),
            "FROM" => current_token = TokenOption::CurrentToken(CurrentToken::From),
            "WHERE" => current_token = TokenOption::CurrentToken(CurrentToken::Where),
            "AND" => {
                finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);
                temp_where_tokens.push(q);
            },
            "OR" => {
                finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);
                temp_where_tokens.push(q);
            }
            _ => {
                match current_token {
                    TokenOption::CurrentToken(CurrentToken::Select) => select_tokens.push(q),
                    TokenOption::CurrentToken(CurrentToken::From) => from_tokens.push(q),
                    TokenOption::CurrentToken(CurrentToken::Where) => temp_where_tokens.push(q),
                    _ => {}
                }
            }
        }
    }
    finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);

    let query = Query {
        select: select_tokens,
        from: from_tokens.join(""),
        where_clause: match where_tokens.len() {
            0 => None,
            _ => Some(where_tokens),
        }
    };
    query
}

fn finalize_where_clause(temp_where_tokens: &mut Vec<String>, where_tokens: &mut Vec<WhereClause>) {
    if !temp_where_tokens.is_empty() {
        where_tokens.push(build_where_clause(temp_where_tokens.clone()));
        temp_where_tokens.clear();
    }
}

enum TokenOption {
    CurrentToken(CurrentToken),
}

enum CurrentToken {
    Select,
    From,
    Where,
    None,
}

#[derive(Debug)]
pub struct Query {
    pub select: Vec<String>,
    pub from: String,
    pub where_clause: Option<Vec<WhereClause>>,
}

fn build_where_clause(mut where_tokens: Vec<String>) -> WhereClause {
    let connector = if (where_tokens[0] == String::from("AND")) || (where_tokens[0] == String::from("OR")) {
        let found = where_tokens.remove(0);
        match found.as_str() {
            "AND" => Some(Connector::AND),
            _ => Some(Connector::OR)
        }
    } else {
        None
    };
    WhereClause {
        left_hand: where_tokens.get(0).cloned().unwrap_or_default(),
        operator: match where_tokens.get(1).map(|s| s.as_str()) {
            Some("=") => Condition::Equals,
            Some("!=") => Condition::NotEquals,
            Some(">") => Condition::GreaterThan,
            Some("<") => Condition::LessThan,
            _ => Condition::Invalid
        },
        right_hand: match where_tokens.get(2) {
            Some(t) if t.parse::<i32>().is_ok() => HandType::Integer(i64::from(t.parse::<i64>().unwrap())),
            Some(t) if t.parse::<f64>().is_ok() => HandType::Float(f64::from(t.parse::<f64>().unwrap())),
            Some(t) if t.parse::<bool>().is_ok() => HandType::Boolean(bool::from(t.parse::<bool>().unwrap())),
            _ => {HandType::String(where_tokens[2].clone())},
        },
        connector,
    }
}

#[derive(Debug)]
pub struct WhereClause {
    left_hand: String,
    operator: Condition,
    right_hand: HandType,
    connector: Option<Connector>,
}

#[derive(Debug)]
enum Connector {
    AND,
    OR,
}

#[derive(Debug)]
enum HandType {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
}

#[derive(Debug)]
enum Condition {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    Invalid
}

pub fn evaluate_query(row: &HashMap<Value, Value>, query: &Query) -> HashMap<Value, Value> {
    let clauses = match &query.where_clause {
        Some(clauses) => clauses,
        None => return row.clone(),
    };

    let output = row.clone().into_iter().filter(|v| passes_clauses(v.clone(), clauses) ).collect();
    output
}

fn passes_clauses(mut v: (Value, Value), clauses: &Vec<WhereClause>) -> bool {
    let mut result = {
        evaluate_clause(&mut v, &clauses[0])
    };

    for clause in clauses.iter().skip(1) {
        let clause_result = evaluate_clause(&mut v, clause);
        match clause.connector {
            Some(Connector::AND) => result = result && clause_result,
            Some(Connector::OR) => result = result || clause_result,
            None => { result = result && clause_result}
        }
    }

    result
}

fn evaluate_clause(data: &mut (Value, Value), clause: &WhereClause) -> bool {
    let left = data.1.get(&clause.left_hand).unwrap_or(&Value::Null);
    //let left = &data.1.take();
    let right = &clause.right_hand;
    match (left, right) {
        (Value::Number(l), HandType::Integer(r)) => {
            let l_i = l.as_i64().unwrap();
            match clause.operator {
                Condition::Equals => &l_i == r,
                Condition::NotEquals => &l_i != r,
                Condition::GreaterThan => &l_i > r,
                Condition::LessThan => &l_i < r,
                Condition::Invalid => false
            }
        },
        (Value::Number(l), HandType::Float(r)) => {
            let l_f = l.as_f64().unwrap();
            match clause.operator {
                Condition::Equals => &l_f == r,
                Condition::NotEquals => &l_f != r,
                Condition::GreaterThan => &l_f > r,
                Condition::LessThan => &l_f < r,
                Condition::Invalid => false
            }
        },
        (Value::String(l), HandType::String(r)) => {
            match clause.operator {
                Condition::Equals => l == r,
                Condition::NotEquals => l != r,
                Condition::GreaterThan => l > r,
                Condition::LessThan => l < r,
                Condition::Invalid => false
            }
        },
        (Value::Bool(l), HandType::Boolean(r)) => {
            match clause.operator {
                Condition::Equals => l == r,
                Condition::NotEquals => l != r,
                Condition::GreaterThan => l > r,
                Condition::LessThan => l < r,
                Condition::Invalid => false
            }
        },
        (_, _) => {
            false
        }
    }
}
