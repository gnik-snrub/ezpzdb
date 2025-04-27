
use serde_json::{self, Value};
use std::collections::{HashMap, HashSet};
use std::ops::Bound::{Excluded, Unbounded};
use crate::models::IndexNumber;
use crate::{models::{FieldDef, Index, IndexStore, Table}, storage::load_from_disk};

pub fn select(query: Vec<String>) -> SelectReturn {
    let built_query: Query = build_query(query);
    let mut table: Table = load_from_disk(&built_query.from);

    let filtered_store: HashMap<Value, Value> = evaluate_query(&table, &built_query);
    if filtered_store.is_empty() {
        SelectReturn { filtered: filtered_store, missing: vec![], schema: vec![] }
    } else {
        // List missing fields
        let all_fields: HashSet<String> = table.data.values().filter_map(|v| v.as_object()).flat_map(|obj| obj.keys().cloned()).collect();
        let mut missing_fields: Vec<String> = Vec::new();
        for field in &built_query.select {
            if !all_fields.contains(field) && field != &"*".to_string() {
                missing_fields.push(field.clone());
            }
        }

        table.schema.sort_by_key(|f| !f.primary_key);
        let sorted_schema: Vec<(bool, FieldDef)> = table.schema.clone().into_iter().map(|x| -> (bool, FieldDef) {
            if built_query.select.contains(&x.name) || built_query.select == vec!["*".to_string()]{
                (true, x)
            } else {
                (false, x)
            }
        }).collect();

        SelectReturn {
            filtered: filtered_store,
            missing: missing_fields,
            schema: sorted_schema,
        }
    }
}

pub struct SelectReturn {
    pub filtered: HashMap<Value, Value>,
    pub missing: Vec<String>,
    pub schema: Vec<(bool, FieldDef)>
}

pub fn build_query(query_tokens: Vec<String>) -> Query {
    let mut select_tokens = vec![];
    let mut from_tokens = vec![];
    let mut where_tokens: Vec<WhereClause> = vec![];

    let mut temp_where_tokens: Vec<String> = vec![];

    let mut current_token: TokenOption = TokenOption::CurrentToken(CurrentToken::Select);

    for q in query_tokens {
        if temp_where_tokens.len() > 3 {
            finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);
        } else if temp_where_tokens.len() > 2 && !(temp_where_tokens.contains(&String::from("AND")) || temp_where_tokens.contains(&String::from("OR"))) {
            finalize_where_clause(&mut temp_where_tokens, &mut where_tokens);
        }
        match q.as_str() {
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
            None | Some(_) => Condition::Equals,
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
}

pub fn evaluate_query(table: &Table, query: &Query) -> HashMap<Value, Value> {
    let clauses = match &query.where_clause {
        Some(clauses) => clauses,
        None => return table.data.clone(),
    };

    let index = find_best_index(table, clauses);

    match index {
        Some(i) => {
            match i.1 {
            }
        },
        None => {
            let output = table.data.clone().into_iter().filter(|v| passes_clauses(v.clone(), clauses) ).collect();
            output
        }
    }
}

fn find_best_index<'a>(table: &'a Table, clauses: &'a Vec<WhereClause>) -> Option<(&'a WhereClause, &'a IndexStore)> {
    let mut best: Option<(&WhereClause, &IndexStore, usize)> = None;

    for clause in clauses {
        if let Some(index) = table.indexes.get(&clause.left_hand) {
            let hits = count_hits(&index.index_data, clause);

            if best.map_or(true, |(_, _, best_hits)| hits < best_hits) {
                best = Some((clause, &index.index_data, hits));
            }
        }
    }
    best.map(|(clause, index, _)| (clause, index))
}

fn count_hits(index: &IndexStore, clause: &WhereClause) -> usize {
    match index {
        IndexStore::Text(map) => {
            match &clause.right_hand {
                HandType::String(right) => {
                    match clause.operator {
                        Condition::Equals => {
                            map.get(right).map_or(0, |rows| rows.len())
                        },
                        Condition::NotEquals => {
                            // Copied from Equals, I will change this to make sense for its arm.
                            map.iter()
                                .filter(|(k, _)| *k != right)
                                .map(|(_, v)| v.len())
                                .sum::<usize>();
                            map.get(right).map_or(0, |rows| rows.len())
                        },
                        Condition::GreaterThan => {
                            map.range::<String, _>((Excluded(right), Unbounded))
                                .map(|(_, v)| v.len())
                                .sum::<usize>()
                        },
                        Condition::LessThan => {
                            map.range::<String, _>((Unbounded, Excluded(right)))
                                .map(|(_, v)| v.len())
                                .sum::<usize>()
                        }
                    }
                },
                _ => {
                    println!("Error: WHERE condition does not match index data type");
                    return 0;
                }
            }
        },
        IndexStore::Boolean(map) => {
            match &clause.right_hand {
                HandType::Boolean(right) => {
                    match clause.operator {
                        Condition::Equals => {
                            map.get(right).map_or(0, |rows| rows.len())
                        },
                        Condition::NotEquals => {
                            map.iter()
                                .filter(|(k, _)| k != &right)
                                .map(|(_, v)| v.len())
                                .sum()
                        },
                        _ => {0},
                    }
                },
                _ => {
                    println!("Error: WHERE condition does not match index data type");
                    return 0;
                }
            }
        },
        IndexStore::Number(map) => {
            match &clause.right_hand {
                HandType::Integer(i) => {
                    let cmp_val = *i as f64;
                    match clause.operator {
                        Condition::Equals => {
                            map.iter().filter(|(k, _)| {
                                match k {
                                    IndexNumber::Int(i) => (*i as f64) == cmp_val,
                                    IndexNumber::Float(f) => f.0 == cmp_val,
                                }
                            }).map(|(_, v)| v.len())
                                .sum::<usize>()
                        },
                        Condition::NotEquals => {
                            map.iter().filter(|(k, _)| {
                                match k {
                                    IndexNumber::Int(i) => (*i as f64) != cmp_val,
                                    IndexNumber::Float(f) => f.0 != cmp_val,
                                }
                            }).map(|(_, v)| v.len())
                                .sum::<usize>()
                        },
                        Condition::GreaterThan => {
                            map.iter().filter(|(k, _)| {
                                match k {
                                    IndexNumber::Int(i) => (*i as f64) > cmp_val,
                                    IndexNumber::Float(f) => f.0 > cmp_val,
                                }
                            }).map(|(_, v)| v.len())
                                .sum::<usize>()
                        },
                        Condition::LessThan => {
                            map.iter().filter(|(k, _)| {
                                match k {
                                    IndexNumber::Int(i) => (*i as f64) < cmp_val,
                                    IndexNumber::Float(f) => f.0 < cmp_val,
                                }
                            }).map(|(_, v)| v.len())
                                .sum::<usize>()
                        },
                    }
                },
                HandType::Float(f) => {
                    let cmp_val = *f;
                    match clause.operator {
                        Condition::Equals => {
                            map.iter().filter(|(k, _)| {
                                match k {
                                    IndexNumber::Int(i) => (*i as f64) == cmp_val,
                                    IndexNumber::Float(f) => f.0 == cmp_val,
                                }
                            }).map(|(_, v)| v.len())
                                .sum::<usize>()
                        },
                        Condition::NotEquals => {
                            map.iter().filter(|(k, _)| {
                                match k {
                                    IndexNumber::Int(i) => (*i as f64) != cmp_val,
                                    IndexNumber::Float(f) => f.0 != cmp_val,
                                }
                            }).map(|(_, v)| v.len())
                                .sum::<usize>()
                        },
                        Condition::GreaterThan => {
                            map.iter().filter(|(k, _)| {
                                match k {
                                    IndexNumber::Int(i) => (*i as f64) > cmp_val,
                                    IndexNumber::Float(f) => f.0 > cmp_val,
                                }
                            }).map(|(_, v)| v.len())
                                .sum::<usize>()
                        },
                        Condition::LessThan => {
                            map.iter().filter(|(k, _)| {
                                match k {
                                    IndexNumber::Int(i) => (*i as f64) < cmp_val,
                                    IndexNumber::Float(f) => f.0 < cmp_val,
                                }
                            }).map(|(_, v)| v.len())
                                .sum::<usize>()
                        },
                    }
                },
                _ => {
                    println!("Error: WHERE condition does not match index data type");
                    return 0;
                }
            }
        },
    }
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
            if let Some(l_i) = l.as_i64() {
                match clause.operator {
                    Condition::Equals => &l_i == r,
                    Condition::NotEquals => &l_i != r,
                    Condition::GreaterThan => &l_i > r,
                    Condition::LessThan => &l_i < r,
                }
            } else if let Some(l_f) = l.as_f64() {
                let r_f = *r as f64;
                match clause.operator {
                    Condition::Equals => l_f == r_f,
                    Condition::NotEquals => l_f != r_f,
                    Condition::GreaterThan => l_f > r_f,
                    Condition::LessThan => l_f < r_f,
                }
            } else {
                return false
            }
        },
        (Value::Number(l), HandType::Float(r)) => {
            if let Some(l_i) = l.as_f64() {
                match clause.operator {
                    Condition::Equals => &l_i == r,
                    Condition::NotEquals => &l_i != r,
                    Condition::GreaterThan => &l_i > r,
                    Condition::LessThan => &l_i < r,
                }
            } else if let Some(l_f) = l.as_i64() {
                let r_f = *r as i64;
                match clause.operator {
                    Condition::Equals => l_f == r_f,
                    Condition::NotEquals => l_f != r_f,
                    Condition::GreaterThan => l_f > r_f,
                    Condition::LessThan => l_f < r_f,
                }
            } else {
                return false
            }
        },
        (Value::String(l), HandType::String(r)) => {
            match clause.operator {
                Condition::Equals => l == r,
                Condition::NotEquals => l != r,
                Condition::GreaterThan => l > r,
                Condition::LessThan => l < r,
            }
        },
        (Value::Bool(l), HandType::Boolean(r)) => {
            match clause.operator {
                Condition::Equals => l == r,
                Condition::NotEquals => l != r,
                Condition::GreaterThan => l > r,
                Condition::LessThan => l < r,
            }
        },
        (_, _) => {
            false
        }
    }
}
