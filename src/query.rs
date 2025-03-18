use serde_json::{self, json, Number, Value};
use std::collections::{HashMap, HashSet};

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
    Connector(Connector),
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
            Some(t) if t.parse::<i32>().is_ok() => RightHandType::Integer(i32::from(t.parse::<i32>().unwrap())),
            Some(t) if t.parse::<f64>().is_ok() => RightHandType::Float(f64::from(t.parse::<f64>().unwrap())),
            Some(t) if t.parse::<bool>().is_ok() => RightHandType::Boolean(bool::from(t.parse::<bool>().unwrap())),
            _ => {RightHandType::String(where_tokens[2].clone())},
        },
        connector,
    }
}

#[derive(Debug)]
struct WhereClause {
    left_hand: String,
    operator: Condition,
    right_hand: RightHandType,
    connector: Option<Connector>,
}

#[derive(Debug)]
enum Connector {
    AND,
    OR,
}

#[derive(Debug)]
enum RightHandType {
    String(String),
    Integer(i32),
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

pub fn evaluate_query(row: &HashMap<String, Value>, query: &Query) -> HashMap<String, Value> {
    let clauses = match &query.where_clause {
        Some(clauses) => clauses,
        None => return row.clone(),
    };

    let output = row.clone().into_iter().filter(|v| {
        let mut result = evaluate_clause(v, &clauses[0]);

        for clause in clauses.iter().skip(1) {
            let clause_result = evaluate_clause(v, clause);
            match clause.connector {
                Some(Connector::AND) => result = result && clause_result,
                Some(Connector::OR) => result = result || clause_result,
                None => { result = result && clause_result}
            }
        }

        result
    } ).collect();
    output
}

fn evaluate_clause(data: &(String, Value), clause: &WhereClause) -> bool {
    let left_hand = data.1.get(&clause.left_hand).unwrap_or(&Value::Null);
    let mut right_hand = match &clause.right_hand {
        RightHandType::Integer(i) => Value::Number(Number::from(*i)),
        RightHandType::Float(f) => Value::Number(Number::from_f64(*f).expect("Invalid f64 value")),
        RightHandType::Boolean(b) => Value::Bool(*b),
        RightHandType::String(s) => Value::String(s.clone())
    };
    match clause.operator {
        Condition::Equals => left_hand == &right_hand,
        Condition::NotEquals => left_hand != &right_hand,
        Condition::GreaterThan => compare(left_hand, &right_hand.take(), |l, r| l > r),
        Condition::LessThan => compare(left_hand, &right_hand.take(), |l, r| l < r),
        Condition::Invalid => false
    }
}

fn compare(left: &Value, right: &Value, cmp: impl Fn(f64, f64) -> bool) -> bool {
    if let (Some(l), Some(r)) = (left.as_str().unwrap().parse::<f64>().ok(), right.as_f64()) {
        cmp(l, r)
    } else {
        false
    }
}
