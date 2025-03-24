use cli::ezpzdb_cli;
pub mod cli;
pub mod db;
pub mod query;
pub mod ddl;
pub mod dml;
pub mod dql;

fn main() {
    ezpzdb_cli();
}

