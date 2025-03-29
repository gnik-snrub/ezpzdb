use cli::ezpzdb_cli;
pub mod cli;
pub mod query;
pub mod ddl;
pub mod dml;
pub mod dql;
pub mod storage;
pub mod models;

fn main() {
    ezpzdb_cli();
}

