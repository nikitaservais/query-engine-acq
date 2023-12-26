use std::fmt::Display;
use std::ops::Index;
use std::str::FromStr;

use arrow::array::Array;
use serde::Serialize;

mod data_structure;

fn main() {
    let queries = data_structure::parser::parse_queries("input.txt");
    let query = &queries[3];
    let database = data_structure::schema::get_database();
    query.yannakakis(database);
}

#[test]
fn test() {
    let queries = data_structure::parser::parse_queries("input.txt");
    let query = &queries[3];
    let database = data_structure::schema::get_database();
    query.yannakakis(database);
}
