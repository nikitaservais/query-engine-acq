use crate::data_structure::parser::get_query;

mod data_structure;

fn main() {
    let query = get_query();
    let database = data_structure::schema::get_database();
    query.yannakakis(database);
}
