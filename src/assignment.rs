use crate::data_structure::query::Query;
use crate::data_structure::schema::get_database_with_query;

pub fn is_acyclic(query: &Query) -> bool {
    query.is_acyclic()
}

pub fn bool_answer(query: &Query) -> Option<bool> {
    let database = get_database_with_query(query);
    if query.is_boolean() {
        Some(query.yannakakis_boolean(&database))
    } else {
        None
    }
}

pub fn answers_query(query: &Query) -> [Vec<String>; 4] {
    if query.is_boolean() {
        return [vec![], vec![], vec![], vec![]];
    }
    let database = get_database_with_query(query);
    let query = query.yannakakis(database);
    println!("answer:\n{}", query);
    let mut answer: [Vec<String>; 4] = [vec![], vec![], vec![], vec![]];
    for i in 0..query.data.num_columns() {
        answer[i] = query.get_column_as_vec(i).unwrap();
    }
    answer
}
