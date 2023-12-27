use arrow::array::{Array, StringArray};

#[test]
fn example_query_4() {
    let query = query_engine_acq::data_structure::parser::parse_query(
        "Answer(x,y,z,w):-Beers(u1,v,x,'0.05','18',u2,'Vienna Lager',u3),Locations(u4,v,y,z,w).",
    );
    let database = query_engine_acq::data_structure::schema::get_database_with_query(&query);
    let answer = query.yannakakis(database);
    let binding = answer.get_data();
    let x = binding
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let y = binding
        .column_by_name("y")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let z = binding
        .column_by_name("z")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let w = binding
        .column_by_name("w")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        [x.value(0), y.value(0), z.value(0), w.value(0)],
        [
            "Snake River Lager",
            "50.7428",
            "3.6875",
            "RANGE_INTERPOLATED"
        ]
    );
}
