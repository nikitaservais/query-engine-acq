use std::fs::File;
use std::sync::Arc;

use arrow;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;

use crate::data_structure::database::Database;
use crate::data_structure::query::Query;
use crate::data_structure::table::Table;

fn load(path: &str, schema: &SchemaRef) -> RecordBatch {
    let file = File::open(format!("data/{}", path)).unwrap();
    let csv_reader = arrow_csv::ReaderBuilder::new(schema.clone())
        .with_header(true)
        .build(file)
        .unwrap();
    let batches = csv_reader
        .into_iter()
        .map(|x| x.unwrap())
        .collect::<Vec<_>>();
    concat_batches(schema, &batches).unwrap()
}

pub fn beers() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("beer_id", DataType::Utf8, true),
        Field::new("brew_id", DataType::Utf8, true),
        Field::new("beer", DataType::Utf8, true),
        Field::new("abv", DataType::Utf8, true),
        Field::new("ibu", DataType::Utf8, true),
        Field::new("ounces", DataType::Utf8, true),
        Field::new("style", DataType::Utf8, true),
        Field::new("style2", DataType::Utf8, true),
    ]))
}

pub fn breweries() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("brew_id", DataType::Utf8, true),
        Field::new("brew_name", DataType::Utf8, true),
        Field::new("address1", DataType::Utf8, true),
        Field::new("address2", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("code", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("phone", DataType::Utf8, true),
        Field::new("website", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
    ]))
}

pub fn categories() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("cat_id", DataType::Utf8, true),
        Field::new("cat_name", DataType::Utf8, true),
    ]))
}

pub fn locations() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("loc_id", DataType::Utf8, true),
        Field::new("brew_id", DataType::Utf8, true),
        Field::new("latitude", DataType::Utf8, true),
        Field::new("longitude", DataType::Utf8, true),
        Field::new("accuracy", DataType::Utf8, true),
    ]))
}

pub fn styles() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("style_id", DataType::Utf8, true),
        Field::new("cat_id", DataType::Utf8, true),
        Field::new("style", DataType::Utf8, true),
    ]))
}

pub fn get_database() -> Database {
    let beers = load("beers.csv", &beers());
    let breweries = load("breweries.csv", &breweries());
    let categories = load("categories.csv", &categories());
    let locations = load("locations.csv", &locations());
    let styles = load("styles.csv", &styles());
    Database {
        beers: Table {
            name: "beers".to_string(),
            data: beers,
        },
        breweries: Table {
            name: "breweries".to_string(),
            data: breweries,
        },
        categories: Table {
            name: "categories".to_string(),
            data: categories,
        },
        locations: Table {
            name: "locations".to_string(),
            data: locations,
        },
        styles: Table {
            name: "styles".to_string(),
            data: styles,
        },
    }
}

pub fn get_database_with_query(query: &Query) -> Database {
    let mut database = get_database();
    database.rename(query);
    database
}
