use std::fs::File;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::FileWriter;

pub fn load_data() {
    load("beers.csv", beers()).expect("Failed to load beers");
    load("breweries.csv", breweries()).expect("Failed to load breweries");
    load("categories.csv", categories()).expect("Failed to load categories");
    load("locations.csv", locations()).expect("Failed to load locations");
    load("styles.csv", styles()).expect("Failed to load styles");
}

fn load(path: &str, schema: Schema) -> Result<(), ArrowError> {
    let file = File::open(format!("data/{}", path)).unwrap();
    // get file name without extension
    let file_name = path.split(".").collect::<Vec<&str>>()[0];

    let output_path = format!("db/{}.arrow", file_name);
    let csv_reader = arrow_csv::reader::ReaderBuilder::new(Arc::new(schema))
        .with_header(true)
        .build(file)
        .unwrap();

    let mut writer = FileWriter::try_new(File::create(output_path)?, csv_reader.schema().as_ref())?;

    for batch in csv_reader {
        match batch {
            Ok(batch) => writer.write(&batch)?,
            Err(e) => println!("Error occurred while reading batch: {}", e),
        }
    }
    writer.finish()
}

pub fn beers() -> Schema {
    Schema::new(vec![
        Field::new("beer_id", DataType::Int32, true),
        Field::new("brew_id", DataType::Int32, true),
        Field::new("beer", DataType::Utf8, true),
        Field::new("abv", DataType::Float64, true),
        Field::new("ibu", DataType::Utf8, true), // TODO: change to Int32 because of NA it doesn't work
        Field::new("ounces", DataType::Float64, true),
        Field::new("style", DataType::Utf8, true),
        Field::new("style2", DataType::Utf8, true),
    ])
}

pub fn breweries() -> Schema {
    Schema::new(vec![
        Field::new("brew_id", DataType::Int32, true),
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
    ])
}

pub fn categories() -> Schema {
    Schema::new(vec![
        Field::new("cat_id", DataType::Int32, true),
        Field::new("cat_name", DataType::Utf8, true),
    ])
}

pub fn locations() -> Schema {
    Schema::new(vec![
        Field::new("loc_id", DataType::Int32, true),
        Field::new("brew_id", DataType::Int32, true),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
        Field::new("accuracy", DataType::Utf8, true),
    ])
}

pub fn styles() -> Schema {
    Schema::new(vec![
        Field::new("style_id", DataType::Int32, true),
        Field::new("cat_id", DataType::Int32, true),
        Field::new("style", DataType::Utf8, true),
    ])
}
