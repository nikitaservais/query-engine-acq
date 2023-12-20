use std::fs::File;
use std::ops::Index;
use std::sync::Arc;

use crate::{Atom, Term};
use arrow;
use arrow::array::{BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow::compute::FilterPredicate;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::{pretty_format_batches, pretty_format_columns};
use arrow_csv::ReaderBuilder;

struct Table {
    name: String,
    data: RecordBatch,
}

impl Table {
    fn get_column(&self, index: usize) -> &StringArray {
        self.data
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }
}

pub struct Database {
    beers: Table,
    breweries: Table,
    categories: Table,
    locations: Table,
    styles: Table,
}

impl Database {
    pub fn select(&self, query: &Atom) {
        let table = match query.relation_name.as_str() {
            "Beers" => &self.beers,
            "Breweries" => &self.breweries,
            "Categories" => &self.categories,
            "Locations" => &self.locations,
            "Styles" => &self.styles,
            _ => panic!("Table not found"),
        };
        let mut filter = BooleanArray::from(vec![true; table.data.num_rows()]);
        for (index, term) in query.terms.iter().enumerate() {
            match term {
                Term::Variable(_name) => {}
                Term::Constant(constant) => {
                    let column = table.get_column(index);
                    let constant_filter =
                        arrow_ord::cmp::eq(&column, &StringArray::new_scalar(constant)).unwrap();
                    filter = arrow::compute::and(&filter, &constant_filter).unwrap();
                }
            };
        }
        let batch = arrow::compute::filter_record_batch(&table.data, &filter).unwrap();
        println!("{}", pretty_format_batches(&[batch.clone()]).unwrap());
    }
}

pub fn filter_beers() {
    let beers = load("beers.csv", beers());
    let beer_name = beers.column_by_name("beer").unwrap();
    let boolean_array = beer_name.as_any().downcast_ref::<StringArray>().unwrap();
    let arr: BooleanArray = boolean_array
        .iter()
        .map(|x| x.unwrap().contains("Dale's Pale Ale"))
        .collect::<Vec<_>>()
        .into();
    // let boolean_array = BooleanArray::from_iter(
    //     boolean_array
    //         .iter()
    //         .map(|x| x.unwrap().contains("American"))
    //         .collect(),
    // );
    let beer_id = beers.column_by_name("beer_id").unwrap();
    let filtered = arrow::compute::filter(&beer_id, &arr).unwrap();

    println!(
        "{}, len {}",
        pretty_format_columns("beer_id", &[filtered.clone()]).unwrap(),
        filtered.len()
    );
    let array = Int32Array::from_iter(0..100);
    // let predicate = gt_scalar(&array, 60).unwrap();
    // let filtered = arrow::compute::filter(&array, &predicate).unwrap();

    let expected = Int32Array::from_iter(61..100);
    // println!("{:?}", filtered);
    // assert_eq!(&expected, filtered.as_primitive::<Int32Type>());
}

pub fn load_data() {
    load("beers.csv", beers());
    load("breweries.csv", breweries());
    load("categories.csv", categories());
    load("locations.csv", locations());
    load("styles.csv", styles());
}

fn load(path: &str, schema: Schema) -> RecordBatch {
    let file = File::open(format!("data/{}", path)).unwrap();
    let mut csv_reader = ReaderBuilder::new(Arc::new(schema))
        .with_header(true)
        .build(file)
        .unwrap();

    csv_reader.next().unwrap().unwrap()
}

pub fn beers() -> Schema {
    Schema::new(vec![
        Field::new("beer_id", DataType::Utf8, true),
        Field::new("brew_id", DataType::Utf8, true),
        Field::new("beer", DataType::Utf8, true),
        Field::new("abv", DataType::Utf8, true),
        Field::new("ibu", DataType::Utf8, true), // TODO: change to Int32 because of NA it doesn't work
        Field::new("ounces", DataType::Utf8, true),
        Field::new("style", DataType::Utf8, true),
        Field::new("style2", DataType::Utf8, true),
    ])
}

pub fn breweries() -> Schema {
    Schema::new(vec![
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
    ])
}

pub fn categories() -> Schema {
    Schema::new(vec![
        Field::new("cat_id", DataType::Utf8, true),
        Field::new("cat_name", DataType::Utf8, true),
    ])
}

pub fn locations() -> Schema {
    Schema::new(vec![
        Field::new("loc_id", DataType::Utf8, true),
        Field::new("brew_id", DataType::Utf8, true),
        Field::new("latitude", DataType::Utf8, true),
        Field::new("longitude", DataType::Utf8, true),
        Field::new("accuracy", DataType::Utf8, true),
    ])
}

pub fn styles() -> Schema {
    Schema::new(vec![
        Field::new("style_id", DataType::Utf8, true),
        Field::new("cat_id", DataType::Utf8, true),
        Field::new("style", DataType::Utf8, true),
    ])
}

pub fn get_database() -> Database {
    let beers = load("beers.csv", beers());
    let breweries = load("breweries.csv", breweries());
    let categories = load("categories.csv", categories());
    let locations = load("locations.csv", locations());
    let styles = load("styles.csv", styles());
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
