use std::fs::File;
use std::sync::Arc;

use arrow;
use arrow::array::{BooleanArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;

use crate::{Atom, Term};

pub struct Table {
    name: String,
    data: RecordBatch,
}

impl Table {
    fn get_column(&self, index: &usize) -> Option<&StringArray> {
        self.data
            .column(*index)
            .as_any()
            .downcast_ref::<StringArray>()
    }
}

pub struct Database {
    beers: Table,
    breweries: Table,
    categories: Table,
    locations: Table,
    styles: Table,
}

impl Database {}

impl Database {
    pub fn get_table_by_name(&self, relation_name: &str) -> &Table {
        match relation_name {
            "Beers" => &self.beers,
            "Breweries" => &self.breweries,
            "Categories" => &self.categories,
            "Locations" => &self.locations,
            "Styles" => &self.styles,
            _ => panic!("Table not found"),
        }
    }

    pub fn select(&self, query: &Atom) -> Table {
        let table = self.get_table_by_name(query.relation_name.as_str());
        let mut filter = BooleanArray::from(vec![true; table.data.num_rows()]);
        for (index, term) in query.terms.iter().enumerate() {
            match term {
                Term::Variable(name) => {
                    let same_variables = query
                        .terms
                        .iter()
                        .enumerate()
                        .filter(|(i, t)| {
                            if let Term::Variable(other_name) = t {
                                name == other_name && index < *i
                            } else {
                                false
                            }
                        })
                        .map(|(i, _)| i)
                        .collect::<Vec<_>>();
                    if same_variables.len() == 0 {
                        continue;
                    }
                    filter = same_variables
                        .iter()
                        .map(|i| table.get_column(&i).unwrap())
                        .map(|same_var| {
                            arrow_ord::cmp::eq(&same_var, &table.get_column(&index).unwrap())
                                .unwrap()
                        })
                        .reduce(|a, b| arrow::compute::and(&a, &b).unwrap())
                        .unwrap();
                }
                Term::Constant(constant) => {
                    let column = table.get_column(&index).unwrap();
                    let constant_filter =
                        arrow_ord::cmp::eq(&column, &StringArray::new_scalar(constant)).unwrap();
                    filter = arrow::compute::and(&filter, &constant_filter).unwrap();
                }
            };
        }
        let data = arrow::compute::filter_record_batch(&table.data, &filter).unwrap();
        println!("{}", pretty_format_batches(&[data.clone()]).unwrap());
        Table {
            name: table.name.clone(),
            data,
        }
    }
}

fn load(path: &str, schema: Schema) -> RecordBatch {
    let file = File::open(format!("data/{}", path)).unwrap();
    let mut csv_reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
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
