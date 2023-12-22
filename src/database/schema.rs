use std::collections::HashMap;
use std::fs::File;
use std::ops::Deref;
use std::sync::Arc;

use arrow;
use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::{pretty_format_batches, pretty_format_columns};

use crate::Atom;
use crate::Term::{Constant, Variable};

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
            "beers" => &self.beers,
            "breweries" => &self.breweries,
            "categories" => &self.categories,
            "locations" => &self.locations,
            "styles" => &self.styles,
            _ => panic!("Table not found"),
        }
    }

    pub fn semi_join(&self, query: &Atom, query_2: &Atom) -> Table {
        let table = self.select(query, self.get_table_by_name(&query.relation_name));
        let table_2 = self.select(query_2, self.get_table_by_name(&query_2.relation_name));
        let join_table = self.join(&table, &table_2);

        self.select(query, &join_table)
    }

    fn get_match_indexes(column: &ArrayRef, column_2: &ArrayRef) -> Int32Array {
        let refs = column
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(i, value)| match value {
                None => None,
                Some(value) => {
                    let value = column_2
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .enumerate()
                        .find(|(j, value_2)| match value_2 {
                            Some(v) => &value == v,
                            None => false,
                        })
                        .unwrap()
                        .0;

                    return Some(value);
                }
            })
            .map(|i| i.unwrap())
            .map(|i| i as i32)
            .collect::<Vec<_>>();
        Int32Array::from(refs)
    }

    fn join(&self, table: &Table, table_2: &Table) -> Table {
        let schema = Schema::try_merge(vec![
            table.data.schema().deref().clone(),
            table_2.data.schema().deref().clone(),
        ])
        .unwrap();
        let mut columns = Vec::from(table.data.columns());
        for (table_name, column_name) in table.data.schema().metadata() {
            if *table_name != table_2.name {
                continue;
            }
            let column = table.data.column_by_name(&column_name).unwrap();
            let column_2 = table_2.data.column_by_name(&column_name).unwrap();

            let foreign_key_indexes = Database::get_match_indexes(column, column_2);
            for c in table_2.data.schema().all_fields() {
                if c.name() == column_name {
                    continue;
                }
                let column = table_2.data.column_by_name(c.name()).unwrap();
                let column = arrow_select::take::take(&column, &foreign_key_indexes, None).unwrap();
                columns.push(column);
            }
        }
        let data = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
        println!("{}", pretty_format_batches(&[data.clone()]).unwrap());
        Table {
            name: format!("{}_{}", table.name, table_2.name),
            data,
        }
    }

    pub fn select(&self, query: &Atom, table: &Table) -> Table {
        let mut filter = BooleanArray::from(vec![true; table.data.num_rows()]);
        for (index, term) in query.terms.iter().enumerate() {
            match term {
                Variable(name) => {
                    let same_variables = query
                        .terms
                        .iter()
                        .enumerate()
                        .filter(|(i, t)| {
                            if let Variable(other_name) = t {
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
                Constant(constant) => {
                    let column = table.get_column(&index).unwrap();
                    filter =
                        arrow_ord::cmp::eq(&column, &StringArray::new_scalar(constant)).unwrap();
                }
            };
        }
        let data = arrow::compute::filter_record_batch(&table.data, &filter).unwrap();
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
    let mut metadata = HashMap::new();
    metadata.insert("breweries".to_string(), "brew_id".to_string());
    metadata.insert("styles".to_string(), "style".to_string());
    Schema::new_with_metadata(
        vec![
            Field::new("beer_id", DataType::Utf8, true),
            Field::new("brew_id", DataType::Utf8, true),
            Field::new("beer", DataType::Utf8, true),
            Field::new("abv", DataType::Utf8, true),
            Field::new("ibu", DataType::Utf8, true),
            Field::new("ounces", DataType::Utf8, true),
            Field::new("style", DataType::Utf8, true),
            Field::new("style2", DataType::Utf8, true),
        ],
        metadata,
    )
}

pub fn breweries() -> Schema {
    let mut metadata = HashMap::new();
    metadata.insert("locations".to_string(), "brew_id".to_string());
    Schema::new_with_metadata(
        vec![
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
        ],
        metadata,
    )
}

pub fn categories() -> Schema {
    let mut metadata = HashMap::new();
    metadata.insert("styles".to_string(), "cat_id".to_string());
    Schema::new_with_metadata(
        vec![
            Field::new("cat_id", DataType::Utf8, true),
            Field::new("cat_name", DataType::Utf8, true),
        ],
        metadata,
    )
}

pub fn locations() -> Schema {
    let mut metadata = HashMap::new();
    metadata.insert("breweries".to_string(), "brew_id".to_string());
    Schema::new_with_metadata(
        vec![
            Field::new("loc_id", DataType::Utf8, true),
            Field::new("brew_id", DataType::Utf8, true),
            Field::new("latitude", DataType::Utf8, true),
            Field::new("longitude", DataType::Utf8, true),
            Field::new("accuracy", DataType::Utf8, true),
        ],
        metadata,
    )
}

pub fn styles() -> Schema {
    let mut metadata = HashMap::new();
    metadata.insert("categories".to_string(), "cat_id".to_string());
    metadata.insert("beers".to_string(), "style".to_string());
    Schema::new_with_metadata(
        vec![
            Field::new("style_id", DataType::Utf8, true),
            Field::new("cat_id", DataType::Utf8, true),
            Field::new("style", DataType::Utf8, true),
        ],
        metadata,
    )
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
