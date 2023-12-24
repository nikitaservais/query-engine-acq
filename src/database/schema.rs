use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::sync::Arc;

use arrow;
use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use arrow_schema::ArrowError;

use crate::Term::{Constant, Variable};
use crate::{Atom, Query, Term};

#[derive(Clone)]
pub struct Table {
    name: String,
    data: RecordBatch,
}

impl Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            pretty_format_batches(&[self.data.clone()]).unwrap()
        )
    }
}

impl Table {
    // name setter
    pub fn set_name(&mut self, name: &String) {
        self.name = name.clone();
    }
    pub fn get_data(&self) -> &RecordBatch {
        &self.data
    }
    fn get_column(&self, index: &usize) -> Option<&StringArray> {
        self.data
            .column(*index)
            .as_any()
            .downcast_ref::<StringArray>()
    }
}

#[derive(Clone)]
pub struct Database {
    beers: Table,
    breweries: Table,
    categories: Table,
    locations: Table,
    styles: Table,
}

impl Database {
    pub(crate) fn rename(&mut self, query: &Query) {
        for atom in query.body.iter() {
            let table = self.get_table_by_name(&atom.relation_name);
            let mut new_table = table.clone();
            let mut new_field: Vec<Field> = vec![];
            for (index, term) in atom.terms.iter().enumerate() {
                match term {
                    Variable(name) => {
                        let field = table.data.schema().field(index).clone();
                        new_field.push(field.clone().with_name(name));
                    }
                    Constant(_) => {
                        new_field.push(table.data.schema().field(index).clone());
                    }
                }
            }
            let schema = Schema::new(new_field);
            let new_columns = table.data.columns().to_vec();
            println!("{}", table);

            new_table.data = RecordBatch::try_new(Arc::new(schema), new_columns).unwrap();
            self.set_table(&atom.relation_name, new_table);
        }
    }
}

impl Display for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\n{}\n{}\n{}\n{}",
            self.beers, self.breweries, self.categories, self.locations, self.styles
        )
    }
}

impl Database {
    pub fn set_table(&mut self, table_name: &str, table: Table) {
        match table_name {
            "beers" => self.beers.data = table.data,
            "breweries" => self.breweries.data = table.data,
            "categories" => self.categories.data = table.data,
            "locations" => self.locations.data = table.data,
            "styles" => self.styles.data = table.data,
            _ => panic!("Table not found"),
        }
    }
}

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

    pub fn project(&self, attr: &Vec<Term>, table: &Table) -> Table {
        let mut indices = vec![];
        for term in attr {
            match term {
                Variable(name) => {
                    if let Ok(index) = table.data.schema().index_of(&name) {
                        indices.push(index);
                    }
                }
                Constant(_) => {}
            }
        }
        self.projection(&indices, table)
    }

    pub fn intersection(&self, table: &Table, table_2: &Table) -> Table {
        let column = table.get_column(&0).unwrap();
        let column_2 = table_2.get_column(&0).unwrap();
        let mut filter = BooleanArray::from(vec![false; column.len()]);
        for i in 0..column_2.len() {
            let eq = arrow::compute::kernels::cmp::eq(
                &column,
                &StringArray::new_scalar(column_2.value(i)),
            )
            .unwrap();
            filter = arrow::compute::kernels::boolean::or(&filter, &eq).unwrap();
        }
        let data = arrow::compute::filter_record_batch(&table.data, &filter).unwrap();
        Table {
            name: table.name.clone(),
            data,
        }
    }

    pub fn projection(&self, indices: &[usize], table: &Table) -> Table {
        let data = table.data.project(&indices).unwrap();
        Table {
            name: table.name.clone(),
            data,
        }
    }

    pub fn semi_join(&self, query: &Atom, query_2: &Atom, table: &Table, table_2: &Table) -> Table {
        let join_table = self.join(query, query_2, table, table_2);
        let indices = query
            .terms
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        let mut table = self.projection(&indices, &join_table);
        table.set_name(&query.relation_name);
        table
    }

    pub fn join(
        &self,
        left: &Atom,
        right: &Atom,
        left_table: &Table,
        right_table: &Table,
    ) -> Table {
        let cartesian_product = self.cartesian_product(&left_table, &right_table);
        let union = self.merge(left, right);
        let mut join_table = self.select(&union, &cartesian_product);
        let mut indices = union
            .terms
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        let union = self.union(&left.terms, &right.terms);

        for (table_name, column_name) in left_table.data.schema().metadata() {
            if *table_name != right_table.name {
                continue;
            }
            indices.remove(
                left_table.data.num_columns()
                    + right_table.data.schema().index_of(&column_name).unwrap(),
            );
        }
        join_table = self.projection(&indices, &join_table);
        join_table
    }

    fn union(&self, left: &Vec<Term>, right: &Vec<Term>) -> Vec<Term> {
        let mut result = left.clone();
        for term in right {
            if !result.contains(&term) {
                result.push(term.clone());
            }
        }
        result
    }

    fn merge(&self, left: &Atom, right: &Atom) -> Atom {
        Atom {
            relation_name: format!("{}_{}", left.relation_name, right.relation_name),
            terms: [left.terms.clone(), right.terms.clone()].concat(),
        }
    }

    fn cartesian_product(&self, left: &Table, right: &Table) -> Table {
        let schema = Schema::new(
            [
                left.data
                    .schema()
                    .all_fields()
                    .into_iter()
                    .map(|f| f.clone())
                    .collect::<Vec<_>>(),
                right
                    .data
                    .schema()
                    .all_fields()
                    .into_iter()
                    .map(|f| f.clone())
                    .collect(),
            ]
            .concat(),
        );
        if left.data.num_rows() == 0 || right.data.num_rows() == 0 {
            let d = RecordBatch::new_empty(Arc::new(schema));

            return Table {
                name: format!("{}_{}", left.name, right.name),
                data: d,
            };
        }

        let mut new_left_columns: Vec<ArrayRef> = vec![];
        for column in left.data.columns().clone() {
            let clone = column.clone();
            let v = std::iter::repeat(clone.as_ref())
                .take(right.data.num_rows())
                .collect::<Vec<_>>();
            if v.is_empty() {
                new_left_columns.push(clone.clone());
                continue;
            }
            let new_column = arrow_select::concat::concat(&v).unwrap();

            new_left_columns.push(new_column);
        }
        let mut new_right_columns = vec![];
        for column in right.data.columns().clone() {
            let clone = column.clone();
            let v = std::iter::repeat(clone.as_ref())
                .take(left.data.num_rows())
                .collect::<Vec<_>>();
            if v.is_empty() {
                new_right_columns.push(clone.clone());
                continue;
            }
            let new_column = arrow_select::concat::concat(&v).unwrap();

            new_right_columns.push(new_column);
        }
        let data = RecordBatch::try_new(
            Arc::new(schema),
            [
                new_left_columns
                    .into_iter()
                    .map(|c| c.into())
                    .collect::<Vec<_>>(),
                new_right_columns
                    .into_iter()
                    .map(|c| c.into())
                    .collect::<Vec<_>>(),
            ]
            .concat(),
        )
        .unwrap();
        Table {
            name: format!("{}_{}", left.name, right.name),
            data,
        }
    }

    fn get_match_indexes(column: &ArrayRef, column_2: &ArrayRef) -> Int32Array {
        let filter = arrow::compute::kernels::cmp::distinct(column, column_2).unwrap();
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
                    for same_var in same_variables {
                        let var_filter = arrow_ord::cmp::eq(
                            &table.get_column(&same_var).unwrap(),
                            &table.get_column(&index).unwrap(),
                        )
                        .unwrap();
                        filter = arrow::compute::and(&filter, &var_filter).unwrap();
                    }
                }
                Constant(constant) => {
                    let column = table.get_column(&index).unwrap();
                    let constant_filter =
                        arrow_ord::cmp::eq(&column, &StringArray::new_scalar(constant)).unwrap();
                    filter = arrow::compute::and(&filter, &constant_filter).unwrap();
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
